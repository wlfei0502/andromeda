# Long-Horizon Agent（复杂长任务）设计

日期：2026-09-18  
状态：已批准  
范围：在已落地的 **Cloud Agent Server（SSE）v1** 之上，增加支撑 **分钟～小时级** 复杂任务的能力。  
前置文档：[2026-09-18-cloud-agent-sse-design.md](./2026-09-18-cloud-agent-sse-design.md)

## 目标

使 Andromeda 能稳定执行 **多阶段、可中断、可恢复** 的长任务，同时保持现有架构原则：

- **推理 / 编排在云端**；**业务 tool 仍在客户端执行**。
- 传输仍为 **SSE + POST**（不引入 WebSocket / HTTP/2 bidi）。
- **决策靠模型，骨架与护栏靠运行时**（对齐 DeerFlow 2.0 的 Lead Agent 软编排，不做 1.x 固定角色 DAG）。

具体能力（按优先级）：

| 代号 | 能力 | 用户可感知价值 |
|------|------|----------------|
| LH-P0a | Run 持久化 + 断线续跑 | 网络抖动 / 客户端重启后任务不丢 |
| LH-P0b | 上下文摘要 / 裁剪 | 长对话不炸 context、不拖垮延迟与费用 |
| LH-P1a | Plan Mode（todos） | 多步骤任务有阶段进度，可观测、可纠偏 |
| LH-P1b | 长跑护栏 | 轮次 / 空转 / 总时长上限，防无限跟跑 |
| LH-P2a | 子代理（`task`） | 隔离上下文、可并行调研/子目标 |
| LH-P2b | （可选）并行 tool | 加速独立只读类 tool；非「能跑长」前提 |

## 非目标（本规格）

- 通用工作流 DSL / 可视化 BPMN / DeerFlow 1.x 式固定 `coordinator→planner→researcher` 图
- Server 端执行业务 GIS tool（仍由客户端执行）
- 多租户计费、复杂鉴权
- 无限嵌套子代理、跨机器分布式 worker 池（v1 子代理 = **同进程嵌套 `run_agent`**）
- 自动更换传输协议

## 背景与决策摘要

| 决策 | 选择 | 理由 |
|------|------|------|
| 编排风格 | Lead Agent 软编排（todos + 可选 `task`） | 与现有单 `run_agent` 同构；避免硬编码研究流水线 |
| 持久化位置 | Server 侧 **RunStore**（默认可本地盘；多实例见 §2.4） | 云端持有 context；客户端只补 SSE 与 tool |
| 多实例 | **共享 Checkpoint + 单 owner 亲和**；热状态不跨进程共享 | 避免双 writer；实例挂了可凭 checkpoint 在别的实例续跑 |
| 续跑方式 | `GET/POST` 重新订阅同一 `run_id` 的 SSE | 保持 SSE+POST；不要求原 TCP 不断 |
| 摘要触发 | 运行时策略（token/消息阈值） | 模型不负责「记得自己摘要」 |
| Plan | Server 内置 `write_todos` tool + SSE 事件 | 模型写清单；协议与 UI 结构化 |
| 子代理 | 内置 `task` tool → 嵌套 agent run | 复用编排；结果以 tool result 交回父 |
| 与 v1 关系 | **增量扩展** wire / API；旧客户端在未开 LH 开关时可行为兼容 | 降低桌面仓改造成本 |

参考：DeerFlow 2.0（Lead + TodoMiddleware + `task` 子代理）、Cursor 类 coding agent（单 loop + 委派）。

---

## §1 架构（在 v1 上叠加）

```text
[客户端]                         [andromeda Server]
────────────────────────────────  ─────────────────────────────────
POST /v1/runs  或  续订 SSE         RunStore（落盘）
  ← SSE（含 todos / task 事件）      ├─ Checkpoint（context, todos, phase）
POST tool_results / steer            ├─ Summarizer（超阈裁剪）
POST .../cancel                      ├─ Guards（轮次 / 时长）
                                     └─ run_agent
                                          ├─ 可选 write_todos（server tool）
                                          └─ 可选 task → 嵌套 run_agent
                                               （子 run 的 tool.request 仍走
                                                同一客户端回传通道，见 §6）
```

**原则补充**

1. **Checkpoint 是跨实例的真相源**：某实例上的内存 `RunHandle` 只是 **当前 owner 的热缓存**；关键转换后写入 RunStore。
2. **Server tools vs Client tools**：`write_todos` / `task`（及日后 `ask_clarification`）由 **server 本地执行**；其余 schema 仍来自客户端，经 `tool.request` 下发。
3. **一父一 SSE**：子代理不单独对客户端开 SSE；进度折叠进父 run 的 `task.*` 事件（可选透传子 delta）。
4. **同一时刻至多一个 owner 实例**执行该 `run_id` 的编排循环（见 §2.4）。

---

## §2 持久化与断线续跑（LH-P0a）

### 2.1 存储模型

通过 **`RunStore` trait** 抽象（LH-M1 起就引入），避免把「本地目录」写死进编排逻辑。

**逻辑布局**（本地实现时的路径示意；共享后端用等价 key）：

```text
{data_dir}/runs/{run_id}/
  meta.json          # 创建时间、status、config 快照；可选 owner 租约
  checkpoint.json    # 可恢复快照（见下）
  events.jsonl       # 可选：已发出的 SSE 事件审计 / 重放辅助
```

| 实现 | 适用 | 说明 |
|------|------|------|
| `LocalFsRunStore` | 单实例；或多实例 + **共享卷**（NFS/云盘同一 `data_dir`） | M1 默认实现 |
| `SqliteRunStore` / `PostgresRunStore`（可后置） | 多实例无共享盘 | checkpoint 行存储；M1 可只留 trait + 接口测 |
| 对象存储等 | 超大 context | 非 M1；需要时再加 |

**`checkpoint.json` 最小字段**

```json
{
  "run_id": "...",
  "status": "running | waiting_tool | completed | failed | cancelled",
  "context": [ /* WireMessage[] */ ],
  "tools": [ /* ToolDef[] 客户端 tools */ ],
  "todos": [ /* TodoItem[]，可空 */ ],
  "pending_tool": { "tool_call_id": "...", "name": "...", "arguments": {} } | null,
  "guards": {
    "llm_rounds": 0,
    "follow_up_rounds": 0,
    "started_at": "RFC3339",
    "updated_at": "RFC3339"
  },
  "parent_run_id": null,
  "owner_id": null,
  "revision": 1
}
```

| 字段 | 说明 |
|------|------|
| `status=waiting_tool` | SSE 可断；checkpoint 保留 `pending_tool`，客户端续订后应先看到（或补发）对应 `tool.request` |
| `revision` | 每次成功刷盘 +1；用于乐观并发（同 run 禁止双 writer / 跨实例） |
| `owner_id` | 当前编排 owner 实例标识（多实例）；单实例可为 hostname/uuid |
| `parent_run_id` | 子代理 run 指向父；父 SSE 订阅者看不到独立子 run URL（实现细节可内隐） |

**刷盘时机（至少）**

- 每次 LLM `message.completed` 写入 context 后
- 每次发出 `tool.request` 进入等待前
- 每次收到 `tool_results` / drain steer / 更新 todos 后
- run 终态（finished / error / cancelled）

v1 内存 registry **保留为当前 owner 的热缓存**；进程重启或换实例后，从 RunStore 加载未终态 run。

### 2.2 API：续订 SSE（已定）

```http
GET /v1/runs/{run_id}/events
Accept: text/event-stream
```

不提供 `POST .../subscribe`。

**行为**

1. 若 run 不存在 → `404`
2. 若已终态 → 可选：推送一条 `run.finished`/`error` 后立刻结束；或返回 `410`
3. 若 `running` / `waiting_tool`：
   - 建立新的 SSE writer（**替换**旧 writer；**最后订阅者获胜**，不做多路 fan-out）
   - 先发 `run.resumed`（见 §7）
   - 若 `waiting_tool`：再发（或重发）当前 `tool.request`，以便客户端补执行
   - 之后继续正常编排事件

**客户端约定**

- 原 SSE 断开后：用同一 `run_id` 调续订；**不要**新建 `POST /v1/runs`（除非用户明确「重开任务」）。
- 若断线时本地已执行完 tool 但未 POST 成功：续订看到同一 `tool.request` 后可 **幂等再 POST** `tool_results`（server 对「非等待中」仍 `409`；对匹配的 pending 接受一次）。

### 2.3 与 create 的关系

`POST /v1/runs` 增加可选字段：

```json
{
  "messages": [...],
  "tools": [...],
  "session_id": null,
  "options": {
    "plan_mode": false,
    "subagents": false,
    "persist": true
  }
}
```

| 字段 | 默认 | 说明 |
|------|------|------|
| `persist` | `true`（LH 开启后） | `false` 时行为接近 v1 纯内存（可测 / 短任务） |
| `plan_mode` | `false` | 注入 `write_todos` + todos 状态 |
| `subagents` | `false` | 注入 `task` tool |

### 2.4 多实例部署（已纳入规格）

多实例下有两类状态，不能混为一谈：

| 状态 | 例子 | 能否跨实例共享 |
|------|------|----------------|
| **热状态** | 编排任务、`oneshot` tool waiter、当前 SSE writer | **否**（进程内） |
| **冷状态** | Checkpoint（context / pending_tool / todos / revision） | **必须能**（共享 RunStore） |

```text
                    ┌─── Instance A（owner of run_1）───┐
 Client ──LB──┬────►│  RunHandle + SSE + run_agent      │
              │     │         │ persist / load            │
              │     └─────────┼───────────────────────────┘
              │               ▼
              │        Shared RunStore
              │        (共享盘 或 DB)
              │               ▲
              │     ┌─────────┼───────────────────────────┐
              └────►│  Instance B：无热状态时拒绝写入，   │
                    │  或在续订时接管 owner（见下）        │
                    └─────────────────────────────────────┘
```

**已定规则**

1. **活跃 run 有且仅有一个 owner 实例**  
   - `POST /v1/runs` 创建后，创建所在实例成为 owner，跑编排循环。  
   - `tool_results` / `steer` / `cancel` 必须打到 **owner**（见路由）。  
   - 禁止两个实例同时对同一 `run_id` 跑 `run_agent`（用 `revision` / 可选 lease 防双写）。

2. **路由（部署二选一，协议层兼容）**  
   - **推荐运维**：负载均衡按 `run_id` **粘性**（cookie / header `X-Run-Id` / 一致性哈希），保证续订与回传打到同一实例。  
   - **推荐应用兜底**：若请求落到非 owner 且本地无热状态：  
     - 对 `tool_results` / `steer`：返回 **`409`**，body 可带 `code=not_owner`（客户端重试或依赖粘性修复）；**不**在错误实例上瞎写 checkpoint。  
     - 对 `GET .../events`（续订）：允许 **接管**——从 RunStore 加载 checkpoint，成为新 owner，发 `run.resumed`，若 `waiting_tool` 则重发 `tool.request`（原实例若仍活着，用 lease/`revision` 让旧 owner 退出，见下）。

3. **实例崩溃 / 无粘性误打**  
   - 热状态（waiter）会丢；这是预期。  
   - 客户端 `GET .../events` → 任意健康实例 load checkpoint → 新 owner 续跑。  
   - 因此 **多实例正确性依赖共享 RunStore**，不能只靠每机本地 `./data`（除非始终粘性且可接受「粘到的那台挂了任务只能等该盘恢复」——**不推荐**作为生产多实例方案）。

4. **Owner 租约（M1 最小 / M1.1 加强）**  
   - **M1 最小**：`revision` 乐观并发；写 checkpoint 时若盘上 revision 更新则失败并停旧循环。续订接管时 `revision += 1` 并写入 `owner_id`（实例标识）。  
   - **可选加强**：`lease_until` 心跳；过期才允许他者接管，减少双活窗口。

5. **LH-M1 交付边界**  
   - 必须：`RunStore` trait + `LocalFsRunStore`；单测用临时目录。  
   - 必须：文档写明多实例 = **共享 `data_dir`（或后续 DB）+ 粘性优先 + 续订可接管**。  
   - 不必须：Postgres 实现、自动 lease 心跳（可列为 M1.1）；但接口预留 `owner_id` / `revision` 字段。

**明确不做（本阶段）**

- 把正在流式中的 LLM HTTP 连接热迁移到另一实例  
- 跨实例共享内存 waiter 或广播同一条 SSE 到多机

---

## §3 上下文摘要（LH-P0b）

### 3.1 为何需要

长任务下 `context` 含大量 tool 结果（日志、图层元数据、搜索片段）。不裁剪则：

- 触发供应商 context 上限
- 延迟与费用上升
- 模型注意力被旧噪声稀释

### 3.2 策略（运行时，非模型自发）

在 **下一次 LLM 调用前**，若估算 token（或字符启发式）超过 `summarize_threshold`：

1. 保留前缀：system + 最近 `keep_last_messages` 条（含未完成 tool 链）
2. 将中间段交给 **摘要 LLM 调用**（可用同一 model 或 `summarizer_model`）
3. 用一条 `role=system`（或专用 `role` 若日后扩展）消息替换中间段，内容为结构化摘要
4. 发出 SSE `context.summarized`（便于 UI / 调试）
5. 刷盘 checkpoint（摘要后的 context）

**不摘要的内容**

- 当前 `pending_tool` 相关消息
- 最新用户 steer（尚未被模型消费的）
- 当前 todos 的权威副本（todos 在 checkpoint 独立字段；摘要文本里可再提一句）

### 3.3 配置

```toml
[context]
summarize_threshold_tokens = 80000
keep_last_messages = 24
# summarizer_model = "..."   # 可选；默认跟主 model（见 LH-M2 规格）
```

### 3.4 失败策略

摘要调用失败 → 记录日志；若仍超硬上限 `max_context_tokens` → `error`（`code=context_overflow`）结束 run，避免静默胡来。

---

## §4 Plan Mode（LH-P1a）

### 4.1 数据模型

```json
{
  "id": "t1",
  "content": "盘点当前地图图层",
  "status": "pending | in_progress | completed | cancelled"
}
```

- 权威状态在 checkpoint.`todos`
- 模型通过 **server tool** `write_todos` 整表或部分更新（实现可选 merge；v1 建议 **全量替换** 简单可测）

### 4.2 Server tool：`write_todos`

注入条件：`options.plan_mode=true`。

```json
{
  "name": "write_todos",
  "description": "Replace the task list for this run. Keep items small and actionable.",
  "parameters": {
    "type": "object",
    "properties": {
      "todos": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "id": { "type": "string" },
            "content": { "type": "string" },
            "status": {
              "type": "string",
              "enum": ["pending", "in_progress", "completed", "cancelled"]
            }
          },
          "required": ["id", "content", "status"]
        }
      }
    },
    "required": ["todos"]
  }
}
```

**执行路径**：orchestrator 识别 server tool → 本地更新 todos → 把 tool result（如 `ok` + 当前列表）写入 context → **不**发 `tool.request` 给客户端。

**SSE**：每次变更发：

```text
event: todos.updated
data: { "run_id", "type": "todos.updated", "todos": [ ... ] }
```

### 4.3 系统提示义务（概念）

Plan Mode 开启时，system prompt 追加简短约束，例如：

- 复杂多步任务先写 todos，再执行
- 同时最多一个 `in_progress`
- 完成后标记 `completed`；取消的步骤标 `cancelled`
- 不要把 todos 当聊天废话重复讲一遍（UI 已有结构化事件）

### 4.4 与 steering 的关系

用户 steer「先别做导出，改做缓冲区分析」→ 模型应在下一轮更新 todos。Server **不**自动改 todos。

---

## §5 长跑护栏（LH-P1b）

| 护栏 | 配置键（示意） | 触发 | 行为 |
|------|----------------|------|------|
| LLM 轮次 | `max_llm_rounds` | 每完成一次 LLM 调用 +1 | 超限 → `run.finished(reason=guard_llm_rounds)` |
| Follow-up 轮次 | 沿用 `MAX_FOLLOW_UP_ROUNDS`（可配置化） | 已有 | 超限 → 现有 `FollowUpLimit` |
| 墙钟时长 | `max_run_wall_secs` | 自 `started_at` | 超限 → `reason=guard_timeout` |
| 空转检测 | `max_noop_llm_rounds` | 连续 N 次 LLM 无 tool、无 todos 变化、文本高度相似 | → **直接结束** `run.finished(reason=guard_noop)`（不做 system nudge；用户可 steer 或新开 run） |
| Tool 等待 | 已有 `tool_timeout_secs` | 已有 | 保持；可对「长 GIS tool」允许客户端在 create 时覆盖（可选后续） |

护栏触发时：刷盘终态；SSE 发 `run.finished`（优先）或 `error`；**不要**静默继续。

---

## §6 子代理（LH-P2a）

### 6.1 形态

- 父 run 暴露 server tool `task`
- 调用后 **同进程** 启动子 `run_agent`（独立 context、独立 checkpoint 子目录或 `parent_run_id` 标记）
- 子代理跑完 → 最终 assistant 文本（或结构化 summary）作为父的 tool result
- **默认禁止子代理再调 `task`**（防无限嵌套）
- **默认禁止子代理 `write_todos` 写父清单**（子有自己的可选 todos，或无 plan）

### 6.2 `task` schema（示意）

```json
{
  "name": "task",
  "description": "Delegate a focused subtask to an isolated agent. Returns a summary.",
  "parameters": {
    "type": "object",
    "properties": {
      "goal": { "type": "string" },
      "agent": {
        "type": "string",
        "enum": ["general", "explore"],
        "description": "general: full client tools; explore: read-only tool subset if client marked tools"
      },
      "context_hints": {
        "type": "array",
        "items": { "type": "string" },
        "description": "Optional short facts/paths; do not dump full parent chat"
      }
    },
    "required": ["goal"]
  }
}
```

### 6.3 Tool 回传通道（关键设计）

子代理若需要 **客户端 tool**：

| 方案 | 做法 | 取舍 |
|------|------|------|
| **A. 复用父 SSE（推荐 v1）** | 子发出的 `tool.request` 打上 `agent_id` / `parent_tool_call_id`，经 **父 run 的 SSE** 下发；客户端 `tool_results` 仍 POST 到 **父 run_id**，body 带 `tool_call_id`（全局唯一即可路由到子 waiter） | 客户端改动小；实现要在 registry 路由 waiter |
| B. 子 run 独立 SSE | 客户端再订子 run | 桌面复杂，暂不做 |

**推荐 A**。Wire 扩展：

```text
tool.request 增加可选字段：
  agent_id?: string          # 缺省 = 父 lead
  parent_task_id?: string    # 所属 task 调用
```

客户端：无 `agent_id` 时行为与 v1 完全一致；有则 UI 可显示「子任务正在调工具」。

### 6.4 并行与限制

- 同一父 turn 内多个 `task`：**允许并行**（tokio 任务），默认 `max_concurrent_subagents = 2`
- 超时：`subagent_timeout_secs`（默认 900）
- 超限：拒绝多余 `task` 或串行化（推荐：**多余调用直接 tool result 报错**，行为清晰）

### 6.5 SSE 事件

| type | 含义 |
|------|------|
| `task.started` | `{ task_id, goal, agent }` |
| `task.completed` | `{ task_id, summary }` |
| `task.failed` | `{ task_id, message, code? }` |
| `task.timed_out` | `{ task_id }` |

子代理内部 `message.delta` **默认不**转发给父 UI（子流折叠）；调试可另开 `options.subagent_stream=true`（非 LH 首发范围）。

### 6.6 `explore` vs `general`

若 `ToolDef` 增加可选 `"readonly": true`（客户端标注），`explore` 子代理只注入 readonly tools；未标注则 `explore` 与 `general` 工具集相同但 system 提示强调只读。  
**不要求**首发就做权限沙箱；先协议预留字段。

---

## §7 Wire / API 增量汇总

### 7.1 新 / 扩展 HTTP

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/runs/{id}/events` | 续订 SSE（单订阅者） |
| （已有） | `POST /v1/runs` | 增加 `options` |
| （已有） | `tool_results` / `steer` / `cancel` | 语义扩展见上 |

可选只读：

```http
GET /v1/runs/{run_id}
→ { status, todos, revision, pending_tool? }
```

便于 UI 在无 SSE 时拉快照。

### 7.2 新 SSE 事件

| type | 阶段 |
|------|------|
| `run.resumed` | P0a |
| `context.summarized` | P0b |
| `todos.updated` | P1a |
| `task.started` / `task.completed` / `task.failed` / `task.timed_out` | P2a |

`run.finished.reason` 扩展：`stop` | `cancelled` | `guard_llm_rounds` | `guard_timeout` | `guard_noop` | …

### 7.3 `message.completed.source` 扩展

| 值 | 含义 |
|----|------|
| `assistant` / `steer` / `follow_up` | v1 已有 |
| `summary` | 摘要替换写入（可选；若用 system 静默写入可不暴露） |

---

## §8 编排伪代码（相对 v1 的增量）

```text
loop:
  drain steer
  maybe_summarize(context)          # P0b
  check_guards()                    # P1b

  tool_calls = stream_llm(...)

  if tool_calls empty → follow-up / finish

  for tc in tool_calls:
    if tc is server tool (write_todos | task):
      execute_locally / await_subagent
      append tool result
      emit todos.* or task.*
      persist()
    else:
      emit tool.request (agent_id?)
      persist(status=waiting_tool)
      wait tool_results
      persist()
```

---

## §9 配置

```toml
[persist]
enabled = true
data_dir = "./data"          # 多实例时须指向共享卷，或换 DB 实现
instance_id = ""             # 空则启动时生成；写入 checkpoint.owner_id

[context]
summarize_threshold_tokens = 80000
keep_last_messages = 24
max_context_tokens = 120000

# 以下为后续里程碑（尚可不配）
# max_llm_rounds = 200
# max_run_wall_secs = 7200
# max_noop_llm_rounds = 5
# max_concurrent_subagents = 2
# subagent_timeout_secs = 900
```

`follow_up_policy`、`tool_timeout_secs` 等仍在根配置。  
（历史名 `[long_horizon]` 仍可作为 `[persist]` 的 TOML 别名被解析。）

---

## §10 客户端影响（桌面仓）

**最小必改（要用长任务）**

1. SSE 断开 → 续订同一 `run_id`
2. 处理 `todos.updated`（进度 UI）
3. 识别 server 不再对 `write_todos`/`task` 发 `tool.request`（无需本地实现同名 tool；若 schema 里出现同名，以 server 注入为准、客户端勿重复注册）

**子代理启用时**

4. `tool.request` 可能带 `agent_id`；`tool_call_id` 仍全局唯一，回传父 `run_id` 即可

**可选**

5. `GET /v1/runs/{id}` 展示 todos / status
6. `readonly` 标记 tools

---

## §11 测试策略

| 层 | 用例 |
|----|------|
| Checkpoint | 杀进程后加载 → `waiting_tool` 可续订并接受 tool_results |
| Summarize | 构造超长 context → 调用前插入摘要消息；超硬上限报错 |
| Todos | mock LLM 调 `write_todos` → 无客户端 tool_results；有 `todos.updated` |
| Guards | 压低 `max_llm_rounds` → `reason=guard_llm_rounds` |
| Subagent | mock 子 run 返回 summary → 父 context 收到 tool result；并发上限 |
| 兼容 | `options` 全关 → 与 v1 事件序列兼容（无新事件也可） |

仍以 mock LLM 为主，不依赖真实 MaaS。

---

## §12 里程碑

| 里程碑 | 内容 | 出口标准 |
|--------|------|----------|
| **LH-M0** | 本规格评审通过 | ✅ 2026-09-18 已批准 |
| **LH-M1** | `RunStore` trait + LocalFs checkpoint + 续订 SSE + `owner_id`/`revision` | 集成测：断订→续订→完成 tool；文档含多实例部署约束 |
| **LH-M1.1**（可选） | 共享盘/DB 验收 + lease 心跳 | 两实例：杀 owner 后另一实例续订接管 |
| **LH-M2** | Summarizer + 配置阈值 | 超阈摘要；硬上限失败 |
| **LH-M3** | Plan Mode（`write_todos` + 事件） | UI 可只靠 SSE 画进度 |
| **LH-M4** | Guards 配置化 | 超时/轮次可测 |
| **LH-M5** | `task` 子代理（通道 A） | 单测嵌套 + 可选并行 |
| **LH-M6** | 文档：`api-client.md` 同步；桌面最小对接说明 | 外仓可按文档改 |

**建议落地顺序**：M1 → M2 → M3 → M4 → M5。  
不建议在 M1 完成前做子代理。

---

## §13 已定决策

| # | 议题 | 决定 |
|---|------|------|
| 1 | 续订 API | `GET /v1/runs/{id}/events` |
| 2 | 多订阅 | 最后订阅者获胜；不做 fan-out |
| 3 | 空转护栏 | 直接结束（`reason=guard_noop`） |
| 4 | 子代理流式 | 默认折叠；不透传子 `message.delta` |
| 5 | `session_id` | 本阶段不做会话目录；仅 `run_id` 一级（以后再说） |
| 6 | 多实例 | 共享 RunStore + 单 owner；LB 粘性优先；续订可接管；`tool_results` 打错实例 → `409 not_owner` |

---

## 批准记录

- 2026-09-18：用户确认决策——「GET 续订、单订阅者、空转直接停、子流默认折叠、session 以后再说」；规格状态改为 **已批准**。
- 2026-09-18：用户要求 **考虑服务器多实例**；增补 §2.4（共享 Checkpoint、owner 亲和、续订接管），决策表增加第 6 条。
