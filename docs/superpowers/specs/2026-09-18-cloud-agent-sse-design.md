# Cloud Agent Server（SSE）设计

日期：2026-09-18  
状态：已批准（对话确认 ok）  
范围：本仓库 **整仓重写为云端 Agent Server**；客户端在独立项目。

## 目标

- **推理 / 编排在云端**（本仓库）：会话、调用 LLM、决定是否发起 tool、聚合上下文。
- **Tool 执行在客户端**（另一仓库）：收到 `tool.request` 后本地执行，再 HTTP 回传结果。
- 传输采用：**一次 `POST` 开跑，响应体为 SSE**；tool 结果 / steering 用另一次 `POST` 回传（SSE 连接保持打开）。
- **v1 包含 steering 与 follow-up**（见 §4.1 / §4.2），用单状态机 + 队列实现，不沿用旧双循环文件，但行为对齐「内循环吃 steering/tool、外循环吃 follow-up」。

## 非目标（v1）

- 客户端实现（仅提供协议约定，可选 curl / 最小假客户端说明）
- 复用或包裹现有 `agent_loop` / `LiterBackend` / stub（**旧代码可不保留**）
- 并行 tool 执行
- 断线续跑 / 多订阅者广播
- 复杂鉴权、多租户计费
- HTTP/2 双向流或 WebSocket（v1 固定 SSE + POST）
- 通用工作流 DSL / 可视化编排器（follow-up 仅提供 **策略钩子 + 一个可测的内置示例**）

## 背景与决策摘要

| 决策 | 选择 |
|------|------|
| Agent loop 位置 | 云端（方案 B） |
| 本仓库角色 | **仅 Server**；客户端另仓 |
| 传输 | `POST` 响应体 = SSE；上行 tool 结果 / steering 另 `POST` |
| 代码策略 | **推倒重写**，非旁路新模块 |
| 事件协议 | **全新 wire 事件**，不兼容旧 `AgentEvent` |
| Steering / Follow-up | **v1 必做** |

参考形态（产品层）：Cursor 桌面「云端推理 + 本地工具」；传输上 Cursor 默认 HTTP/2 bidi、SSE 为回退。本项目 v1 **有意采用更简单的 SSE+POST**，便于桌面 / GIS 客户端对接。

---

## §1 架构

```text
[客户端项目]                              [andromeda Server]
─────────────────────────────────         ──────────────────────────────
用户输入 / UI                              POST /v1/runs  → 创建 run
执行本地 tools                             响应: text/event-stream (SSE)
POST .../tool_results  ←────────────────  推送: delta / tool.request / ...
POST .../steer（中途改口）←─────────────  steering 队列 → 下轮 LLM 前写入
显示流式回复 ←──────────────────────────  follow-up 策略 → 可同 run 再续一轮
                                          调 LLM；密钥与 model 仅在 server
```

**原则**

- Server **不执行**业务 tool；只声明「需要客户端执行什么」。
- 客户端在开跑时上传 **tools schema**（name / description / parameters），供模型选型。
- LLM API Key、`base_url`、`model` 只存在于 server 的 `config.toml`。

---

## §2 HTTP API

### 2.1 创建并订阅一次 run

```http
POST /v1/runs
Content-Type: application/json
Accept: text/event-stream
```

**Request body（JSON）**

```json
{
  "messages": [
    { "role": "user", "content": "..." }
  ],
  "tools": [
    {
      "name": "echo",
      "description": "...",
      "parameters": { "type": "object", "properties": {} }
    }
  ],
  "session_id": null
}
```

| 字段 | 说明 |
|------|------|
| `messages` | 本轮输入；v1 至少支持 `user` / `assistant` / `system` / `tool` 角色文本或结构化 tool result（见 §3） |
| `tools` | 客户端可执行工具的 JSON Schema 列表；可为空 |
| `session_id` | 可选；v1 可忽略持久会话，仅单 run |

**Response**

- `200`，`Content-Type: text/event-stream`
- Header 建议带：`X-Run-Id: <run_id>`（同时在首条 SSE 事件里带 `run_id`）
- Body：SSE 事件流，直到 `run.finished` 或 `error` 后结束

### 2.2 回传 tool 结果

```http
POST /v1/runs/{run_id}/tool_results
Content-Type: application/json
```

```json
{
  "tool_call_id": "call_...",
  "content": "...",
  "is_error": false
}
```

- 必须在 **对应 run 的 SSE 仍打开、且 server 正在等待该 tool** 时调用。
- `200`：`{ "ok": true }`
- `409`：run 未在等待该 `tool_call_id` / 已结束
- `404`：未知 `run_id`

### 2.3 Steering（中途插入消息）

```http
POST /v1/runs/{run_id}/steer
Content-Type: application/json
```

```json
{
  "messages": [
    { "role": "user", "content": "改成微辣，再确认一下地址" }
  ]
}
```

| 规则 | 说明 |
|------|------|
| 何时可调 | 对应 run 的 SSE **仍打开**且 run 未 `finished` |
| 效果 | 消息进入该 run 的 **steering 队列**，不立刻打断正在进行的 LLM token 流 |
| 消费时机 | 当前 LLM 流结束后、或当前串行 tool 等待结束后，在 **下一次调模型之前** 整批写入 context（见 §4） |
| 响应 | `200 { "ok": true, "queued": N }`；`404` / `409`（已结束） |

**与「再开一个 run」的区别**：steering 仍在**同一次 SSE / 同一 run_id** 里继续；用户中途改口不必重开连接。

### 2.4 取消（v1）

```http
POST /v1/runs/{run_id}/cancel
```

取消后 SSE 推 `run.finished`（`reason=cancelled`）或 `error`，然后关流；丢弃未消费的 steering / 未完成的 tool 等待。

---

## §3 SSE Wire 协议

每条 SSE：

```text
event: <type>
data: <json>

```

`data` 均为 JSON 对象，且包含公共字段：

```json
{ "run_id": "...", "type": "<同 event>", ... }
```

### 事件类型

| `type` / `event` | 含义 | 主要字段 |
|------------------|------|----------|
| `run.started` | run 已创建 | `run_id` |
| `message.delta` | assistant 流式增量 | `message_id`, `delta`（文本） |
| `message.completed` | 消息定稿（assistant / 已入队的 steering 或 follow-up） | `message_id`, `role`, `content`, `tool_calls?`, `source?`（`assistant` / `steer` / `follow_up`） |
| `tool.request` | **请客户端执行** | `tool_call_id`, `name`, `arguments`（JSON） |
| `run.finished` | 正常结束 | `run_id`, `reason`（`stop` / `cancelled` / …） |
| `error` | 失败 | `message`, `code?` |

**约定**

- 一次 run 内可多次 `tool.request`（串行：发一个 → 等 `tool_results` → 再调模型 → 可能再发）。
- v1 **串行** tool：同时最多一个未完成的 `tool.request`。
- `message.delta` 仅用于展示；客户端以 `message.completed` / 最终上下文为准时可忽略拼接细节，但 UI 应能拼 delta。
- Steering / follow-up 写入 context 时各发一条 `message.completed`（`source=steer|follow_up`），便于客户端 UI 对齐，无需再猜「静默插入」。

---

## §4 Server 编排（重写）

不使用旧双循环**文件**；用**一个 run 状态机**表达同等控制流（tool 内续转 + steering 队列 + follow-up 外续转）。

```text
POST /v1/runs
  create run_id, bind SSE writer
  emit run.started
  context = messages (+ tools schema)
  steering_queue = []
  follow_up_policy = configured policy   # 默认 Noop；可换示例策略

  loop:                                         # ≈ 旧「外循环」
    loop:                                       # ≈ 旧「内循环」
      // 1) 先消化 steering
      drain steering_queue into context
        → 每条 emit message.completed(source=steer)

      // 2) 调模型
      stream LLM(context, tools)
        → emit message.delta*
        → emit message.completed(source=assistant, tool_calls?)

      // 3) 有 tool → 串行执行（客户端）后继续内循环
      if tool_calls 非空:
        for each tool_call in order:
          emit tool.request
          wait tool_results OR 期间可继续 enqueue steer（下轮内循环再 drain）
          append tool result to context
        continue 内循环

      // 4) 无 tool → 跳出内循环，看 follow-up
      break 内循环

    // 5) follow-up：本可结束时由策略决定是否再开一轮
    follow_ups = follow_up_policy.next(&context)
    if follow_ups 非空:
      append to context
      emit message.completed(source=follow_up) for each
      continue 外循环          # 再调模型，用户未新开 run
    else:
      emit run.finished(reason=stop)
      close SSE
      return

  on LLM/timeout/cancel:
    emit error 或 run.finished(cancelled)
    close SSE
```

**LLM**

- 使用 `liter_llm`（或同等 OpenAI 兼容客户端）对接 `config.toml` 的 `api_key` / `base_url` / `model`。
- Server 负责把 wire `messages` / `tools` 映射到供应商请求格式。

**等待 tool**

- 每 run 一个等待槽（`oneshot` / `Mutex<Option<Waiter>>`）。
- 默认超时建议可配置（如 60s）；超时 → `error`，结束 run。
- 等待期间允许 `POST .../steer` 入队，**不**抢占当前 tool 等待；tool 返回后进入下一内循环迭代时再 drain。

### §4.1 Steering（v1）

| 项 | 定义 |
|----|------|
| 是什么 | 客户端在 **同一 run 进行中** 插入的消息（通常是 user），纠正/补充当前任务 |
| 谁触发 | 客户端 `POST /v1/runs/{id}/steer`（人点发送或 UI 自动） |
| 何时生效 | 当前 assistant 流结束或当前 tool 等待结束后、**下一次 LLM 调用前** |
| 例子 | 模型正在调「下单」相关 tool 时，用户说「改成微辣」→ 入队 → tool 回来后先把「改成微辣」写入 context 再调模型 |

### §4.2 Follow-up（v1）

| 项 | 定义 |
|----|------|
| 是什么 | 模型已经 **无 tool、本可结束** 时，由 **server 策略** 自动插入的消息，从而 **同一 run** 再调一轮模型 |
| 谁触发 | `FollowUpPolicy::next(&context) -> Vec<Message>`（不是用户再发一句） |
| 何时生效 | 内循环因「无 tool_calls」退出之后 |
| 与再开 run 的区别 | 不新建 `run_id` / 不新建 SSE；用户可无感 |

**策略接口（概念）**

```text
trait FollowUpPolicy {
  fn next(&self, context: &RunContext) -> Vec<WireMessage>;  // 空 = 真正结束
}
```

**v1 交付**

1. **`NoopFollowUp`**（默认）：永远返回空 → 行为与「无 follow-up」相同。  
2. **`ExampleOrderFollowUp`**（内置可测示例，点外卖）：用简单状态位演示——若 context 中已有成功的 `place_order` tool 结果且尚无成功的 `send_sms`，则返回一条 follow-up：「请调用 send_sms 发送取餐码」。状态可由 tool 名 + `is_error=false` 推断，或 run 级小状态机。  
3. 配置项：`follow_up_policy = "noop" | "example_order"`（名称可调整）。

业务方后续可实现自己的 `FollowUpPolicy`；v1 不要求通用工作流引擎。

---

## §5 配置与进程

- `config.toml`（gitignore 本地密钥）：`api_key`, `base_url?`, `model`, 可选 `tool_timeout_secs`, `follow_up_policy`, `listen`（如 `0.0.0.0:8080`）。
- `config.example.toml` 入库。
- 二进制：`andromeda`（或 `andromeda-server`）启动 HTTP 服务；旧「一次性 cargo run 打一句问候」demo **删除/替换**。

---

## §6 仓库与代码策略

- **整仓重写**：删除或替换现有 `agent_loop`、`liter_backend`、`stub_backend`、旧 `types`/`event_stream` 中与 wire 冲突的部分；不要求兼容旧 API。
- 可保留的思路（非强制拷贝代码）：OpenAI 兼容流式调用、TOML 配置加载。
- 测试：以协议级单测 / 集成测为主（mock LLM + 假 tool_results POST），不依赖真实 MaaS。

---

## §7 客户端契约（给另一仓库）

最小客户端循环：

1. `POST /v1/runs`（带 messages + tools），读 SSE。
2. 收到 `message.delta` → 更新 UI。
3. 收到 `tool.request` → 本地执行 → `POST .../tool_results`。
4. 用户中途改口 → `POST .../steer`（同一 `run_id`，SSE 保持）。
5. 收到 `message.completed(source=follow_up)` → UI 可展示为系统续步（或折叠）。
6. 收到 `run.finished` 或 `error` → 结束。

本仓库 v1 可附：`docs` 中的序列图 + 示例 `curl`（SSE 需支持流式读取的客户端）。

---

## §8 里程碑建议

1. **M1**：HTTP 骨架 + SSE 推送假事件（无 LLM）
2. **M2**：接入真实 LLM 流式 → `message.delta` / `completed`
3. **M3**：`tool.request` + `tool_results` 等待与续跑
4. **M4**：`steer` 队列 + 内循环 drain；`FollowUpPolicy`（noop + example_order）+ 外循环续转
5. **M5**：超时 / cancel / 基础错误码；示例联调说明

---

## 批准记录

- 2026-09-18：用户确认方案 B、本仓为 server、POST+SSE、整仓重写且旧代码可不保留；对话回复 **ok**。
- 2026-09-18：用户要求 **v1 加入 steering 与 follow-up**；规格已修订本节。
