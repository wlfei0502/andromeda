# 桌面最小对接说明（LH-M6）

面向 **andromeda-desktop**（或其它外仓客户端）。完整字段与事件表见 [api-client.md](api-client.md)。本文是落地清单：按优先级改，避免一次铺开。

云端长任务能力（LH-M1–M5）已就绪；桌面侧负责 SSE、本地 tool 执行，以及可选的 Plan / 子代理 UI。

---

## 0. 约定

| 角色 | 职责 |
|------|------|
| 云端 andromeda | 编排、LLM、checkpoint、server tools（`write_todos` / `task`）、护栏 |
| 桌面 | 展示流式结果、执行 **client tools**、续订 SSE、steer / cancel |

- 所有 HTTP 均针对 **父** `run_id`（响应头 `X-Run-Id`）。
- 不要在本地注册或执行名为 `write_todos`、`task` 的 tool。

---

## 1. 必做（任何长任务 / 断线场景）

1. **创建 run**  
   `POST /v1/runs`，`Accept: text/event-stream`，保存 `X-Run-Id`。

2. **SSE 主循环**  
   处理至少：`message.delta`、`message.completed`、`tool.request`、`run.finished`、`error`。

3. **执行 tool**  
   收到 `tool.request` → 本地执行 →  
   `POST /v1/runs/{run_id}/tool_results`  
   `{ tool_call_id, content, is_error }`。

4. **断线续订**  
   SSE 断开且 run 未结束 →  
   `GET /v1/runs/{run_id}/events`  
   （同一 `run_id`，**不要**新建 run）。  
   可能先收到 `run.resumed`，再收到 **一条或多条** 补发的 `tool.request`（含子代理并行等待）。

5. **`409` + `code=not_owner`**  
   `tool_results` / `steer` 打到非 owner 实例时：先对健康实例 `GET .../events` 接管，再重试 POST。

6. **结束**  
   `run.finished` 或 `error` → 关流、停 UI；可选展示 `reason` / `code`。

建议同时支持：`POST .../steer`、`POST .../cancel`。

---

## 2. 启用 Plan Mode 时再做

创建时：`"options": { "plan_mode": true }`。

| 桌面行为 | 说明 |
|----------|------|
| 渲染 `todos.updated` | 用 `todos[]` 画进度列表即可 |
| 勿实现 `write_todos` | 不会收到该名的 `tool.request` |
| 开关 UX | 「是否开 Plan」由桌面决定；云端只认 boolean |

---

## 3. 启用子代理时再做

创建时：`"options": { "subagents": true }`。

| 桌面行为 | 说明 |
|----------|------|
| 处理 `task.started` / `completed` / `failed` / `timed_out` | 子进度 UI；子代理 **没有** 独立 SSE |
| `tool.request` 可选字段 | `agent_id`、`parent_task_id` 仅用于展示「子任务在调工具」；回传仍用父 `run_id` + `tool_call_id` |
| 勿实现 `task` | server tool |
| `readonly: true`（可选） | 标在 client `tools[]` 上，供 `explore` 子代理过滤 |

未开 `subagents` 时忽略 `task.*` 即可。

---

## 4. 建议处理（体验 / 排障）

| 事件 / 字段 | 建议 |
|-------------|------|
| `reasoning.delta` / `message.completed.reasoning_content` | 可选「思考」面板；有 tool 的回合需把 reasoning 随历史带回（若你本地拼 messages） |
| `context.summarized` | 可忽略或打日志 |
| `run.finished.reason` | `guard_llm_rounds` / `guard_timeout` / `guard_noop` / `cancelled` / `stop` → 友好文案 |
| `error` + `code=context_overflow` | 提示上下文超限 |
| `error` + `code=follow_up_limit` | 跟跑轮次用尽 |

---

## 5. 明确不必做（云端已覆盖或后置）

- 本地实现 OS 沙箱 / Docker（产品策略，非本 API 合同）
- 为子代理再开第二条 SSE
- `session_id` 多 run 会话目录（云端尚未做）
- `GET /v1/runs/{id}` 快照（规格可选，当前未作为对接前提）

---

## 6. 验收清单（桌面自测）

- [ ] 短问答：无 tool，能流式显示并 `run.finished reason=stop`
- [ ] 一次 tool：请求 → 本地执行 → `tool_results` → 继续对话
- [ ] 故意断 SSE → `GET .../events` 能续上；若卡在 tool，能看到补发的 `tool.request`
- [ ] `plan_mode=true`：能收到 `todos.updated`，本地无 `write_todos`
- [ ] `subagents=true`：能收到 `task.started/completed`；子 tool 的 `tool.request` 可带回父 run
- [ ] Cancel：`run.finished reason=cancelled`

对照字段细节时以 [api-client.md](api-client.md) 为准。
