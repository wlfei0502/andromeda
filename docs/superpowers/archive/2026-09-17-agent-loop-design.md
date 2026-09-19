> **SUPERSEDED / archived.** See `docs/superpowers/archive/README.md`. Current architecture: cloud SSE server specs under `docs/superpowers/specs/`.

# Agent Loop 双循环设计

日期：2026-09-17  
状态：已批准；Phase A/B 骨架已实现（2026-09-17）  
参考：[earendil-works/pi `agent-loop.ts`](https://github.com/earendil-works/pi/blob/main/packages/agent/src/agent-loop.ts)

## 目标

实现与 TypeScript 双循环控制流对齐的 Rust agent loop（`agent_loop.rs` 及配套模块），要求：

- 循环内使用自有消息/事件类型（不用 `liter_llm` 类型）
- 基于 trait 的 I/O 钩子（`AgentBackend`）
- 流式 assistant 响应
- 完整的 `EventStream` 订阅 API
- 分阶段交付：**A（stub）→ B（`liter_llm` + 串行 tool）**

## 非目标（本轮不做）

- 并行 tool 执行 / 按 tool 的 `executionMode`
- `prepareNextTurn`（压缩上下文 / 中途换模型）
- 因 `length` 截断而对整批 tool 失败的语义
- `declareToolChanges` 系统消息中的 tool 增减声明
- 多订阅者广播版 EventStream（v1 仅单消费者，与 pi 一致）

## 方案

采用 **自有类型 + `AgentBackend` trait**（不把循环直接绑在 `liter_llm::Message` 上）。

- 循环负责控制流与事件发射
- Backend 负责 LLM 流式、tool 执行、steering / follow-up 队列
- 仅在 LLM 边界做 `AgentMessage ↔ liter_llm::Message` 转换（Phase B）

---

## §1 模块与边界

| 文件 | 职责 |
|------|------|
| `src/types.rs` | `AgentMessage`、`AgentContext`、`AgentEvent`、`StopReason`、tool 相关类型 |
| `src/event_stream.rs` | 通用 `EventStream<T, R>`（异步订阅 + `result()`） |
| `src/agent_loop.rs` | 双循环；`agent_loop` / `agent_loop_continue` / `run_loop` |
| `src/backend.rs` | `AgentBackend` trait 与默认空实现 |
| `src/stub_backend.rs` | Phase A：假流式 + 假 tool |
| `src/liter_backend.rs` | Phase B：`liter_llm` 的 `chat_stream` + tool 分发 |
| `src/main.rs` | 组装配置 / backend，演示运行 |
| `src/lib.rs` | 模块导出（便于单测） |

流式边界：

```text
agent_loop  → 消费 AgentEvent / 定稿 AssistantMessage
  └─ backend.stream_assistant(...)
       └─ (B) LlmClient::chat_stream → 映射为 MessageUpdate 事件
```

循环始终在 context 中持有 partial assistant（与参考实现 `streamAssistantResponse` 相同）。

---

## §2 双循环与事件顺序

对齐参考 `runLoop`：

```text
emit agent_start, turn_start
pending = get_steering()

loop {                                    // 外循环：follow-up
  has_more_tools = true
  while has_more_tools || !pending.is_empty() {   // 内循环
    // 将 pending 写入 context；每条 emit message_start/end
    pending.clear()

    assistant = stream_assistant(...)     // start → deltas → end
    if stop in {Error, Aborted}:
      emit turn_end, agent_end; return

    if 存在 tool_calls:
      results = execute_tools_serial(...)
      将 results 写入 context
      has_more_tools = !全部 terminate
    else:
      has_more_tools = false

    emit turn_end
    if should_stop_after_turn: emit agent_end; return
    pending = get_steering()
  }

  follow_ups = get_follow_up()
  if follow_ups 非空:
    pending = follow_ups; continue
  break
}
emit agent_end
```

对外入口（对齐 TS）：

- `agent_loop(prompts, context, backend) -> EventStream<AgentEvent, Vec<AgentMessage>>`  
  追加 prompts，发出 start 事件，跑循环，返回 stream。
- `agent_loop_continue(context, backend) -> EventStream<...>`  
  不加新 prompt；校验最后一条消息不能是 assistant。

一次成功 turn 的事件顺序（含流式）：

1. `TurnStart`
2. （可选）steering / user：`MessageStart` → `MessageEnd`
3. Assistant：`MessageStart` → 多次 `MessageUpdate` → `MessageEnd`
4. 每个 tool：`ToolExecutionStart` →（可选 update）→ `ToolExecutionEnd` → toolResult 的 `MessageStart` / `MessageEnd`
5. `TurnEnd`
6. 整轮结束：`AgentEnd { messages }`

---

## §3 类型、Backend、EventStream

### 消息

```text
AgentMessage =
  | User { content: String }
  | Assistant { parts: Vec<ContentPart>, stop_reason: StopReason }
  | ToolResult { tool_call_id, tool_name, content, is_error }
  | System { content: String }   // 预留；v1 少用

ContentPart = Text { text } | ToolCall { id, name, arguments: serde_json::Value }

StopReason = Stop | ToolCalls | Length | Error | Aborted

// 命名：AssistantMessage / ToolResultMessage / ToolCall 为对应
// AgentMessage / ContentPart 变体的类型别名或 newtype。
```

### 上下文

```text
AgentContext {
  messages: Vec<AgentMessage>,
  tools: Vec<AgentTool>,  // name、description、JSON Schema 参数；执行走 AgentBackend
}
```

### 事件

```text
AgentEvent =
  | AgentStart
  | AgentEnd { messages: Vec<AgentMessage> }
  | TurnStart
  | TurnEnd { message: AgentMessage, tool_results: Vec<AgentMessage> }
  | MessageStart { message }
  | MessageUpdate { message }      // partial assistant 快照
  | MessageEnd { message }
  | ToolExecutionStart { tool_call_id, tool_name, args }
  | ToolExecutionUpdate { tool_call_id, partial }
  | ToolExecutionEnd { tool_call_id, result, is_error }
```

### `AgentBackend`

```rust
#[async_trait]
trait AgentBackend: Send + Sync {
    async fn stream_assistant(
        &self,
        ctx: &mut AgentContext,
        emit: Emit<'_>,
        cancel: &CancellationToken,
    ) -> Result<AssistantMessage, LoopError>;

    async fn execute_tool(
        &self,
        call: &ToolCall,
        emit: Emit<'_>,
        cancel: &CancellationToken,
    ) -> Result<ToolResultMessage, LoopError>;

    async fn get_steering(&self) -> Vec<AgentMessage>;      // 默认空
    async fn get_follow_up(&self) -> Vec<AgentMessage>;     // 默认空
    async fn should_stop_after_turn(&self, turn: &TurnSnapshot) -> bool; // 默认 false
}
```

循环内部仍用 `emit` 回调；对外只暴露 `EventStream`（把 emit 接到 `stream.push`）。

### 流式（Phase B）

- 调用 `LlmClient::chat_stream`
- 累积 `StreamDelta`（text + tool_call 分片）到 partial `Assistant`
- 每个 chunk → `MessageUpdate`；流结束 → 定稿 `stop_reason`，`MessageEnd`
- 固定映射函数 `to_llm_messages` / `from_llm_assistant`（v1 不做可插拔配置）

### `EventStream<T, R>`（必做）

对齐 pi 的 `EventStream`：

- 构造时传入 `is_complete: Fn(&T) -> bool` 与 `extract_result: Fn(&T) -> R`
- `push(event)`：若已完成则兑现最终 `R`；交付给等待者或入队
- `end(Optional<R>)`：标记结束并唤醒等待者
- `result() -> Future<Output = R>`：等待最终值
- 实现 `futures::Stream`（或等价异步迭代）供消费
- Agent 工厂：`is_complete` 当且仅当 `AgentEnd`；`extract_result` = `AgentEnd.messages`
- 传输：v1 用无界 `tokio::sync::mpsc` + `oneshot` / `Shared` 承载 `result`
- **v1 仅单消费者**

用法（必需模式——共享 handle，使迭代与 `result()` 可并行，对齐 TS）：

```rust
let stream = agent_loop(prompts, ctx, backend);
let result_handle = stream.result_handle(); // 克隆 oneshot/shared future
tokio::spawn(async move {
    while let Some(ev) = stream.next().await {
        // UI / 日志
    }
});
let messages = result_handle.await;
```

也可以只靠 `while let Some(ev)`、从最终 `AgentEnd` 取 messages；但必须提供 `result_handle()`，并在 push 到 `AgentEnd`（或调用 `end()`）时兑现。

---

## §4 错误处理与验收

### 错误行为

| 来源 | 行为 |
|------|------|
| LLM 流式网络/鉴权失败 | 定稿 assistant，`StopReason::Error`；`turn_end` + `agent_end`；结束循环 |
| 用户取消 | `StopReason::Aborted`；同样收尾 |
| 单个 tool 失败 | `ToolResult { is_error: true }`；内循环继续 |
| 全部 tool `terminate` | `has_more_tools = false`；再看 follow-up |
| 空 `choices` / 空流 | 视为 `Error` stop |

### Phase A 验收（stub，不碰网络）

1. Stub 流式吐出若干 `MessageUpdate` 后定稿
2. 一次 tool_call → 执行 → 下一轮无 tool → `agent_end`
3. 内循环本应结束后注入 follow-up → 外循环再转一轮
4. 单测断言事件顺序与 `result()` 消息内容
5. EventStream：订阅方按序收到事件并以 `agent_end` 结束；`result()` 与 `AgentEnd.messages` 一致

### Phase B 验收（`liter_llm`）

1. `chat_stream` 映射为自有事件；订阅方可看到增量文本
2. Tool 闭环：调用 → ToolResult → 再生成
3. `main` 跑一轮真实 MaaS 对话；凭据来自环境变量（如 `ANDROMEDA_API_KEY`），不写死
4. 鉴权失败走 Error 路径，进程不 panic

### 依赖（预期）

- 已有：`liter-llm`、`tokio`
- 按需增加：`async-trait` 或 RPITIT、`futures`、`tokio-util`（`CancellationToken`）、`serde` / `serde_json`、`thiserror`

---

## 实现阶段

1. **类型 + EventStream** — 编译通过，单测 push / 迭代 / result
2. **`run_loop` + stub backend** — Phase A 验收通过
3. **`liter_backend` 流式** — 真实 chat（暂无 tool）
4. **串行 tool + main 演示** — Phase B 验收
5. （后续）并行 tool、prepareNextTurn、tool 声明差分

## 已决议事项

| 议题 | 决议 |
|------|------|
| 架构 | 自有类型 + trait 钩子 |
| Phase B 是否含 tool | 是，串行 |
| 流式 | 必做 |
| EventStream API | 完整订阅 + `result()`，单消费者 |
| 并行 tool / prepareNextTurn / declareToolChanges | 延后 |
