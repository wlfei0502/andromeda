> **SUPERSEDED / archived.** See `docs/superpowers/archive/README.md`. Current architecture: cloud SSE server specs under `docs/superpowers/specs/`.

# Agent Loop 双循环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现与 pi `agent-loop.ts` 对齐的 Rust 双循环 agent loop：自有类型、`EventStream` 订阅 API、`AgentBackend` 钩子；Phase A stub 验收 + Phase B `liter_llm` 流式与串行 tool。

**Architecture:** 循环只做控制流与 `emit`；I/O 经 `AgentBackend`。对外返回 `EventStream<AgentEvent, Vec<AgentMessage>>`（单消费者）。Phase A 用 `StubBackend`；Phase B 用 `LiterBackend` 调 `chat_stream`，仅在边界做消息映射。

**Tech Stack:** Rust 2024 edition、`tokio`、`futures`、`tokio-util`、`serde`/`serde_json`、`thiserror`、`async-trait`（或 RPITIT）、`liter-llm`。

## Global Constraints

- 循环内禁止使用 `liter_llm` 消息类型；仅 `liter_backend` 边界转换。
- v1 tool **仅串行**；不做并行 / `prepareNextTurn` / `declareToolChanges` / length 截断批失败。
- `EventStream` **单消费者**；必须提供 `result_handle()`。
- API Key 等凭据只从环境变量读取，禁止写死进仓库。
- 测试优先：每任务先写失败测试再实现（TDD）。
- 规格来源：`docs/superpowers/specs/2026-09-17-agent-loop-design.md`。

## File Structure

| File | Responsibility |
|------|----------------|
| `src/lib.rs` | 模块导出 |
| `src/types.rs` | `AgentMessage` / `AgentEvent` / `AgentContext` / `StopReason` / tool 类型 |
| `src/error.rs` | `LoopError` |
| `src/event_stream.rs` | `EventStream<T, R>` |
| `src/backend.rs` | `AgentBackend` + `Emit` + `TurnSnapshot` |
| `src/agent_loop.rs` | `agent_loop` / `agent_loop_continue` / `run_loop` |
| `src/stub_backend.rs` | Phase A 假流式 + 假 tool |
| `src/liter_backend.rs` | Phase B `liter_llm` 映射与流式 |
| `src/main.rs` | 演示入口（env 配置） |
| `Cargo.toml` | 依赖与 `[lib]` |

---

### Task 1: 类型 + EventStream + 库骨架

**Files:**
- Create: `src/lib.rs`, `src/types.rs`, `src/error.rs`, `src/event_stream.rs`
- Modify: `Cargo.toml`
- Test: `src/event_stream.rs` 内 `#[cfg(test)]`

**Interfaces:**
- Produces:
  - `AgentMessage`, `ContentPart`, `StopReason`, `AgentEvent`, `AgentContext`, `AgentTool`, `ToolCall`（从 `ContentPart::ToolCall` 抽取或独立 struct）
  - `EventStream<T, R>::new(is_complete, extract)`, `push`, `end`, `result_handle`, `Stream` impl
  - `LoopError`

- [ ] **Step 1: 更新 Cargo.toml 依赖**

```toml
[package]
name = "andromeda"
version = "0.1.0"
edition = "2024"

[dependencies]
liter-llm = "2.0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync"] }
tokio-util = "0.7"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
async-trait = "0.1"

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
```

- [ ] **Step 2: 写 EventStream 失败测试**

在 `src/event_stream.rs`（先写测试模块，类型可先空壳）:

```rust
#[tokio::test]
async fn push_complete_event_resolves_result_handle() {
    use futures::StreamExt;
    let stream = EventStream::new(
        |e: &i32| *e < 0,
        |e: &i32| e.abs(),
    );
    let handle = stream.result_handle();
    stream.push(1);
    stream.push(-7);
    assert_eq!(handle.await.unwrap(), 7);
    // 仍可把已缓冲事件读完；完成后 next 为 None
    let mut s = stream;
    assert_eq!(s.next().await, Some(1));
    assert_eq!(s.next().await, Some(-7));
    assert_eq!(s.next().await, None);
}
```

- [ ] **Step 3: Run test — expect FAIL**

Run: `cargo test push_complete_event_resolves_result_handle -- --nocapture`  
Expected: compile error / 找不到 `EventStream`

- [ ] **Step 4: 实现 types / error / EventStream / lib.rs**

`types.rs` 核心：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum AgentMessage {
    User { content: String },
    Assistant { parts: Vec<ContentPart>, stop_reason: StopReason },
    ToolResult { tool_call_id: String, tool_name: String, content: String, is_error: bool, terminate: bool },
    System { content: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContentPart {
    Text { text: String },
    ToolCall { id: String, name: String, arguments: serde_json::Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason { Stop, ToolCalls, Length, Error, Aborted }

#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    AgentStart,
    AgentEnd { messages: Vec<AgentMessage> },
    TurnStart,
    TurnEnd { message: AgentMessage, tool_results: Vec<AgentMessage> },
    MessageStart { message: AgentMessage },
    MessageUpdate { message: AgentMessage },
    MessageEnd { message: AgentMessage },
    ToolExecutionStart { tool_call_id: String, tool_name: String, args: serde_json::Value },
    ToolExecutionUpdate { tool_call_id: String, partial: String },
    ToolExecutionEnd { tool_call_id: String, result: String, is_error: bool },
}

#[derive(Debug, Clone, Default)]
pub struct AgentContext {
    pub messages: Vec<AgentMessage>,
    pub tools: Vec<AgentTool>,
}

#[derive(Debug, Clone)]
pub struct AgentTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}
```

`EventStream`：无界 `mpsc` + `tokio::sync::watch` 或 `Shared<oneshot>` 实现 `result_handle`；`push` 在 `is_complete` 时 resolve；实现 `Stream`。

`lib.rs`:

```rust
pub mod agent_loop;
pub mod backend;
pub mod error;
pub mod event_stream;
pub mod liter_backend;
pub mod stub_backend;
pub mod types;
```

（尚未实现的模块先 `pub mod` 空文件占位，或本任务只导出已有模块，后续任务再加。）

- [ ] **Step 5: Run test — expect PASS**

Run: `cargo test push_complete_event_resolves_result_handle`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/types.rs src/error.rs src/event_stream.rs
git commit -m "feat: add agent types and EventStream"
```

---

### Task 2: AgentBackend trait + run_loop 骨架（无真实 LLM）

**Files:**
- Create: `src/backend.rs`
- Modify: `src/agent_loop.rs`, `src/lib.rs`
- Test: `src/agent_loop.rs` 内测试（配合 Task 3 stub；本任务可先测 `agent_loop_continue` 校验）

**Interfaces:**
- Consumes: `types::*`, `EventStream`, `LoopError`
- Produces:
  - `type Emit<'a> = Arc<dyn Fn(AgentEvent) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send + Sync + 'a>` 或更简单的 `async fn` 回调封装：`Emit = Box<dyn Fn(AgentEvent) + Send + Sync>` 若同步 push 足够（EventStream::push 是同步的 → **Emit 可为 `Arc<dyn Fn(AgentEvent) + Send + Sync>`**）
  - `TurnSnapshot { message, tool_results, context_messages_len, new_messages }`
  - `#[async_trait] trait AgentBackend`
  - `agent_loop(...) -> EventStream<AgentEvent, Vec<AgentMessage>>`
  - `agent_loop_continue(...) -> EventStream<...>`

- [ ] **Step 1: 写 continue 校验失败测试**

```rust
#[tokio::test]
async fn continue_rejects_empty_and_assistant_tail() {
    let backend = /* 最小 stub，Task 3 可提前做空 Stub */;
    let empty = AgentContext::default();
    // agent_loop_continue 应立即以 Error 结束或返回 Err——规格：throw；Rust 用 push AgentEnd 前先校验：
    // 约定：校验失败时 stream 立即 end，result 为空或带错误事件。
    // 实现选择：返回 Result<EventStream, LoopError> 在启动前校验。
    assert!(matches!(
        agent_loop_continue(empty, Arc::new(backend)),
        Err(LoopError::InvalidContinue(_))
    ));
}
```

采用 **`Result<EventStream, LoopError>`** 做启动前校验（比静默失败更清晰）。

- [ ] **Step 2: 实现 backend.rs + agent_loop 入口与双循环**

`AgentBackend`（默认方法返回空 / false）：

```rust
#[async_trait]
pub trait AgentBackend: Send + Sync {
    async fn stream_assistant(
        &self,
        ctx: &mut AgentContext,
        emit: Emit,
        cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError>; // Assistant 变体

    async fn execute_tool(
        &self,
        call: &ContentPart, // ToolCall
        emit: Emit,
        cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError>; // ToolResult 变体

    async fn get_steering(&self) -> Vec<AgentMessage> { vec![] }
    async fn get_follow_up(&self) -> Vec<AgentMessage> { vec![] }
    async fn should_stop_after_turn(&self, _turn: &TurnSnapshot) -> bool { false }
}
```

`run_loop` 按规格 §2 伪代码实现；`emit` 接到 `stream.push`；`AgentEnd` 触发 complete。  
`stream_assistant` 失败：构造 `Assistant { stop_reason: Error|Aborted }`，turn_end + agent_end。  
串行执行 tool：从 assistant parts 收集 ToolCall；全部 `terminate==true` 则 `has_more_tools=false`。

- [ ] **Step 3: Run continue 校验测试 — PASS**

- [ ] **Step 4: Commit**

```bash
git add src/backend.rs src/agent_loop.rs src/lib.rs src/error.rs
git commit -m "feat: add AgentBackend and dual-loop control flow"
```

---

### Task 3: StubBackend + Phase A 验收测试

**Files:**
- Create: `src/stub_backend.rs`
- Modify: `src/lib.rs`
- Test: `src/agent_loop.rs` 或 `tests/phase_a.rs`

**Interfaces:**
- Consumes: `AgentBackend`, `agent_loop`
- Produces: `StubBackend` 可配置脚本：回合序列（文本 / 带 tool / 再文本）、可选 follow-up 一次

- [ ] **Step 1: 写 Phase A 集成测试（失败）**

```rust
#[tokio::test]
async fn phase_a_tool_then_final_text_event_order() {
    use futures::StreamExt;
    let backend = StubBackend::script(vec![
        StubTurn::AssistantWithTool { name: "echo", args: json!({"x":1}) },
        StubTurn::AssistantText("done".into()),
    ]);
    let ctx = AgentContext::default();
    let stream = agent_loop(
        vec![AgentMessage::User { content: "hi".into() }],
        ctx,
        Arc::new(backend),
    ).unwrap();
    let handle = stream.result_handle();
    let mut events = vec![];
    let mut s = stream;
    while let Some(e) = s.next().await { events.push(e); }
    let messages = handle.await.unwrap();
    assert!(matches!(events.first(), Some(AgentEvent::AgentStart)));
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd { .. })));
    // 断言中间出现 MessageUpdate、ToolExecutionStart、ToolExecutionEnd
    assert!(events.iter().any(|e| matches!(e, AgentEvent::MessageUpdate { .. })));
    assert!(events.iter().any(|e| matches!(e, AgentEvent::ToolExecutionStart { .. })));
    assert_eq!(handle_messages_contain_tool_and_final(&messages), true);
}
```

再加 `phase_a_follow_up_restarts_outer_loop`：第一次结束后 `get_follow_up` 返回一条 User，再跑一轮。

- [ ] **Step 2: 实现 StubBackend**

- 第一次 `stream_assistant`：emit MessageStart → 若干 Update（假 delta）→ MessageEnd，parts 含 ToolCall  
- `execute_tool`：emit start/end，返回 `ToolResult { content: "ok", is_error: false, terminate: false }`  
- 第二次：纯文本 Stop  
- follow-up：用 `Mutex<VecDeque>` 只弹出一次

- [ ] **Step 3: cargo test phase_a — PASS**

- [ ] **Step 4: Commit**

```bash
git add src/stub_backend.rs src/agent_loop.rs src/lib.rs tests/
git commit -m "feat: stub backend and Phase A agent loop tests"
```

---

### Task 4: LiterBackend 流式（暂无 tool 强制）

**Files:**
- Create: `src/liter_backend.rs`
- Modify: `src/lib.rs`, `Cargo.toml` if needed
- Test: 可用 mock 或 `#[ignore]` 的网络测试；至少单测 `to_llm_messages` / delta 累积纯函数

**Interfaces:**
- Consumes: `liter_llm::{LlmClient, ChatCompletionRequest, ...}`
- Produces: `LiterBackend<C: LlmClient>`, `to_llm_messages`, 流式累积逻辑

- [ ] **Step 1: 写映射单元测试**

```rust
#[test]
fn to_llm_messages_maps_user_assistant_tool() { /* ... */ }
```

- [ ] **Step 2: 实现 liter_backend**

- `stream_assistant`：`client.chat_stream(req)`，累积 text / tool_calls 分片，每 chunk `emit(MessageUpdate)`，结束定稿 `stop_reason`  
- 空流 / 错误 → `StopReason::Error` 的 Assistant（或返回 `LoopError`，由 loop 定稿——与规格表一致：定稿 Error assistant）  
- `execute_tool`：按 `ctx.tools` 名查找；v1 可先返回 “tool not registered” 的 error ToolResult，Task 5 再接真实执行器

- [ ] **Step 3: 测试 PASS + Commit**

```bash
git commit -m "feat: liter_llm streaming backend mapping"
```

---

### Task 5: 串行 tool 执行器 + main 演示

**Files:**
- Modify: `src/liter_backend.rs`, `src/main.rs`, `src/backend.rs`（若需 tool handler 回调）
- Test: stub 已覆盖串行；可选 liter 集成 `#[ignore]`

**Interfaces:**
- Produces: `ToolExecutor` trait 或 `LiterBackend` 内 `HashMap<String, Box<dyn Fn...>>`  
- `main`：读 `ANDROMEDA_API_KEY`、`ANDROMEDA_BASE_URL`、`ANDROMEDA_MODEL`；订阅 EventStream 打印增量文本

- [ ] **Step 1: 实现可注册的 echo tool + 串行调度已在 loop 中**

- [ ] **Step 2: 改写 main 使用 agent_loop + LiterBackend（无 key 时打印提示并 exit 0/1）**

```rust
let api_key = std::env::var("ANDROMEDA_API_KEY")?;
// ...
let stream = agent_loop(prompts, ctx, backend)?;
let handle = stream.result_handle();
while let Some(ev) = stream.next().await {
    if let AgentEvent::MessageUpdate { message } = ev {
        // 打印增量
    }
}
let _ = handle.await;
```

- [ ] **Step 3: `cargo build` PASS；有 key 时手动 `cargo run` 验证**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: wire liter backend tools and demo main"
```

---

### Task 6: 规格对齐检查与文档状态更新

**Files:**
- Modify: `docs/superpowers/specs/2026-09-17-agent-loop-design.md` 状态改为「已实现 Phase A/B 骨架」
- Run: `cargo test` 全绿

- [ ] **Step 1: `cargo test` 全量**
- [ ] **Step 2: 对照规格验收列表逐条打勾（缺失则补测）**
- [ ] **Step 3: Commit**

```bash
git commit -m "docs: mark agent loop implementation status"
```

---

## Spec coverage (self-review)

| 规格项 | 任务 |
|--------|------|
| 自有类型 | Task 1 |
| EventStream + result_handle | Task 1 |
| 双循环 + agent_loop/continue | Task 2 |
| AgentBackend | Task 2 |
| Stub + Phase A 验收 | Task 3 |
| liter 流式 | Task 4 |
| 串行 tool + main env | Task 5 |
| 非目标（并行等） | 不实现 |
| HITL beforeToolCall | 未列入规格 v1 → 不做 |

## Placeholder scan

无 TBD；Emit 具体类型在 Task 2 固定为同步 `Arc<dyn Fn(AgentEvent) + Send + Sync>`（因 `EventStream::push` 同步）。
