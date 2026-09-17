# Cloud Agent Server (SSE) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `andromeda` into a cloud Agent Server: `POST /v1/runs` returns SSE; tools run on an external client via `tool_results`; v1 includes steering and follow-up.

**Architecture:** Axum HTTP server owns run state. Orchestrator loop drains steering → streams LLM → serial `tool.request`/wait → optional `FollowUpPolicy` continue. Wire events are a new JSON SSE protocol (not old `AgentEvent`). Old agent_loop / LiterBackend / stub code is deleted.

**Tech Stack:** Rust 2024, axum, tokio, tower-http, serde/serde_json, uuid, thiserror, async-trait, toml, liter-llm, futures

**Spec:** `docs/superpowers/specs/2026-09-18-cloud-agent-sse-design.md`

## Global Constraints

- This repo is **server only**; clients live elsewhere.
- Transport: **POST response body = SSE**; tool results and steer are separate POSTs while SSE stays open.
- **No parallel tools** in v1 (serial only).
- **Full rewrite** — do not wrap old `agent_loop` / `LiterBackend`.
- Wire events: `run.started`, `message.delta`, `message.completed`, `tool.request`, `run.finished`, `error`.
- Default `FollowUpPolicy` is noop; ship `example_order` for tests.
- LLM secrets only in server `config.toml` (gitignored).

## File Structure

| Path | Responsibility |
|------|----------------|
| `src/main.rs` | Load config, bind listen addr, serve axum router |
| `src/lib.rs` | Module exports for integration tests |
| `src/config.rs` | `AppConfig` from `config.toml` |
| `src/error.rs` | `AppError` → HTTP status mapping |
| `src/wire.rs` | Wire messages, tools, SSE event enums/structs |
| `src/sse.rs` | Format `event:` / `data:` frames; `SseTx` helper |
| `src/run.rs` | `RunId`, `RunHandle`, registry, steer queue, tool waiter |
| `src/llm.rs` | `LlmPort` trait, chunk type, `MockLlm`, `LiterAdapter` |
| `src/follow_up.rs` | `FollowUpPolicy`, `NoopFollowUp`, `ExampleOrderFollowUp` |
| `src/orchestrator.rs` | Run loop (steer → LLM → tools → follow-up) |
| `src/http.rs` | Routes: create run (SSE), tool_results, steer, cancel |
| `config.example.toml` | Documented defaults |
| `tests/*.rs` | Protocol/integration tests with `MockLlm` |
| Delete | Old `agent_loop.rs`, `liter_backend.rs`, `stub_backend.rs`, `backend.rs`, `types.rs`, `event_stream.rs`, old `tests/phase_a.rs` |

---

### Task 1: Greenfield crate skeleton + delete old modules

**Files:**
- Delete: `src/agent_loop.rs`, `src/liter_backend.rs`, `src/stub_backend.rs`, `src/backend.rs`, `src/types.rs`, `src/event_stream.rs`, `tests/phase_a.rs`
- Replace: `src/lib.rs`, `src/main.rs`, `src/error.rs`, `src/config.rs`
- Modify: `Cargo.toml` (add axum, tower-http, uuid, bytes; keep liter-llm/tokio/serde/toml)
- Create: `config.example.toml`

**Interfaces:**
- Produces: `AppConfig { api_key, base_url: Option<String>, model, listen: String, tool_timeout_secs: u64, follow_up_policy: String }`, `AppConfig::load(path) -> Result<Self, String>`

- [ ] **Step 1: Update `Cargo.toml` dependencies**

```toml
[package]
name = "andromeda"
version = "0.1.0"
edition = "2024"

[dependencies]
liter-llm = "2.0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
tokio-util = "0.7"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
async-trait = "0.1"
toml = "0.8"
axum = { version = "0.8", features = ["macros"] }
tower-http = { version = "0.6", features = ["trace", "cors"] }
uuid = { version = "1", features = ["v4", "serde"] }
bytes = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time", "test-util"] }
http-body-util = "0.1"
tower = { version = "0.5", features = ["util"] }
```

- [ ] **Step 2: Delete obsolete source/test files listed above**

- [ ] **Step 3: Write minimal `src/config.rs`, `src/error.rs`, `src/lib.rs`, `src/main.rs`**

`main` only loads config (or prints error if missing) and logs listen address; HTTP router comes in Task 5. Until then `main` can `tokio::signal::ctrl_c().await` after printing config summary (no secrets).

- [ ] **Step 4: Write `config.example.toml`**

```toml
api_key = "sk-..."
# base_url = "https://api.openai.com/v1"
model = "gpt-4o-mini"
listen = "127.0.0.1:8080"
tool_timeout_secs = 60
follow_up_policy = "noop"   # or "example_order"
```

- [ ] **Step 5: Ensure `config.toml` stays gitignored; `cargo check` passes**

Run: `cargo check`
Expected: success

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "refactor: greenfield server skeleton; remove old agent loop"
```

---

### Task 2: Wire types + SSE framing

**Files:**
- Create: `src/wire.rs`, `src/sse.rs`
- Test: `tests/wire_sse.rs`
- Modify: `src/lib.rs` (export modules)

**Interfaces:**
- Produces:
  - `WireMessage { role: Role, content: String, tool_call_id: Option<String>, name: Option<String> }`
  - `ToolDef { name, description, parameters: serde_json::Value }`
  - `CreateRunRequest { messages: Vec<WireMessage>, tools: Vec<ToolDef>, session_id: Option<String> }`
  - `ToolResultRequest { tool_call_id, content, is_error }`
  - `SteerRequest { messages: Vec<WireMessage> }`
  - `SseEvent` enum covering all spec event types with `run_id`
  - `fn sse_frame(event: &SseEvent) -> String` → `event: ...\ndata: ...\n\n`

- [ ] **Step 1: Write failing test for SSE frame encoding**

```rust
#[test]
fn sse_frame_includes_event_and_json_data() {
    let ev = andromeda::wire::SseEvent::RunStarted {
        run_id: "r1".into(),
    };
    let frame = andromeda::sse::sse_frame(&ev);
    assert!(frame.starts_with("event: run.started\n"));
    assert!(frame.contains("\"run_id\":\"r1\""));
    assert!(frame.ends_with("\n\n"));
}
```

- [ ] **Step 2: Run test — expect FAIL (module missing)**

Run: `cargo test --test wire_sse sse_frame_includes_event_and_json_data -- --nocapture`
Expected: compile fail or link fail

- [ ] **Step 3: Implement `wire.rs` + `sse.rs` minimally to pass**

Use `#[serde(tag = "type")]` or explicit `type` field matching spec event names (`run.started`, etc.).

- [ ] **Step 4: Run test — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add src/wire.rs src/sse.rs tests/wire_sse.rs src/lib.rs
git commit -m "feat: add wire types and SSE frame encoding"
```

---

### Task 3: Run registry, steer queue, tool waiter

**Files:**
- Create: `src/run.rs`
- Test: `tests/run_registry.rs`

**Interfaces:**
- Produces:
  - `RunId(String)` / `uuid`
  - `struct RunRegistry`
  - `RunRegistry::create() -> (RunId, RunHandle)`
  - `RunHandle::enqueue_steer(msgs)`
  - `RunHandle::drain_steer() -> Vec<WireMessage>`
  - `RunHandle::wait_tool(tool_call_id, timeout) -> Result<ToolResultRequest, WaitError>`
  - `RunHandle::submit_tool_result(ToolResultRequest) -> Result<(), SubmitError>`
  - `RunHandle::cancel()`
  - `RunRegistry::get(&RunId) -> Option<RunHandle>`

- [ ] **Step 1: Write failing tests**

```rust
#[tokio::test]
async fn steer_queues_until_drained() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    h.enqueue_steer(vec![/* user msg */]).unwrap();
    let drained = h.drain_steer();
    assert_eq!(drained.len(), 1);
    assert!(h.drain_steer().is_empty());
}

#[tokio::test]
async fn tool_result_unblocks_waiter() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    let wait = tokio::spawn({
        let h = h.clone();
        async move { h.wait_tool("call_1".into(), std::time::Duration::from_secs(2)).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    h.submit_tool_result(andromeda::wire::ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    }).unwrap();
    let got = wait.await.unwrap().unwrap();
    assert_eq!(got.content, "ok");
}
```

- [ ] **Step 2: Run tests — expect FAIL**

- [ ] **Step 3: Implement `RunRegistry` with `Arc<TokioMutex<...>>`, oneshot for tool wait**

Reject `submit_tool_result` when not waiting or id mismatch (`SubmitError::Conflict`).

- [ ] **Step 4: Run tests — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add src/run.rs tests/run_registry.rs src/lib.rs
git commit -m "feat: add run registry with steer queue and tool waiter"
```

---

### Task 4: LlmPort + MockLlm

**Files:**
- Create: `src/llm.rs`
- Test: `tests/mock_llm.rs`

**Interfaces:**
- Produces:
```rust
#[async_trait]
pub trait LlmPort: Send + Sync {
    async fn stream(
        &self,
        messages: &[WireMessage],
        tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String>;
}

pub enum LlmChunk {
    TextDelta(String),
    Completed {
        content: String,
        tool_calls: Vec<ToolCall>, // id, name, arguments: Value
    },
}
```
- `MockLlm::script(Vec<MockTurn>)` where `MockTurn` is text-only or tool-calls.

- [ ] **Step 1: Write failing test — mock yields delta then completed with tool call**

- [ ] **Step 2: Implement `LlmPort` + `MockLlm`**

- [ ] **Step 3: Tests PASS**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: add LlmPort trait and MockLlm for tests"
```

---

### Task 5: Follow-up policies

**Files:**
- Create: `src/follow_up.rs`
- Test: `tests/follow_up.rs`

**Interfaces:**
```rust
pub trait FollowUpPolicy: Send + Sync {
    fn next(&self, context: &[WireMessage]) -> Vec<WireMessage>;
}
pub struct NoopFollowUp;
pub struct ExampleOrderFollowUp;
pub fn policy_from_name(name: &str) -> Arc<dyn FollowUpPolicy>;
```

- [ ] **Step 1: Test noop returns empty**

- [ ] **Step 2: Test example_order — after successful tool message name `place_order` without `send_sms` success, returns one follow-up user/system message mentioning `send_sms`**

Represent tool results in context as `WireMessage { role: Tool, name: Some("place_order"), content, tool_call_id: Some(..) }`.

- [ ] **Step 3: Implement policies**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: add FollowUpPolicy noop and example_order"
```

---

### Task 6: Orchestrator loop (core)

**Files:**
- Create: `src/orchestrator.rs`
- Test: `tests/orchestrator.rs`

**Interfaces:**
```rust
pub async fn run_agent(
    run: RunHandle,
    mut context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    sse: SseTx,  // mpsc::Sender<SseEvent> or similar
    tool_timeout: Duration,
) -> Result<(), OrchestratorError>;
```

Behavior must match spec §4 (inner: drain steer → LLM → tools; outer: follow-up).

- [ ] **Step 1: Test — MockLlm text-only → events include `run.started` (caller), deltas, `message.completed`, `run.finished`; no tool**

Caller of test sends `run.started` or orchestrator emits it — pick **orchestrator emits all events except HTTP binding**; test asserts finished reason `stop`.

- [ ] **Step 2: Test — MockLlm returns tool then text; test task submits tool_result on channel after seeing `tool.request` event**

- [ ] **Step 3: Test — steer enqueued before second LLM call is drained and appears as `message.completed` with `source=steer`**

- [ ] **Step 4: Test — ExampleOrderFollowUp triggers second LLM turn after place_order tool result**

- [ ] **Step 5: Implement orchestrator to satisfy tests**

Emit `message.completed` with `source` field: `assistant` | `steer` | `follow_up`.

- [ ] **Step 6: All orchestrator tests PASS; commit**

```bash
git commit -m "feat: implement orchestrator with steer, tools, follow-up"
```

---

### Task 7: HTTP routes + SSE response

**Files:**
- Create: `src/http.rs`
- Modify: `src/main.rs`, `src/lib.rs`
- Test: `tests/http_api.rs` (axum `oneshot` / hyper client)

**Interfaces:**
- `fn router(state: AppState) -> Router`
- `AppState { registry, llm, follow_up, tool_timeout, /* config bits */ }`
- Routes:
  - `POST /v1/runs` → SSE stream
  - `POST /v1/runs/{id}/tool_results`
  - `POST /v1/runs/{id}/steer`
  - `POST /v1/runs/{id}/cancel`

- [ ] **Step 1: Write integration test with `MockLlm` injected via `AppState` — POST /v1/runs, read body stream until `run.finished`**

Use `tower::ServiceExt::oneshot` and read streaming body chunks; parse SSE frames.

- [ ] **Step 2: Test tool_results path — spawn reader; when `tool.request` seen, POST tool_results; expect finished**

- [ ] **Step 3: Test steer returns 200 while run active; 409 after finished**

- [ ] **Step 4: Implement `http.rs` + wire `main.rs` to serve router**

For `POST /v1/runs`: create run, spawn `run_agent` task writing to `tokio::sync::mpsc`, return `axum::response::sse::Sse` stream mapping channel to `Event`.

Header `X-Run-Id` on response.

- [ ] **Step 5: Tests PASS; commit**

```bash
git commit -m "feat: expose /v1/runs SSE API with tool_results, steer, cancel"
```

---

### Task 8: Liter-llm adapter (real model)

**Files:**
- Modify: `src/llm.rs` (add `LiterAdapter`)
- Modify: `src/main.rs` / `http.rs` to use `LiterAdapter` when not under test
- Optional smoke: manual only (no CI dependency on MaaS)

**Interfaces:**
- `LiterAdapter::from_config(&AppConfig) -> Result<Self, String>`
- Implements `LlmPort` using `liter_llm` chat_stream; map chunks to `LlmChunk`.

- [ ] **Step 1: Unit-test mapping helpers with synthetic chunk structs if easy; otherwise a thin compile-only construction test**

- [ ] **Step 2: Implement adapter; `main` builds `LiterAdapter` from config**

- [ ] **Step 3: `cargo check`; commit**

```bash
git commit -m "feat: add LiterAdapter LlmPort backed by liter-llm"
```

---

### Task 9: Docs for external client + example curl notes

**Files:**
- Create: `docs/api-client.md` (sequence + event table + steer/follow-up notes)
- Modify: `README.md` (server purpose, how to run, link spec + plan + api-client)

- [ ] **Step 1: Write `docs/api-client.md` from spec §2–§4 / §7**

- [ ] **Step 2: Update README — GIS desktop client is separate; this repo is the cloud server**

- [ ] **Step 3: Commit**

```bash
git commit -m "docs: add client API contract and update README"
```

---

### Task 10: Final verification

- [ ] **Step 1: Run full suite**

Run: `cargo test`
Expected: all pass

- [ ] **Step 2: Run `cargo check --release`**

- [ ] **Step 3: Spec coverage checklist (manual)**

Confirm each exists in code/tests: create run SSE, tool_results, steer, cancel, noop follow-up, example_order follow-up, serial tools, config listen/timeout/policy.

- [ ] **Step 4: Commit any fixes**

---

## Spec coverage mapping

| Spec section | Task(s) |
|--------------|---------|
| §2.1 POST /v1/runs SSE | 7 |
| §2.2 tool_results | 3, 6, 7 |
| §2.3 steer | 3, 6, 7 |
| §2.4 cancel | 3, 7 |
| §3 wire events | 2, 6 |
| §4 orchestrator | 6 |
| §4.1 steering | 3, 6, 7 |
| §4.2 follow-up | 5, 6 |
| §5 config | 1, 8 |
| §6 rewrite / delete old | 1 |
| §7 client docs | 9 |
| liter_llm | 8 |

## Plan self-review

- No TBD placeholders in tasks.
- Types named consistently: `WireMessage`, `ToolDef`, `SseEvent`, `LlmPort`, `RunHandle`, `FollowUpPolicy`.
- Old dual-loop files removed in Task 1; behavior reimplemented in Task 6.
