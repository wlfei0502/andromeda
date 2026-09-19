# Context Summarization (LH-M2) Implementation Plan

**Status: completed** (LH-M2). Historical checkboxes may still show `- [ ]`.


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Before each main LLM call, if estimated context size exceeds configured thresholds, compress the middle of the message list via the same `LlmPort`, emit `context.summarized`, checkpoint when persist is on, and fail the run with `context_overflow` when still over the hard cap.

**Architecture:** Add `ContextConfig` under TOML `[context]`. Put pure split/estimate helpers and async `maybe_summarize` in `src/agent/summarize.rs`. Orchestrator calls it immediately before `stream_llm`. Wire a new SSE variant; map `OrchestratorError::ContextOverflow` to `error` with `code=context_overflow`. Independent of `[persist].enabled`.

**Tech Stack:** Existing Rust crate (tokio, axum, serde, futures, thiserror, MockLlm). No new tokenizer crates.

**Spec:** [docs/superpowers/specs/2026-09-19-context-summarization-design.md](../specs/2026-09-19-context-summarization-design.md)

## Global Constraints

- Token estimate: `ceil(sum(utf8 bytes of content + name? + tool_calls JSON?) / 4)` — no tiktoken.
- Same `LlmPort` / main `model` for summarization; no `summarizer_model`.
- No `summarize_enabled` flag — raise `summarize_threshold_tokens` to effectively disable.
- Summary message: `role=system`, content starts with `[conversation summary]`.
- Prefix system messages exclude those whose content starts with `[conversation summary]`.
- Summarizer LLM calls do **not** increment `guards.llm_rounds` / follow-up counters.
- Do not stream summarizer deltas to the client.
- M2 does not retry summarization in a loop after success-still-over-max.

## Out of scope

- Real tokenizer, separate summarizer model, Plan/todos, guards, subagents, long-term memory.

---

## File structure

| Path | Responsibility |
|------|----------------|
| `src/config/mod.rs` | `ContextConfig` + defaults; `AppConfig.context` |
| `config.example.toml` | Document `[context]` |
| `src/protocol/mod.rs` | `SseEvent::ContextSummarized` + `event_name` |
| `src/agent/summarize.rs` | `estimate_tokens`, `split_context`, `maybe_summarize` |
| `src/agent/mod.rs` | `mod summarize`; re-exports as needed |
| `src/agent/orchestrator.rs` | Call summarize before `stream_llm`; `ContextOverflow` |
| `src/llm/mod.rs` | Optional `MockTurn::Fail` for summarizer-failure tests |
| `src/api/http.rs`, `src/main.rs` | Pass `ContextConfig` into orchestrator |
| `tests/context_summarize.rs` | Integration: SSE + overflow |
| `docs/api-client.md` | Event + error code + config |

---

### Task 1: Config + `context.summarized` wire

**Files:**
- Modify: `src/config/mod.rs`
- Modify: `src/protocol/mod.rs`
- Modify: `config.example.toml`
- Modify: `src/llm/mod.rs` (AppConfig literal in tests — add `context: Default::default()`)

**Interfaces:**
- Produces: `ContextConfig { summarize_threshold_tokens: u64, keep_last_messages: usize, max_context_tokens: u64 }` with defaults `80000`, `24`, `120000`
- Produces: `SseEvent::ContextSummarized { run_id, before_tokens, after_tokens, kept_prefix, kept_suffix }` (all numeric fields `u64` except `kept_prefix`/`kept_suffix` as `usize` serialized as numbers)

- [ ] **Step 1: Write failing config test**

In `src/config/mod.rs` tests:

```rust
#[test]
fn context_defaults_when_section_omitted() {
    let cfg: AppConfig = toml::from_str(r#"api_key = "sk""#).unwrap();
    assert_eq!(cfg.context.summarize_threshold_tokens, 80_000);
    assert_eq!(cfg.context.keep_last_messages, 24);
    assert_eq!(cfg.context.max_context_tokens, 120_000);
}

#[test]
fn context_section_overrides() {
    let cfg: AppConfig = toml::from_str(
        r#"
        api_key = "sk"
        [context]
        summarize_threshold_tokens = 100
        keep_last_messages = 4
        max_context_tokens = 200
        "#,
    )
    .unwrap();
    assert_eq!(cfg.context.summarize_threshold_tokens, 100);
    assert_eq!(cfg.context.keep_last_messages, 4);
    assert_eq!(cfg.context.max_context_tokens, 200);
}
```

- [ ] **Step 2: Run test — expect fail** (field missing)

Run: `cargo test -p andromeda --lib context_defaults -- --nocapture`  
Expected: compile/link error or missing field `context`

- [ ] **Step 3: Implement `ContextConfig`**

```rust
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ContextConfig {
    #[serde(default = "default_summarize_threshold_tokens")]
    pub summarize_threshold_tokens: u64,
    #[serde(default = "default_keep_last_messages")]
    pub keep_last_messages: usize,
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: u64,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            summarize_threshold_tokens: default_summarize_threshold_tokens(),
            keep_last_messages: default_keep_last_messages(),
            max_context_tokens: default_max_context_tokens(),
        }
    }
}

fn default_summarize_threshold_tokens() -> u64 { 80_000 }
fn default_keep_last_messages() -> usize { 24 }
fn default_max_context_tokens() -> u64 { 120_000 }
```

Add to `AppConfig`:

```rust
#[serde(default)]
pub context: ContextConfig,
```

Fix any `AppConfig { ... }` struct literals (e.g. in `src/llm/mod.rs` tests) with `context: Default::default()`.

- [ ] **Step 4: Add SSE variant**

In `SseEvent`:

```rust
#[serde(rename = "context.summarized")]
ContextSummarized {
    run_id: String,
    before_tokens: u64,
    after_tokens: u64,
    kept_prefix: usize,
    kept_suffix: usize,
},
```

Update `event_name()` → `"context.summarized"`.

Add a serde roundtrip unit test in `protocol` tests (same style as `run.resumed`).

- [ ] **Step 5: Update `config.example.toml`**

```toml
[context]
summarize_threshold_tokens = 80000
keep_last_messages = 24
max_context_tokens = 120000
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p andromeda --lib config:: protocol:: -- --nocapture`  
Expected: PASS

- [ ] **Step 7: Commit** (only if user asked to commit)

```bash
git add src/config/mod.rs src/protocol/mod.rs config.example.toml src/llm/mod.rs
git commit -m "$(cat <<'EOF'
feat(m2): add ContextConfig and context.summarized SSE event

EOF
)"
```

---

### Task 2: Estimate + split (pure, no LLM)

**Files:**
- Create: `src/agent/summarize.rs`
- Modify: `src/agent/mod.rs` (`mod summarize;`)

**Interfaces:**
- Consumes: `WireMessage`, `Role`, `PendingTool` (from `crate::store`)
- Produces:
  - `pub const SUMMARY_PREFIX: &str = "[conversation summary]";`
  - `pub fn estimate_tokens(messages: &[WireMessage]) -> u64`
  - `pub struct ContextSplit { pub prefix: Vec<WireMessage>, pub middle: Vec<WireMessage>, pub suffix: Vec<WireMessage> }`
  - `pub fn split_context(messages: &[WireMessage], keep_last: usize, pending: Option<&PendingTool>) -> ContextSplit`

- [ ] **Step 1: Write failing unit tests in `summarize.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Role, ToolCallWire, WireMessage};
    use serde_json::json;

    fn msg(role: Role, content: &str) -> WireMessage {
        WireMessage {
            role,
            content: content.into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    #[test]
    fn estimate_tokens_chars_div_4() {
        let m = msg(Role::User, "abcd"); // 4 bytes → 1 token
        assert_eq!(estimate_tokens(&[m]), 1);
    }

    #[test]
    fn split_keeps_leading_real_system_and_tail() {
        let messages = vec![
            msg(Role::System, "you are helpful"),
            msg(Role::User, "u1"),
            msg(Role::Assistant, "a1"),
            msg(Role::User, "u2"),
            msg(Role::Assistant, "a2"),
        ];
        let split = split_context(&messages, 2, None);
        assert_eq!(split.prefix.len(), 1);
        assert_eq!(split.suffix.len(), 2);
        assert_eq!(split.middle.len(), 2);
        assert_eq!(split.middle[0].content, "u1");
    }

    #[test]
    fn split_excludes_summary_system_from_prefix() {
        let messages = vec![
            msg(Role::System, "you are helpful"),
            msg(Role::System, &format!("{SUMMARY_PREFIX}\nold")),
            msg(Role::User, "u1"),
            msg(Role::Assistant, "a1"),
        ];
        let split = split_context(&messages, 2, None);
        assert_eq!(split.prefix.len(), 1);
        assert!(split.middle[0].content.starts_with(SUMMARY_PREFIX));
    }

    #[test]
    fn split_extends_suffix_for_open_tool_chain() {
        let messages = vec![
            msg(Role::User, "start"),
            WireMessage {
                role: Role::Assistant,
                content: "".into(),
                tool_call_id: None,
                name: None,
                tool_calls: Some(vec![ToolCallWire {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }]),
            },
            // no tool result yet — keep_last=1 would otherwise cut the assistant
            msg(Role::User, "later"),
        ];
        // Force a small keep_last so extension matters: use messages without the trailing user
        let open = vec![
            msg(Role::User, "start"),
            msg(Role::User, "pad"),
            WireMessage {
                role: Role::Assistant,
                content: "".into(),
                tool_call_id: None,
                name: None,
                tool_calls: Some(vec![ToolCallWire {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }]),
            },
        ];
        let split = split_context(&open, 1, None);
        assert!(
            split.suffix.iter().any(|m| m.tool_calls.is_some()),
            "open tool assistant must stay in suffix"
        );
        assert!(split.suffix.len() >= 2);
    }
}
```

- [ ] **Step 2: Run tests — expect fail**

Run: `cargo test -p andromeda --lib agent::summarize -- --nocapture`  
Expected: module not found / unresolved

- [ ] **Step 3: Implement estimate + split**

`estimate_tokens`:

```rust
pub fn estimate_tokens(messages: &[WireMessage]) -> u64 {
    let mut bytes = 0usize;
    for m in messages {
        bytes += m.content.len();
        if let Some(name) = &m.name {
            bytes += name.len();
        }
        if let Some(tcs) = &m.tool_calls {
            bytes += serde_json::to_string(tcs).map(|s| s.len()).unwrap_or(0);
        }
    }
    bytes.div_ceil(4) as u64
}
```

`split_context` algorithm:

1. Let `prefix_end = 0`; while `prefix_end < len` and message is `System` and `!content.starts_with(SUMMARY_PREFIX)`, increment.
2. Let `suffix_start = len.saturating_sub(keep_last.max(1))` but if `len <= prefix_end` return empty middle; clamp `suffix_start = suffix_start.max(prefix_end)`.
3. While `suffix_start > prefix_end` and cutting would split an open tool chain (an assistant with `tool_calls` in suffix whose ids lack matching `Tool` messages still in suffix), decrement `suffix_start`.
4. If `pending` is Some, ensure any message with `tool_call_id == pending.tool_call_id` or assistant `tool_calls` containing that id is not in middle — expand suffix (lower `suffix_start`) to include them.
5. `prefix = [0..prefix_end)`, `middle = [prefix_end..suffix_start)`, `suffix = [suffix_start..)`.

Open tool chain helper: for each assistant in the candidate suffix with `tool_calls`, every `id` must have a later `role=Tool` with that `tool_call_id` still inside the suffix; else include that assistant (and anything after it already in suffix) by moving `suffix_start` to the assistant index.

- [ ] **Step 4: Run tests — expect pass**

Run: `cargo test -p andromeda --lib agent::summarize -- --nocapture`  
Expected: PASS

- [ ] **Step 5: Commit** (if user asked)

```bash
git add src/agent/summarize.rs src/agent/mod.rs
git commit -m "$(cat <<'EOF'
feat(m2): add context token estimate and split helpers

EOF
)"
```

---

### Task 3: `maybe_summarize` + MockLlm fail turn

**Files:**
- Modify: `src/agent/summarize.rs`
- Modify: `src/llm/mod.rs` (`MockTurn::Fail`)

**Interfaces:**
- Consumes: `ContextConfig`, `Arc<dyn LlmPort>`, `split_context`, `estimate_tokens`
- Produces:
  - `pub enum SummarizeOutcome { Unchanged, Summarized { context: Vec<WireMessage>, before_tokens: u64, after_tokens: u64, kept_prefix: usize, kept_suffix: usize } }`
  - `pub enum SummarizeError { ContextOverflow { before_tokens: u64, after_tokens: u64 } }`
  - `pub async fn maybe_summarize(context: Vec<WireMessage>, cfg: &ContextConfig, llm: &dyn LlmPort, pending: Option<&PendingTool>) -> Result<SummarizeOutcome, SummarizeError>`

Behavior (exact):

1. `before = estimate_tokens(&context)`.
2. If `before < cfg.summarize_threshold_tokens`: if `before >= cfg.max_context_tokens` → `Err(ContextOverflow)`; else `Ok(Unchanged)`.
3. `split = split_context(...)`. If `middle.is_empty()`: same overflow check on original; else `Unchanged` if under max.
4. Build summarizer prompt messages (system instruction + one user message with middle transcript). Call `llm.stream(&prompt, &[])`, drain all chunks, take `Completed.content`. On stream/`Completed` failure or empty trim → log via `tracing::warn!`, keep **original** context, then if `estimate_tokens(original) >= max` → `Err(ContextOverflow)` else `Ok(Unchanged)`.
5. On success: `new_context = prefix + [system summary msg] + suffix`. `after = estimate_tokens(&new_context)`. If `after >= max` → `Err(ContextOverflow { before, after })`. Else `Ok(Summarized { ... })`.

Summary system content:

```text
[conversation summary]
{llm_text}
```

Fixed summarizer system prompt (constant in file), English or Chinese — pick one and keep stable; require sections: Goal / Done / Facts / Open.

- [ ] **Step 1: Extend `MockTurn` with failure**

```rust
pub enum MockTurn {
    // existing...
    Fail { message: String },
}
```

In `turn_to_chunks` / `stream`: for `Fail`, return `Err(message)` from `stream` (do not push a stream). Still record the incoming messages before failing.

- [ ] **Step 2: Write failing `maybe_summarize` tests**

```rust
#[tokio::test]
async fn below_threshold_unchanged() {
    let cfg = ContextConfig {
        summarize_threshold_tokens: 10_000,
        keep_last_messages: 2,
        max_context_tokens: 20_000,
    };
    let ctx = vec![msg(Role::User, "hi")];
    let llm = MockLlm::script(vec![]);
    let out = maybe_summarize(ctx.clone(), &cfg, &llm, None).await.unwrap();
    assert!(matches!(out, SummarizeOutcome::Unchanged));
    assert!(llm.recorded_contexts().is_empty());
}

#[tokio::test]
async fn over_threshold_replaces_middle() {
    let cfg = ContextConfig {
        summarize_threshold_tokens: 1,
        keep_last_messages: 2,
        max_context_tokens: 100_000,
    };
    // Build enough messages that middle is non-empty with keep_last=2
    let mut ctx = vec![msg(Role::System, "persona")];
    for i in 0..6 {
        ctx.push(msg(Role::User, &format!("user-{i}-{}", "x".repeat(20))));
        ctx.push(msg(Role::Assistant, &format!("asst-{i}")));
    }
    let llm = MockLlm::script(vec![MockTurn::TextOnly {
        content: "Goal: test\nDone: steps\nFacts: f\nOpen: none".into(),
        deltas: vec![],
    }]);
    let out = maybe_summarize(ctx, &cfg, &llm, None).await.unwrap();
    let SummarizeOutcome::Summarized { context, .. } = out else {
        panic!("expected summarized");
    };
    assert!(context.iter().any(|m| m.content.starts_with(SUMMARY_PREFIX)));
    assert_eq!(llm.recorded_contexts().len(), 1);
}

#[tokio::test]
async fn summarize_fail_over_max_overflows() {
    let cfg = ContextConfig {
        summarize_threshold_tokens: 1,
        keep_last_messages: 2,
        max_context_tokens: 1, // tiny hard cap
    };
    let mut ctx = vec![msg(Role::System, "persona")];
    for i in 0..6 {
        ctx.push(msg(Role::User, &format!("user-{i}-{}", "x".repeat(40))));
    }
    let llm = MockLlm::script(vec![MockTurn::Fail {
        message: "boom".into(),
    }]);
    let err = maybe_summarize(ctx, &cfg, &llm, None).await.unwrap_err();
    assert!(matches!(err, SummarizeError::ContextOverflow { .. }));
}
```

- [ ] **Step 3: Run — expect fail**

Run: `cargo test -p andromeda --lib agent::summarize -- --nocapture`  
Expected: `maybe_summarize` missing / `Fail` missing

- [ ] **Step 4: Implement `maybe_summarize` + prompt helpers**

Helper to drain stream:

```rust
async fn complete_text(llm: &dyn LlmPort, messages: &[WireMessage]) -> Result<String, String> {
    let mut stream = llm.stream(messages, &[]).await?;
    let mut content = None;
    while let Some(chunk) = stream.next().await {
        match chunk? {
            LlmChunk::TextDelta(_) => {}
            LlmChunk::Completed { content: c, .. } => content = Some(c),
        }
    }
    content.filter(|s| !s.trim().is_empty()).ok_or_else(|| "empty summary".into())
}
```

- [ ] **Step 5: Run — expect pass**

Run: `cargo test -p andromeda --lib agent::summarize llm:: -- --nocapture`  
Expected: PASS

- [ ] **Step 6: Commit** (if user asked)

```bash
git add src/agent/summarize.rs src/llm/mod.rs
git commit -m "$(cat <<'EOF'
feat(m2): implement maybe_summarize with overflow handling

EOF
)"
```

---

### Task 4: Orchestrator + HTTP/main wiring

**Files:**
- Modify: `src/agent/orchestrator.rs`
- Modify: `src/api/http.rs`
- Modify: `src/main.rs`
- Modify: `tests/http_api.rs`, `tests/checkpoint_resume.rs` (AppState field)

**Interfaces:**
- Consumes: `maybe_summarize`, `SummarizeOutcome`, `SummarizeError`, `ContextConfig`
- Produces: `OrchestratorError::ContextOverflow`; `run_agent*` takes `context_cfg: ContextConfig`
- `AppState.context: ContextConfig`

- [ ] **Step 1: Extend `OrchestratorError` + `error_code`**

```rust
#[error("context exceeded max_context_tokens")]
ContextOverflow,
```

```rust
OrchestratorError::ContextOverflow => Some("context_overflow"),
```

`terminal_for` → `RunStatus::Failed`.

- [ ] **Step 2: Thread `ContextConfig` through entrypoints**

Add parameter `context_cfg: ContextConfig` to:

- `run_agent`
- `run_agent_with_options`
- `continue_after_pending_tool`
- `run_agent_loop`

Pass into a new helper called at the start of each main-model iteration **after** `drain_steer` / cancel check and **before** `stream_llm`:

```rust
async fn apply_summarize_if_needed(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    llm: &Arc<dyn LlmPort>,
    cfg: &ContextConfig,
    ps: &mut PersistSession,
    run_id: &str,
) -> Result<(), OrchestratorError> {
    let pending = ps.pending_tool.clone();
    match maybe_summarize(
        context.clone(),
        cfg,
        llm.as_ref(),
        pending.as_ref(),
    )
    .await
    {
        Ok(SummarizeOutcome::Unchanged) => Ok(()),
        Ok(SummarizeOutcome::Summarized {
            context: new_ctx,
            before_tokens,
            after_tokens,
            kept_prefix,
            kept_suffix,
        }) => {
            *context = new_ctx;
            emit(
                run,
                SseEvent::ContextSummarized {
                    run_id: run_id.to_string(),
                    before_tokens,
                    after_tokens,
                    kept_prefix,
                    kept_suffix,
                },
            )
            .await;
            ps.checkpoint(run, context, tools, RunStatus::Running, ps.pending_tool.clone())
                .await?;
            Ok(())
        }
        Err(SummarizeError::ContextOverflow { .. }) => Err(OrchestratorError::ContextOverflow),
    }
}
```

On `ContextOverflow`, existing `stream_llm` error path already emits `error` + finalize — call `apply_summarize_if_needed` with the same error handling as `stream_llm`.

- [ ] **Step 3: Wire `AppState` + `main`**

```rust
pub struct AppState {
    // ...
    pub context: ContextConfig,
}
```

`main.rs`: `context: config.context.clone()`.

Update all `run_agent` / `run_agent_with_options` / `continue_after_pending_tool` call sites in `http.rs` to pass `state.context.clone()`.

Update test `AppState { ... }` constructors with `context: ContextConfig::default()` or test-specific low thresholds where needed.

- [ ] **Step 4: Compile + existing tests**

Run: `cargo test -- --nocapture`  
Expected: all existing tests PASS (update signatures as needed).

- [ ] **Step 5: Commit** (if user asked)

```bash
git add src/agent/orchestrator.rs src/api/http.rs src/main.rs tests/
git commit -m "$(cat <<'EOF'
feat(m2): run summarization before each main LLM call

EOF
)"
```

---

### Task 5: Integration test + client docs

**Files:**
- Create: `tests/context_summarize.rs`
- Modify: `docs/api-client.md`

**Interfaces:**
- Consumes: HTTP router + `MockLlm` script (summarizer turn then assistant turn)
- Produces: green integration coverage for SSE `context.summarized` and overflow

- [ ] **Step 1: Write integration test — summarize emits SSE**

Pattern like `tests/http_api.rs`: build `AppState` with:

```rust
context: ContextConfig {
    summarize_threshold_tokens: 1,
    keep_last_messages: 2,
    max_context_tokens: 100_000,
},
```

`MockLlm::script` with **two** turns: (1) summary text, (2) final assistant `"done"`.

POST `/v1/runs` with a long `messages` list (system + many user/assistant pairs). Collect SSE until `run.finished`. Assert:

- some event `type == context.summarized`
- a later `message.completed` with content `"done"`
- `llm.recorded_contexts().len() >= 2`
- second recorded context contains a system message starting with `[conversation summary]`

- [ ] **Step 2: Write integration test — overflow**

`max_context_tokens: 1`, summarizer `MockTurn::Fail`, long messages. Assert SSE `error` with `code == context_overflow`.

- [ ] **Step 3: Run**

Run: `cargo test --test context_summarize -- --nocapture`  
Expected: PASS

- [ ] **Step 4: Update `docs/api-client.md`**

- Link the M2 design under the intro list.
- Event table: add `context.summarized` with fields `before_tokens`, `after_tokens`, `kept_prefix`, `kept_suffix`.
- Note `error.code=context_overflow`.
- Short “Context window” section: server may compress history before LLM calls; configure `[context]` in server `config.toml`; clients may ignore the event; must handle overflow error.

- [ ] **Step 5: Full verification**

Run: `cargo test -- --nocapture`  
Expected: all PASS

- [ ] **Step 6: Commit** (if user asked)

```bash
git add tests/context_summarize.rs docs/api-client.md
git commit -m "$(cat <<'EOF'
test(m2): cover context summarize SSE and document client contract

EOF
)"
```

---

## Self-review

| Spec requirement | Task |
|------------------|------|
| `[context]` defaults + example | Task 1 |
| `chars/4` estimate | Task 2 |
| Prefix / middle / suffix + tool-chain + summary-prefix rule | Task 2 |
| `maybe_summarize` + same LlmPort + no client deltas | Task 3 |
| Failure → keep original; hard cap → overflow | Task 3 |
| Hook before main LLM; SSE; checkpoint | Task 4 |
| Independent of persist enable | Task 4/5 (tests can use `persist_enabled: false`) |
| Integration + `api-client.md` | Task 5 |
| No summarizer_model / no enable flag / no second-pass | Global constraints |

No TBD placeholders. Types consistent: `ContextConfig`, `SummarizeOutcome`, `SummarizeError`, `OrchestratorError::ContextOverflow`, `SseEvent::ContextSummarized`.
