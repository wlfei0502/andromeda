# Agent Middleware Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a `before_llm` middleware chain under `src/agent/middleware/`, adapt existing summarization as `SummarizeMiddleware`, and remove the orchestrator’s inline `apply_summarize_if_needed` — with no behavior change vs LH-M2.

**Architecture:** `AgentMiddleware` + `MwCtx` + `MwEffect`; `run_before_llm` merges effects; orchestrator emits SSE / checkpoints. Core `maybe_summarize` stays in `agent/summarize.rs`. `run_agent*` takes `middlewares` instead of bare `ContextConfig`.

**Tech Stack:** Existing crate (`async-trait`, tokio, serde). No new dependencies.

**Spec:** [docs/superpowers/specs/2026-09-19-agent-middleware-design.md](../specs/2026-09-19-agent-middleware-design.md)

## Global Constraints

- Behavior unchanged: same summarize thresholds, SSE `context.summarized`, `context_overflow`.
- Middleware must not hold `RunHandle`; side effects only via `MwEffect`.
- First version: only `before_llm` / `MwAction::Continue`; errors via `OrchestratorError`.
- Do not add noop Memory middleware files.
- Keep `agent/summarize.rs` unit tests working (algorithm file stays).

---

## File structure

| Path | Responsibility |
|------|----------------|
| `src/agent/middleware/mod.rs` | Trait, `MwCtx`, `MwEffect`, `run_before_llm` |
| `src/agent/middleware/summarize.rs` | `SummarizeMiddleware` |
| `src/agent/summarize.rs` | Unchanged algorithms (maybe_summarize etc.) |
| `src/agent/orchestrator.rs` | Call chain; drop `apply_summarize_if_needed` / `context_cfg` |
| `src/agent/mod.rs` | `mod middleware`; re-exports |
| `src/api/http.rs`, `src/main.rs` | `AppState.middlewares` |
| `tests/*` | Pass middleware chain |

Helper for tests (put in `middleware/mod.rs` or tests):

```rust
pub fn default_summarize_chain(cfg: ContextConfig) -> Arc<[Arc<dyn AgentMiddleware>]> {
    Arc::from(vec![Arc::new(SummarizeMiddleware { config: cfg }) as Arc<dyn AgentMiddleware>])
}
```

---

### Task 1: Middleware trait + SummarizeMiddleware + chain runner

**Files:**
- Create: `src/agent/middleware/mod.rs`
- Create: `src/agent/middleware/summarize.rs`
- Modify: `src/agent/mod.rs`

**Interfaces:**
- Produces: `AgentMiddleware`, `MwCtx`, `MwEffect`, `MwAction`, `run_before_llm`, `SummarizeMiddleware`, `default_summarize_chain`
- Consumes: `maybe_summarize`, `SummarizeOutcome`, `SummarizeError`, `OrchestratorError`, `SseEvent`

- [ ] **Step 1: Add unit test for chain merge** (in `middleware/mod.rs`)

```rust
struct RecordingMw;
#[async_trait]
impl AgentMiddleware for RecordingMw {
    fn name(&self) -> &'static str { "rec" }
    async fn before_llm(&self, _ctx: &mut MwCtx<'_>) -> Result<MwAction, OrchestratorError> {
        Ok(MwAction::Continue(MwEffect {
            events: vec![],
            checkpoint: true,
        }))
    }
}
// Build empty context MwCtx with MockLlm; run_before_llm([RecordingMw]); assert checkpoint true
```

- [ ] **Step 2: Implement trait + `run_before_llm` + `SummarizeMiddleware`**

`SummarizeMiddleware` body per spec §3.

- [ ] **Step 3: `cargo test -p andromeda --lib agent::middleware agent::summarize`**

Expected: PASS

- [ ] **Step 4: Commit** (if authorized)

```bash
git commit -m "feat: add before_llm middleware chain and SummarizeMiddleware"
```

---

### Task 2: Wire orchestrator + AppState + all call sites

**Files:**
- Modify: `src/agent/orchestrator.rs`
- Modify: `src/api/http.rs`
- Modify: `src/main.rs`
- Modify: `tests/orchestrator.rs`, `tests/http_api.rs`, `tests/checkpoint_*.rs`, `tests/context_summarize.rs`

**Interfaces:**
- `run_agent(..., middlewares: Arc<[Arc<dyn AgentMiddleware>]>, ...)` — remove `context_cfg`
- Same for `run_agent_with_options`, `continue_after_pending_tool`, `run_agent_loop`
- `AppState { middlewares, ... }` — remove `context: ContextConfig` (config only used when building chain in main)

- [ ] **Step 1: Replace `apply_summarize_if_needed` with**

```rust
let pending = ps.pending_tool.clone();
let mut mw_ctx = MwCtx {
    run_id,
    context,
    tools,
    pending_tool: pending.as_ref(),
    llm: llm.as_ref(),
};
let effect = run_before_llm(&middlewares, &mut mw_ctx).await?;
for ev in effect.events {
    emit(&run, ev).await;
}
if effect.checkpoint {
    ps.checkpoint(run, context, tools, RunStatus::Running, ps.pending_tool.clone()).await?;
}
```

On `Err`, same emit_error + finalize path as today.

- [ ] **Step 2: Update HTTP/main/tests** to `default_summarize_chain(cfg)` or custom cfg for context_summarize tests.

- [ ] **Step 3: `cargo test -- --nocapture`**

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor: run summarization via middleware chain before LLM"
```

---

### Task 3: Docs touch + verification

**Files:**
- Modify: `docs/api-client.md` (one short sentence under Context window)
- Optionally mark middleware design status 已批准

- [ ] **Step 1: Doc line** — server may run a `before_llm` middleware chain; currently includes summarization.
- [ ] **Step 2: Full `cargo test`**
- [ ] **Step 3: Commit** `docs: note before_llm middleware chain in client API`

---

## Self-review

| Spec item | Task |
|-----------|------|
| trait + MwEffect + run_before_llm | 1 |
| SummarizeMiddleware adapter | 1 |
| orchestrator uses chain; no apply_summarize_if_needed | 2 |
| AppState / main / tests wired | 2 |
| No behavior change / tests green | 2–3 |
| No noop memory file | Global |
