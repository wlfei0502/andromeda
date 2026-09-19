# LH-M1 Implementation Plan — Checkpoint + Resume SSE

> **For agentic workers:** Implement task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Persist each run’s cold state to a pluggable `RunStore`, allow `GET /v1/runs/{id}/events` to resume SSE (last subscriber wins), and survive process restart when waiting on a client tool—including multi-instance basics (`owner_id` / `revision`, `409 not_owner`).

**Architecture:** Keep in-memory `RunRegistry` as the **owner hot cache**. Add `RunStore` (default `LocalFsRunStore`) as the **shared cold source of truth**. Replace the one-shot SSE `mpsc` with a **swappable subscriber hub** on the run so disconnect does not kill the orchestrator. Persist checkpoints at the transitions defined in the spec; on resume, emit `run.resumed` (+ pending `tool.request` if needed).

**Tech Stack:** Existing crate (axum, tokio, serde, uuid, thiserror). New: filesystem IO for JSON checkpoints (no new DB crate in M1).

**Spec:** [docs/superpowers/specs/2026-09-18-long-horizon-design.md](../specs/2026-09-18-long-horizon-design.md) — especially §2 (P0a), §2.4 (multi-instance), §7 (`run.resumed`), §12 LH-M1 / M1.1.

## Out of scope (M1)

- Summarizer, Plan/`write_todos`, guards beyond what’s needed to store counters, subagents
- Postgres/`SqliteRunStore` (trait only; LocalFs default)
- Lease heartbeat (M1.1); M1 uses `revision` + `owner_id` only
- Fan-out multi-subscriber SSE
- Hot-migrating an in-flight LLM HTTP stream to another instance

## Global constraints

- Transport remains SSE + POST; resume is **`GET /v1/runs/{id}/events`**.
- Checkpoint path: `{data_dir}/runs/{run_id}/checkpoint.json` (+ `meta.json`).
- `options.persist` default `true` when LH enabled; `false` ≈ today’s memory-only behavior for tests.
- SSE disconnect **must not** finish/cancel the run (change from v1 “channel closed → orchestrator error”).
- `tool_results` / `steer` on an instance with no hot run: if checkpoint exists but this process is not owner → **`409`** with `code=not_owner`; if unknown → `404`.

---

## File structure

| Path | Responsibility |
|------|----------------|
| `src/protocol/` | Wire messages, tools, SSE event enums (was `wire`) |
| `src/runtime/` | `RunId`, `RunHandle`, registry, steer queue, tool waiter; SSE hub (M1) |
| `src/llm/` | `LlmPort`, `MockLlm`, `LiterAdapter` |
| `src/agent/` | `run_agent`, follow-up policies; persist hooks (M1) |
| `src/api/` | HTTP routes + SSE framing; resume `GET .../events` (M1) |
| `src/store/` (new) | `RunStore` trait, `Checkpoint`, `LocalFsRunStore` |
| `src/config/` | `AppConfig` + `[persist]`（原 `[long_horizon]`） |
| `config.example.toml` | Documented defaults |
| `tests/checkpoint_resume.rs` (new) | Persist → drop SSE → resume → tool_results |
| `docs/api-client.md` | Resume + `run.resumed` + `not_owner` |

---

### Task 1: Config + wire deltas

**Files:**
- Modify: `src/config/mod.rs`, `src/protocol/mod.rs`
- Create/restore: `config.example.toml`

- [x] **Step 1: Extend config**
- [x] **Step 2: Wire — create options + `run.resumed`**
- [x] **Step 3: `cargo check` / `cargo test`**
- [ ] **Step 4: Commit** — `feat(lh-m1): config and wire for persist/resume`（待你要求再提交）

---

### Task 2: RunStore + LocalFsRunStore

**Files:**
- Create: src/store/mod.rs
- Modify: src/lib.rs, Cargo.toml (dev-dep 	empfile), .gitignore (/data/)

- [x] **Step 1: Define types**
- [x] **Step 2: Implement LocalFsRunStore**
- [x] **Step 3: Tests** — save/load roundtrip; CAS conflict when revision skew
- [ ] **Step 4: Commit** — eat(lh-m1): LocalFs RunStore with revision CAS（待你要求再提交）

---

### Task 3: SSE hub on \RunHandle\ (disconnect ≠ death)

**Files:**
- Modify: \src/runtime/mod.rs\, \src/agent/orchestrator.rs\, \src/api/http.rs\, \src/api/sse.rs
- [x] **Step 1: Add SSE hub** (\subscribe\ / \emit_event\; last subscriber wins)
- [x] **Step 2: un_agent\ emits via \RunHandle\ (no external \SseTx\)
- [x] **Step 3: Tests** — hub unit tests + drop SSE while waiting tool then resubscribe
- [ ] **Step 4: Commit** — \eat(lh-m1): swappable SSE hub; disconnect does not finish run\（待你要求再提交）

---

### Task 4: Persist hooks in orchestrator

**Files:**
- Modify: src/agent/orchestrator.rs, src/api/http.rs, src/main.rs
- Create: 	ests/checkpoint_persist.rs

- [x] **Step 1: Persistence helper** (RunPersist + CAS via PersistSession)
- [x] **Step 2: Call persist** at LLM/tool/steer/terminal transitions
- [x] **Step 3: cargo test** green (incl. waiting_tool checkpoint)
- [ ] **Step 4: Commit** — eat(lh-m1): checkpoint persist in orchestrator（待你要求再提交）

---

### Task 5: GET /v1/runs/{id}/events + create path wiring

**Files:** src/api/http.rs, src/agent/orchestrator.rs, src/runtime/mod.rs

- [x] **Step 1–2:** AppState + create_run persist (from Task 4)
- [x] **Step 3:** GET .../events hot/cold resume + 
un.resumed / re-emit 	ool.request
- [x] **Step 4:** 	ool_results / steer → 409 code=not_owner
- [ ] **Step 5: Commit**（待你要求再提交）

---

### Task 6: Integration tests + docs

- [x] Soft disconnect resume
- [x] Cold disk resume (waiting_tool)
- [x] 
ot_owner conflict
- [x] docs/api-client.md updated
- [x] cargo test full suite green
- [ ] Commit（待你要求再提交）

---

## Milestone exit criteria

| Check | Done when |
|-------|-----------|
| Persist | `waiting_tool` visible on disk under `data_dir` |
| Soft resume | Drop SSE → GET events → same run continues |
| Hard resume | New process + same store → waiting tool completes |
| Multi-instance baseline | Stale owner write fails CAS; wrong instance tool_results → `not_owner` |
| Regression | Prior orchestrator / http tests pass |

## Suggested order

Task 1 → 2 → 3 → 4 → 5 → 6. Do not start LH-M2 (summarizer) until M1 exit criteria pass.

## Deploy note (for implementers / ops)

Production multi-instance: point all replicas at the **same** `data_dir` (shared volume) or a future DB `RunStore`; prefer LB sticky by `run_id` or accept IP hash + resume takeover. M1.1 can add lease heartbeat.
