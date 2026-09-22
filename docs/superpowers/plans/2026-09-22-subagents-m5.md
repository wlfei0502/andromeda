# LH-M5 Implementation Plan — Subagents (`task`)

**Status: completed** (LH-M5 Subagents). Historical checkboxes may still show incomplete.

**Goal:** When `options.subagents=true`, inject server tool `task`, nest a same-process `run_agent` with isolated context (channel A: child client tools ride the parent SSE / `tool_results`), emit `task.*` events, enforce concurrency + timeout, and never nest `task` inside a child.

**Architecture:** Extend server-tool classification (`write_todos` | `task`). `task` parses `goal` / `agent` / `context_hints`, emits `task.started`, runs a folded sub-loop on the **parent** `RunHandle` (no second SSE), returns summary as the parent tool result, then `task.completed` / `failed` / `timed_out`. Runtime supports **multiple** tool waiters keyed by `tool_call_id` so parallel subagents can await client tools. Sub-loop: `plan_mode=false`, `subagents=false`, no `run.started`/`run.finished`, suppress `message.delta` / `reasoning.delta` / `message.completed` (still emit `tool.request` with `agent_id` + `parent_task_id`). Persist: children do **not** write their own checkpoint; mid-subagent process crash resume is out of scope for M5.

**Spec:** [docs/superpowers/specs/2026-09-18-long-horizon-design.md](../specs/2026-09-18-long-horizon-design.md) — §6, §7, §12 LH-M5.

## Decisions

| Topic | Decision |
|-------|----------|
| Channel | A — parent SSE only |
| Nested persist | Disabled for child loop |
| Overflow tasks | Extra `task` calls in one turn → immediate tool-result error |
| `explore` | Prefer `readonly: true` tools; if none marked, all client tools + explore system nudge |
| Follow-up in child | `NoopFollowUp` |
| Resume mid-`task` | Not supported in M5 |

## File structure

| Path | Responsibility |
|------|----------------|
| `src/protocol/mod.rs` | `ToolDef.readonly`; `tool.request` agent fields; `task.*` SSE |
| `src/config/mod.rs` | `SubagentsConfig` |
| `src/runtime/mod.rs` | Multi waiter map |
| `src/agent/task.rs` | `task` ToolDef, parse, inject, explore filter, nudges |
| `src/agent/orchestrator.rs` | Wire subagents; execute `task`; fold stream |
| `src/agent/plan.rs` | `is_server_tool` includes `task` when enabled |
| `src/api/http.rs` / `main.rs` | Pass config + `options.subagents` |
| `src/store/mod.rs` | `Checkpoint.subagents` |
| `tests/subagents.rs` | Nested + parallel + explore + overflow |
| `docs/api-client.md` | Document M5 |

---

### Task 1: Protocol + config + multi-waiter
### Task 2: `task` helpers + orchestrator wiring
### Task 3: HTTP / store / tests / docs
