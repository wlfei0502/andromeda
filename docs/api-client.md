# Client API contract

This document is for **external clients** (e.g. the GIS desktop agent in a separate repo). Long-horizon milestones **LH-M1–M5** are implemented on the server; this page is the wire contract. For a desktop-oriented checklist, see [Desktop integration (minimal)](desktop-integration.md).

Design references:

- [Cloud Agent Server (SSE) design](../superpowers/specs/2026-09-18-cloud-agent-sse-design.md)
- [Long-horizon design](../superpowers/specs/2026-09-18-long-horizon-design.md) (checkpoint / resume / Plan Mode / guards / subagents)
- [Context summarization design](../superpowers/specs/2026-09-19-context-summarization-design.md) (LH-M2)
- [Agent middleware design](../superpowers/specs/2026-09-19-agent-middleware-design.md)

## Feature map (LH)

| Milestone | Client-visible switch / events |
|-----------|--------------------------------|
| M1 Persist / resume | `options.persist`; `GET .../events`; `run.resumed`; `409 code=not_owner` |
| M2 Summarize | `context.summarized`; `error code=context_overflow` |
| M3 Plan Mode | `options.plan_mode`; `todos.updated`; do not implement `write_todos` |
| M4 Guards | `run.finished` reasons `guard_*` |
| M5 Subagents | `options.subagents`; `task.*`; `tool.request` may include `agent_id` / `parent_task_id` |

With all `options.*` left at defaults (`persist` true if server persist is on; `plan_mode`/`subagents` false), event shape stays compatible with the original v1 loop.

## End-to-end sequence

```text
Client                                    Server
  |                                         |
  |  POST /v1/runs (messages, tools, options)
  |  Accept: text/event-stream              |
  |---------------------------------------->|
  |  200, SSE body + X-Run-Id               |
  |<----------------------------------------|
  |  event: run.started                     |
  |  event: message.delta* / reasoning.delta*
  |  event: message.completed               |
  |  [optional] todos.updated / task.*      |
  |                                         |
  |  [optional] POST .../steer              |
  |---------------------------------------->|
  |  200 { ok, queued }                     |
  |                                         |
  |  event: tool.request                    |
  |  execute tool locally                   |
  |  POST .../tool_results                  |
  |---------------------------------------->|
  |  200 { ok: true }                       |
  |  ... more deltas / tools / steer ...    |
  |  event: run.finished | error            |
  |<----------------------------------------|
```

**Minimal client loop**

1. `POST /v1/runs` with `messages` and optional `tools` / `options`; read the response as SSE.
2. On `message.delta` → update streaming UI; on `reasoning.delta` → optional thinking UI.
3. On `tool.request` → run the named tool locally → `POST /v1/runs/{run_id}/tool_results` (always the **parent** `run_id`).
4. User changes intent mid-run → `POST /v1/runs/{run_id}/steer` (same `run_id`, keep SSE open).
5. On `message.completed` with `source=follow_up` → optional UI for server-driven continuation.
6. On `run.finished` or `error` → close the stream and stop.
7. If the SSE connection drops mid-run → `GET /v1/runs/{run_id}/events` to resume (same `run_id`); do **not** create a new run.

## HTTP routes

### Create run (SSE)

```http
POST /v1/runs
Content-Type: application/json
Accept: text/event-stream
```

**Body**

| Field | Description |
|-------|-------------|
| `messages` | Input for this run: `user` / `assistant` / `system` / `tool`. |
| `tools` | Client-executable tools (JSON Schema). May be empty. See [Tool definitions](#tool-definitions). |
| `session_id` | Optional; **not** a multi-run session directory yet (ignored for storage layout). |
| `options.persist` | Default `true`. When server `[persist].enabled` is true, write checkpoints for resume. |
| `options.plan_mode` | Default `false`. Injects server tool `write_todos`; emits `todos.updated`. |
| `options.subagents` | Default `false`. Injects server tool `task`; emits `task.*`. |

**Response:** `200`, `Content-Type: text/event-stream`, header `X-Run-Id`. Stream ends after `run.finished` or `error` (or when the client disconnects — the **run may continue** server-side).

#### Tool definitions

```json
{
  "name": "read_layer",
  "description": "Read layer metadata",
  "parameters": { "type": "object", "properties": { "path": { "type": "string" } } },
  "readonly": true
}
```

| Field | Required | Notes |
|-------|----------|--------|
| `name` | yes | Must not collide with server tools `write_todos` / `task` (server wins if both appear). |
| `description` | yes | |
| `parameters` | yes | JSON Schema object for the model. |
| `readonly` | no | When `true`, eligible for `explore` subagent filtering (LH-M5). If no tool sets `readonly`, explore falls back to the full client tool list + a read-only system nudge. |

#### Example create body

```json
{
  "messages": [{ "role": "user", "content": "缓冲分析并列出步骤" }],
  "tools": [
    {
      "name": "echo",
      "description": "echo",
      "parameters": { "type": "object" },
      "readonly": true
    }
  ],
  "options": {
    "persist": true,
    "plan_mode": true,
    "subagents": false
  }
}
```

### Resume / re-subscribe SSE

```http
GET /v1/runs/{run_id}/events
Accept: text/event-stream
```

| Case | Behavior |
|------|----------|
| Hot run on this instance | Last subscriber wins; emits `run.resumed`; re-emits **all** outstanding `tool.request`s (including parallel subagent waits). |
| Cold checkpoint (shared store) | Claims ownership (`owner_id` / `revision`), emits `run.resumed`, continues orchestration. |
| Terminal checkpoint | Emits `run.finished` (or equivalent) and ends the stream. |
| Unknown | `404` |

Multi-instance: prefer LB sticky by `run_id` (or IP hash). Point all replicas at the **same** `data_dir` (or future DB store). If `tool_results` / `steer` hit a non-owner instance with no hot run → `409` with `"code":"not_owner"` — call `GET .../events` on a healthy instance to take over, then retry.

### Tool results

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

Call while the run is waiting for that `tool_call_id` (SSE may be open or you may POST after resume).

| Status | Meaning |
|--------|---------|
| `200` | `{ "ok": true }` |
| `404` | Unknown `run_id` |
| `409` | Run not waiting for this tool / already finished / **`code=not_owner`** (wrong instance) |

Lead-agent client tools run **serially**. With `options.subagents=true`, multiple subagents may wait on different `tool_call_id`s at once; POST still targets the **parent** `run_id`. Server tools (`write_todos`, `task`) never wait on this endpoint.

### Steering

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

| Rule | Behavior |
|------|----------|
| When | Run not finished (SSE preferably open). |
| Effect | Messages enter a **steering queue**; they do **not** cut off an in-flight LLM token stream. |
| Applied | After the current assistant stream or tool wait finishes, **before** the next LLM call; each inserted message is emitted as `message.completed` with `source=steer`. |
| Response | `200 { "ok": true, "queued": N }`; `404` / `409` if invalid or finished. |

> Desktop product path: mid-run follow-ups use a **local queue + cancel/reopen** (new `POST /v1/runs`), not `steer`. The HTTP steer API remains available for other clients.

Steering keeps the **same** `run_id`—unlike starting a new run.

### Cancel

```http
POST /v1/runs/{run_id}/cancel
```

Ends the run; SSE receives `run.finished` (`reason=cancelled`) or `error`, then closes.

## SSE wire format

Each frame:

```text
event: <type>
data: <json>

```

`data` is JSON and includes at least `run_id` and `type` (matching `event`).

### Event types

| `event` / `type` | Meaning | Main fields |
|------------------|---------|-------------|
| `run.started` | Run created | `run_id` |
| `run.resumed` | SSE re-subscribed / ownership taken | `run_id`, `revision`, `status` (`running` \| `waiting_tool` \| …) |
| `context.summarized` | Server compressed message history | `before_tokens`, `after_tokens`, `kept_prefix`, `kept_suffix` |
| `message.delta` | Assistant streaming chunk | `message_id`, `delta` |
| `reasoning.delta` | Provider thinking / reasoning stream | `message_id`, `delta` |
| `message.completed` | Message finalized | `message_id`, `role`, `content`, `tool_calls?`, `source?` (`assistant` \| `steer` \| `follow_up`), `reasoning_content?` |
| `tool.request` | Client must execute tool | `tool_call_id`, `name`, `arguments`; optional `agent_id`, `parent_task_id` |
| `todos.updated` | Plan Mode list replaced | `todos` (`[{id, content, status}]`) |
| `task.started` | Subagent started | `task_id`, `goal`, `agent` (`general` \| `explore`) |
| `task.completed` | Subagent finished | `task_id`, `summary` |
| `task.failed` | Subagent failed | `task_id`, `message`, `code?` |
| `task.timed_out` | Subagent hit timeout | `task_id` |
| `run.finished` | Normal end or guard stop | `run_id`, `reason` |
| `error` | Failure | `message`, `code?` |

**UI notes**

- Prefer `message.completed` for correctness; still render deltas for live typing.
- Steering and follow-up appear as `message.completed` with explicit `source`.
- Plan Mode: render from `todos.updated`; never local-execute `write_todos`.
- Subagents: render from `task.*`; child token streams are **not** forwarded (folded).

## Follow-up (server-side)

Follow-up is **not** a separate HTTP call. When the model finishes without tool calls, a configured **follow-up policy** may inject messages and run another LLM round on the **same** SSE / `run_id`.

| | Steering | Follow-up |
|---|----------|-----------|
| Trigger | Client `POST .../steer` | Server `FollowUpPolicy` |
| Typical source | User mid-run | Automated next step |
| Wire | `message.completed`, `source=steer` | `message.completed`, `source=follow_up` |

Default policy is `noop`. Server config: `follow_up_policy = "noop"` \| `"example_order"`.

## Plan Mode (LH-M3)

Set `options.plan_mode: true` on create.

- Server injects `write_todos` (full replace) and a short system nudge.
- Updates run on the server; clients see `todos.updated` only.
- Restored from checkpoint on resume.

## Guards (LH-M4)

Server `[guards]` (per run). `0` disables that limit. Defaults: 200 LLM rounds, 7200s wall clock, 8 follow-up rounds, 5 similar no-progress replies.

| `run.finished.reason` | Meaning |
|-----------------------|---------|
| `stop` | Normal completion |
| `cancelled` | Client cancel |
| `guard_llm_rounds` | Hit `max_llm_rounds` |
| `guard_timeout` | Hit `max_run_wall_secs` |
| `guard_noop` | Too many similar no-tool replies |

Follow-up cap still ends with `error` / `code=follow_up_limit`. Guarded runs checkpoint as `failed` with `finish_reason` for later resume.

## Subagents (LH-M5)

Set `options.subagents: true`.

- Server tool `task`: `goal` (required), `agent` (`general`\|`explore`, default `general`), `context_hints` (optional string array).
- Nested run shares the **parent** SSE (channel A). Child `message.delta` / `message.completed` are folded; progress is `task.started` / `completed` / `failed` / `timed_out`.
- Child client tools arrive as parent `tool.request` with `agent_id` + `parent_task_id`; POST `tool_results` to the parent `run_id`.
- Hot resume re-emits **all** outstanding `tool.request`s.
- Children cannot call `task`. Extra `task` calls beyond `[subagents].max_concurrent_subagents` (default 2) in one turn get an error tool result.
- Mid-`task` process crash resume is not supported; subagent wall-clock timeout clears orphaned waiters for that `task_id`.

## Context window (server-side)

Before each main LLM call, a `before_llm` middleware chain runs (today: summarization). Clients may see `context.summarized`. On hard overflow: `error` with `code=context_overflow` — end the run UI.

Configure under server `[context]`: `summarize_threshold_tokens`, `keep_last_messages`, `max_context_tokens`.

## Server config knobs (clients do not set these)

Documented so desktop/ops know what the cloud admin controls:

| TOML | Purpose |
|------|---------|
| `[persist]` | Checkpoint enable, `data_dir`, `instance_id` |
| `[context]` | Summarizer thresholds |
| `[guards]` | Run safety limits |
| `[subagents]` | `max_concurrent_subagents`, `subagent_timeout_secs` |
| `tool_timeout_secs` | Per client-tool wait |
| `follow_up_policy` | `noop` / `example_order` |

See `config.example.toml`.

## Example curl

```bash
curl -N -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"messages":[{"role":"user","content":"Hello"}],"tools":[],"options":{"plan_mode":false,"subagents":false}}'
```

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/steer" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Use shorter answers"}]}'
```

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/tool_results" \
  -H 'Content-Type: application/json' \
  -d '{"tool_call_id":"call_abc","content":"{\"ok\":true}","is_error":false}'
```

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/cancel"
```

```bash
curl -N "http://127.0.0.1:8080/v1/runs/RUN_ID/events" \
  -H 'Accept: text/event-stream'
```
