# Client API contract

This document is for **external clients** (e.g. the GIS desktop agent in a separate repo). The server implements the full design in [Cloud Agent Server (SSE) design](../superpowers/specs/2026-09-18-cloud-agent-sse-design.md).

## End-to-end sequence

```text
Client                                    Server
  |                                         |
  |  POST /v1/runs (messages, tools)        |
  |  Accept: text/event-stream              |
  |---------------------------------------->|
  |  200, SSE body + X-Run-Id               |
  |<----------------------------------------|
  |  event: run.started                     |
  |  event: message.delta*                  |
  |  event: message.completed              |
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
  |  event: run.finished | error              |
  |<----------------------------------------|
```

**Minimal client loop**

1. `POST /v1/runs` with `messages` and optional `tools`; read the response as SSE.
2. On `message.delta` → update streaming UI.
3. On `tool.request` → run the named tool locally → `POST /v1/runs/{run_id}/tool_results`.
4. User changes intent mid-run → `POST /v1/runs/{run_id}/steer` (same `run_id`, keep SSE open).
5. On `message.completed` with `source=follow_up` → optional UI for server-driven continuation.
6. On `run.finished` or `error` → close the stream and stop.

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
| `messages` | Input for this run: `user` / `assistant` / `system` / `tool` (text or structured tool results). |
| `tools` | JSON Schema list of tools the client can execute; may be empty. |
| `session_id` | Optional; v1 may ignore persistence. |

**Response:** `200`, `Content-Type: text/event-stream`, header `X-Run-Id`. Stream ends after `run.finished` or `error`.

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

Call while the run’s SSE is still open and the server is waiting for that `tool_call_id`.

| Status | Meaning |
|--------|---------|
| `200` | `{ "ok": true }` |
| `404` | Unknown `run_id` |
| `409` | Run not waiting for this tool / already finished |

v1 executes tools **serially**: at most one outstanding `tool.request` per run.

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
| When | SSE still open and run not finished. |
| Effect | Messages enter a **steering queue**; they do **not** cut off an in-flight LLM token stream. |
| Applied | After the current assistant stream or tool wait finishes, **before** the next LLM call; each inserted message is emitted as `message.completed` with `source=steer`. |
| Response | `200 { "ok": true, "queued": N }`; `404` / `409` if invalid or finished. |

Steering keeps the **same** `run_id` and SSE connection—unlike starting a new run.

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
| `message.delta` | Assistant streaming chunk | `message_id`, `delta` (text) |
| `message.completed` | Message finalized | `message_id`, `role`, `content`, `tool_calls?`, `source?` (`assistant` \| `steer` \| `follow_up`) |
| `tool.request` | Client must execute tool | `tool_call_id`, `name`, `arguments` (JSON) |
| `run.finished` | Normal end | `run_id`, `reason` (`stop`, `cancelled`, …) |
| `error` | Failure | `message`, `code?` |

**UI notes**

- Prefer `message.completed` (and final context) over reassembling deltas if you only need correctness; still render deltas for live typing.
- Steering and follow-up appear as explicit `message.completed` events so the client does not guess silent context changes.

## Follow-up (server-side)

Follow-up is **not** a separate HTTP call. When the model finishes without tool calls, a configured **follow-up policy** on the server may inject messages and run another LLM round on the **same** SSE / `run_id`.

| | Steering | Follow-up |
|---|----------|-----------|
| Trigger | Client `POST .../steer` | Server `FollowUpPolicy` |
| Typical source | User mid-run | Automated next step (e.g. after order placed) |
| Wire | `message.completed`, `source=steer` | `message.completed`, `source=follow_up` |

Default policy is `noop` (no extra rounds). Server config may set `follow_up_policy = "example_order"` for the built-in demo policy.

## Example curl

Start a run (requires a client that reads streaming SSE; `curl -N` works for smoke tests):

```bash
curl -N -X POST http://127.0.0.1:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"messages":[{"role":"user","content":"Hello"}],"tools":[]}'
```

Steer (replace `RUN_ID` from `X-Run-Id` or first SSE event):

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/steer" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Use shorter answers"}]}'
```

Tool result:

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/tool_results" \
  -H 'Content-Type: application/json' \
  -d '{"tool_call_id":"call_abc","content":"{\"ok\":true}","is_error":false}'
```

Cancel:

```bash
curl -X POST "http://127.0.0.1:8080/v1/runs/RUN_ID/cancel"
```
