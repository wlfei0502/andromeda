# Andromeda — Cloud Agent Server

HTTP service that runs the **agent loop in the cloud**: session orchestration, LLM streaming, tool *requests*, steering, and follow-up. **Tools execute on the client** (local GIS desktop or any HTTP client); this repo does not ship that UI.

The **GIS desktop intelligent agent** lives in a **separate repository**. Integrate against [docs/api-client.md](docs/api-client.md).

## Documentation

| Document | Purpose |
|----------|---------|
| [Client API contract](docs/api-client.md) | Sequence, routes, events, curl examples for external clients |
| [Cloud Agent SSE design](docs/superpowers/specs/2026-09-18-cloud-agent-sse-design.md) | Architecture, API, SSE protocol, steering / follow-up |
| [Long-horizon design](docs/superpowers/specs/2026-09-18-long-horizon-design.md) | Checkpoint, resume, plan/todos, subagents, multi-instance |
| [Context summarization](docs/superpowers/specs/2026-09-19-context-summarization-design.md) | LH-M2 window compression |
| [Agent middleware](docs/superpowers/specs/2026-09-19-agent-middleware-design.md) | `before_llm` chain (summarize today; memory later) |
| [Archive](docs/superpowers/archive/README.md) | Superseded dual-loop docs |
| [Plans](docs/superpowers/plans/) | Completed implementation plans (status banners at top) |

## How to run

1. Copy `config.example.toml` to `config.toml` and set `api_key`, `model`, and optionally `base_url`, `listen`, `tool_timeout_secs`, `follow_up_policy` (`noop` or `example_order`).
2. Build and start the server:

```bash
cargo run
```

Default listen address is `127.0.0.1:8080` (see `config.toml`). Logs use `RUST_LOG` (e.g. `RUST_LOG=info`).

3. Create a run with `POST /v1/runs` and `Accept: text/event-stream`; see [docs/api-client.md](docs/api-client.md) for tool results, steer, and cancel.

## Development

```bash
cargo test
```

Integration tests use a mock LLM and do not require a live API key for most cases.
