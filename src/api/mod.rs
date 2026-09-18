//! HTTP/SSE transport layer.
//!
//! Depends on: `agent`, `runtime`, `llm`, `protocol`.
//! Must not be depended on by `agent`, `runtime`, `llm`, or `protocol`.

mod http;
mod sse;

pub use http::{AppState, router};
pub use sse::sse_frame;
