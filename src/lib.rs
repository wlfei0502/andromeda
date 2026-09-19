//! Andromeda cloud agent server library.
//!
//! # Module layers (dependencies point downward only)
//!
//! ```text
//! api          HTTP + SSE transport
//!   ↓
//! agent        orchestrator + follow-up
//!   ↓
//! llm          model port / adapters
//! runtime      in-memory run registry & waiters
//! store        durable checkpoints (→ protocol)
//!   ↓
//! protocol     wire types (messages, SSE events)
//!
//! config       cross-cutting; used by main and adapters
//! ```

pub mod agent;
pub mod api;
pub mod config;
pub mod llm;
pub mod protocol;
pub mod runtime;
pub mod store;

pub use config::AppConfig;
pub use llm::{LiterAdapter, LlmChunk, LlmPort, MockLlm, MockTurn, ToolCall};
