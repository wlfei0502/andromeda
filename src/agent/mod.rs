//! Agent orchestration layer (lead loop + follow-up policies).
//!
//! Depends on: `runtime`, `llm`, `protocol`, `store`.
//! Must not depend on `api`.

mod follow_up;
mod middleware;
mod orchestrator;

pub use follow_up::{
    ExampleOrderFollowUp, FollowUpPolicy, NoopFollowUp, policy_from_name,
};
pub use middleware::{
    AgentMiddleware, MwAction, MwCtx, MwEffect, SummarizeMiddleware, default_summarize_chain,
    run_before_llm,
};
pub use orchestrator::{
    MAX_FOLLOW_UP_ROUNDS, OrchestratorError, RunPersist, continue_after_pending_tool, run_agent,
    run_agent_with_options,
};
