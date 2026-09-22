//! Agent orchestration layer (lead loop + follow-up policies).
//!
//! Depends on: `runtime`, `llm`, `protocol`, `store`.
//! Must not depend on `api`.

mod follow_up;
mod guards;
mod middleware;
mod orchestrator;
mod plan;
mod task;

pub use follow_up::{ExampleOrderFollowUp, FollowUpPolicy, NoopFollowUp, policy_from_name};
pub use guards::{REASON_LLM_ROUNDS, REASON_NOOP, REASON_TIMEOUT};
pub use middleware::{
    AgentMiddleware, MwAction, MwCtx, MwEffect, SummarizeMiddleware, default_summarize_chain,
    run_before_llm,
};
pub use orchestrator::{
    MAX_FOLLOW_UP_ROUNDS, OrchestratorError, RunAgentOpts, RunPersist, continue_after_pending_tool,
    run_agent, run_agent_with_options,
};
pub use plan::{WRITE_TODOS_NAME, apply_write_todos, inject_plan_tools};
pub use task::{TASK_NAME, inject_lead_tools, parse_task_args};
