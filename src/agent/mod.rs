//! Agent orchestration layer (lead loop + follow-up policies).
//!
//! Depends on: `runtime`, `llm`, `protocol`, `store`.
//! Must not depend on `api`.

mod follow_up;
mod guards;
mod middleware;
mod orchestrator;
mod tools;

pub use follow_up::{ExampleOrderFollowUp, FollowUpPolicy, NoopFollowUp, policy_from_name};
pub use guards::{REASON_LLM_ROUNDS, REASON_NOOP, REASON_TIMEOUT};
pub use middleware::{
    AgentMiddleware, MwAction, MwCtx, MwEffect, SummarizeMiddleware, TIME_CONTEXT_PREFIX,
    TimeContextMiddleware, default_summarize_chain, ensure_time_context, run_before_llm,
};
pub use orchestrator::{
    MAX_FOLLOW_UP_ROUNDS, OrchestratorError, RunAgentOpts, RunPersist, continue_after_pending_tool,
    run_agent, run_agent_with_options,
};
pub use tools::{
    CHINESE_CALENDAR_NAME, TASK_NAME, WRITE_TODOS_NAME, apply_chinese_calendar, apply_write_todos,
    chinese_calendar_tool_def, inject_lead_tools, inject_plan_tools, parse_task_args,
};
