//! Built-in server tools (calendar, plan/write_todos, task/subagents).

mod calendar;
mod plan;
mod task;

pub use calendar::{CHINESE_CALENDAR_NAME, apply_chinese_calendar, chinese_calendar_tool_def};
pub use plan::{
    WRITE_TODOS_NAME, apply_write_todos, ensure_plan_nudge, inject_plan_tools, tool_result_err,
    tool_result_ok,
};
pub use task::{
    TASK_NAME, build_subagent_context, filter_tools_for_agent, inject_lead_tools,
    is_server_tool_name, is_task_tool, parse_task_args, task_result_err, task_result_ok,
};
