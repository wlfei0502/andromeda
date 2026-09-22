//! Plan Mode helpers: `write_todos` server tool + system nudge.

use serde_json::{Value, json};

use crate::protocol::{Role, TodoItem, TodoStatus, ToolDef, WireMessage};

pub const WRITE_TODOS_NAME: &str = "write_todos";

pub const PLAN_NUDGE_PREFIX: &str = "[plan mode]";

pub fn write_todos_tool_def() -> ToolDef {
    ToolDef {
        name: WRITE_TODOS_NAME.into(),
        description: "Replace the task list for this run. Keep items small and actionable.".into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "todos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" },
                            "content": { "type": "string" },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed", "cancelled"]
                            }
                        },
                        "required": ["id", "content", "status"]
                    }
                }
            },
            "required": ["todos"]
        }),
        readonly: None,
    }
}

pub fn plan_mode_system_nudge() -> WireMessage {
    WireMessage {
        role: Role::System,
        content: format!(
            "{PLAN_NUDGE_PREFIX} For multi-step work, call `{WRITE_TODOS_NAME}` to maintain a \
             structured todo list before and while executing. Keep at most one item \
             `in_progress`. Mark finished steps `completed` and dropped steps `cancelled`. \
             Do not narrate the todo list in chat; the client already receives structured updates."
        ),
        tool_call_id: None,
        name: None,
        tool_calls: None,
        reasoning_content: None,
    }
}

pub fn ensure_plan_nudge(context: &mut Vec<WireMessage>) {
    let already = context
        .iter()
        .any(|m| m.role == Role::System && m.content.starts_with(PLAN_NUDGE_PREFIX));
    if !already {
        context.insert(0, plan_mode_system_nudge());
    }
}

/// Server `write_todos` first; drop any client tool with the same name.
///
/// Thin wrapper over [`super::task::inject_lead_tools`] with `plan_mode=true`, `subagents=false`.
pub fn inject_plan_tools(client_tools: &[ToolDef]) -> Vec<ToolDef> {
    super::task::inject_lead_tools(client_tools, true, false)
}

#[allow(dead_code)] // retained for plan-mode unit tests / callers
pub fn is_server_tool(name: &str) -> bool {
    name == WRITE_TODOS_NAME
}

/// Full-replace parse. Invalid payload → `Err` (caller leaves stored todos unchanged).
pub fn apply_write_todos(args: &Value) -> Result<Vec<TodoItem>, String> {
    let todos_val = args
        .get("todos")
        .ok_or_else(|| "missing field `todos`".to_string())?;
    let arr = todos_val
        .as_array()
        .ok_or_else(|| "`todos` must be an array".to_string())?;

    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("todos[{i}]: missing string `id`"))?
            .to_string();
        let content = item
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("todos[{i}]: missing string `content`"))?
            .to_string();
        let status_str = item
            .get("status")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("todos[{i}]: missing string `status`"))?;
        let status = parse_status(status_str)
            .ok_or_else(|| format!("todos[{i}]: invalid status `{status_str}`"))?;
        out.push(TodoItem {
            id,
            content,
            status,
        });
    }
    Ok(out)
}

fn parse_status(s: &str) -> Option<TodoStatus> {
    match s {
        "pending" => Some(TodoStatus::Pending),
        "in_progress" => Some(TodoStatus::InProgress),
        "completed" => Some(TodoStatus::Completed),
        "cancelled" => Some(TodoStatus::Cancelled),
        _ => None,
    }
}

pub fn tool_result_ok(todos: &[TodoItem]) -> String {
    json!({ "ok": true, "todos": todos }).to_string()
}

pub fn tool_result_err(error: &str) -> String {
    json!({ "ok": false, "error": error }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn apply_write_todos_happy_path() {
        let args = json!({
            "todos": [
                {"id": "t1", "content": "a", "status": "pending"},
                {"id": "t2", "content": "b", "status": "in_progress"}
            ]
        });
        let list = apply_write_todos(&args).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[1].status, TodoStatus::InProgress);
    }

    #[test]
    fn apply_write_todos_rejects_bad_status() {
        let args = json!({
            "todos": [{"id": "t1", "content": "a", "status": "doing"}]
        });
        let err = apply_write_todos(&args).unwrap_err();
        assert!(err.contains("invalid status"));
    }

    #[test]
    fn inject_plan_tools_drops_client_duplicate() {
        let client = vec![
            ToolDef {
                name: "echo".into(),
                description: "e".into(),
                parameters: json!({}),
                readonly: None,
            },
            ToolDef {
                name: WRITE_TODOS_NAME.into(),
                description: "client should lose".into(),
                parameters: json!({}),
                readonly: None,
            },
        ];
        let tools = inject_plan_tools(&client);
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name, WRITE_TODOS_NAME);
        assert!(tools[0].description.contains("Replace"));
        assert_eq!(tools[1].name, "echo");
    }

    #[test]
    fn ensure_plan_nudge_is_idempotent() {
        let mut ctx = vec![WireMessage {
            role: Role::User,
            content: "hi".into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        }];
        ensure_plan_nudge(&mut ctx);
        ensure_plan_nudge(&mut ctx);
        assert_eq!(ctx.len(), 2);
        assert!(ctx[0].content.starts_with(PLAN_NUDGE_PREFIX));
        assert_eq!(ctx[1].role, Role::User);
    }
}
