//! LH-M5 `task` server tool helpers.

use serde_json::{Value, json};

use crate::protocol::{Role, ToolDef, WireMessage};

pub const TASK_NAME: &str = "task";

pub const EXPLORE_NUDGE_PREFIX: &str = "[subagent:explore]";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskAgentKind {
    General,
    Explore,
}

impl TaskAgentKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskAgentKind::General => "general",
            TaskAgentKind::Explore => "explore",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "general" => Some(TaskAgentKind::General),
            "explore" => Some(TaskAgentKind::Explore),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskArgs {
    pub goal: String,
    pub agent: TaskAgentKind,
    pub context_hints: Vec<String>,
}

pub fn task_tool_def() -> ToolDef {
    ToolDef {
        name: TASK_NAME.into(),
        description: "Delegate a focused subtask to an isolated agent. Returns a summary.".into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "goal": { "type": "string" },
                "agent": {
                    "type": "string",
                    "enum": ["general", "explore"],
                    "description": "general: full client tools; explore: read-only tool subset if client marked tools"
                },
                "context_hints": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional short facts/paths; do not dump full parent chat"
                }
            },
            "required": ["goal"]
        }),
        readonly: None,
    }
}

pub fn parse_task_args(args: &Value) -> Result<TaskArgs, String> {
    let goal = args
        .get("goal")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing string `goal`".to_string())?
        .trim()
        .to_string();
    if goal.is_empty() {
        return Err("`goal` must be non-empty".into());
    }
    let agent = match args.get("agent") {
        None => TaskAgentKind::General,
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| "`agent` must be a string".to_string())?;
            TaskAgentKind::parse(s).ok_or_else(|| format!("invalid agent `{s}`"))?
        }
    };
    let context_hints = match args.get("context_hints") {
        None => Vec::new(),
        Some(v) => {
            let arr = v
                .as_array()
                .ok_or_else(|| "`context_hints` must be an array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                let s = item
                    .as_str()
                    .ok_or_else(|| format!("context_hints[{i}] must be a string"))?;
                out.push(s.to_string());
            }
            out
        }
    };
    Ok(TaskArgs {
        goal,
        agent,
        context_hints,
    })
}

/// Inject `task` (and optionally `write_todos`) ahead of client tools; drop client duplicates.
pub fn inject_lead_tools(
    client_tools: &[ToolDef],
    plan_mode: bool,
    subagents: bool,
) -> Vec<ToolDef> {
    let mut out = Vec::new();
    if plan_mode {
        out.push(super::plan::write_todos_tool_def());
    }
    if subagents {
        out.push(task_tool_def());
    }
    for t in client_tools {
        if t.name == super::plan::WRITE_TODOS_NAME || t.name == TASK_NAME {
            continue;
        }
        out.push(t.clone());
    }
    out
}

pub fn is_task_tool(name: &str) -> bool {
    name == TASK_NAME
}

pub fn is_server_tool_name(name: &str, plan_mode: bool, subagents: bool) -> bool {
    (plan_mode && name == super::plan::WRITE_TODOS_NAME) || (subagents && name == TASK_NAME)
}

/// Prefer tools marked `readonly: true`. If none are marked, keep the full client set.
pub fn filter_tools_for_agent(client_tools: &[ToolDef], agent: &TaskAgentKind) -> Vec<ToolDef> {
    match agent {
        TaskAgentKind::General => client_tools
            .iter()
            .filter(|t| t.name != TASK_NAME && t.name != super::plan::WRITE_TODOS_NAME)
            .cloned()
            .collect(),
        TaskAgentKind::Explore => {
            let readonly: Vec<ToolDef> = client_tools
                .iter()
                .filter(|t| t.readonly == Some(true))
                .cloned()
                .collect();
            if readonly.is_empty() {
                client_tools
                    .iter()
                    .filter(|t| t.name != TASK_NAME && t.name != super::plan::WRITE_TODOS_NAME)
                    .cloned()
                    .collect()
            } else {
                readonly
            }
        }
    }
}

pub fn build_subagent_context(args: &TaskArgs) -> Vec<WireMessage> {
    let mut msgs = Vec::new();
    if args.agent == TaskAgentKind::Explore {
        msgs.push(WireMessage {
            role: Role::System,
            content: format!(
                "{EXPLORE_NUDGE_PREFIX} You are an explore subagent. Prefer read-only investigation. \
                 Do not modify files or run destructive commands. Return a concise summary of findings."
            ),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        });
    } else {
        msgs.push(WireMessage {
            role: Role::System,
            content: "[subagent:general] You are a focused subagent. Complete the goal and return a concise summary."
                .into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        });
    }
    if !args.context_hints.is_empty() {
        let hints = args
            .context_hints
            .iter()
            .map(|h| format!("- {h}"))
            .collect::<Vec<_>>()
            .join("\n");
        msgs.push(WireMessage {
            role: Role::System,
            content: format!("Context hints:\n{hints}"),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        });
    }
    msgs.push(WireMessage {
        role: Role::User,
        content: args.goal.clone(),
        tool_call_id: None,
        name: None,
        tool_calls: None,
        reasoning_content: None,
    });
    msgs
}

pub fn task_result_ok(summary: &str) -> String {
    json!({ "ok": true, "summary": summary }).to_string()
}

pub fn task_result_err(error: &str) -> String {
    json!({ "ok": false, "error": error }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_task_args_defaults_agent() {
        let args = parse_task_args(&json!({ "goal": "find layers" })).unwrap();
        assert_eq!(args.agent, TaskAgentKind::General);
        assert!(args.context_hints.is_empty());
    }

    #[test]
    fn filter_explore_prefers_readonly() {
        let tools = vec![
            ToolDef {
                name: "read".into(),
                description: "r".into(),
                parameters: json!({}),
                readonly: Some(true),
            },
            ToolDef {
                name: "write".into(),
                description: "w".into(),
                parameters: json!({}),
                readonly: None,
            },
        ];
        let filtered = filter_tools_for_agent(&tools, &TaskAgentKind::Explore);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "read");
    }

    #[test]
    fn inject_lead_tools_orders_server_first() {
        let client = vec![ToolDef {
            name: "echo".into(),
            description: "e".into(),
            parameters: json!({}),
            readonly: None,
        }];
        let tools = inject_lead_tools(&client, true, true);
        assert_eq!(tools[0].name, "write_todos");
        assert_eq!(tools[1].name, "task");
        assert_eq!(tools[2].name, "echo");
    }
}
