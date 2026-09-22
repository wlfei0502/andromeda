use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WireMessage {
    pub role: Role,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCallWire>>,
    /// Provider thinking tokens (DeepSeek / Qwen / etc.). Must round-trip on
    /// subsequent requests when the prior assistant turn used tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    /// When `true`, eligible for `explore` subagent tool filtering (LH-M5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readonly: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TodoItem {
    pub id: String,
    pub content: String,
    pub status: TodoStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunOptions {
    /// Persist checkpoint to RunStore (when server `[persist]` is enabled).
    #[serde(default = "default_persist")]
    pub persist: bool,
    /// When true, inject server tool `write_todos` and emit `todos.updated`.
    #[serde(default)]
    pub plan_mode: bool,
    /// When true, inject server tool `task` and allow nested subagents (LH-M5).
    #[serde(default)]
    pub subagents: bool,
}

fn default_persist() -> bool {
    true
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            persist: default_persist(),
            plan_mode: false,
            subagents: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateRunRequest {
    pub messages: Vec<WireMessage>,
    pub tools: Vec<ToolDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub options: RunOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolResultRequest {
    pub tool_call_id: String,
    pub content: String,
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SteerRequest {
    pub messages: Vec<WireMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallWire {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageSource {
    Assistant,
    Steer,
    FollowUp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum SseEvent {
    #[serde(rename = "run.started")]
    RunStarted { run_id: String },
    #[serde(rename = "run.resumed")]
    RunResumed {
        run_id: String,
        revision: u64,
        status: String,
    },
    #[serde(rename = "context.summarized")]
    ContextSummarized {
        run_id: String,
        before_tokens: u64,
        after_tokens: u64,
        kept_prefix: usize,
        kept_suffix: usize,
    },
    #[serde(rename = "message.delta")]
    MessageDelta {
        run_id: String,
        message_id: String,
        delta: String,
    },
    #[serde(rename = "reasoning.delta")]
    ReasoningDelta {
        run_id: String,
        message_id: String,
        delta: String,
    },
    #[serde(rename = "message.completed")]
    MessageCompleted {
        run_id: String,
        message_id: String,
        role: Role,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<ToolCallWire>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<MessageSource>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning_content: Option<String>,
    },
    #[serde(rename = "tool.request")]
    ToolRequest {
        run_id: String,
        tool_call_id: String,
        name: String,
        arguments: Value,
        /// Subagent id when the request originates from a nested `task` (LH-M5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_id: Option<String>,
        /// Parent `task` tool_call_id when set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_task_id: Option<String>,
    },
    #[serde(rename = "todos.updated")]
    TodosUpdated {
        run_id: String,
        todos: Vec<TodoItem>,
    },
    #[serde(rename = "task.started")]
    TaskStarted {
        run_id: String,
        task_id: String,
        goal: String,
        agent: String,
    },
    #[serde(rename = "task.completed")]
    TaskCompleted {
        run_id: String,
        task_id: String,
        summary: String,
    },
    #[serde(rename = "task.failed")]
    TaskFailed {
        run_id: String,
        task_id: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
    #[serde(rename = "task.timed_out")]
    TaskTimedOut {
        run_id: String,
        task_id: String,
    },
    #[serde(rename = "run.finished")]
    RunFinished { run_id: String, reason: String },
    #[serde(rename = "error")]
    Error {
        run_id: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
}

impl SseEvent {
    pub fn event_name(&self) -> &'static str {
        match self {
            SseEvent::RunStarted { .. } => "run.started",
            SseEvent::RunResumed { .. } => "run.resumed",
            SseEvent::ContextSummarized { .. } => "context.summarized",
            SseEvent::MessageDelta { .. } => "message.delta",
            SseEvent::ReasoningDelta { .. } => "reasoning.delta",
            SseEvent::MessageCompleted { .. } => "message.completed",
            SseEvent::ToolRequest { .. } => "tool.request",
            SseEvent::TodosUpdated { .. } => "todos.updated",
            SseEvent::TaskStarted { .. } => "task.started",
            SseEvent::TaskCompleted { .. } => "task.completed",
            SseEvent::TaskFailed { .. } => "task.failed",
            SseEvent::TaskTimedOut { .. } => "task.timed_out",
            SseEvent::RunFinished { .. } => "run.finished",
            SseEvent::Error { .. } => "error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_run_request_options_default_via_json() {
        let req: CreateRunRequest = serde_json::from_value(json!({
            "messages": [{ "role": "user", "content": "hi" }],
            "tools": []
        }))
        .unwrap();
        assert!(req.options.persist);
        assert!(!req.options.plan_mode);
        assert!(!req.options.subagents);
    }

    #[test]
    fn context_summarized_roundtrips() {
        let ev = SseEvent::ContextSummarized {
            run_id: "r1".into(),
            before_tokens: 90_000,
            after_tokens: 40_000,
            kept_prefix: 2,
            kept_suffix: 24,
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "context.summarized");
        assert_eq!(v["before_tokens"], 90_000);
        assert_eq!(v["kept_suffix"], 24);
        let back: SseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, ev);
        assert_eq!(back.event_name(), "context.summarized");
    }

    #[test]
    fn run_resumed_roundtrips() {
        let ev = SseEvent::RunResumed {
            run_id: "r1".into(),
            revision: 3,
            status: "waiting_tool".into(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "run.resumed");
        assert_eq!(v["revision"], 3);
        let back: SseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, ev);
        assert_eq!(back.event_name(), "run.resumed");
    }

    #[test]
    fn todos_updated_event_name_and_roundtrip() {
        let ev = SseEvent::TodosUpdated {
            run_id: "r1".into(),
            todos: vec![TodoItem {
                id: "t1".into(),
                content: "map layers".into(),
                status: TodoStatus::InProgress,
            }],
        };
        assert_eq!(ev.event_name(), "todos.updated");
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "todos.updated");
        assert_eq!(v["todos"][0]["status"], "in_progress");
        let back: SseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn tool_request_omits_agent_fields_by_default() {
        let ev = SseEvent::ToolRequest {
            run_id: "r1".into(),
            tool_call_id: "c1".into(),
            name: "echo".into(),
            arguments: json!({}),
            agent_id: None,
            parent_task_id: None,
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert!(v.get("agent_id").is_none());
        assert!(v.get("parent_task_id").is_none());
        let back: SseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn task_events_roundtrip() {
        let started = SseEvent::TaskStarted {
            run_id: "r1".into(),
            task_id: "t1".into(),
            goal: "explore".into(),
            agent: "explore".into(),
        };
        assert_eq!(started.event_name(), "task.started");
        let v = serde_json::to_value(&started).unwrap();
        assert_eq!(v["type"], "task.started");
        let back: SseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, started);

        let done = SseEvent::TaskCompleted {
            run_id: "r1".into(),
            task_id: "t1".into(),
            summary: "found X".into(),
        };
        assert_eq!(done.event_name(), "task.completed");
    }

    #[test]
    fn tool_def_readonly_optional() {
        let bare: ToolDef = serde_json::from_value(json!({
            "name": "echo",
            "description": "e",
            "parameters": {}
        }))
        .unwrap();
        assert!(bare.readonly.is_none());
        let ro: ToolDef = serde_json::from_value(json!({
            "name": "read",
            "description": "r",
            "parameters": {},
            "readonly": true
        }))
        .unwrap();
        assert_eq!(ro.readonly, Some(true));
    }
}
