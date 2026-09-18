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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunOptions {
    /// Persist checkpoint to RunStore (when long-horizon store is enabled).
    #[serde(default = "default_persist")]
    pub persist: bool,
    /// Reserved for LH-M3; ignored in M1.
    #[serde(default)]
    pub plan_mode: bool,
    /// Reserved for LH-M5; ignored in M1.
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
    #[serde(rename = "message.delta")]
    MessageDelta {
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
    },
    #[serde(rename = "tool.request")]
    ToolRequest {
        run_id: String,
        tool_call_id: String,
        name: String,
        arguments: Value,
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
            SseEvent::MessageDelta { .. } => "message.delta",
            SseEvent::MessageCompleted { .. } => "message.completed",
            SseEvent::ToolRequest { .. } => "tool.request",
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
}
