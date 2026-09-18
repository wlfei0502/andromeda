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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateRunRequest {
    pub messages: Vec<WireMessage>,
    pub tools: Vec<ToolDef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
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
            SseEvent::MessageDelta { .. } => "message.delta",
            SseEvent::MessageCompleted { .. } => "message.completed",
            SseEvent::ToolRequest { .. } => "tool.request",
            SseEvent::RunFinished { .. } => "run.finished",
            SseEvent::Error { .. } => "error",
        }
    }
}
