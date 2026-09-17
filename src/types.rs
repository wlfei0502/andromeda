#[derive(Debug, Clone, PartialEq)]
pub enum AgentMessage {
    User { content: String },
    Assistant {
        parts: Vec<ContentPart>,
        stop_reason: StopReason,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        content: String,
        is_error: bool,
        terminate: bool,
    },
    System { content: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContentPart {
    Text { text: String },
    ToolCall {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Stop,
    ToolCalls,
    Length,
    Error,
    Aborted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    AgentStart,
    AgentEnd { messages: Vec<AgentMessage> },
    TurnStart,
    TurnEnd {
        message: AgentMessage,
        tool_results: Vec<AgentMessage>,
    },
    MessageStart { message: AgentMessage },
    MessageUpdate { message: AgentMessage },
    MessageEnd { message: AgentMessage },
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: serde_json::Value,
    },
    ToolExecutionUpdate {
        tool_call_id: String,
        partial: String,
    },
    ToolExecutionEnd {
        tool_call_id: String,
        result: String,
        is_error: bool,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AgentContext {
    pub messages: Vec<AgentMessage>,
    pub tools: Vec<AgentTool>,
}

#[derive(Debug, Clone)]
pub struct AgentTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

impl ToolCall {
    pub fn from_content_part(part: &ContentPart) -> Option<Self> {
        match part {
            ContentPart::ToolCall { id, name, arguments } => Some(ToolCall {
                id: id.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            }),
            ContentPart::Text { .. } => None,
        }
    }
}
