use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::backend::{AgentBackend, Emit};
use crate::error::LoopError;
use crate::types::{AgentContext, AgentEvent, AgentMessage, ContentPart, StopReason};

/// One scripted assistant turn for [`StubBackend`].
#[derive(Debug, Clone)]
pub enum StubTurn {
    AssistantWithTool {
        name: String,
        args: serde_json::Value,
    },
    AssistantText(String),
}

/// Scripted Phase A backend: fake streaming + fake tool results, no liter_llm.
pub struct StubBackend {
    turns: Mutex<VecDeque<StubTurn>>,
    follow_ups: Mutex<VecDeque<AgentMessage>>,
    call_ids: AtomicU64,
}

impl StubBackend {
    pub fn script(turns: Vec<StubTurn>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            follow_ups: Mutex::new(VecDeque::new()),
            call_ids: AtomicU64::new(0),
        }
    }

    /// Queue a single follow-up message (popped once by `get_follow_up`).
    pub fn with_follow_up(self, message: AgentMessage) -> Self {
        self.follow_ups.lock().unwrap().push_back(message);
        self
    }

    fn next_call_id(&self) -> String {
        let n = self.call_ids.fetch_add(1, Ordering::Relaxed);
        format!("call-{n}")
    }
}

#[async_trait]
impl AgentBackend for StubBackend {
    async fn stream_assistant(
        &self,
        ctx: &mut AgentContext,
        emit: Emit,
        _cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError> {
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| LoopError::Backend("stub script exhausted".into()))?;

        let message = match turn {
            StubTurn::AssistantWithTool { name, args } => {
                let id = self.next_call_id();
                AgentMessage::Assistant {
                    parts: vec![ContentPart::ToolCall {
                        id,
                        name,
                        arguments: args,
                    }],
                    stop_reason: StopReason::ToolCalls,
                }
            }
            StubTurn::AssistantText(text) => AgentMessage::Assistant {
                parts: vec![ContentPart::Text { text }],
                stop_reason: StopReason::Stop,
            },
        };

        (emit)(AgentEvent::MessageStart {
            message: message.clone(),
        });
        // Fake streaming delta(s).
        (emit)(AgentEvent::MessageUpdate {
            message: message.clone(),
        });
        (emit)(AgentEvent::MessageEnd {
            message: message.clone(),
        });

        ctx.messages.push(message.clone());
        Ok(message)
    }

    async fn execute_tool(
        &self,
        call: &ContentPart,
        emit: Emit,
        _cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError> {
        let (tool_call_id, tool_name, args) = match call {
            ContentPart::ToolCall {
                id,
                name,
                arguments,
            } => (id.clone(), name.clone(), arguments.clone()),
            _ => {
                return Err(LoopError::Backend(
                    "execute_tool expected ToolCall content part".into(),
                ));
            }
        };

        (emit)(AgentEvent::ToolExecutionStart {
            tool_call_id: tool_call_id.clone(),
            tool_name: tool_name.clone(),
            args,
        });
        (emit)(AgentEvent::ToolExecutionEnd {
            tool_call_id: tool_call_id.clone(),
            result: "ok".into(),
            is_error: false,
        });

        Ok(AgentMessage::ToolResult {
            tool_call_id,
            tool_name,
            content: "ok".into(),
            is_error: false,
            terminate: false,
        })
    }

    async fn get_follow_up(&self) -> Vec<AgentMessage> {
        match self.follow_ups.lock().unwrap().pop_front() {
            Some(msg) => vec![msg],
            None => vec![],
        }
    }
}
