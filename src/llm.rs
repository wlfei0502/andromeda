use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::wire::{ToolDef, WireMessage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmChunk {
    TextDelta(String),
    Completed {
        content: String,
        tool_calls: Vec<ToolCall>,
    },
}

#[derive(Debug, Clone)]
pub enum MockTurn {
    TextOnly {
        content: String,
        deltas: Vec<String>,
    },
    WithToolCalls {
        content: String,
        deltas: Vec<String>,
        tool_calls: Vec<ToolCall>,
    },
}

pub struct MockLlm {
    script: Mutex<Vec<MockTurn>>,
}

impl MockLlm {
    pub fn script(turns: Vec<MockTurn>) -> Self {
        Self {
            script: Mutex::new(turns),
        }
    }

    fn turn_to_chunks(turn: MockTurn) -> Vec<Result<LlmChunk, String>> {
        match turn {
            MockTurn::TextOnly { content, deltas } => {
                let mut out: Vec<Result<LlmChunk, String>> = deltas
                    .into_iter()
                    .map(|d| Ok(LlmChunk::TextDelta(d)))
                    .collect();
                out.push(Ok(LlmChunk::Completed {
                    content,
                    tool_calls: vec![],
                }));
                out
            }
            MockTurn::WithToolCalls {
                content,
                deltas,
                tool_calls,
            } => {
                let mut out: Vec<Result<LlmChunk, String>> = deltas
                    .into_iter()
                    .map(|d| Ok(LlmChunk::TextDelta(d)))
                    .collect();
                out.push(Ok(LlmChunk::Completed {
                    content,
                    tool_calls,
                }));
                out
            }
        }
    }
}

#[async_trait]
impl LlmPort for MockLlm {
    async fn stream(
        &self,
        _messages: &[WireMessage],
        _tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String> {
        let turn = {
            let mut script = self.script.lock().await;
            if script.is_empty() {
                return Err("mock LLM script exhausted".to_string());
            }
            script.remove(0)
        };
        Ok(Box::pin(stream::iter(Self::turn_to_chunks(turn))))
    }
}

#[async_trait]
pub trait LlmPort: Send + Sync {
    async fn stream(
        &self,
        messages: &[WireMessage],
        tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String>;
}
