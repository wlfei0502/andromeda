//! Phase B: `liter_llm` streaming backend + AgentMessage ↔ liter mapping.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use liter_llm::client::LlmClient;
use liter_llm::types::{
    AssistantContent, AssistantMessage, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionTool,
    FinishReason, FunctionCall, FunctionDefinition, Message as LlmMessage, StreamChoice,
    StreamToolCall, SystemMessage, ToolCall as LlmToolCall, ToolMessage, ToolType, UserMessage,
};
use tokio_util::sync::CancellationToken;

use crate::backend::{AgentBackend, Emit};
use crate::error::LoopError;
use crate::types::{
    AgentContext, AgentEvent, AgentMessage, AgentTool, ContentPart, StopReason,
};

/// Outcome of a registered tool invocation.
#[derive(Debug, Clone)]
pub struct ToolExecResult {
    pub content: String,
    pub is_error: bool,
    pub terminate: bool,
}

/// Sync tool handler registered on [`LiterBackend`].
pub trait ToolExecutor: Send + Sync {
    fn execute(&self, args: &serde_json::Value) -> ToolExecResult;
}

impl<F> ToolExecutor for F
where
    F: Fn(&serde_json::Value) -> ToolExecResult + Send + Sync,
{
    fn execute(&self, args: &serde_json::Value) -> ToolExecResult {
        (self)(args)
    }
}

/// Echo tool: returns `message` string arg, or the full JSON args as text.
pub fn echo_executor(args: &serde_json::Value) -> ToolExecResult {
    let content = args
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| args.to_string());
    ToolExecResult {
        content,
        is_error: false,
        terminate: false,
    }
}

/// Schema for the built-in `echo` tool (for `AgentContext::tools`).
pub fn echo_tool_def() -> AgentTool {
    AgentTool {
        name: "echo".into(),
        description: "Echo back the given message string.".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "message": { "type": "string", "description": "Text to echo" }
            },
            "required": ["message"]
        }),
    }
}

/// Backend that streams assistants via [`LlmClient::chat_stream`].
pub struct LiterBackend<C: LlmClient> {
    client: C,
    model: String,
    executors: HashMap<String, Arc<dyn ToolExecutor>>,
}

impl<C: LlmClient> LiterBackend<C> {
    pub fn new(client: C, model: impl Into<String>) -> Self {
        Self {
            client,
            model: model.into(),
            executors: HashMap::new(),
        }
    }

    /// Register a named tool executor (overwrites any prior registration).
    pub fn register_tool(
        &mut self,
        name: impl Into<String>,
        executor: impl ToolExecutor + 'static,
    ) -> &mut Self {
        self.executors
            .insert(name.into(), Arc::new(executor));
        self
    }

    /// Register the built-in `echo` executor.
    pub fn with_echo(mut self) -> Self {
        self.register_tool("echo", echo_executor);
        self
    }
}

/// Map agent context messages to liter_llm request messages.
pub fn to_llm_messages(messages: &[AgentMessage]) -> Vec<LlmMessage> {
    messages.iter().filter_map(agent_message_to_llm).collect()
}

fn agent_message_to_llm(message: &AgentMessage) -> Option<LlmMessage> {
    match message {
        AgentMessage::User { content } => Some(LlmMessage::User(UserMessage {
            content: content.clone().into(),
            name: None,
        })),
        AgentMessage::System { content } => Some(LlmMessage::System(SystemMessage {
            content: content.clone().into(),
            name: None,
        })),
        AgentMessage::Assistant { parts, .. } => {
            let mut text = String::new();
            let mut tool_calls = Vec::new();
            for part in parts {
                match part {
                    ContentPart::Text { text: t } => text.push_str(t),
                    ContentPart::ToolCall {
                        id,
                        name,
                        arguments,
                    } => {
                        tool_calls.push(LlmToolCall {
                            id: id.clone(),
                            call_type: ToolType::Function,
                            function: FunctionCall {
                                name: name.clone(),
                                arguments: arguments.to_string(),
                            },
                        });
                    }
                }
            }
            Some(LlmMessage::Assistant(AssistantMessage {
                content: if text.is_empty() {
                    None
                } else {
                    Some(AssistantContent::Text(text))
                },
                name: None,
                tool_calls: if tool_calls.is_empty() {
                    None
                } else {
                    Some(tool_calls)
                },
                refusal: None,
                function_call: None,
                reasoning_content: None,
            }))
        }
        AgentMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            ..
        } => Some(LlmMessage::Tool(ToolMessage {
            content: content.clone().into(),
            tool_call_id: tool_call_id.clone(),
            name: if tool_name.is_empty() {
                None
            } else {
                Some(tool_name.clone())
            },
        })),
    }
}

fn to_llm_tools(tools: &[AgentTool]) -> Option<Vec<ChatCompletionTool>> {
    if tools.is_empty() {
        return None;
    }
    Some(
        tools
            .iter()
            .map(|t| ChatCompletionTool {
                tool_type: ToolType::Function,
                function: FunctionDefinition {
                    name: t.name.clone(),
                    description: if t.description.is_empty() {
                        None
                    } else {
                        Some(t.description.clone())
                    },
                    parameters: Some(t.parameters.clone()),
                    strict: None,
                },
            })
            .collect(),
    )
}

fn map_finish_reason(reason: Option<&FinishReason>, has_tools: bool, has_text: bool) -> StopReason {
    match reason {
        Some(FinishReason::Stop) => StopReason::Stop,
        Some(FinishReason::Length) => StopReason::Length,
        Some(FinishReason::ToolCalls) | Some(FinishReason::FunctionCall) => StopReason::ToolCalls,
        Some(FinishReason::ContentFilter) | Some(FinishReason::Other) => StopReason::Error,
        None if has_tools => StopReason::ToolCalls,
        None if has_text => StopReason::Stop,
        None => StopReason::Error,
    }
}

#[derive(Debug, Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
}

/// Pure streaming accumulator: text + tool_call fragments → assistant snapshot.
#[derive(Debug, Default)]
pub struct StreamAccumulator {
    text: String,
    tool_calls: BTreeMap<u32, PartialToolCall>,
    finish_reason: Option<FinishReason>,
    saw_choice: bool,
    stream_error: bool,
}

impl StreamAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_chunk(&mut self, chunk: &ChatCompletionChunk) {
        if chunk.choices.is_empty() {
            return;
        }
        self.saw_choice = true;
        for choice in &chunk.choices {
            self.apply_choice(choice);
        }
    }

    pub fn mark_stream_error(&mut self) {
        self.stream_error = true;
    }

    fn apply_choice(&mut self, choice: &StreamChoice) {
        if let Some(reason) = choice.finish_reason.clone() {
            self.finish_reason = Some(reason);
        }
        let delta = &choice.delta;
        if let Some(content) = &delta.content {
            self.text.push_str(content);
        }
        if let Some(tool_calls) = &delta.tool_calls {
            for tc in tool_calls {
                self.apply_tool_delta(tc);
            }
        }
    }

    fn apply_tool_delta(&mut self, tc: &StreamToolCall) {
        let entry = self.tool_calls.entry(tc.index).or_default();
        if let Some(id) = &tc.id {
            entry.id = id.clone();
        }
        if let Some(function) = &tc.function {
            if let Some(name) = &function.name {
                entry.name = name.clone();
            }
            if let Some(args) = &function.arguments {
                entry.arguments.push_str(args);
            }
        }
    }

    pub fn snapshot(&self, stop_reason: StopReason) -> AgentMessage {
        AgentMessage::Assistant {
            parts: self.parts(),
            stop_reason,
        }
    }

    /// Finalize assistant after stream ends (or on error / empty).
    pub fn finalize(&self) -> AgentMessage {
        if self.stream_error || !self.saw_choice {
            return AgentMessage::Assistant {
                parts: self.parts(),
                stop_reason: StopReason::Error,
            };
        }
        let has_tools = !self.tool_calls.is_empty();
        let has_text = !self.text.is_empty();
        let stop_reason = map_finish_reason(self.finish_reason.as_ref(), has_tools, has_text);
        AgentMessage::Assistant {
            parts: self.parts(),
            stop_reason,
        }
    }

    fn parts(&self) -> Vec<ContentPart> {
        let mut parts = Vec::new();
        if !self.text.is_empty() {
            parts.push(ContentPart::Text {
                text: self.text.clone(),
            });
        }
        for tc in self.tool_calls.values() {
            let arguments = serde_json::from_str(&tc.arguments)
                .unwrap_or_else(|_| serde_json::Value::String(tc.arguments.clone()));
            parts.push(ContentPart::ToolCall {
                id: tc.id.clone(),
                name: tc.name.clone(),
                arguments,
            });
        }
        parts
    }
}

fn empty_partial() -> AgentMessage {
    AgentMessage::Assistant {
        parts: vec![],
        stop_reason: StopReason::Stop,
    }
}

async fn consume_stream<S>(
    mut stream: S,
    emit: &Emit,
    cancel: &CancellationToken,
) -> AgentMessage
where
    S: futures::Stream<Item = liter_llm::error::Result<ChatCompletionChunk>> + Unpin,
{
    let mut acc = StreamAccumulator::new();
    let start = empty_partial();
    (emit)(AgentEvent::MessageStart {
        message: start.clone(),
    });

    loop {
        if cancel.is_cancelled() {
            let message = AgentMessage::Assistant {
                parts: acc.parts(),
                stop_reason: StopReason::Aborted,
            };
            (emit)(AgentEvent::MessageEnd {
                message: message.clone(),
            });
            return message;
        }

        match stream.next().await {
            None => break,
            Some(Err(err)) => {
                eprintln!("LLM stream chunk error: {err}");
                acc.mark_stream_error();
                break;
            }
            Some(Ok(chunk)) => {
                acc.apply_chunk(&chunk);
                let snap = acc.snapshot(StopReason::Stop);
                (emit)(AgentEvent::MessageUpdate { message: snap });
            }
        }
    }

    let message = acc.finalize();
    (emit)(AgentEvent::MessageEnd {
        message: message.clone(),
    });
    message
}

#[async_trait]
impl<C: LlmClient> AgentBackend for LiterBackend<C> {
    async fn stream_assistant(
        &self,
        ctx: &mut AgentContext,
        emit: Emit,
        cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError> {
        let request = ChatCompletionRequest {
            model: self.model.clone(),
            messages: to_llm_messages(&ctx.messages),
            tools: to_llm_tools(&ctx.tools),
            ..Default::default()
        };

        let message = match self.client.chat_stream(request).await {
            Ok(stream) => consume_stream(stream, &emit, cancel).await,
            Err(err) => {
                eprintln!("LLM request failed: {err}");
                let message = AgentMessage::Assistant {
                    parts: vec![ContentPart::Text {
                        text: format!("LLM request failed: {err}"),
                    }],
                    stop_reason: StopReason::Error,
                };
                (emit)(AgentEvent::MessageStart {
                    message: message.clone(),
                });
                (emit)(AgentEvent::MessageEnd {
                    message: message.clone(),
                });
                message
            }
        };

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
            args: args.clone(),
        });

        let outcome = dispatch_tool(&self.executors, &tool_name, &args);

        (emit)(AgentEvent::ToolExecutionEnd {
            tool_call_id: tool_call_id.clone(),
            result: outcome.content.clone(),
            is_error: outcome.is_error,
        });

        Ok(AgentMessage::ToolResult {
            tool_call_id,
            tool_name,
            content: outcome.content,
            is_error: outcome.is_error,
            terminate: outcome.terminate,
        })
    }
}

fn dispatch_tool(
    executors: &HashMap<String, Arc<dyn ToolExecutor>>,
    tool_name: &str,
    args: &serde_json::Value,
) -> ToolExecResult {
    match executors.get(tool_name) {
        Some(executor) => executor.execute(args),
        None => ToolExecResult {
            content: format!("tool not registered: {tool_name}"),
            is_error: true,
            terminate: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liter_llm::types::{StreamDelta, StreamFunctionCall};

    #[test]
    fn to_llm_messages_maps_user_assistant_tool() {
        let messages = vec![
            AgentMessage::User {
                content: "hello".into(),
            },
            AgentMessage::Assistant {
                parts: vec![
                    ContentPart::Text {
                        text: "calling".into(),
                    },
                    ContentPart::ToolCall {
                        id: "call-1".into(),
                        name: "echo".into(),
                        arguments: serde_json::json!({"x": 1}),
                    },
                ],
                stop_reason: StopReason::ToolCalls,
            },
            AgentMessage::ToolResult {
                tool_call_id: "call-1".into(),
                tool_name: "echo".into(),
                content: "ok".into(),
                is_error: false,
                terminate: false,
            },
        ];

        let llm = to_llm_messages(&messages);
        assert_eq!(llm.len(), 3);

        match &llm[0] {
            LlmMessage::User(u) => {
                assert_eq!(u.content.as_text().as_deref(), Some("hello"));
            }
            other => panic!("expected user, got {other:?}"),
        }

        match &llm[1] {
            LlmMessage::Assistant(a) => {
                assert_eq!(a.text().as_deref(), Some("calling"));
                let calls = a.tool_calls.as_ref().expect("tool_calls");
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call-1");
                assert_eq!(calls[0].function.name, "echo");
                assert_eq!(calls[0].function.arguments, r#"{"x":1}"#);
            }
            other => panic!("expected assistant, got {other:?}"),
        }

        match &llm[2] {
            LlmMessage::Tool(t) => {
                assert_eq!(t.tool_call_id, "call-1");
                assert_eq!(t.name.as_deref(), Some("echo"));
                assert_eq!(t.content.as_text().as_deref(), Some("ok"));
            }
            other => panic!("expected tool, got {other:?}"),
        }
    }

    fn text_chunk(content: &str) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: "c".into(),
            object: "chat.completion.chunk".into(),
            created: 0,
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    content: Some(content.into()),
                    ..Default::default()
                },
                finish_reason: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    fn finish_chunk(reason: FinishReason) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: "c".into(),
            object: "chat.completion.chunk".into(),
            created: 0,
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta::default(),
                finish_reason: Some(reason),
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    #[test]
    fn accumulate_text_deltas_builds_assistant() {
        let mut acc = StreamAccumulator::new();
        acc.apply_chunk(&text_chunk("Hel"));
        acc.apply_chunk(&text_chunk("lo"));
        acc.apply_chunk(&finish_chunk(FinishReason::Stop));

        let msg = acc.finalize();
        match msg {
            AgentMessage::Assistant { parts, stop_reason } => {
                assert_eq!(stop_reason, StopReason::Stop);
                assert_eq!(
                    parts,
                    vec![ContentPart::Text {
                        text: "Hello".into()
                    }]
                );
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn accumulate_tool_call_fragments() {
        let mut acc = StreamAccumulator::new();
        acc.apply_chunk(&ChatCompletionChunk {
            id: "c".into(),
            object: "chat.completion.chunk".into(),
            created: 0,
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    tool_calls: Some(vec![StreamToolCall {
                        index: 0,
                        id: Some("call-9".into()),
                        call_type: Some(ToolType::Function),
                        function: Some(StreamFunctionCall {
                            name: Some("echo".into()),
                            arguments: Some(r#"{"a":"#.into()),
                        }),
                    }]),
                    ..Default::default()
                },
                finish_reason: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        });
        acc.apply_chunk(&ChatCompletionChunk {
            id: "c".into(),
            object: "chat.completion.chunk".into(),
            created: 0,
            model: "m".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta: StreamDelta {
                    tool_calls: Some(vec![StreamToolCall {
                        index: 0,
                        id: None,
                        call_type: None,
                        function: Some(StreamFunctionCall {
                            name: None,
                            arguments: Some(r#"1}"#.into()),
                        }),
                    }]),
                    ..Default::default()
                },
                finish_reason: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        });
        acc.apply_chunk(&finish_chunk(FinishReason::ToolCalls));

        let msg = acc.finalize();
        match msg {
            AgentMessage::Assistant { parts, stop_reason } => {
                assert_eq!(stop_reason, StopReason::ToolCalls);
                assert_eq!(
                    parts,
                    vec![ContentPart::ToolCall {
                        id: "call-9".into(),
                        name: "echo".into(),
                        arguments: serde_json::json!({"a": 1}),
                    }]
                );
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn accumulate_empty_stream_is_error() {
        let acc = StreamAccumulator::new();
        let msg = acc.finalize();
        assert!(matches!(
            msg,
            AgentMessage::Assistant {
                stop_reason: StopReason::Error,
                ..
            }
        ));
    }

    #[test]
    fn echo_executor_returns_message_field() {
        let result = echo_executor(&serde_json::json!({"message": "hi"}));
        assert_eq!(result.content, "hi");
        assert!(!result.is_error);
        assert!(!result.terminate);
    }

    #[test]
    fn echo_executor_falls_back_to_json() {
        let result = echo_executor(&serde_json::json!({"x": 1}));
        assert_eq!(result.content, r#"{"x":1}"#);
        assert!(!result.is_error);
    }

    #[test]
    fn dispatch_tool_runs_echo_or_reports_unregistered() {
        let mut map = HashMap::new();
        map.insert(
            "echo".to_string(),
            Arc::new(echo_executor) as Arc<dyn ToolExecutor>,
        );
        let ok = dispatch_tool(&map, "echo", &serde_json::json!({"message": "ping"}));
        assert_eq!(ok.content, "ping");
        assert!(!ok.is_error);

        let missing = dispatch_tool(&map, "unknown", &serde_json::json!({}));
        assert!(missing.is_error);
        assert!(missing.content.contains("not registered"));
    }
}
