use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, Stream};
use liter_llm::{
    AssistantContent, AssistantMessage, ChatCompletionChunk, ChatCompletionRequest,
    ChatCompletionTool, ClientConfigBuilder, DefaultClient, FunctionCall, FunctionDefinition,
    LlmClient, Message, SystemMessage, ToolMessage, ToolType, UserMessage,
};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::AppConfig;
use crate::protocol::{Role, ToolDef, WireMessage};

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
    Fail {
        message: String,
    },
}

pub struct MockLlm {
    script: Mutex<Vec<MockTurn>>,
    repeat: Option<MockTurn>,
    recorded: std::sync::Mutex<Vec<Vec<WireMessage>>>,
}

impl MockLlm {
    pub fn script(turns: Vec<MockTurn>) -> Self {
        Self {
            script: Mutex::new(turns),
            repeat: None,
            recorded: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn script_then_repeat(turns: Vec<MockTurn>, repeat: MockTurn) -> Self {
        Self {
            script: Mutex::new(turns),
            repeat: Some(repeat),
            recorded: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn recorded_contexts(&self) -> Vec<Vec<WireMessage>> {
        self.recorded.lock().expect("recorded mutex").clone()
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
            MockTurn::Fail { .. } => {
                unreachable!("Fail is handled in stream() before turn_to_chunks")
            }
        }
    }
}

#[async_trait]
impl LlmPort for MockLlm {
    async fn stream(
        &self,
        messages: &[WireMessage],
        _tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String> {
        self.recorded
            .lock()
            .expect("recorded mutex")
            .push(messages.to_vec());
        let turn = {
            let mut script = self.script.lock().await;
            if !script.is_empty() {
                script.remove(0)
            } else if let Some(repeat) = &self.repeat {
                repeat.clone()
            } else {
                return Err("mock LLM script exhausted".to_string());
            }
        };
        if let MockTurn::Fail { message } = turn {
            return Err(message);
        }
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

pub struct LiterAdapter {
    client: DefaultClient,
    model: String,
}

impl LiterAdapter {
    pub fn from_config(config: &AppConfig) -> Result<Self, String> {
        let mut builder = ClientConfigBuilder::new(config.api_key.clone()).load_env(false);
        if let Some(url) = &config.base_url {
            builder = builder.base_url(url.clone());
        }
        let client = DefaultClient::new(builder.build(), Some(config.model.as_str()))
            .map_err(|err| err.to_string())?;
        Ok(Self {
            client,
            model: config.model.clone(),
        })
    }
}

#[async_trait]
impl LlmPort for LiterAdapter {
    async fn stream(
        &self,
        messages: &[WireMessage],
        tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String> {
        let tools = tool_defs_to_liter(tools);
        let req = ChatCompletionRequest {
            model: self.model.clone(),
            messages: wire_messages_to_liter(messages),
            tools: if tools.is_empty() { None } else { Some(tools) },
            ..Default::default()
        };
        let liter_stream = self
            .client
            .chat_stream(req)
            .await
            .map_err(|err| err.to_string())?;
        Ok(Box::pin(MappedLiterStream {
            inner: liter_stream,
            acc: StreamAcc::default(),
            pending: VecDeque::new(),
            ended: false,
        }))
    }
}

fn wire_messages_to_liter(messages: &[WireMessage]) -> Vec<Message> {
    messages.iter().map(wire_to_liter).collect()
}

fn wire_to_liter(msg: &WireMessage) -> Message {
    match msg.role {
        Role::System => Message::System(SystemMessage {
            content: msg.content.clone().into(),
            name: msg.name.clone(),
        }),
        Role::User => Message::User(UserMessage {
            content: msg.content.clone().into(),
            name: msg.name.clone(),
        }),
        Role::Assistant => Message::Assistant(AssistantMessage {
            content: Some(AssistantContent::Text(msg.content.clone())),
            name: msg.name.clone(),
            tool_calls: msg.tool_calls.as_ref().map(|calls| {
                calls
                    .iter()
                    .map(|tc| liter_llm::ToolCall {
                        id: tc.id.clone(),
                        call_type: ToolType::Function,
                        function: FunctionCall {
                            name: tc.name.clone(),
                            arguments: tc.arguments.to_string(),
                        },
                    })
                    .collect()
            }),
            refusal: None,
            function_call: None,
            reasoning_content: None,
        }),
        Role::Tool => Message::Tool(ToolMessage {
            content: msg.content.clone().into(),
            tool_call_id: msg.tool_call_id.clone().unwrap_or_default(),
            name: msg.name.clone(),
        }),
    }
}

fn tool_defs_to_liter(tools: &[ToolDef]) -> Vec<ChatCompletionTool> {
    tools
        .iter()
        .map(|tool| ChatCompletionTool {
            tool_type: ToolType::Function,
            function: FunctionDefinition {
                name: tool.name.clone(),
                description: Some(tool.description.clone()),
                parameters: Some(tool.parameters.clone()),
                strict: None,
            },
        })
        .collect()
}

#[derive(Default)]
struct PendingTool {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Default)]
struct StreamAcc {
    content: String,
    tools: BTreeMap<u32, PendingTool>,
    completed: bool,
}

impl StreamAcc {
    fn apply(&mut self, chunk: &ChatCompletionChunk) -> Vec<LlmChunk> {
        let mut out = Vec::new();
        for choice in &chunk.choices {
            if let Some(text) = &choice.delta.content
                && !text.is_empty()
            {
                self.content.push_str(text);
                out.push(LlmChunk::TextDelta(text.clone()));
            }
            if let Some(tool_calls) = &choice.delta.tool_calls {
                for stc in tool_calls {
                    let pending = self.tools.entry(stc.index).or_default();
                    if let Some(id) = &stc.id {
                        pending.id = id.clone();
                    }
                    if let Some(function) = &stc.function {
                        if let Some(name) = &function.name {
                            pending.name = name.clone();
                        }
                        if let Some(args) = &function.arguments {
                            pending.arguments.push_str(args);
                        }
                    }
                }
            }
            if choice.finish_reason.is_some() && !self.completed {
                self.completed = true;
                out.push(self.completed_chunk());
            }
        }
        out
    }

    fn finish(&mut self) -> Option<LlmChunk> {
        if self.completed {
            None
        } else {
            self.completed = true;
            Some(self.completed_chunk())
        }
    }

    fn completed_chunk(&self) -> LlmChunk {
        LlmChunk::Completed {
            content: self.content.clone(),
            tool_calls: self
                .tools
                .values()
                .map(|tool| ToolCall {
                    id: tool.id.clone(),
                    name: tool.name.clone(),
                    arguments: parse_tool_arguments(&tool.arguments),
                })
                .collect(),
        }
    }
}

fn parse_tool_arguments(raw: &str) -> Value {
    if raw.is_empty() {
        return Value::Object(serde_json::Map::new());
    }
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

struct MappedLiterStream {
    inner: liter_llm::BoxStream<'static, liter_llm::Result<ChatCompletionChunk>>,
    acc: StreamAcc,
    pending: VecDeque<Result<LlmChunk, String>>,
    ended: bool,
}

impl Stream for MappedLiterStream {
    type Item = Result<LlmChunk, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(item) = this.pending.pop_front() {
                return Poll::Ready(Some(item));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.ended = true;
                    if let Some(chunk) = this.acc.finish() {
                        this.pending.push_back(Ok(chunk));
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(err.to_string())));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    this.pending
                        .extend(this.acc.apply(&chunk).into_iter().map(Ok));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::protocol::{Role, ToolCallWire};
    use liter_llm::{
        ChatCompletionChunk, FinishReason, Message, StreamChoice, StreamDelta, StreamFunctionCall,
        StreamToolCall,
    };
    use serde_json::json;

    fn wire(role: Role, content: &str) -> WireMessage {
        WireMessage {
            role,
            content: content.into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    fn chunk(delta: StreamDelta, finish: Option<FinishReason>) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: "c".into(),
            object: "chat.completion.chunk".into(),
            created: 0,
            model: "test".into(),
            choices: vec![StreamChoice {
                index: 0,
                delta,
                finish_reason: finish,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    #[test]
    fn maps_wire_roles_to_liter_messages() {
        let messages = [
            wire(Role::System, "sys"),
            wire(Role::User, "hi"),
            wire(Role::Assistant, "ok"),
            WireMessage {
                role: Role::Tool,
                content: "result".into(),
                tool_call_id: Some("call_1".into()),
                name: Some("echo".into()),
                tool_calls: None,
            },
        ];
        let mapped = wire_messages_to_liter(&messages);
        assert_eq!(mapped.len(), 4);
        match &mapped[0] {
            Message::System(m) => assert_eq!(m.content.as_text().as_deref(), Some("sys")),
            other => panic!("expected System, got {other:?}"),
        }
        match &mapped[1] {
            Message::User(m) => assert_eq!(m.content.as_text().as_deref(), Some("hi")),
            other => panic!("expected User, got {other:?}"),
        }
        match &mapped[2] {
            Message::Assistant(m) => {
                assert_eq!(m.text().as_deref(), Some("ok"));
                assert!(m.tool_calls.is_none());
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
        match &mapped[3] {
            Message::Tool(m) => {
                assert_eq!(m.tool_call_id, "call_1");
                assert_eq!(m.name.as_deref(), Some("echo"));
                assert_eq!(m.content.as_text().as_deref(), Some("result"));
            }
            other => panic!("expected Tool, got {other:?}"),
        }
    }

    #[test]
    fn maps_assistant_tool_calls_to_liter() {
        let messages = [WireMessage {
            role: Role::Assistant,
            content: "calling echo".into(),
            tool_call_id: None,
            name: None,
            tool_calls: Some(vec![ToolCallWire {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({ "msg": "hi" }),
            }]),
        }];
        let mapped = wire_messages_to_liter(&messages);
        match &mapped[0] {
            Message::Assistant(m) => {
                let calls = m.tool_calls.as_ref().expect("assistant tool_calls");
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_1");
                assert_eq!(calls[0].function.name, "echo");
                let args: Value =
                    serde_json::from_str(&calls[0].function.arguments).expect("json args");
                assert_eq!(args, json!({ "msg": "hi" }));
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
    }

    #[test]
    fn maps_tool_defs_to_liter_functions() {
        let tools = [ToolDef {
            name: "echo".into(),
            description: "repeat".into(),
            parameters: json!({ "type": "object" }),
        }];
        let mapped = tool_defs_to_liter(&tools);
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].function.name, "echo");
        assert_eq!(mapped[0].function.description.as_deref(), Some("repeat"));
        assert_eq!(
            mapped[0].function.parameters,
            Some(json!({ "type": "object" }))
        );
    }

    #[test]
    fn maps_text_deltas_then_completed() {
        let mut acc = StreamAcc::default();
        let first = acc.apply(&chunk(
            StreamDelta {
                content: Some("Hel".into()),
                ..Default::default()
            },
            None,
        ));
        let second = acc.apply(&chunk(
            StreamDelta {
                content: Some("lo".into()),
                ..Default::default()
            },
            Some(FinishReason::Stop),
        ));
        assert_eq!(first, vec![LlmChunk::TextDelta("Hel".into())]);
        assert_eq!(
            second,
            vec![
                LlmChunk::TextDelta("lo".into()),
                LlmChunk::Completed {
                    content: "Hello".into(),
                    tool_calls: vec![],
                },
            ]
        );
        assert!(acc.finish().is_none());
    }

    #[test]
    fn maps_streamed_tool_calls_and_flushes_if_no_finish() {
        let mut acc = StreamAcc::default();
        let start = acc.apply(&chunk(
            StreamDelta {
                tool_calls: Some(vec![StreamToolCall {
                    index: 0,
                    id: Some("call_1".into()),
                    call_type: None,
                    function: Some(StreamFunctionCall {
                        name: Some("echo".into()),
                        arguments: Some("{\"msg\":".into()),
                    }),
                }]),
                ..Default::default()
            },
            None,
        ));
        let more = acc.apply(&chunk(
            StreamDelta {
                tool_calls: Some(vec![StreamToolCall {
                    index: 0,
                    id: None,
                    call_type: None,
                    function: Some(StreamFunctionCall {
                        name: None,
                        arguments: Some("\"hi\"}".into()),
                    }),
                }]),
                ..Default::default()
            },
            None,
        ));
        assert!(start.is_empty());
        assert!(more.is_empty());
        match acc.finish() {
            Some(LlmChunk::Completed {
                content,
                tool_calls,
            }) => {
                assert_eq!(content, "");
                assert_eq!(tool_calls.len(), 1);
                assert_eq!(tool_calls[0].id, "call_1");
                assert_eq!(tool_calls[0].name, "echo");
                assert_eq!(tool_calls[0].arguments, json!({ "msg": "hi" }));
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    fn from_config_builds_adapter_without_network() {
        let config = AppConfig {
            api_key: "sk-test".into(),
            base_url: Some("https://example.invalid/v1".into()),
            model: "gpt-4o-mini".into(),
            listen: "127.0.0.1:8080".into(),
            tool_timeout_secs: 60,
            follow_up_policy: "noop".into(),
            log_level: "info".into(),
            persist: Default::default(),
            context: Default::default(),
        };
        LiterAdapter::from_config(&config).expect("client construction should not hit the network");
    }
}
