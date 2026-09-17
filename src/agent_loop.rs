use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::backend::{AgentBackend, Emit, TurnSnapshot};
use crate::error::LoopError;
use crate::event_stream::EventStream;
use crate::types::{AgentContext, AgentEvent, AgentMessage, ContentPart, StopReason};

/// Start a new agent loop, appending `prompts` to the context.
///
/// Pass `Some(token)` to cancel from the caller; `None` creates an internal token.
pub fn agent_loop(
    prompts: Vec<AgentMessage>,
    mut context: AgentContext,
    backend: Arc<dyn AgentBackend>,
    cancel: Option<CancellationToken>,
) -> Result<EventStream<AgentEvent, Vec<AgentMessage>>, LoopError> {
    let stream = create_agent_stream();
    let pusher = stream.pusher();
    let cancel = cancel.unwrap_or_else(CancellationToken::new);

    tokio::spawn(async move {
        let emit: Emit = Arc::new(move |event| {
            pusher.push(event);
        });

        let mut new_messages = Vec::new();

        (emit)(AgentEvent::AgentStart);
        (emit)(AgentEvent::TurnStart);

        for prompt in prompts {
            (emit)(AgentEvent::MessageStart {
                message: prompt.clone(),
            });
            (emit)(AgentEvent::MessageEnd {
                message: prompt.clone(),
            });
            context.messages.push(prompt.clone());
            new_messages.push(prompt);
        }

        run_loop(context, new_messages, backend, emit, cancel).await;
    });

    Ok(stream)
}

/// Continue an existing context without adding a new prompt.
///
/// Fails if the context is empty or the last message is an assistant.
/// Pass `Some(token)` to cancel from the caller; `None` creates an internal token.
pub fn agent_loop_continue(
    context: AgentContext,
    backend: Arc<dyn AgentBackend>,
    cancel: Option<CancellationToken>,
) -> Result<EventStream<AgentEvent, Vec<AgentMessage>>, LoopError> {
    validate_continue(&context)?;

    let stream = create_agent_stream();
    let pusher = stream.pusher();
    let cancel = cancel.unwrap_or_else(CancellationToken::new);

    tokio::spawn(async move {
        let emit: Emit = Arc::new(move |event| {
            pusher.push(event);
        });

        (emit)(AgentEvent::AgentStart);
        (emit)(AgentEvent::TurnStart);

        run_loop(context, Vec::new(), backend, emit, cancel).await;
    });

    Ok(stream)
}

fn validate_continue(context: &AgentContext) -> Result<(), LoopError> {
    if context.messages.is_empty() {
        return Err(LoopError::InvalidContinue(
            "no messages in context".into(),
        ));
    }
    if matches!(
        context.messages.last(),
        Some(AgentMessage::Assistant { .. })
    ) {
        return Err(LoopError::InvalidContinue(
            "cannot continue from assistant message".into(),
        ));
    }
    Ok(())
}

fn create_agent_stream() -> EventStream<AgentEvent, Vec<AgentMessage>> {
    EventStream::new(
        |event: &AgentEvent| matches!(event, AgentEvent::AgentEnd { .. }),
        |event: &AgentEvent| match event {
            AgentEvent::AgentEnd { messages } => messages.clone(),
            _ => Vec::new(),
        },
    )
}

async fn run_loop(
    mut context: AgentContext,
    mut new_messages: Vec<AgentMessage>,
    backend: Arc<dyn AgentBackend>,
    emit: Emit,
    cancel: CancellationToken,
) {
    let mut pending = backend.get_steering().await;
    let mut last_turn: Option<TurnSnapshot> = None;

    // Outer loop: follow-up messages after the agent would otherwise stop.
    loop {
        let mut has_more_tools = true;

        // Inner loop: tool calls and steering.
        while has_more_tools || !pending.is_empty() {
            if last_turn.is_some() {
                (emit)(AgentEvent::TurnStart);
            }

            for message in pending.drain(..) {
                (emit)(AgentEvent::MessageStart {
                    message: message.clone(),
                });
                (emit)(AgentEvent::MessageEnd {
                    message: message.clone(),
                });
                context.messages.push(message.clone());
                new_messages.push(message);
            }

            let assistant = match backend
                .stream_assistant(&mut context, Arc::clone(&emit), &cancel)
                .await
            {
                Ok(message) => message,
                Err(_) => {
                    let stop_reason = if cancel.is_cancelled() {
                        StopReason::Aborted
                    } else {
                        StopReason::Error
                    };
                    let message = AgentMessage::Assistant {
                        parts: vec![],
                        stop_reason,
                    };
                    context.messages.push(message.clone());
                    new_messages.push(message.clone());
                    (emit)(AgentEvent::TurnEnd {
                        message,
                        tool_results: vec![],
                    });
                    (emit)(AgentEvent::AgentEnd {
                        messages: new_messages,
                    });
                    return;
                }
            };

            // Backend already appended the assistant into `context.messages`
            // (pi-style). Track it for this run's return set only.
            new_messages.push(assistant.clone());

            if matches!(
                &assistant,
                AgentMessage::Assistant {
                    stop_reason: StopReason::Error | StopReason::Aborted,
                    ..
                }
            ) {
                (emit)(AgentEvent::TurnEnd {
                    message: assistant,
                    tool_results: vec![],
                });
                (emit)(AgentEvent::AgentEnd {
                    messages: new_messages,
                });
                return;
            }

            let tool_calls: Vec<ContentPart> = match &assistant {
                AgentMessage::Assistant { parts, .. } => parts
                    .iter()
                    .filter(|p| matches!(p, ContentPart::ToolCall { .. }))
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };

            let mut tool_results = Vec::new();
            if tool_calls.is_empty() {
                has_more_tools = false;
            } else {
                let mut all_terminate = true;
                for call in &tool_calls {
                    let result = match backend
                        .execute_tool(call, Arc::clone(&emit), &cancel)
                        .await
                    {
                        Ok(msg) => msg,
                        Err(err) => error_tool_result(call, err),
                    };
                    if !is_terminate(&result) {
                        all_terminate = false;
                    }
                    context.messages.push(result.clone());
                    new_messages.push(result.clone());
                    // Design §2: after ToolExecutionEnd, emit toolResult message pair.
                    (emit)(AgentEvent::MessageStart {
                        message: result.clone(),
                    });
                    (emit)(AgentEvent::MessageEnd {
                        message: result.clone(),
                    });
                    tool_results.push(result);
                }
                has_more_tools = !all_terminate;
            }

            (emit)(AgentEvent::TurnEnd {
                message: assistant.clone(),
                tool_results: tool_results.clone(),
            });

            let snapshot = TurnSnapshot {
                message: assistant,
                tool_results,
                context_messages_len: context.messages.len(),
                new_messages: new_messages.clone(),
            };

            if backend.should_stop_after_turn(&snapshot).await {
                (emit)(AgentEvent::AgentEnd {
                    messages: new_messages,
                });
                return;
            }

            last_turn = Some(snapshot);
            pending = backend.get_steering().await;
        }

        let follow_ups = backend.get_follow_up().await;
        if !follow_ups.is_empty() {
            pending = follow_ups;
            continue;
        }
        break;
    }

    (emit)(AgentEvent::AgentEnd {
        messages: new_messages,
    });
}

fn is_terminate(message: &AgentMessage) -> bool {
    matches!(
        message,
        AgentMessage::ToolResult {
            terminate: true,
            ..
        }
    )
}

fn error_tool_result(call: &ContentPart, err: LoopError) -> AgentMessage {
    let (tool_call_id, tool_name) = match call {
        ContentPart::ToolCall { id, name, .. } => (id.clone(), name.clone()),
        _ => (String::new(), String::new()),
    };
    AgentMessage::ToolResult {
        tool_call_id,
        tool_name,
        content: err.to_string(),
        is_error: true,
        terminate: false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::StreamExt;
    use tokio_util::sync::CancellationToken;

    use super::{agent_loop, agent_loop_continue};
    use crate::backend::{AgentBackend, Emit};
    use crate::error::LoopError;
    use crate::types::{AgentContext, AgentEvent, AgentMessage, ContentPart, StopReason};

    /// Validation-only mock: never called. Real backends MUST append the
    /// assistant into `ctx.messages` before returning `Ok` from
    /// `stream_assistant` (see trait docs).
    struct RejectOnlyBackend;

    #[async_trait]
    impl AgentBackend for RejectOnlyBackend {
        async fn stream_assistant(
            &self,
            _ctx: &mut AgentContext,
            _emit: Emit,
            _cancel: &CancellationToken,
        ) -> Result<AgentMessage, LoopError> {
            unreachable!("continue validation must not start the loop")
        }

        async fn execute_tool(
            &self,
            _call: &ContentPart,
            _emit: Emit,
            _cancel: &CancellationToken,
        ) -> Result<AgentMessage, LoopError> {
            unreachable!("continue validation must not start the loop")
        }
    }

    /// Honors cancel: waits briefly, then Err if cancelled so the loop maps to Aborted.
    struct CancelAwareBackend;

    #[async_trait]
    impl AgentBackend for CancelAwareBackend {
        async fn stream_assistant(
            &self,
            _ctx: &mut AgentContext,
            _emit: Emit,
            cancel: &CancellationToken,
        ) -> Result<AgentMessage, LoopError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if cancel.is_cancelled() {
                return Err(LoopError::Backend("cancelled".into()));
            }
            unreachable!("expected cancel before stream_assistant returns")
        }

        async fn execute_tool(
            &self,
            _call: &ContentPart,
            _emit: Emit,
            _cancel: &CancellationToken,
        ) -> Result<AgentMessage, LoopError> {
            unreachable!("cancel test must not execute tools")
        }
    }

    #[tokio::test]
    async fn continue_rejects_empty_and_assistant_tail() {
        let empty = AgentContext::default();
        assert!(matches!(
            agent_loop_continue(empty, Arc::new(RejectOnlyBackend), None),
            Err(LoopError::InvalidContinue(_))
        ));

        let assistant_tail = AgentContext {
            messages: vec![AgentMessage::Assistant {
                parts: vec![ContentPart::Text {
                    text: "hi".into(),
                }],
                stop_reason: StopReason::Stop,
            }],
            tools: vec![],
        };
        assert!(matches!(
            agent_loop_continue(assistant_tail, Arc::new(RejectOnlyBackend), None),
            Err(LoopError::InvalidContinue(_))
        ));
    }

    #[tokio::test]
    async fn cancel_token_yields_aborted() {
        let cancel = CancellationToken::new();
        let stream = agent_loop(
            vec![AgentMessage::User {
                content: "hi".into(),
            }],
            AgentContext::default(),
            Arc::new(CancelAwareBackend),
            Some(cancel.clone()),
        )
        .unwrap();
        let handle = stream.result_handle();
        cancel.cancel();

        let mut events = vec![];
        let mut s = stream;
        while let Some(e) = s.next().await {
            events.push(e);
        }
        let messages = handle.await.unwrap();

        assert!(matches!(events.last(), Some(AgentEvent::AgentEnd { .. })));
        let last = messages.last().expect("expected assistant on abort");
        assert!(
            matches!(
                last,
                AgentMessage::Assistant {
                    stop_reason: StopReason::Aborted,
                    ..
                }
            ),
            "expected StopReason::Aborted, got {last:?}"
        );
    }
}
