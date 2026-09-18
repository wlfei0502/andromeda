use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use crate::follow_up::FollowUpPolicy;
use crate::llm::{LlmChunk, LlmPort, ToolCall};
use crate::run::{RunHandle, WaitError};
use crate::wire::{MessageSource, Role, SseEvent, ToolCallWire, ToolDef, WireMessage};

pub use crate::sse::SseTx;

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("llm error: {0}")]
    Llm(String),
    #[error("tool wait timed out")]
    ToolTimeout,
    #[error("run cancelled")]
    Cancelled,
    #[error("sse channel closed")]
    SseClosed,
}

pub async fn run_agent(
    run: RunHandle,
    mut context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    sse: SseTx,
    tool_timeout: Duration,
) -> Result<(), OrchestratorError> {
    let run_id = run.id().0.clone();
    emit(
        &sse,
        SseEvent::RunStarted {
            run_id: run_id.clone(),
        },
    )
    .await?;

    loop {
        loop {
            drain_steer(&run, &mut context, &sse, &run_id).await?;

            let tool_calls = match stream_llm(&llm, &mut context, &tools, &sse, &run_id).await {
                Ok(calls) => calls,
                Err(err) => {
                    let _ = emit_error(&sse, &run_id, err.to_string(), error_code(&err)).await;
                    return Err(err);
                }
            };

            if tool_calls.is_empty() {
                break;
            }

            for tc in tool_calls {
                emit(
                    &sse,
                    SseEvent::ToolRequest {
                        run_id: run_id.clone(),
                        tool_call_id: tc.id.clone(),
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                    },
                )
                .await?;

                let result = match run.wait_tool(tc.id.clone(), tool_timeout).await {
                    Ok(result) => result,
                    Err(WaitError::Timeout) => {
                        let _ = emit_error(
                            &sse,
                            &run_id,
                            "tool wait timed out".into(),
                            Some("timeout"),
                        )
                        .await;
                        return Err(OrchestratorError::ToolTimeout);
                    }
                    Err(WaitError::Cancelled) => {
                        let _ = emit(
                            &sse,
                            SseEvent::RunFinished {
                                run_id: run_id.clone(),
                                reason: "cancelled".into(),
                            },
                        )
                        .await;
                        return Err(OrchestratorError::Cancelled);
                    }
                };

                let content = if result.is_error {
                    format!("tool_error:{}", result.content)
                } else {
                    result.content
                };
                context.push(WireMessage {
                    role: Role::Tool,
                    content,
                    tool_call_id: Some(result.tool_call_id),
                    name: Some(tc.name),
                });
            }
        }

        let follow_ups = follow_up.next(&context);
        if follow_ups.is_empty() {
            emit(
                &sse,
                SseEvent::RunFinished {
                    run_id: run_id.clone(),
                    reason: "stop".into(),
                },
            )
            .await?;
            return Ok(());
        }

        for msg in follow_ups {
            context.push(msg.clone());
            emit(
                &sse,
                SseEvent::MessageCompleted {
                    run_id: run_id.clone(),
                    message_id: uuid::Uuid::new_v4().to_string(),
                    role: msg.role,
                    content: msg.content,
                    tool_calls: None,
                    source: Some(MessageSource::FollowUp),
                },
            )
            .await?;
        }
    }
}

async fn drain_steer(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    sse: &SseTx,
    run_id: &str,
) -> Result<(), OrchestratorError> {
    for msg in run.drain_steer() {
        context.push(msg.clone());
        emit(
            sse,
            SseEvent::MessageCompleted {
                run_id: run_id.to_string(),
                message_id: uuid::Uuid::new_v4().to_string(),
                role: msg.role,
                content: msg.content,
                tool_calls: None,
                source: Some(MessageSource::Steer),
            },
        )
        .await?;
    }
    Ok(())
}

async fn stream_llm(
    llm: &Arc<dyn LlmPort>,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    sse: &SseTx,
    run_id: &str,
) -> Result<Vec<ToolCall>, OrchestratorError> {
    let mut stream = llm
        .stream(context, tools)
        .await
        .map_err(OrchestratorError::Llm)?;

    let message_id = uuid::Uuid::new_v4().to_string();
    let mut completed_content = None;
    let mut tool_calls = Vec::new();

    while let Some(chunk) = stream.next().await {
        match chunk.map_err(OrchestratorError::Llm)? {
            LlmChunk::TextDelta(delta) => {
                emit(
                    sse,
                    SseEvent::MessageDelta {
                        run_id: run_id.to_string(),
                        message_id: message_id.clone(),
                        delta,
                    },
                )
                .await?;
            }
            LlmChunk::Completed {
                content,
                tool_calls: calls,
            } => {
                completed_content = Some(content);
                tool_calls = calls;
            }
        }
    }

    let Some(content) = completed_content else {
        return Err(OrchestratorError::Llm(
            "llm stream ended without completed chunk".into(),
        ));
    };

    let tool_calls_wire: Vec<ToolCallWire> = tool_calls
        .iter()
        .map(|tc| ToolCallWire {
            id: tc.id.clone(),
            name: tc.name.clone(),
            arguments: tc.arguments.clone(),
        })
        .collect();

    emit(
        sse,
        SseEvent::MessageCompleted {
            run_id: run_id.to_string(),
            message_id,
            role: Role::Assistant,
            content: content.clone(),
            tool_calls: if tool_calls_wire.is_empty() {
                None
            } else {
                Some(tool_calls_wire)
            },
            source: Some(MessageSource::Assistant),
        },
    )
    .await?;

    context.push(WireMessage {
        role: Role::Assistant,
        content,
        tool_call_id: None,
        name: None,
    });
    Ok(tool_calls)
}

fn error_code(err: &OrchestratorError) -> Option<&'static str> {
    match err {
        OrchestratorError::Llm(_) => Some("llm"),
        OrchestratorError::ToolTimeout => Some("timeout"),
        OrchestratorError::Cancelled => Some("cancelled"),
        OrchestratorError::SseClosed => None,
    }
}

async fn emit(sse: &SseTx, event: SseEvent) -> Result<(), OrchestratorError> {
    sse.send(event)
        .await
        .map_err(|_| OrchestratorError::SseClosed)
}

async fn emit_error(
    sse: &SseTx,
    run_id: &str,
    message: String,
    code: Option<&str>,
) -> Result<(), OrchestratorError> {
    emit(
        sse,
        SseEvent::Error {
            run_id: run_id.to_string(),
            message,
            code: code.map(str::to_string),
        },
    )
    .await
}
