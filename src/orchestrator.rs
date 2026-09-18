use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use crate::follow_up::FollowUpPolicy;
use crate::llm::{LlmChunk, LlmPort, ToolCall};
use crate::run::{RunHandle, WaitError};
use crate::wire::{MessageSource, Role, SseEvent, ToolCallWire, ToolDef, WireMessage};

pub use crate::sse::SseTx;

pub const MAX_FOLLOW_UP_ROUNDS: u32 = 8;

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("llm error: {0}")]
    Llm(String),
    #[error("tool wait timed out")]
    ToolTimeout,
    #[error("run cancelled")]
    Cancelled,
    #[error("follow-up round limit exceeded")]
    FollowUpLimit,
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

    let mut follow_up_rounds = 0u32;
    loop {
        loop {
            drain_steer(&run, &mut context, &sse, &run_id).await?;
            if let Err(err) = ensure_not_cancelled(&run) {
                let _ = emit_cancelled(&sse, &run_id, &run).await;
                return Err(err);
            }

            let tool_calls = match stream_llm(&run, &llm, &mut context, &tools, &sse, &run_id).await
            {
                Ok(calls) => calls,
                Err(OrchestratorError::Cancelled) => {
                    let _ = emit_cancelled(&sse, &run_id, &run).await;
                    return Err(OrchestratorError::Cancelled);
                }
                Err(err) => {
                    let _ = emit_error(&sse, &run_id, err.to_string(), error_code(&err), &run).await;
                    return Err(err);
                }
            };

            if tool_calls.is_empty() {
                if drain_steer(&run, &mut context, &sse, &run_id).await? {
                    continue;
                }
                break;
            }

            for tc in tool_calls {
                let rx = match run.begin_wait_tool(tc.id.clone()) {
                    Ok(rx) => rx,
                    Err(WaitError::Cancelled) => {
                        let _ = emit_cancelled(&sse, &run_id, &run).await;
                        return Err(OrchestratorError::Cancelled);
                    }
                    Err(WaitError::Timeout) => {
                        let _ = emit_error(
                            &sse,
                            &run_id,
                            "tool wait timed out".into(),
                            Some("timeout"),
                            &run,
                        )
                        .await;
                        return Err(OrchestratorError::ToolTimeout);
                    }
                };

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

                let result = match run.recv_tool(rx, tool_timeout).await {
                    Ok(result) => result,
                    Err(WaitError::Timeout) => {
                        let _ = emit_error(
                            &sse,
                            &run_id,
                            "tool wait timed out".into(),
                            Some("timeout"),
                            &run,
                        )
                        .await;
                        return Err(OrchestratorError::ToolTimeout);
                    }
                    Err(WaitError::Cancelled) => {
                        let _ = emit_cancelled(&sse, &run_id, &run).await;
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

        if let Err(err) = ensure_not_cancelled(&run) {
            let _ = emit_cancelled(&sse, &run_id, &run).await;
            return Err(err);
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
            run.finish();
            return Ok(());
        }

        if follow_up_rounds >= MAX_FOLLOW_UP_ROUNDS {
            let err = OrchestratorError::FollowUpLimit;
            let _ = emit_error(&sse, &run_id, err.to_string(), error_code(&err), &run).await;
            return Err(err);
        }
        follow_up_rounds += 1;

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

fn ensure_not_cancelled(run: &RunHandle) -> Result<(), OrchestratorError> {
    if run.is_cancelled() {
        Err(OrchestratorError::Cancelled)
    } else {
        Ok(())
    }
}

async fn drain_steer(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    sse: &SseTx,
    run_id: &str,
) -> Result<bool, OrchestratorError> {
    let msgs = run.drain_steer();
    let any = !msgs.is_empty();
    for msg in msgs {
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
    Ok(any)
}

async fn stream_llm(
    run: &RunHandle,
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
        if run.is_cancelled() {
            return Err(OrchestratorError::Cancelled);
        }
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

    if run.is_cancelled() {
        return Err(OrchestratorError::Cancelled);
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
        OrchestratorError::FollowUpLimit => Some("follow_up_limit"),
        OrchestratorError::SseClosed => None,
    }
}

async fn emit(sse: &SseTx, event: SseEvent) -> Result<(), OrchestratorError> {
    sse.send(event)
        .await
        .map_err(|_| OrchestratorError::SseClosed)
}

async fn emit_cancelled(
    sse: &SseTx,
    run_id: &str,
    run: &RunHandle,
) -> Result<(), OrchestratorError> {
    emit(
        sse,
        SseEvent::RunFinished {
            run_id: run_id.to_string(),
            reason: "cancelled".into(),
        },
    )
    .await?;
    run.finish();
    Ok(())
}

async fn emit_error(
    sse: &SseTx,
    run_id: &str,
    message: String,
    code: Option<&str>,
    run: &RunHandle,
) -> Result<(), OrchestratorError> {
    emit(
        sse,
        SseEvent::Error {
            run_id: run_id.to_string(),
            message,
            code: code.map(str::to_string),
        },
    )
    .await?;
    run.finish();
    Ok(())
}
