use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use super::follow_up::FollowUpPolicy;
use super::middleware::{AgentMiddleware, MwCtx, run_before_llm};
use super::plan::{
    apply_write_todos, ensure_plan_nudge, inject_plan_tools, is_server_tool, tool_result_err,
    tool_result_ok,
};
use crate::llm::{LlmChunk, LlmPort, ToolCall};
use crate::protocol::{
    MessageSource, Role, SseEvent, TodoItem, ToolCallWire, ToolDef, WireMessage,
};
use crate::runtime::{RunHandle, WaitError};
use crate::store::{
    Checkpoint, GuardsSnapshot, PendingTool, RunStatus, RunStore, StoreError,
};

pub const MAX_FOLLOW_UP_ROUNDS: u32 = 8;

#[derive(Clone)]
pub struct RunPersist {
    pub store: Arc<dyn RunStore>,
    pub instance_id: String,
}

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
    #[error("lost run ownership (checkpoint conflict)")]
    OwnershipLost,
    #[error("checkpoint store error: {0}")]
    Store(String),
    #[error("context exceeded max_context_tokens")]
    ContextOverflow,
}

struct PersistSession {
    store: Option<Arc<dyn RunStore>>,
    instance_id: String,
    enabled: bool,
    revision: u64,
    guards: GuardsSnapshot,
    pending_tool: Option<PendingTool>,
    todos: Vec<TodoItem>,
    plan_mode: bool,
}

impl PersistSession {
    fn new(
        persist: Option<RunPersist>,
        enabled: bool,
        initial_revision: u64,
        plan_mode: bool,
        todos: Vec<TodoItem>,
    ) -> Self {
        let active = enabled && persist.is_some();
        Self {
            store: persist.as_ref().map(|p| p.store.clone()),
            instance_id: persist.map(|p| p.instance_id).unwrap_or_default(),
            enabled: active,
            revision: initial_revision,
            guards: GuardsSnapshot::new_now(),
            pending_tool: None,
            todos,
            plan_mode,
        }
    }

    async fn checkpoint(
        &mut self,
        run: &RunHandle,
        context: &[WireMessage],
        tools: &[ToolDef],
        status: RunStatus,
        pending_tool: Option<PendingTool>,
    ) -> Result<(), OrchestratorError> {
        if !self.enabled {
            self.pending_tool = pending_tool;
            return Ok(());
        }
        let Some(store) = self.store.as_ref() else {
            return Ok(());
        };
        if !run.persist_enabled().await {
            self.pending_tool = pending_tool;
            return Ok(());
        }

        self.guards.touch();
        let expected = self.revision;
        let next = expected + 1;
        let cp = Checkpoint {
            run_id: run.id().0.clone(),
            status,
            context: context.to_vec(),
            tools: tools.to_vec(),
            todos: self.todos.clone(),
            plan_mode: self.plan_mode,
            pending_tool: pending_tool.clone(),
            guards: self.guards.clone(),
            parent_run_id: None,
            owner_id: Some(self.instance_id.clone()),
            revision: next,
        };
        match store.save_cas(&cp, expected).await {
            Ok(()) => {
                self.revision = next;
                self.pending_tool = pending_tool;
                Ok(())
            }
            Err(StoreError::Conflict) => Err(OrchestratorError::OwnershipLost),
            Err(err) => Err(OrchestratorError::Store(err.to_string())),
        }
    }
}

pub async fn run_agent(
    run: RunHandle,
    context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    persist: Option<RunPersist>,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    plan_mode: bool,
) -> Result<(), OrchestratorError> {
    run_agent_with_options(
        run,
        context,
        tools,
        llm,
        follow_up,
        tool_timeout,
        persist,
        true,
        middlewares,
        plan_mode,
        Vec::new(),
    )
    .await
}

/// Like `run_agent`, but can skip `run.started` (cold resume / post-tool continue).
pub async fn run_agent_with_options(
    run: RunHandle,
    mut context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    persist: Option<RunPersist>,
    emit_started: bool,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    plan_mode: bool,
    initial_todos: Vec<TodoItem>,
) -> Result<(), OrchestratorError> {
    let run_id = run.id().0.clone();
    let enabled = run.persist_enabled().await;
    let initial_revision = match (&persist, enabled) {
        (Some(p), true) => p
            .store
            .load(&run_id)
            .await
            .ok()
            .flatten()
            .map(|cp| cp.revision)
            .unwrap_or(0),
        _ => 0,
    };
    let mut ps = PersistSession::new(
        persist,
        enabled,
        initial_revision,
        plan_mode,
        initial_todos,
    );

    if plan_mode {
        ensure_plan_nudge(&mut context);
    }

    if emit_started {
        emit(
            &run,
            SseEvent::RunStarted {
                run_id: run_id.clone(),
            },
        )
        .await;
    }

    run_agent_loop(
        run,
        &mut context,
        &tools,
        llm,
        follow_up,
        tool_timeout,
        &mut ps,
        &run_id,
        middlewares,
    )
    .await
}

/// After cold resume in `waiting_tool`: wait for the pending tool result, then continue the loop.
pub async fn continue_after_pending_tool(
    run: RunHandle,
    mut context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    pending: PendingTool,
    rx: tokio::sync::oneshot::Receiver<crate::protocol::ToolResultRequest>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    persist: Option<RunPersist>,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    plan_mode: bool,
    initial_todos: Vec<TodoItem>,
) -> Result<(), OrchestratorError> {
    let run_id = run.id().0.clone();
    let enabled = run.persist_enabled().await;
    let initial_revision = match (&persist, enabled) {
        (Some(p), true) => p
            .store
            .load(&run_id)
            .await
            .ok()
            .flatten()
            .map(|cp| cp.revision)
            .unwrap_or(0),
        _ => 0,
    };
    let mut ps = PersistSession::new(
        persist,
        enabled,
        initial_revision,
        plan_mode,
        initial_todos,
    );
    if plan_mode {
        ensure_plan_nudge(&mut context);
    }
    ps.pending_tool = Some(pending.clone());
    run.set_pending_tool(Some(pending.clone())).await;

    let result = match run.recv_tool(rx, tool_timeout).await {
        Ok(result) => result,
        Err(WaitError::Timeout) => {
            return Err(fail_run(
                &run,
                &run_id,
                &context,
                &tools,
                &mut ps,
                OrchestratorError::ToolTimeout,
            )
            .await);
        }
        Err(WaitError::Cancelled) => {
            return Err(cancel_run(&run, &run_id, &context, &tools, &mut ps).await);
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
        name: Some(pending.name),
        tool_calls: None,
            reasoning_content: None,
        });
    run.set_pending_tool(None).await;
    ps.checkpoint(&run, &context, &tools, RunStatus::Running, None)
        .await?;

    run_agent_loop(
        run,
        &mut context,
        &tools,
        llm,
        follow_up,
        tool_timeout,
        &mut ps,
        &run_id,
        middlewares,
    )
    .await
}

async fn run_agent_loop(
    run: RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    ps: &mut PersistSession,
    run_id: &str,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
) -> Result<(), OrchestratorError> {
    let mut follow_up_rounds = ps.guards.follow_up_rounds;
    let llm_tools: Vec<ToolDef> = if ps.plan_mode {
        inject_plan_tools(tools)
    } else {
        tools.to_vec()
    };

    loop {
        loop {
            if let Err(err) = drain_steer(&run, context, tools, run_id, ps).await {
                return Err(fail_run(&run, run_id, context, tools, ps, err).await);
            }
            if ensure_not_cancelled(&run).await.is_err() {
                return Err(cancel_run(&run, run_id, context, tools, ps).await);
            }

            let pending = ps.pending_tool.clone();
            let mut mw_ctx = MwCtx {
                run_id,
                context,
                tools,
                pending_tool: pending.as_ref(),
                llm: llm.as_ref(),
            };
            match run_before_llm(&middlewares, &mut mw_ctx).await {
                Ok(effect) => {
                    for ev in effect.events {
                        emit(&run, ev).await;
                    }
                    if effect.checkpoint {
                        if let Err(err) = ps
                            .checkpoint(
                                &run,
                                context,
                                tools,
                                RunStatus::Running,
                                ps.pending_tool.clone(),
                            )
                            .await
                        {
                            return Err(fail_run(&run, run_id, context, tools, ps, err).await);
                        }
                    }
                }
                Err(err) => {
                    return Err(fail_run(&run, run_id, context, tools, ps, err).await);
                }
            }

            let tool_calls =
                match stream_llm(&run, &llm, context, &llm_tools, tools, run_id, ps).await {
                    Ok(calls) => calls,
                    Err(OrchestratorError::Cancelled) => {
                        return Err(cancel_run(&run, run_id, context, tools, ps).await);
                    }
                    Err(err) => {
                        return Err(fail_run(&run, run_id, context, tools, ps, err).await);
                    }
                };

            if tool_calls.is_empty() {
                let drained = drain_steer(&run, context, tools, run_id, ps).await?;
                if drained {
                    continue;
                }
                break;
            }

            let (server_calls, client_calls): (Vec<_>, Vec<_>) =
                tool_calls.into_iter().partition(|tc| {
                    ps.plan_mode && is_server_tool(&tc.name)
                });

            for tc in server_calls {
                if let Err(err) =
                    execute_server_tool(&run, context, tools, run_id, ps, tc).await
                {
                    return Err(fail_run(&run, run_id, context, tools, ps, err).await);
                }
            }

            for tc in client_calls {
                let rx = match run.begin_wait_tool(tc.id.clone()).await {
                    Ok(rx) => rx,
                    Err(WaitError::Cancelled) => {
                        return Err(cancel_run(&run, run_id, context, tools, ps).await);
                    }
                    Err(WaitError::Timeout) => {
                        return Err(fail_run(
                            &run,
                            run_id,
                            context,
                            tools,
                            ps,
                            OrchestratorError::ToolTimeout,
                        )
                        .await);
                    }
                };

                let pending = PendingTool {
                    tool_call_id: tc.id.clone(),
                    name: tc.name.clone(),
                    arguments: tc.arguments.clone(),
                };
                run.set_pending_tool(Some(pending.clone())).await;
                emit(
                    &run,
                    SseEvent::ToolRequest {
                        run_id: run_id.to_string(),
                        tool_call_id: tc.id.clone(),
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                    },
                )
                .await;
                ps.checkpoint(
                    &run,
                    context,
                    tools,
                    RunStatus::WaitingTool,
                    Some(pending),
                )
                .await?;

                let result = match run.recv_tool(rx, tool_timeout).await {
                    Ok(result) => result,
                    Err(WaitError::Timeout) => {
                        return Err(fail_run(
                            &run,
                            run_id,
                            context,
                            tools,
                            ps,
                            OrchestratorError::ToolTimeout,
                        )
                        .await);
                    }
                    Err(WaitError::Cancelled) => {
                        return Err(cancel_run(&run, run_id, context, tools, ps).await);
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
                    tool_calls: None,
            reasoning_content: None,
        });
                run.set_pending_tool(None).await;
                ps.checkpoint(&run, context, tools, RunStatus::Running, None)
                    .await?;
            }
        }

        if ensure_not_cancelled(&run).await.is_err() {
            return Err(cancel_run(&run, run_id, context, tools, ps).await);
        }

        let follow_ups = follow_up.next(context);
        if follow_ups.is_empty() {
            emit(
                &run,
                SseEvent::RunFinished {
                    run_id: run_id.to_string(),
                    reason: "stop".into(),
                },
            )
            .await;
            ps.guards.follow_up_rounds = follow_up_rounds;
            finalize(&run, context, tools, ps, RunStatus::Completed).await?;
            return Ok(());
        }

        if follow_up_rounds >= MAX_FOLLOW_UP_ROUNDS {
            return Err(fail_run(
                &run,
                run_id,
                context,
                tools,
                ps,
                OrchestratorError::FollowUpLimit,
            )
            .await);
        }
        follow_up_rounds += 1;
        ps.guards.follow_up_rounds = follow_up_rounds;

        for msg in follow_ups {
            context.push(msg.clone());
            emit(
                &run,
                SseEvent::MessageCompleted {
                    run_id: run_id.to_string(),
                    message_id: uuid::Uuid::new_v4().to_string(),
                    role: msg.role,
                    content: msg.content,
                    tool_calls: None,
                    source: Some(MessageSource::FollowUp),
                    reasoning_content: None,
                },
            )
            .await;
        }
        ps.checkpoint(
            &run,
            context,
            tools,
            RunStatus::Running,
            ps.pending_tool.clone(),
        )
        .await?;
    }
}

async fn execute_server_tool(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    run_id: &str,
    ps: &mut PersistSession,
    tc: ToolCall,
) -> Result<(), OrchestratorError> {
    let (content, updated) = match apply_write_todos(&tc.arguments) {
        Ok(list) => {
            ps.todos = list.clone();
            (tool_result_ok(&list), true)
        }
        Err(err) => (tool_result_err(&err), false),
    };

    if updated {
        emit(
            run,
            SseEvent::TodosUpdated {
                run_id: run_id.to_string(),
                todos: ps.todos.clone(),
            },
        )
        .await;
    }

    context.push(WireMessage {
        role: Role::Tool,
        content,
        tool_call_id: Some(tc.id),
        name: Some(tc.name),
        tool_calls: None,
            reasoning_content: None,
        });
    ps.checkpoint(run, context, tools, RunStatus::Running, None)
        .await?;
    Ok(())
}

fn terminal_for(err: &OrchestratorError) -> RunStatus {
    match err {
        OrchestratorError::Cancelled => RunStatus::Cancelled,
        OrchestratorError::OwnershipLost => RunStatus::Failed,
        _ => RunStatus::Failed,
    }
}

async fn finalize(
    run: &RunHandle,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
    status: RunStatus,
) -> Result<(), OrchestratorError> {
    let _ = ps.checkpoint(run, context, tools, status, None).await;
    run.finish().await;
    Ok(())
}

/// Emit error SSE, finalize checkpoint, return the same error (for `return Err(...)`).
async fn fail_run(
    run: &RunHandle,
    run_id: &str,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
    err: OrchestratorError,
) -> OrchestratorError {
    emit_error_event(run, run_id, err.to_string(), error_code(&err)).await;
    let _ = finalize(run, context, tools, ps, terminal_for(&err)).await;
    err
}

async fn cancel_run(
    run: &RunHandle,
    run_id: &str,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
) -> OrchestratorError {
    emit_cancelled_event(run, run_id).await;
    let _ = finalize(run, context, tools, ps, RunStatus::Cancelled).await;
    OrchestratorError::Cancelled
}

async fn ensure_not_cancelled(run: &RunHandle) -> Result<(), OrchestratorError> {
    if run.is_cancelled().await {
        Err(OrchestratorError::Cancelled)
    } else {
        Ok(())
    }
}

async fn drain_steer(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    run_id: &str,
    ps: &mut PersistSession,
) -> Result<bool, OrchestratorError> {
    let msgs = run.drain_steer().await;
    let any = !msgs.is_empty();
    for msg in msgs {
        context.push(msg.clone());
        emit(
            run,
            SseEvent::MessageCompleted {
                run_id: run_id.to_string(),
                message_id: uuid::Uuid::new_v4().to_string(),
                role: msg.role,
                content: msg.content,
                tool_calls: None,
                source: Some(MessageSource::Steer),
                reasoning_content: None,
            },
        )
        .await;
    }
    if any {
        ps.checkpoint(
            run,
            context,
            tools,
            RunStatus::Running,
            ps.pending_tool.clone(),
        )
        .await?;
    }
    Ok(any)
}

/// `llm_tools` are passed to the model; `client_tools` are what we persist on checkpoint.
async fn stream_llm(
    run: &RunHandle,
    llm: &Arc<dyn LlmPort>,
    context: &mut Vec<WireMessage>,
    llm_tools: &[ToolDef],
    client_tools: &[ToolDef],
    run_id: &str,
    ps: &mut PersistSession,
) -> Result<Vec<ToolCall>, OrchestratorError> {
    let mut stream = llm
        .stream(context, llm_tools)
        .await
        .map_err(OrchestratorError::Llm)?;

    let message_id = uuid::Uuid::new_v4().to_string();
    let mut completed_content = None;
    let mut completed_reasoning = None;
    let mut tool_calls = Vec::new();

    while let Some(chunk) = stream.next().await {
        if run.is_cancelled().await {
            return Err(OrchestratorError::Cancelled);
        }
        match chunk.map_err(OrchestratorError::Llm)? {
            LlmChunk::TextDelta(delta) => {
                emit(
                    run,
                    SseEvent::MessageDelta {
                        run_id: run_id.to_string(),
                        message_id: message_id.clone(),
                        delta,
                    },
                )
                .await;
            }
            LlmChunk::ReasoningDelta(delta) => {
                emit(
                    run,
                    SseEvent::ReasoningDelta {
                        run_id: run_id.to_string(),
                        message_id: message_id.clone(),
                        delta,
                    },
                )
                .await;
            }
            LlmChunk::Completed {
                content,
                tool_calls: calls,
                reasoning_content,
            } => {
                completed_content = Some(content);
                completed_reasoning = reasoning_content;
                tool_calls = calls;
            }
        }
    }

    if run.is_cancelled().await {
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

    let tool_calls_for_context = if tool_calls_wire.is_empty() {
        None
    } else {
        Some(tool_calls_wire.clone())
    };

    emit(
        run,
        SseEvent::MessageCompleted {
            run_id: run_id.to_string(),
            message_id,
            role: Role::Assistant,
            content: content.clone(),
            tool_calls: tool_calls_for_context.clone(),
            source: Some(MessageSource::Assistant),
            reasoning_content: completed_reasoning.clone(),
        },
    )
    .await;

    context.push(WireMessage {
        role: Role::Assistant,
        content,
        tool_call_id: None,
        name: None,
        tool_calls: tool_calls_for_context,
        reasoning_content: completed_reasoning,
    });
    ps.guards.llm_rounds = ps.guards.llm_rounds.saturating_add(1);
    ps.checkpoint(run, context, client_tools, RunStatus::Running, None)
        .await?;
    Ok(tool_calls)
}

fn error_code(err: &OrchestratorError) -> Option<&'static str> {
    match err {
        OrchestratorError::Llm(_) => Some("llm"),
        OrchestratorError::ToolTimeout => Some("timeout"),
        OrchestratorError::Cancelled => Some("cancelled"),
        OrchestratorError::FollowUpLimit => Some("follow_up_limit"),
        OrchestratorError::OwnershipLost => Some("not_owner"),
        OrchestratorError::Store(_) => Some("store"),
        OrchestratorError::ContextOverflow => Some("context_overflow"),
    }
}

async fn emit(run: &RunHandle, event: SseEvent) {
    let _ = run.emit_event(event).await;
}

async fn emit_cancelled_event(run: &RunHandle, run_id: &str) {
    emit(
        run,
        SseEvent::RunFinished {
            run_id: run_id.to_string(),
            reason: "cancelled".into(),
        },
    )
    .await;
}

async fn emit_error_event(run: &RunHandle, run_id: &str, message: String, code: Option<&str>) {
    emit(
        run,
        SseEvent::Error {
            run_id: run_id.to_string(),
            message,
            code: code.map(str::to_string),
        },
    )
    .await;
}
