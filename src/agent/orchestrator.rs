use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::StreamExt;

use super::follow_up::{FollowUpPolicy, NoopFollowUp};
use super::guards::{
    REASON_TIMEOUT, clear_noop_progress, observe_noop_turn, pre_llm_guard, unix_now, wall_exceeded,
};
use super::middleware::{AgentMiddleware, MwCtx, run_before_llm};
use super::plan::{apply_write_todos, ensure_plan_nudge, tool_result_err, tool_result_ok};
use super::task::{
    build_subagent_context, filter_tools_for_agent, inject_lead_tools, is_server_tool_name,
    is_task_tool, parse_task_args, task_result_err, task_result_ok,
};
use crate::config::{GuardsConfig, SubagentsConfig};
use crate::llm::{LlmChunk, LlmPort, ToolCall};
use crate::protocol::{
    MessageSource, Role, SseEvent, TodoItem, ToolCallWire, ToolDef, WireMessage,
};
use crate::runtime::{RunHandle, WaitError};
use crate::store::{Checkpoint, GuardsSnapshot, PendingTool, RunStatus, RunStore, StoreError};

pub const MAX_FOLLOW_UP_ROUNDS: u32 = 8;

/// Mode / resume knobs for `run_agent` (keeps the call surface small).
#[derive(Clone)]
pub struct RunAgentOpts {
    pub plan_mode: bool,
    pub subagents: bool,
    pub subagents_cfg: SubagentsConfig,
    pub guards_cfg: GuardsConfig,
    pub initial_todos: Vec<TodoItem>,
    pub initial_guards: Option<GuardsSnapshot>,
    /// Emit `run.started` (false on cold resume / continue).
    pub emit_started: bool,
    /// Nested subagent: fold message SSE; never finish the parent handle.
    pub fold_stream: bool,
    pub agent_id: Option<String>,
    pub parent_task_id: Option<String>,
}

impl Default for RunAgentOpts {
    fn default() -> Self {
        Self {
            plan_mode: false,
            subagents: false,
            subagents_cfg: SubagentsConfig::default(),
            guards_cfg: GuardsConfig::default(),
            initial_todos: Vec::new(),
            initial_guards: None,
            emit_started: true,
            fold_stream: false,
            agent_id: None,
            parent_task_id: None,
        }
    }
}

impl RunAgentOpts {
    pub fn lead(
        plan_mode: bool,
        subagents: bool,
        guards_cfg: GuardsConfig,
        subagents_cfg: SubagentsConfig,
    ) -> Self {
        Self {
            plan_mode,
            subagents,
            guards_cfg,
            subagents_cfg,
            ..Default::default()
        }
    }

    pub fn resume_running(
        plan_mode: bool,
        subagents: bool,
        todos: Vec<TodoItem>,
        guards_cfg: GuardsConfig,
        initial_guards: Option<GuardsSnapshot>,
        subagents_cfg: SubagentsConfig,
    ) -> Self {
        Self {
            plan_mode,
            subagents,
            initial_todos: todos,
            guards_cfg,
            initial_guards,
            subagents_cfg,
            emit_started: false,
            ..Default::default()
        }
    }

    fn for_subagent(
        parent_task_id: String,
        agent_id: String,
        guards_cfg: GuardsConfig,
        subagents_cfg: SubagentsConfig,
    ) -> Self {
        Self {
            guards_cfg,
            subagents_cfg,
            emit_started: false,
            fold_stream: true,
            agent_id: Some(agent_id),
            parent_task_id: Some(parent_task_id),
            ..Default::default()
        }
    }
}

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
    #[error("guard: {0}")]
    Guard(&'static str),
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
    subagents: bool,
    subagents_cfg: SubagentsConfig,
    /// When true, this loop is a nested subagent (fold SSE; never finish the handle).
    fold_stream: bool,
    agent_id: Option<String>,
    parent_task_id: Option<String>,
    guards_cfg: GuardsConfig,
    finish_reason: Option<String>,
}

impl PersistSession {
    fn new(
        persist: Option<RunPersist>,
        enabled: bool,
        initial_revision: u64,
        plan_mode: bool,
        todos: Vec<TodoItem>,
        guards_cfg: GuardsConfig,
        guards: GuardsSnapshot,
        subagents: bool,
        subagents_cfg: SubagentsConfig,
        fold_stream: bool,
        agent_id: Option<String>,
        parent_task_id: Option<String>,
    ) -> Self {
        let active = enabled && persist.is_some();
        Self {
            store: persist.as_ref().map(|p| p.store.clone()),
            instance_id: persist.map(|p| p.instance_id).unwrap_or_default(),
            enabled: active,
            revision: initial_revision,
            guards,
            pending_tool: None,
            todos,
            plan_mode,
            subagents,
            subagents_cfg,
            fold_stream,
            agent_id,
            parent_task_id,
            guards_cfg,
            finish_reason: None,
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
            subagents: self.subagents,
            pending_tool: pending_tool.clone(),
            guards: self.guards.clone(),
            parent_run_id: None,
            owner_id: Some(self.instance_id.clone()),
            revision: next,
            finish_reason: self.finish_reason.clone(),
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
    opts: RunAgentOpts,
) -> Result<(), OrchestratorError> {
    let mut context = context;
    let _ = run_agent_inner(
        run,
        &mut context,
        &tools,
        llm,
        follow_up,
        tool_timeout,
        persist,
        middlewares,
        opts,
    )
    .await?;
    Ok(())
}

/// Alias kept for resume call sites that previously skipped `run.started` via a flag.
pub async fn run_agent_with_options(
    run: RunHandle,
    context: Vec<WireMessage>,
    tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    persist: Option<RunPersist>,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    opts: RunAgentOpts,
) -> Result<(), OrchestratorError> {
    run_agent(
        run,
        context,
        tools,
        llm,
        follow_up,
        tool_timeout,
        persist,
        middlewares,
        opts,
    )
    .await
}

async fn run_agent_inner(
    run: RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    llm: Arc<dyn LlmPort>,
    follow_up: Arc<dyn FollowUpPolicy>,
    tool_timeout: Duration,
    persist: Option<RunPersist>,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    opts: RunAgentOpts,
) -> Result<String, OrchestratorError> {
    let run_id = run.id().0.clone();
    let enabled = run.persist_enabled().await && !opts.fold_stream;
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
        opts.plan_mode,
        opts.initial_todos,
        opts.guards_cfg,
        opts.initial_guards
            .unwrap_or_else(GuardsSnapshot::new_now),
        opts.subagents,
        opts.subagents_cfg,
        opts.fold_stream,
        opts.agent_id,
        opts.parent_task_id,
    );

    if opts.plan_mode {
        ensure_plan_nudge(context);
    }

    if opts.emit_started && !opts.fold_stream {
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
        context,
        tools,
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
    opts: RunAgentOpts,
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
        opts.plan_mode,
        opts.initial_todos,
        opts.guards_cfg,
        opts.initial_guards
            .unwrap_or_else(GuardsSnapshot::new_now),
        opts.subagents,
        opts.subagents_cfg,
        false,
        None,
        None,
    );
    if opts.plan_mode {
        ensure_plan_nudge(&mut context);
    }
    ps.pending_tool = Some(pending.clone());
    run.upsert_pending_tool(pending.clone()).await;

    let result = match run
        .recv_tool(&pending.tool_call_id, rx, tool_timeout)
        .await
    {
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
    run.remove_pending_tool(&pending.tool_call_id).await;
    ps.checkpoint(&run, &context, &tools, RunStatus::Running, None)
        .await?;

    let _ = run_agent_loop(
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
    .await?;
    Ok(())
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
) -> Result<String, OrchestratorError> {
    let mut follow_up_rounds = ps.guards.follow_up_rounds;
    let llm_tools: Vec<ToolDef> = inject_lead_tools(tools, ps.plan_mode, ps.subagents);

    loop {
        loop {
            if let Err(err) = drain_steer(&run, context, tools, run_id, ps).await {
                return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
            }
            if ensure_not_cancelled(&run).await.is_err() {
                return Err(cancel_or_propagate(&run, run_id, context, tools, ps).await);
            }
            if let Some(reason) = pre_llm_guard(&ps.guards, &ps.guards_cfg, unix_now()) {
                return Err(finish_guard(&run, run_id, context, tools, ps, reason).await);
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
                    if !ps.fold_stream {
                        for ev in effect.events {
                            emit(&run, ev).await;
                        }
                    }
                    if effect.checkpoint && !ps.fold_stream {
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
                            return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                        }
                    }
                }
                Err(err) => {
                    return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                }
            }

            let tool_calls =
                match stream_llm(&run, &llm, context, &llm_tools, tools, run_id, ps).await {
                    Ok(calls) => calls,
                    Err(OrchestratorError::Cancelled) => {
                        return Err(cancel_or_propagate(&run, run_id, context, tools, ps).await);
                    }
                    Err(err) => {
                        return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                    }
                };

            if tool_calls.is_empty() {
                let drained = drain_steer(&run, context, tools, run_id, ps).await?;
                if drained {
                    clear_noop_progress(&mut ps.guards);
                    continue;
                }
                let assistant = last_assistant_text(context);
                if let Some(reason) = observe_noop_turn(&mut ps.guards, assistant, &ps.guards_cfg) {
                    return Err(finish_guard(&run, run_id, context, tools, ps, reason).await);
                }
                break;
            }

            clear_noop_progress(&mut ps.guards);
            if wall_exceeded(&ps.guards, &ps.guards_cfg, unix_now()) {
                return Err(finish_guard(&run, run_id, context, tools, ps, REASON_TIMEOUT).await);
            }

            let (server_calls, client_calls): (Vec<_>, Vec<_>) = tool_calls
                .into_iter()
                .partition(|tc| is_server_tool_name(&tc.name, ps.plan_mode, ps.subagents));

            let (task_calls, other_server): (Vec<_>, Vec<_>) =
                server_calls.into_iter().partition(|tc| is_task_tool(&tc.name));

            for tc in other_server {
                if let Err(err) = execute_write_todos(&run, context, tools, run_id, ps, tc).await {
                    return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                }
            }

            if !task_calls.is_empty() {
                if let Err(err) = execute_task_batch(
                    &run,
                    context,
                    tools,
                    run_id,
                    ps,
                    task_calls,
                    llm.clone(),
                    tool_timeout,
                    middlewares.clone(),
                )
                .await
                {
                    return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                }
            }

            for tc in client_calls {
                let pending = PendingTool {
                    tool_call_id: tc.id.clone(),
                    name: tc.name.clone(),
                    arguments: tc.arguments.clone(),
                    agent_id: ps.agent_id.clone(),
                    parent_task_id: ps.parent_task_id.clone(),
                };
                // Register pending before arming the waiter so timeout scrub can find it.
                run.upsert_pending_tool(pending.clone()).await;
                let rx = match run.begin_wait_tool(tc.id.clone()).await {
                    Ok(rx) => rx,
                    Err(WaitError::Cancelled) => {
                        run.remove_pending_tool(&tc.id).await;
                        return Err(cancel_or_propagate(&run, run_id, context, tools, ps).await);
                    }
                    Err(WaitError::Timeout) => {
                        run.remove_pending_tool(&tc.id).await;
                        return Err(fail_or_propagate(
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
                emit(
                    &run,
                    SseEvent::ToolRequest {
                        run_id: run_id.to_string(),
                        tool_call_id: tc.id.clone(),
                        name: tc.name.clone(),
                        arguments: tc.arguments.clone(),
                        agent_id: ps.agent_id.clone(),
                        parent_task_id: ps.parent_task_id.clone(),
                    },
                )
                .await;
                if !ps.fold_stream {
                    ps.checkpoint(
                        &run,
                        context,
                        tools,
                        RunStatus::WaitingTool,
                        Some(pending.clone()),
                    )
                    .await?;
                } else {
                    ps.pending_tool = Some(pending);
                }

                let result = match run.recv_tool(&tc.id, rx, tool_timeout).await {
                    Ok(result) => result,
                    Err(WaitError::Timeout) => {
                        return Err(fail_or_propagate(
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
                        return Err(cancel_or_propagate(&run, run_id, context, tools, ps).await);
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
                run.remove_pending_tool(&tc.id).await;
                if !ps.fold_stream {
                    ps.checkpoint(&run, context, tools, RunStatus::Running, None)
                        .await?;
                } else {
                    ps.pending_tool = None;
                }
            }
        }

        if ensure_not_cancelled(&run).await.is_err() {
            return Err(cancel_or_propagate(&run, run_id, context, tools, ps).await);
        }

        let follow_ups = if ps.fold_stream {
            Vec::new()
        } else {
            follow_up.next(context)
        };
        if follow_ups.is_empty() {
            match drain_steer(&run, context, tools, run_id, ps).await {
                Ok(true) => {
                    clear_noop_progress(&mut ps.guards);
                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    return Err(fail_or_propagate(&run, run_id, context, tools, ps, err).await);
                }
            }
            let summary = last_assistant_text(context).to_string();
            if !ps.fold_stream {
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
            }
            return Ok(summary);
        }

        if follow_up_rounds >= ps.guards_cfg.max_follow_up_rounds
            && ps.guards_cfg.max_follow_up_rounds > 0
        {
            return Err(fail_or_propagate(
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

async fn execute_write_todos(
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

async fn execute_task_batch(
    run: &RunHandle,
    context: &mut Vec<WireMessage>,
    tools: &[ToolDef],
    run_id: &str,
    ps: &mut PersistSession,
    task_calls: Vec<ToolCall>,
    llm: Arc<dyn LlmPort>,
    tool_timeout: Duration,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
) -> Result<(), OrchestratorError> {
    let max = ps.subagents_cfg.max_concurrent_subagents.max(1) as usize;
    let mut ordered = task_calls;
    let overflow = if ordered.len() > max {
        ordered.split_off(max)
    } else {
        Vec::new()
    };

    for tc in &overflow {
        context.push(WireMessage {
            role: Role::Tool,
            content: task_result_err(&format!(
                "max_concurrent_subagents ({max}) exceeded"
            )),
            tool_call_id: Some(tc.id.clone()),
            name: Some(tc.name.clone()),
            tool_calls: None,
            reasoning_content: None,
        });
    }

    let sub_cfg = ps.subagents_cfg.clone();
    let guards_cfg = ps.guards_cfg.clone();
    let client_tools = tools.to_vec();
    let parent_run_id = run_id.to_string();

    let futs = ordered.into_iter().map(|tc| {
        let run = run.clone();
        let llm = llm.clone();
        let middlewares = middlewares.clone();
        let sub_cfg = sub_cfg.clone();
        let guards_cfg = guards_cfg.clone();
        let client_tools = client_tools.clone();
        let parent_run_id = parent_run_id.clone();
        async move {
            let content =
                run_single_task(&run, &parent_run_id, tc, client_tools, llm, tool_timeout, middlewares, sub_cfg, guards_cfg)
                    .await;
            content
        }
    });

    let results = join_all(futs).await;
    for (id, name, content) in results {
        context.push(WireMessage {
            role: Role::Tool,
            content,
            tool_call_id: Some(id),
            name: Some(name),
            tool_calls: None,
            reasoning_content: None,
        });
    }

    ps.checkpoint(run, context, tools, RunStatus::Running, None)
        .await?;
    Ok(())
}

async fn run_single_task(
    run: &RunHandle,
    run_id: &str,
    tc: ToolCall,
    client_tools: Vec<ToolDef>,
    llm: Arc<dyn LlmPort>,
    tool_timeout: Duration,
    middlewares: Arc<[Arc<dyn AgentMiddleware>]>,
    sub_cfg: SubagentsConfig,
    guards_cfg: GuardsConfig,
) -> (String, String, String) {
    let task_id = tc.id.clone();
    let task_name = tc.name.clone();

    let args = match parse_task_args(&tc.arguments) {
        Ok(a) => a,
        Err(err) => {
            emit(
                run,
                SseEvent::TaskFailed {
                    run_id: run_id.to_string(),
                    task_id: task_id.clone(),
                    message: err.clone(),
                    code: Some("bad_arguments".into()),
                },
            )
            .await;
            return (task_id, task_name, task_result_err(&err));
        }
    };

    emit(
        run,
        SseEvent::TaskStarted {
            run_id: run_id.to_string(),
            task_id: task_id.clone(),
            goal: args.goal.clone(),
            agent: args.agent.as_str().to_string(),
        },
    )
    .await;

    let agent_id = format!("sub-{task_id}");
    let sub_tools = filter_tools_for_agent(&client_tools, &args.agent);
    let mut sub_context = build_subagent_context(&args);
    let timeout = Duration::from_secs(sub_cfg.subagent_timeout_secs.max(1));

    let nested = run_agent_inner(
        run.clone(),
        &mut sub_context,
        &sub_tools,
        llm,
        Arc::new(NoopFollowUp),
        tool_timeout,
        None,
        middlewares,
        RunAgentOpts::for_subagent(task_id.clone(), agent_id, guards_cfg, sub_cfg),
    );

    let outcome = tokio::time::timeout(timeout, nested).await;
    // Dropping a timed-out nested future can leave orphan waiters; scrub by task id.
    run.cancel_waits_for_parent_task(&task_id).await;
    match outcome {
        Ok(Ok(summary)) => {
            emit(
                run,
                SseEvent::TaskCompleted {
                    run_id: run_id.to_string(),
                    task_id: task_id.clone(),
                    summary: summary.clone(),
                },
            )
            .await;
            (task_id, task_name, task_result_ok(&summary))
        }
        Ok(Err(OrchestratorError::Cancelled)) => {
            emit(
                run,
                SseEvent::TaskFailed {
                    run_id: run_id.to_string(),
                    task_id: task_id.clone(),
                    message: "cancelled".into(),
                    code: Some("cancelled".into()),
                },
            )
            .await;
            (task_id, task_name, task_result_err("cancelled"))
        }
        Ok(Err(err)) => {
            let message = err.to_string();
            emit(
                run,
                SseEvent::TaskFailed {
                    run_id: run_id.to_string(),
                    task_id: task_id.clone(),
                    message: message.clone(),
                    code: error_code(&err).map(str::to_string),
                },
            )
            .await;
            (task_id, task_name, task_result_err(&message))
        }
        Err(_) => {
            emit(
                run,
                SseEvent::TaskTimedOut {
                    run_id: run_id.to_string(),
                    task_id: task_id.clone(),
                },
            )
            .await;
            (task_id, task_name, task_result_err("subagent timed out"))
        }
    }
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

/// Emit `run.finished` for a guard, persist `failed`, return `Guard`.
async fn finish_guard(
    run: &RunHandle,
    run_id: &str,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
    reason: &'static str,
) -> OrchestratorError {
    if ps.fold_stream {
        return OrchestratorError::Guard(reason);
    }
    ps.finish_reason = Some(reason.to_string());
    emit(
        run,
        SseEvent::RunFinished {
            run_id: run_id.to_string(),
            reason: reason.to_string(),
        },
    )
    .await;
    let _ = finalize(run, context, tools, ps, RunStatus::Failed).await;
    OrchestratorError::Guard(reason)
}

async fn fail_or_propagate(
    run: &RunHandle,
    run_id: &str,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
    err: OrchestratorError,
) -> OrchestratorError {
    if ps.fold_stream {
        err
    } else {
        fail_run(run, run_id, context, tools, ps, err).await
    }
}

async fn cancel_or_propagate(
    run: &RunHandle,
    run_id: &str,
    context: &[WireMessage],
    tools: &[ToolDef],
    ps: &mut PersistSession,
) -> OrchestratorError {
    if ps.fold_stream {
        OrchestratorError::Cancelled
    } else {
        cancel_run(run, run_id, context, tools, ps).await
    }
}

fn last_assistant_text(context: &[WireMessage]) -> &str {
    context
        .iter()
        .rev()
        .find(|m| m.role == Role::Assistant)
        .map(|m| m.content.as_str())
        .unwrap_or("")
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
    if ps.fold_stream {
        return Ok(false);
    }
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
                if !ps.fold_stream {
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
            }
            LlmChunk::ReasoningDelta(delta) => {
                if !ps.fold_stream {
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

    if !ps.fold_stream {
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
    }

    context.push(WireMessage {
        role: Role::Assistant,
        content,
        tool_call_id: None,
        name: None,
        tool_calls: tool_calls_for_context,
        reasoning_content: completed_reasoning,
    });
    ps.guards.llm_rounds = ps.guards.llm_rounds.saturating_add(1);
    if !ps.fold_stream {
        ps.checkpoint(run, context, client_tools, RunStatus::Running, None)
            .await?;
    }
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
        OrchestratorError::Guard(reason) => Some(reason),
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
