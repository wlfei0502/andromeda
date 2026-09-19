use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream;
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::agent::{
    FollowUpPolicy, RunPersist, continue_after_pending_tool, run_agent, run_agent_with_options,
};
use crate::llm::LlmPort;
use crate::protocol::{CreateRunRequest, SseEvent, SteerRequest, ToolResultRequest};
use crate::runtime::{RunHandle, RunId, RunRegistry, SubmitError};
use crate::store::{Checkpoint, GuardsSnapshot, RunStatus, RunStore};

#[derive(Clone)]
pub struct AppState {
    pub registry: RunRegistry,
    pub store: Option<Arc<dyn RunStore>>,
    pub instance_id: String,
    pub persist_enabled: bool,
    pub llm: Arc<dyn LlmPort>,
    pub follow_up: Arc<dyn FollowUpPolicy>,
    pub tool_timeout: Duration,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{id}/events", get(subscribe_events))
        .route("/v1/runs/{id}/tool_results", post(tool_results))
        .route("/v1/runs/{id}/steer", post(steer))
        .route("/v1/runs/{id}/cancel", post(cancel_run))
        .with_state(state)
}

#[derive(Serialize)]
struct OkBody {
    ok: bool,
}

#[derive(Serialize)]
struct SteerBody {
    ok: bool,
    queued: usize,
}

enum ApiError {
    NotFound,
    Conflict { code: Option<&'static str> },
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "run not found" })),
            )
                .into_response(),
            ApiError::Conflict { code: Some(code) } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "conflict", "code": code })),
            )
                .into_response(),
            ApiError::Conflict { code: None } => {
                (StatusCode::CONFLICT, Json(json!({ "error": "conflict" }))).into_response()
            }
        }
    }
}

fn sse_response(run_id: String, rx: mpsc::Receiver<SseEvent>) -> Response {
    let stream = stream::unfold(rx, |mut rx| async move {
        let event: SseEvent = rx.recv().await?;
        let frame = Event::default()
            .event(event.event_name())
            .json_data(&event)
            .expect("SseEvent serializes to JSON");
        Some((Ok::<_, Infallible>(frame), rx))
    });
    (
        [(
            HeaderName::from_static("x-run-id"),
            HeaderValue::from_str(&run_id).expect("run id is ascii"),
        )],
        Sse::new(stream).keep_alive(KeepAlive::default()),
    )
        .into_response()
}

async fn create_run(
    State(state): State<AppState>,
    Json(req): Json<CreateRunRequest>,
) -> impl IntoResponse {
    let (id, run) = state.registry.create().await;
    let run_id = id.0.clone();
    let should_persist = state.persist_enabled && req.options.persist && state.store.is_some();
    if should_persist {
        run.set_persist(true).await;
        if let Some(store) = state.store.as_ref() {
            let cp = Checkpoint {
                run_id: run_id.clone(),
                status: RunStatus::Running,
                context: req.messages.clone(),
                tools: req.tools.clone(),
                todos: vec![],
                pending_tool: None,
                guards: GuardsSnapshot::new_now(),
                parent_run_id: None,
                owner_id: Some(state.instance_id.clone()),
                revision: 1,
            };
            if let Err(err) = store.save_cas(&cp, 0).await {
                tracing::warn!(run_id = %run_id, error = %err, "initial checkpoint write failed");
            } else {
                let _ = store
                    .write_meta(
                        &run_id,
                        &json!({
                            "owner_id": state.instance_id,
                            "created": true,
                        }),
                    )
                    .await;
            }
        }
    }

    let rx = run.subscribe().await;
    let llm = state.llm.clone();
    let follow_up = state.follow_up.clone();
    let tool_timeout = state.tool_timeout;
    let run_for_task = run.clone();
    let persist = if should_persist {
        state.store.clone().map(|store| RunPersist {
            store,
            instance_id: state.instance_id.clone(),
        })
    } else {
        None
    };

    tokio::spawn(async move {
        let _ = run_agent(
            run_for_task.clone(),
            req.messages,
            req.tools,
            llm,
            follow_up,
            tool_timeout,
            persist,
        )
        .await;
        run_for_task.finish().await;
    });

    sse_response(run_id, rx)
}

async fn subscribe_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let run_id = id;
    if let Some(run) = state.registry.get(&RunId(run_id.clone())).await {
        if run.is_finished().await {
            return Ok(terminal_sse_from_hot(&run_id, "stop").await);
        }
        let revision = load_revision(state.store.as_deref(), &run_id).await;
        let status = if run.is_waiting_tool().await {
            "waiting_tool"
        } else {
            "running"
        };
        let rx = run.subscribe().await;
        let _ = run
            .emit_event(SseEvent::RunResumed {
                run_id: run_id.clone(),
                revision,
                status: status.into(),
            })
            .await;
        if let Some(pending) = run.pending_tool().await {
            let _ = run
                .emit_event(SseEvent::ToolRequest {
                    run_id: run_id.clone(),
                    tool_call_id: pending.tool_call_id,
                    name: pending.name,
                    arguments: pending.arguments,
                })
                .await;
        }
        return Ok(sse_response(run_id, rx));
    }

    let store = state.store.as_ref().ok_or(ApiError::NotFound)?;
    let cp = store
        .load(&run_id)
        .await
        .map_err(|_| ApiError::NotFound)?
        .ok_or(ApiError::NotFound)?;

    if cp.status.is_terminal() {
        let reason = match cp.status {
            RunStatus::Cancelled => "cancelled",
            RunStatus::Failed => "error",
            _ => "stop",
        };
        return Ok(terminal_sse_from_hot(&run_id, reason).await);
    }

    // Claim ownership.
    let expected = cp.revision;
    let mut claimed = cp.clone();
    claimed.revision = expected + 1;
    claimed.owner_id = Some(state.instance_id.clone());
    store
        .save_cas(&claimed, expected)
        .await
        .map_err(|_| ApiError::Conflict {
            code: Some("not_owner"),
        })?;

    let run = RunHandle::reopen(RunId(run_id.clone()));
    run.set_persist(true).await;
    state.registry.insert(run.clone()).await;

    let rx = run.subscribe().await;
    let _ = run
        .emit_event(SseEvent::RunResumed {
            run_id: run_id.clone(),
            revision: claimed.revision,
            status: claimed.status.as_str().into(),
        })
        .await;

    let persist = Some(RunPersist {
        store: store.clone(),
        instance_id: state.instance_id.clone(),
    });
    let llm = state.llm.clone();
    let follow_up = state.follow_up.clone();
    let tool_timeout = state.tool_timeout;

    match claimed.status {
        RunStatus::WaitingTool => {
            let pending = claimed.pending_tool.clone().ok_or(ApiError::Conflict {
                code: None,
            })?;
            let tool_rx = run
                .begin_wait_tool(pending.tool_call_id.clone())
                .await
                .map_err(|_| ApiError::Conflict { code: None })?;
            run.set_pending_tool(Some(pending.clone())).await;
            let _ = run
                .emit_event(SseEvent::ToolRequest {
                    run_id: run_id.clone(),
                    tool_call_id: pending.tool_call_id.clone(),
                    name: pending.name.clone(),
                    arguments: pending.arguments.clone(),
                })
                .await;

            let run_task = run.clone();
            let context = claimed.context;
            let tools = claimed.tools;
            tokio::spawn(async move {
                let _ = continue_after_pending_tool(
                    run_task.clone(),
                    context,
                    tools,
                    pending,
                    tool_rx,
                    llm,
                    follow_up,
                    tool_timeout,
                    persist,
                )
                .await;
                run_task.finish().await;
            });
        }
        RunStatus::Running => {
            let run_task = run.clone();
            let context = claimed.context;
            let tools = claimed.tools;
            tokio::spawn(async move {
                let _ = run_agent_with_options(
                    run_task.clone(),
                    context,
                    tools,
                    llm,
                    follow_up,
                    tool_timeout,
                    persist,
                    false,
                )
                .await;
                run_task.finish().await;
            });
        }
        _ => return Err(ApiError::Conflict { code: None }),
    }

    Ok(sse_response(run_id, rx))
}

async fn load_revision(store: Option<&dyn RunStore>, run_id: &str) -> u64 {
    match store {
        Some(store) => store
            .load(run_id)
            .await
            .ok()
            .flatten()
            .map(|cp| cp.revision)
            .unwrap_or(0),
        None => 0,
    }
}

async fn terminal_sse_from_hot(run_id: &str, reason: &str) -> Response {
    let (tx, rx) = mpsc::channel(4);
    let _ = tx
        .send(SseEvent::RunFinished {
            run_id: run_id.to_string(),
            reason: reason.into(),
        })
        .await;
    drop(tx);
    sse_response(run_id.to_string(), rx)
}

async fn resolve_run_or_owner(
    state: &AppState,
    id: &str,
) -> Result<RunHandle, ApiError> {
    if let Some(run) = state.registry.get(&RunId(id.to_string())).await {
        return Ok(run);
    }
    let Some(store) = state.store.as_ref() else {
        return Err(ApiError::NotFound);
    };
    let Some(cp) = store.load(id).await.ok().flatten() else {
        return Err(ApiError::NotFound);
    };
    if cp.status.is_terminal() {
        return Err(ApiError::Conflict { code: None });
    }
    if cp.owner_id.as_deref() != Some(state.instance_id.as_str()) {
        return Err(ApiError::Conflict {
            code: Some("not_owner"),
        });
    }
    // Same instance but no hot run: client must resume via GET .../events first.
    Err(ApiError::Conflict { code: None })
}

async fn tool_results(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ToolResultRequest>,
) -> Result<Json<OkBody>, ApiError> {
    let run = resolve_run_or_owner(&state, &id).await?;
    run.submit_tool_result(body)
        .await
        .map_err(|SubmitError::Conflict| ApiError::Conflict { code: None })?;
    Ok(Json(OkBody { ok: true }))
}

async fn steer(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SteerRequest>,
) -> Result<Json<SteerBody>, ApiError> {
    let run = resolve_run_or_owner(&state, &id).await?;
    let queued = body.messages.len();
    run.enqueue_steer(body.messages)
        .await
        .map_err(|SubmitError::Conflict| ApiError::Conflict { code: None })?;
    Ok(Json(SteerBody { ok: true, queued }))
}

async fn cancel_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<OkBody>, ApiError> {
    let run = state
        .registry
        .get(&RunId(id))
        .await
        .ok_or(ApiError::NotFound)?;
    run.cancel().await;
    Ok(Json(OkBody { ok: true }))
}
