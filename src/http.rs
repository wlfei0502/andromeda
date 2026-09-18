use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::stream;
use serde::Serialize;

use crate::follow_up::FollowUpPolicy;
use crate::llm::LlmPort;
use crate::orchestrator::run_agent;
use crate::run::{RunId, RunRegistry, SubmitError};
use crate::wire::{CreateRunRequest, SseEvent, SteerRequest, ToolResultRequest};

#[derive(Clone)]
pub struct AppState {
    pub registry: RunRegistry,
    pub llm: Arc<dyn LlmPort>,
    pub follow_up: Arc<dyn FollowUpPolicy>,
    pub tool_timeout: Duration,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/runs", post(create_run))
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
    Conflict,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "run not found"),
            ApiError::Conflict => (StatusCode::CONFLICT, "conflict"),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

async fn create_run(
    State(state): State<AppState>,
    Json(req): Json<CreateRunRequest>,
) -> impl IntoResponse {
    let (id, run) = state.registry.create();
    let run_id = id.0.clone();
    let (sse, rx) = tokio::sync::mpsc::channel(64);
    // Keep a sender alive until after `finish()` so the SSE stream cannot end
    // before later POSTs observe the terminal run state.
    let sse_hold = sse.clone();
    let llm = state.llm.clone();
    let follow_up = state.follow_up.clone();
    let tool_timeout = state.tool_timeout;
    let run_for_task = run.clone();

    tokio::spawn(async move {
        // Orchestrator already emits `run.finished`/`error`; HTTP must not duplicate
        // `Cancelled` as an extra SSE error.
        let _ = run_agent(
            run_for_task.clone(),
            req.messages,
            req.tools,
            llm,
            follow_up,
            sse,
            tool_timeout,
        )
        .await;
        run_for_task.finish();
        drop(sse_hold);
    });

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
        Sse::new(stream),
    )
}

async fn tool_results(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<ToolResultRequest>,
) -> Result<Json<OkBody>, ApiError> {
    let run = state.registry.get(&RunId(id)).ok_or(ApiError::NotFound)?;
    run.submit_tool_result(body)
        .map_err(|SubmitError::Conflict| ApiError::Conflict)?;
    Ok(Json(OkBody { ok: true }))
}

async fn steer(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SteerRequest>,
) -> Result<Json<SteerBody>, ApiError> {
    let run = state.registry.get(&RunId(id)).ok_or(ApiError::NotFound)?;
    let queued = body.messages.len();
    run.enqueue_steer(body.messages)
        .map_err(|SubmitError::Conflict| ApiError::Conflict)?;
    Ok(Json(SteerBody { ok: true, queued }))
}

async fn cancel_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<OkBody>, ApiError> {
    let run = state.registry.get(&RunId(id)).ok_or(ApiError::NotFound)?;
    run.cancel();
    Ok(Json(OkBody { ok: true }))
}
