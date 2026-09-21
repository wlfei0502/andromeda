mod common;

use std::sync::Arc;
use std::time::Duration;

use andromeda::api::router;
use andromeda::config::ContextConfig;
use andromeda::llm::{MockLlm, MockTurn, ToolCall};
use andromeda::protocol::{
    CreateRunRequest, Role, RunOptions, SseEvent, ToolResultRequest, WireMessage,
};
use andromeda::runtime::RunId;
use andromeda::store::{
    Checkpoint, GuardsSnapshot, LocalFsRunStore, PendingTool, RunStatus, RunStore,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

use common::{app_state, echo_tool, json_post, user_msg};

fn state(
    llm: MockLlm,
    store: Option<Arc<dyn RunStore>>,
    instance_id: &str,
    persist_enabled: bool,
) -> andromeda::api::AppState {
    app_state(
        Arc::new(llm),
        store,
        instance_id,
        persist_enabled,
        ContextConfig::default(),
    )
}

async fn read_sse_until_tool(body: Body) -> (String, Vec<SseEvent>) {
    let bytes = body.collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    // For streaming tests we need incremental read — use a channel approach via oneshot spawn.
    // Fallback: this helper is only used when stream already ended.
    let mut events = Vec::new();
    let mut cur_data = String::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data: ") {
            cur_data.push_str(rest);
        } else if line.is_empty() && !cur_data.is_empty() {
            if let Ok(ev) = serde_json::from_str::<SseEvent>(&cur_data) {
                events.push(ev);
            }
            cur_data.clear();
        }
    }
    let run_id = events
        .iter()
        .find_map(|e| match e {
            SseEvent::RunStarted { run_id } | SseEvent::RunResumed { run_id, .. } => {
                Some(run_id.clone())
            }
            _ => None,
        })
        .unwrap_or_default();
    (run_id, events)
}

#[tokio::test]
async fn soft_disconnect_then_get_events_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn RunStore> = Arc::new(LocalFsRunStore::new(dir.path()));
    let llm = MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "call".into(),
            deltas: vec!["call".into()],
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({}),
            }],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]);
    let app = router(state(llm, Some(store.clone()), "node-a", true));

    let response = app
        .clone()
        .oneshot(json_post(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![echo_tool()],
                session_id: None,
                options: RunOptions {
                    persist: true,
                    ..Default::default()
                },
            },
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let run_id = response
        .headers()
        .get("x-run-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    // Drop the original SSE without reading — simulates client disconnect.
    drop(response);

    let _cp = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(cp) = store.load(&run_id).await.unwrap() {
                if cp.status == RunStatus::WaitingTool {
                    return cp;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiting_tool checkpoint");

    let resume = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/events"))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resume.status(), StatusCode::OK);

    // Submit tool while resumed stream is open.
    let tr = app
        .clone()
        .oneshot(json_post(
            &format!("/v1/runs/{run_id}/tool_results"),
            &ToolResultRequest {
                tool_call_id: "call_1".into(),
                content: "ok".into(),
                is_error: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(tr.status(), StatusCode::OK);

    let (_rid, events) = read_sse_until_tool(resume.into_body()).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunResumed { .. })),
        "expected run.resumed in {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop")),
        "expected finished, got {events:?}"
    );
}

#[tokio::test]
async fn cold_resume_from_disk_waiting_tool() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn RunStore> = Arc::new(LocalFsRunStore::new(dir.path()));

    // Simulate a crashed owner: checkpoint on disk, empty registry.
    let run_id = "cold-run-1".to_string();
    store
        .save(&Checkpoint {
            run_id: run_id.clone(),
            status: RunStatus::WaitingTool,
            context: vec![
                user_msg("hi"),
                WireMessage {
                    role: Role::Assistant,
                    content: "call".into(),
                    tool_call_id: None,
                    name: None,
                    tool_calls: None,
                    reasoning_content: None,
                },
            ],
            tools: vec![echo_tool()],
            todos: vec![],
            plan_mode: false,
            pending_tool: Some(PendingTool {
                tool_call_id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({}),
            }),
            guards: GuardsSnapshot::new_now(),
            parent_run_id: None,
            owner_id: Some("dead-node".into()),
            revision: 3,
            finish_reason: None,
        })
        .await
        .unwrap();

    let llm = MockLlm::script(vec![MockTurn::TextOnly {
        content: "done".into(),
        deltas: vec!["done".into()],
    }]);
    let app = router(state(llm, Some(store.clone()), "node-b", true));

    let resume = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/runs/{run_id}/events"))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resume.status(), StatusCode::OK);

    let claimed = store.load(&run_id).await.unwrap().unwrap();
    assert_eq!(claimed.owner_id.as_deref(), Some("node-b"));
    assert_eq!(claimed.revision, 4);

    let tr = app
        .clone()
        .oneshot(json_post(
            &format!("/v1/runs/{run_id}/tool_results"),
            &ToolResultRequest {
                tool_call_id: "call_1".into(),
                content: "ok".into(),
                is_error: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(tr.status(), StatusCode::OK);

    let (_rid, events) = read_sse_until_tool(resume.into_body()).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunResumed { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::ToolRequest { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop")),
        "{events:?}"
    );
}

#[tokio::test]
async fn tool_results_not_owner_when_checkpoint_owned_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn RunStore> = Arc::new(LocalFsRunStore::new(dir.path()));
    let run_id = "owned-elsewhere".to_string();
    store
        .save(&Checkpoint {
            run_id: run_id.clone(),
            status: RunStatus::WaitingTool,
            context: vec![user_msg("hi")],
            tools: vec![],
            todos: vec![],
            plan_mode: false,
            pending_tool: Some(PendingTool {
                tool_call_id: "c1".into(),
                name: "echo".into(),
                arguments: json!({}),
            }),
            guards: GuardsSnapshot::new_now(),
            parent_run_id: None,
            owner_id: Some("other-node".into()),
            revision: 2,
            finish_reason: None,
        })
        .await
        .unwrap();

    let app = router(state(
        MockLlm::script(vec![]),
        Some(store),
        "this-node",
        true,
    ));

    // No hot run in registry.
    let resp = app
        .oneshot(json_post(
            &format!("/v1/runs/{run_id}/tool_results"),
            &ToolResultRequest {
                tool_call_id: "c1".into(),
                content: "x".into(),
                is_error: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        response_status_and_code(resp).await,
        (StatusCode::CONFLICT, Some("not_owner".into()))
    );
}

async fn response_status_and_code(resp: axum::response::Response) -> (StatusCode, Option<String>) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(json!({}));
    let code = v.get("code").and_then(|c| c.as_str()).map(str::to_string);
    (status, code)
}

#[allow(dead_code)]
fn _ensure_run_id_type() {
    let _ = RunId(String::new());
}
