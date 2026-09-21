mod common;

use std::sync::Arc;
use std::time::Duration;

use andromeda::api::router;
use andromeda::llm::{MockLlm, MockTurn, ToolCall};
use andromeda::protocol::{CreateRunRequest, RunOptions, SseEvent, TodoStatus};
use andromeda::store::{LocalFsRunStore, RunStore};
use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use common::{
    app_state, collect_sse, collect_sse_with, echo_tool, json_post, json_request, user_msg,
};

#[tokio::test]
async fn plan_mode_write_todos_emits_sse_without_tool_request() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_todos".into(),
                name: "write_todos".into(),
                arguments: json!({
                    "todos": [
                        {"id": "t1", "content": "inventory layers", "status": "in_progress"},
                        {"id": "t2", "content": "export", "status": "pending"}
                    ]
                }),
            }],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]));

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn RunStore> = Arc::new(LocalFsRunStore::new(dir.path()));
    let app = router(app_state(
        llm,
        Some(store.clone()),
        "node-plan",
        true,
        Default::default(),
    ));

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("plan a buffer analysis")],
                tools: vec![],
                session_id: None,
                options: RunOptions {
                    persist: true,
                    plan_mode: true,
                    subagents: false,
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
    let events = collect_sse(response.into_body()).await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::TodosUpdated { .. })),
        "expected todos.updated, got: {:?}",
        events.iter().map(|e| e.event_name()).collect::<Vec<_>>()
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SseEvent::ToolRequest { .. })),
        "server tool must not emit tool.request"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop"))
    );

    let todos_ev = events
        .iter()
        .find_map(|e| match e {
            SseEvent::TodosUpdated { todos, .. } => Some(todos),
            _ => None,
        })
        .expect("todos");
    assert_eq!(todos_ev.len(), 2);
    assert_eq!(todos_ev[0].status, TodoStatus::InProgress);

    tokio::time::sleep(Duration::from_millis(80)).await;
    let cp = store.load(&run_id).await.unwrap().expect("checkpoint");
    assert!(cp.plan_mode);
    assert_eq!(cp.todos.len(), 2);
    assert_eq!(cp.todos[0].id, "t1");
}

#[tokio::test]
async fn plan_mode_invalid_write_todos_keeps_list_and_finishes() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_bad".into(),
                name: "write_todos".into(),
                arguments: json!({
                    "todos": [{"id": "t1", "content": "x", "status": "doing"}]
                }),
            }],
        },
        MockTurn::TextOnly {
            content: "ok".into(),
            deltas: vec!["ok".into()],
        },
    ]));
    let app = router(common::memory_state(llm));

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![],
                session_id: None,
                options: RunOptions {
                    persist: false,
                    plan_mode: true,
                    subagents: false,
                },
            },
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let events = collect_sse(response.into_body()).await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SseEvent::TodosUpdated { .. })),
        "invalid write_todos must not emit todos.updated"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SseEvent::ToolRequest { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop"))
    );
}

#[tokio::test]
async fn plan_mode_mixed_turn_order_and_client_tool() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![
                ToolCall {
                    id: "call_todos".into(),
                    name: "write_todos".into(),
                    arguments: json!({
                        "todos": [
                            {"id": "t1", "content": "echo something", "status": "in_progress"}
                        ]
                    }),
                },
                ToolCall {
                    id: "call_echo".into(),
                    name: "echo".into(),
                    arguments: json!({"text": "hi"}),
                },
            ],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]));
    let app = router(common::memory_state(llm));

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![echo_tool()],
                session_id: None,
                options: RunOptions {
                    persist: false,
                    plan_mode: true,
                    subagents: false,
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

    let events = collect_sse_with(response.into_body(), |ev| {
        let app = app.clone();
        let run_id = run_id.clone();
        async move {
            if let SseEvent::ToolRequest {
                tool_call_id, name, ..
            } = &ev
            {
                assert_eq!(name, "echo");
                let _ = app
                    .oneshot(json_post(
                        &format!("/v1/runs/{run_id}/tool_results"),
                        &json!({
                            "tool_call_id": tool_call_id,
                            "content": "{\"ok\":true}",
                            "is_error": false
                        }),
                    ))
                    .await
                    .unwrap();
            }
        }
    })
    .await;

    let names: Vec<&str> = events.iter().map(|e| e.event_name()).collect();
    let todos_pos = names
        .iter()
        .position(|n| *n == "todos.updated")
        .expect("todos");
    let tool_pos = names
        .iter()
        .position(|n| *n == "tool.request")
        .expect("tool");
    assert!(todos_pos < tool_pos);
    assert!(names.contains(&"run.finished"));
}
