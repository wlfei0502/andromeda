mod common;

use std::sync::Arc;

use andromeda::api::router;
use andromeda::llm::{MockLlm, MockTurn, ToolCall};
use andromeda::protocol::{
    CreateRunRequest, SseEvent, SteerRequest, ToolResultRequest,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use common::{collect_sse_with as collect_sse, echo_tool, json_request, memory_state, user_msg};

fn state_with(llm: MockLlm) -> andromeda::api::AppState {
    memory_state(Arc::new(llm))
}

#[tokio::test]
async fn create_run_streams_until_finished() {
    let app = router(state_with(MockLlm::script(vec![MockTurn::TextOnly {
        content: "hello world".into(),
        deltas: vec!["hello ".into(), "world".into()],
    }])));

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![],
                session_id: None,
                options: Default::default(),
            },
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let run_id = response
        .headers()
        .get("x-run-id")
        .expect("X-Run-Id header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(!run_id.is_empty());
    let content_type = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.starts_with("text/event-stream"),
        "content-type={content_type}"
    );

    let events = collect_sse(response.into_body(), |_| async {}).await;
    let types: Vec<&str> = events.iter().map(SseEvent::event_name).collect();
    assert_eq!(
        types,
        vec![
            "run.started",
            "message.delta",
            "message.delta",
            "message.completed",
            "run.finished",
        ]
    );
    match events.first() {
        Some(SseEvent::RunStarted { run_id: started }) => assert_eq!(started, &run_id),
        other => panic!("expected run.started, got {other:?}"),
    }
    match events.last() {
        Some(SseEvent::RunFinished { reason, run_id: id }) => {
            assert_eq!(id, &run_id);
            assert_eq!(reason, "stop");
        }
        other => panic!("expected run.finished, got {other:?}"),
    }
}

#[tokio::test]
async fn steer_after_run_finished_returns_conflict() {
    let app = router(state_with(MockLlm::script(vec![MockTurn::TextOnly {
        content: "hello".into(),
        deltas: vec!["hello".into()],
    }])));

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![],
                session_id: None,
                options: Default::default(),
            },
        ))
        .await
        .unwrap();

    let run_id = response
        .headers()
        .get("x-run-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let events = collect_sse(response.into_body(), |_| async {}).await;
    assert!(matches!(events.last(), Some(SseEvent::RunFinished { .. })));

    let steer_resp = app
        .oneshot(json_request(
            &format!("/v1/runs/{run_id}/steer"),
            &SteerRequest {
                messages: vec![user_msg("too late")],
            },
        ))
        .await
        .unwrap();
    assert_eq!(steer_resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn tool_results_unblocks_run_until_finished() {
    let app = router(state_with(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "calling echo".into(),
            deltas: vec!["calling echo".into()],
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({ "msg": "hi" }),
            }],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ])));

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![echo_tool()],
                session_id: None,
                options: Default::default(),
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

    let events = collect_sse(response.into_body(), |ev| {
        let app = app.clone();
        let run_id = run_id.clone();
        async move {
            if let SseEvent::ToolRequest {
                tool_call_id, name, ..
            } = ev
            {
                assert_eq!(name, "echo");
                let res = app
                    .oneshot(json_request(
                        &format!("/v1/runs/{run_id}/tool_results"),
                        &ToolResultRequest {
                            tool_call_id: tool_call_id.clone(),
                            content: "hi".into(),
                            is_error: false,
                        },
                    ))
                    .await
                    .unwrap();
                assert_eq!(res.status(), StatusCode::OK);
            }
        }
    })
    .await;

    let types: Vec<&str> = events.iter().map(SseEvent::event_name).collect();
    assert!(
        types.contains(&"tool.request"),
        "missing tool.request in {types:?}"
    );
    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished, got {other:?}"),
    }
}

#[tokio::test]
async fn steer_ok_while_active_conflict_after_finished() {
    let app = router(state_with(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "need echo".into(),
            deltas: vec!["need echo".into()],
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({ "msg": "hi" }),
            }],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ])));

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![echo_tool()],
                session_id: None,
                options: Default::default(),
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

    let events = collect_sse(response.into_body(), |ev| {
        let app = app.clone();
        let run_id = run_id.clone();
        async move {
            if let SseEvent::ToolRequest { tool_call_id, .. } = ev {
                let steer = app
                    .clone()
                    .oneshot(json_request(
                        &format!("/v1/runs/{run_id}/steer"),
                        &SteerRequest {
                            messages: vec![user_msg("改成微辣")],
                        },
                    ))
                    .await
                    .unwrap();
                assert_eq!(steer.status(), StatusCode::OK);

                let res = app
                    .oneshot(json_request(
                        &format!("/v1/runs/{run_id}/tool_results"),
                        &ToolResultRequest {
                            tool_call_id: tool_call_id.clone(),
                            content: "ok".into(),
                            is_error: false,
                        },
                    ))
                    .await
                    .unwrap();
                assert_eq!(res.status(), StatusCode::OK);
            }
        }
    })
    .await;

    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished, got {other:?}"),
    }

    let after = app
        .oneshot(json_request(
            &format!("/v1/runs/{run_id}/steer"),
            &SteerRequest {
                messages: vec![user_msg("too late")],
            },
        ))
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn cancel_finishes_without_duplicate_error() {
    let app = router(state_with(MockLlm::script(vec![MockTurn::WithToolCalls {
        content: "need echo".into(),
        deltas: vec!["need echo".into()],
        tool_calls: vec![ToolCall {
            id: "call_1".into(),
            name: "echo".into(),
            arguments: json!({ "msg": "hi" }),
        }],
    }])));

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("hi")],
                tools: vec![echo_tool()],
                session_id: None,
                options: Default::default(),
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

    let events = collect_sse(response.into_body(), |ev| {
        let app = app.clone();
        let run_id = run_id.clone();
        async move {
            if let SseEvent::ToolRequest { .. } = ev {
                let res = app
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri(format!("/v1/runs/{run_id}/cancel"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(res.status(), StatusCode::OK);
            }
        }
    })
    .await;

    let error_count = events
        .iter()
        .filter(|e| matches!(e, SseEvent::Error { .. }))
        .count();
    assert_eq!(
        error_count, 0,
        "cancel must not emit a duplicate error event"
    );
    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "cancelled"),
        other => panic!("expected run.finished cancelled, got {other:?}"),
    }
}
