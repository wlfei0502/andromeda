use std::sync::Arc;
use std::time::Duration;

use andromeda::agent::NoopFollowUp;
use andromeda::api::{AppState, router};
use andromeda::config::ContextConfig;
use andromeda::llm::{MockLlm, MockTurn};
use andromeda::protocol::{CreateRunRequest, Role, SseEvent, WireMessage};
use andromeda::runtime::RunRegistry;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

const SUMMARY_PREFIX: &str = "[conversation summary]";

fn wire_msg(role: Role, content: &str) -> WireMessage {
    WireMessage {
        role,
        content: content.into(),
        tool_call_id: None,
        name: None,
        tool_calls: None,
    }
}

fn long_history() -> Vec<WireMessage> {
    let mut messages = vec![wire_msg(Role::System, "persona")];
    for i in 0..6 {
        messages.push(wire_msg(
            Role::User,
            &format!("user-{i}-{}", "x".repeat(20)),
        ));
        messages.push(wire_msg(Role::Assistant, &format!("asst-{i}")));
    }
    messages
}

fn state_with(llm: Arc<MockLlm>, context: ContextConfig) -> AppState {
    AppState {
        registry: RunRegistry::new(),
        store: None,
        instance_id: "test-node".into(),
        persist_enabled: false,
        llm,
        follow_up: Arc::new(NoopFollowUp),
        tool_timeout: Duration::from_secs(2),
        context,
    }
}

fn json_request(uri: &str, body: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

fn parse_sse_frame(frame: &str) -> Option<SseEvent> {
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&data)
                .unwrap_or_else(|err| panic!("invalid SSE data JSON: {err}; data={data:?}")),
        )
    }
}

async fn collect_sse(body: Body) -> Vec<SseEvent> {
    let mut body = body;
    let mut buf = String::new();
    let mut events = Vec::new();
    loop {
        let chunk = match body.frame().await {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => data,
                Err(_) => continue,
            },
            Some(Err(err)) => panic!("body error: {err}"),
            None => break,
        };
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = buf.find("\n\n") {
            let raw = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            if let Some(ev) = parse_sse_frame(&raw) {
                let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
                events.push(ev);
                if terminal {
                    return events;
                }
            }
        }
    }
    events
}

#[tokio::test]
async fn summarize_emits_sse_and_compresses_context_for_main_llm() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::TextOnly {
            content: "Goal: test\nDone: steps\nFacts: f\nOpen: none".into(),
            deltas: vec![],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]));
    let context = ContextConfig {
        summarize_threshold_tokens: 1,
        keep_last_messages: 2,
        max_context_tokens: 100_000,
    };
    let app = router(state_with(llm.clone(), context));

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: long_history(),
                tools: vec![],
                session_id: None,
                options: Default::default(),
            },
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let events = collect_sse(response.into_body()).await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::ContextSummarized { .. })),
        "expected context.summarized, got {:?}",
        events.iter().map(SseEvent::event_name).collect::<Vec<_>>()
    );

    let summarized_idx = events
        .iter()
        .position(|e| matches!(e, SseEvent::ContextSummarized { .. }))
        .expect("context.summarized");
    let done_idx = events
        .iter()
        .position(|e| {
            matches!(
                e,
                SseEvent::MessageCompleted { content, .. } if content == "done"
            )
        })
        .expect("message.completed with done");
    assert!(
        summarized_idx < done_idx,
        "context.summarized should precede assistant done"
    );

    let contexts = llm.recorded_contexts();
    assert!(
        contexts.len() >= 2,
        "expected summarizer + main LLM contexts, got {}",
        contexts.len()
    );
    assert!(
        contexts[1]
            .iter()
            .any(|m| m.role == Role::System && m.content.starts_with(SUMMARY_PREFIX)),
        "main LLM context should include summary system message: {:?}",
        contexts[1]
    );

    assert!(matches!(events.last(), Some(SseEvent::RunFinished { .. })));
}

#[tokio::test]
async fn summarize_failure_over_hard_cap_emits_context_overflow() {
    let llm = Arc::new(MockLlm::script(vec![MockTurn::Fail {
        message: "boom".into(),
    }]));
    let context = ContextConfig {
        summarize_threshold_tokens: 1,
        keep_last_messages: 2,
        max_context_tokens: 1,
    };
    let app = router(state_with(llm, context));

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: long_history(),
                tools: vec![],
                session_id: None,
                options: Default::default(),
            },
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let events = collect_sse(response.into_body()).await;

    match events.last() {
        Some(SseEvent::Error { code, .. }) => {
            assert_eq!(code.as_deref(), Some("context_overflow"));
        }
        other => panic!("expected error with context_overflow, got {other:?}; all={events:?}"),
    }
}
