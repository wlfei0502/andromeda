mod common;

use std::sync::Arc;

use andromeda::api::router;
use andromeda::config::ContextConfig;
use andromeda::llm::{MockLlm, MockTurn};
use andromeda::protocol::{CreateRunRequest, Role, SseEvent, WireMessage};
use axum::http::StatusCode;
use tower::ServiceExt;

use common::{collect_sse, json_request, memory_state_with_context};

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
    let app = router(memory_state_with_context(llm.clone(), context));

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
    let app = router(memory_state_with_context(llm, context));

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
