mod common;

use std::sync::Arc;

use andromeda::api::router;
use andromeda::config::SubagentsConfig;
use andromeda::llm::{MockLlm, MockTurn, ToolCall};
use andromeda::protocol::{CreateRunRequest, RunOptions, SseEvent, ToolDef};
use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use common::{
    app_state, collect_sse, collect_sse_with, echo_tool, json_post, json_request, user_msg,
};

fn readonly_echo() -> ToolDef {
    ToolDef {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({ "type": "object" }),
        readonly: Some(true),
    }
}

#[tokio::test]
async fn subagent_task_returns_summary_without_nested_deltas() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_task".into(),
                name: "task".into(),
                arguments: json!({
                    "goal": "summarize layers",
                    "agent": "general"
                }),
            }],
        },
        MockTurn::TextOnly {
            content: "found 3 layers".into(),
            deltas: vec!["found 3 layers".into()],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]));

    let mut state = app_state(llm, None, "node-sub", false, Default::default());
    state.subagents = SubagentsConfig {
        max_concurrent_subagents: 2,
        subagent_timeout_secs: 30,
    };
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("delegate")],
                tools: vec![echo_tool()],
                session_id: None,
                options: RunOptions {
                    persist: false,
                    plan_mode: false,
                    subagents: true,
                },
            },
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let events = collect_sse(response.into_body()).await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::TaskStarted { .. })),
        "expected task.started: {:?}",
        events.iter().map(|e| e.event_name()).collect::<Vec<_>>()
    );
    assert!(events.iter().any(|e| matches!(
        e,
        SseEvent::TaskCompleted { summary, .. } if summary == "found 3 layers"
    )));
    let deltas: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            SseEvent::MessageDelta { delta, .. } => Some(delta.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        !deltas.iter().any(|d| *d == "found 3 layers"),
        "subagent deltas must be folded, got {deltas:?}"
    );
    assert!(deltas.iter().any(|d| *d == "done"));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop"))
    );
}

#[tokio::test]
async fn subagent_client_tool_channel_a() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_task".into(),
                name: "task".into(),
                arguments: json!({ "goal": "echo hi", "agent": "explore" }),
            }],
        },
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_echo".into(),
                name: "echo".into(),
                arguments: json!({ "text": "hi" }),
            }],
        },
        MockTurn::TextOnly {
            content: "echoed".into(),
            deltas: vec![],
        },
        MockTurn::TextOnly {
            content: "parent done".into(),
            deltas: vec!["parent done".into()],
        },
    ]));

    let mut state = app_state(llm, None, "node-sub3", false, Default::default());
    state.subagents.subagent_timeout_secs = 30;
    let app = router(state);

    let response = app
        .clone()
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("delegate with tool")],
                tools: vec![readonly_echo()],
                session_id: None,
                options: RunOptions {
                    persist: false,
                    plan_mode: false,
                    subagents: true,
                },
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

    let events = collect_sse_with(response.into_body(), |ev| {
        let app = app.clone();
        let run_id = run_id.clone();
        async move {
            if let SseEvent::ToolRequest {
                tool_call_id,
                agent_id,
                parent_task_id,
                ..
            } = &ev
            {
                assert_eq!(agent_id.as_deref(), Some("sub-call_task"));
                assert_eq!(parent_task_id.as_deref(), Some("call_task"));
                let _ = app
                    .oneshot(json_post(
                        &format!("/v1/runs/{run_id}/tool_results"),
                        &json!({
                            "tool_call_id": tool_call_id,
                            "content": "hi",
                            "is_error": false
                        }),
                    ))
                    .await
                    .unwrap();
            }
        }
    })
    .await;

    assert!(events.iter().any(|e| matches!(
        e,
        SseEvent::TaskCompleted { summary, .. } if summary == "echoed"
    )));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop"))
    );
}

#[tokio::test]
async fn subagent_timeout_while_waiting_tool_clears_orphans() {
    use std::time::Duration;

    use andromeda::agent::{NoopFollowUp, default_summarize_chain, run_agent};
    use andromeda::config::{ContextConfig, SubagentsConfig};
    use andromeda::llm::LlmPort;
    use andromeda::runtime::RunRegistry;

    let registry = RunRegistry::new();
    let (_id, run) = registry.create().await;
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_task".into(),
                name: "task".into(),
                arguments: json!({ "goal": "hang", "agent": "general" }),
            }],
        },
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_echo".into(),
                name: "echo".into(),
                arguments: json!({ "text": "x" }),
            }],
        },
        MockTurn::TextOnly {
            content: "parent after timeout".into(),
            deltas: vec!["parent after timeout".into()],
        },
    ]));

    let mut rx = run.subscribe().await;
    let run_for_assert = run.clone();
    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("delegate hang")],
        vec![echo_tool()],
        llm,
        Arc::new(NoopFollowUp),
        Duration::from_secs(30),
        None,
        default_summarize_chain(ContextConfig::default()),
        andromeda::agent::RunAgentOpts {
            subagents: true,
            subagents_cfg: SubagentsConfig {
                max_concurrent_subagents: 2,
                subagent_timeout_secs: 1,
            },
            ..Default::default()
        },
    ));

    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) => {
                let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
                events.push(ev);
                if terminal {
                    break;
                }
            }
            _ => break,
        }
    }
    let _ = agent.await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::TaskTimedOut { task_id, .. } if task_id == "call_task")),
        "expected task.timed_out, got {:?}",
        events.iter().map(|e| e.event_name()).collect::<Vec<_>>()
    );
    assert!(
        !run_for_assert.is_waiting_tool().await,
        "orphan waiter must be cleared after subagent timeout"
    );
    assert!(run_for_assert.pending_tools().await.is_empty());
}

#[tokio::test]
async fn overflow_tasks_return_error_tool_result() {
    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "".into(),
            deltas: vec![],
            tool_calls: vec![
                ToolCall {
                    id: "t1".into(),
                    name: "task".into(),
                    arguments: json!({ "goal": "a" }),
                },
                ToolCall {
                    id: "t2".into(),
                    name: "task".into(),
                    arguments: json!({ "goal": "b" }),
                },
                ToolCall {
                    id: "t3".into(),
                    name: "task".into(),
                    arguments: json!({ "goal": "c" }),
                },
            ],
        },
        MockTurn::TextOnly {
            content: "sa".into(),
            deltas: vec![],
        },
        MockTurn::TextOnly {
            content: "sb".into(),
            deltas: vec![],
        },
        MockTurn::TextOnly {
            content: "parent".into(),
            deltas: vec!["parent".into()],
        },
    ]));

    let mut state = app_state(llm, None, "node-ov", false, Default::default());
    state.subagents = SubagentsConfig {
        max_concurrent_subagents: 2,
        subagent_timeout_secs: 30,
    };
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "/v1/runs",
            &CreateRunRequest {
                messages: vec![user_msg("three tasks")],
                tools: vec![],
                session_id: None,
                options: RunOptions {
                    persist: false,
                    plan_mode: false,
                    subagents: true,
                },
            },
        ))
        .await
        .unwrap();

    let events = collect_sse(response.into_body()).await;
    let started = events
        .iter()
        .filter(|e| matches!(e, SseEvent::TaskStarted { .. }))
        .count();
    assert_eq!(started, 2, "only first two tasks should start");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::TaskCompleted { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunFinished { reason, .. } if reason == "stop"))
    );
}
