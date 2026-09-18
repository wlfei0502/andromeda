use std::sync::Arc;
use std::time::Duration;

use andromeda::follow_up::{ExampleOrderFollowUp, FollowUpPolicy, NoopFollowUp};
use andromeda::llm::{LlmChunk, LlmPort, MockLlm, MockTurn, ToolCall};
use andromeda::orchestrator::{run_agent, MAX_FOLLOW_UP_ROUNDS};
use andromeda::run::{RunHandle, RunRegistry};
use andromeda::wire::{MessageSource, Role, SseEvent, ToolDef, ToolResultRequest, WireMessage};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, Notify};

fn user_msg(content: &str) -> WireMessage {
    WireMessage {
        role: Role::User,
        content: content.into(),
        tool_call_id: None,
        name: None,
    }
}

fn echo_tool() -> ToolDef {
    ToolDef {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({ "type": "object" }),
    }
}

fn submit_now(run: &RunHandle, result: ToolResultRequest) {
    run.submit_tool_result(result)
        .expect("waiter must be armed before tool.request");
}

struct AlwaysFollowUp;

impl FollowUpPolicy for AlwaysFollowUp {
    fn next(&self, _context: &[WireMessage]) -> Vec<WireMessage> {
        vec![WireMessage {
            role: Role::System,
            content: "keep going".into(),
            tool_call_id: None,
            name: None,
        }]
    }
}

fn text_turn(content: &str, deltas: &[&str]) -> Vec<Result<LlmChunk, String>> {
    let mut chunks: Vec<Result<LlmChunk, String>> = deltas
        .iter()
        .map(|d| Ok(LlmChunk::TextDelta((*d).into())))
        .collect();
    chunks.push(Ok(LlmChunk::Completed {
        content: content.into(),
        tool_calls: vec![],
    }));
    chunks
}

struct GatedScriptLlm {
    turns: Mutex<Vec<Vec<Result<LlmChunk, String>>>>,
    gate: Mutex<Option<Arc<Notify>>>,
}

impl GatedScriptLlm {
    fn new(turns: Vec<Vec<Result<LlmChunk, String>>>, resume: Option<Arc<Notify>>) -> Self {
        Self {
            turns: Mutex::new(turns),
            gate: Mutex::new(resume),
        }
    }
}

fn gated_stream(
    chunks: Vec<Result<LlmChunk, String>>,
    resume: Option<Arc<Notify>>,
) -> BoxStream<'static, Result<LlmChunk, String>> {
    Box::pin(stream::unfold(
        (chunks.into_iter(), 0usize, resume),
        |(mut iter, idx, resume)| async move {
            if idx == 1 {
                if let Some(resume) = &resume {
                    resume.notified().await;
                }
            }
            let item = iter.next()?;
            Some((item, (iter, idx + 1, resume)))
        },
    ))
}

#[async_trait]
impl LlmPort for GatedScriptLlm {
    async fn stream(
        &self,
        _messages: &[WireMessage],
        _tools: &[ToolDef],
    ) -> Result<BoxStream<'static, Result<LlmChunk, String>>, String> {
        let chunks = {
            let mut turns = self.turns.lock().await;
            if turns.is_empty() {
                return Err("gated script exhausted".into());
            }
            turns.remove(0)
        };
        let gate = self.gate.lock().await.take();
        Ok(gated_stream(chunks, gate))
    }
}

async fn on_first_delta(
    rx: &mut mpsc::Receiver<SseEvent>,
    mut inject: impl FnMut(),
) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut injected = false;
    while let Some(ev) = rx.recv().await {
        if !injected && matches!(ev, SseEvent::MessageDelta { .. }) {
            injected = true;
            inject();
        }
        let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
        events.push(ev);
        if terminal {
            break;
        }
    }
    events
}

async fn collect_events<F, Fut>(rx: &mut mpsc::Receiver<SseEvent>, mut on_tool: F) -> Vec<SseEvent>
where
    F: FnMut(String, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut events = Vec::new();
    while let Some(ev) = rx.recv().await {
        if let SseEvent::ToolRequest {
            tool_call_id, name, ..
        } = &ev
        {
            on_tool(tool_call_id.clone(), name.clone()).await;
        }
        let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
        events.push(ev);
        if terminal {
            break;
        }
    }
    events
}

fn sources(events: &[SseEvent]) -> Vec<&MessageSource> {
    events
        .iter()
        .filter_map(|e| match e {
            SseEvent::MessageCompleted { source, .. } => source.as_ref(),
            _ => None,
        })
        .collect()
}

fn event_types(events: &[SseEvent]) -> Vec<&str> {
    events.iter().map(SseEvent::event_name).collect()
}

#[tokio::test]
async fn text_only_emits_started_deltas_completed_finished_without_tools() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![MockTurn::TextOnly {
        content: "hello world".into(),
        deltas: vec!["hello ".into(), "world".into()],
    }]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |_, _| async {}).await;
    agent.await.unwrap().unwrap();

    assert_eq!(
        event_types(&events),
        vec![
            "run.started",
            "message.delta",
            "message.delta",
            "message.completed",
            "run.finished",
        ]
    );
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, SseEvent::ToolRequest { .. })),
        "text-only run must not emit tool.request"
    );

    match &events[1] {
        SseEvent::MessageDelta { delta, .. } => assert_eq!(delta, "hello "),
        other => panic!("expected first delta, got {other:?}"),
    }
    match &events[2] {
        SseEvent::MessageDelta { delta, .. } => assert_eq!(delta, "world"),
        other => panic!("expected second delta, got {other:?}"),
    }
    match &events[3] {
        SseEvent::MessageCompleted {
            role,
            content,
            tool_calls,
            source,
            ..
        } => {
            assert_eq!(*role, Role::Assistant);
            assert_eq!(content, "hello world");
            assert!(tool_calls.is_none());
            assert_eq!(*source, Some(MessageSource::Assistant));
        }
        other => panic!("expected message.completed, got {other:?}"),
    }
    match &events[4] {
        SseEvent::RunFinished { reason, .. } => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished, got {other:?}"),
    }
}

#[tokio::test]
async fn tool_then_text_waits_for_client_tool_result() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![
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
            content: "echoed".into(),
            deltas: vec!["echoed".into()],
        },
    ]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("echo hi")],
        vec![echo_tool()],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |tool_call_id, name| {
        let run = run.clone();
        async move {
            assert_eq!(tool_call_id, "call_1");
            assert_eq!(name, "echo");
            submit_now(
                &run,
                ToolResultRequest {
                    tool_call_id,
                    content: "hi".into(),
                    is_error: false,
                },
            );
        }
    })
    .await;
    agent.await.unwrap().unwrap();

    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::ToolRequest { tool_call_id, name, .. } if tool_call_id == "call_1" && name == "echo")),
        "expected tool.request for echo, got {events:?}"
    );

    let completed: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            SseEvent::MessageCompleted {
                source,
                content,
                tool_calls,
                ..
            } => Some((source, content.as_str(), tool_calls.as_ref())),
            _ => None,
        })
        .collect();
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].0, &Some(MessageSource::Assistant));
    assert_eq!(completed[0].1, "calling echo");
    assert!(
        completed[0]
            .2
            .is_some_and(|tc| tc.len() == 1 && tc[0].id == "call_1"),
        "first completed should include the echo tool call"
    );
    assert_eq!(completed[1].0, &Some(MessageSource::Assistant));
    assert_eq!(completed[1].1, "echoed");
    assert!(completed[1].2.is_none());

    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished stop, got {other:?}"),
    }
}

#[tokio::test]
async fn steer_enqueued_before_second_llm_call_emits_completed_source_steer() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "will echo".into(),
            deltas: vec!["will echo".into()],
            tool_calls: vec![ToolCall {
                id: "call_steer".into(),
                name: "echo".into(),
                arguments: json!({ "msg": "hi" }),
            }],
        },
        MockTurn::TextOnly {
            content: "微辣已记下".into(),
            deltas: vec!["微辣已记下".into()],
        },
    ]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("下单")],
        vec![echo_tool()],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |tool_call_id, _name| {
        let run = run.clone();
        async move {
            run.enqueue_steer(vec![user_msg("改成微辣")])
                .expect("steer while waiting for tool");
            submit_now(
                &run,
                ToolResultRequest {
                    tool_call_id,
                    content: "queued".into(),
                    is_error: false,
                },
            );
        }
    })
    .await;
    agent.await.unwrap().unwrap();

    let steer_pos = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Steer),
                content,
                role: Role::User,
                ..
            } if content == "改成微辣"
        )
    });
    let second_assistant_pos = events
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match e {
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Assistant),
                content,
                ..
            } if content == "微辣已记下" => Some(i),
            _ => None,
        })
        .next();

    let steer_pos = steer_pos.expect("expected message.completed source=steer");
    let second_assistant_pos =
        second_assistant_pos.expect("expected second assistant message.completed");
    assert!(
        steer_pos < second_assistant_pos,
        "steer must be drained before the second LLM completed event; types={:?}",
        event_types(&events)
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e,
                SseEvent::MessageCompleted {
                    source: Some(MessageSource::Steer),
                    ..
                }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn example_order_follow_up_triggers_another_llm_turn_after_place_order() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "placing order".into(),
            deltas: vec!["placing order".into()],
            tool_calls: vec![ToolCall {
                id: "call_order".into(),
                name: "place_order".into(),
                arguments: json!({ "item": "noodles" }),
            }],
        },
        MockTurn::TextOnly {
            content: "order placed".into(),
            deltas: vec!["order placed".into()],
        },
        MockTurn::WithToolCalls {
            content: "sending sms".into(),
            deltas: vec!["sending sms".into()],
            tool_calls: vec![ToolCall {
                id: "call_sms".into(),
                name: "send_sms".into(),
                arguments: json!({ "code": "1234" }),
            }],
        },
        MockTurn::TextOnly {
            content: "sms sent".into(),
            deltas: vec!["sms sent".into()],
        },
    ]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(ExampleOrderFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("帮我点外卖")],
        vec![
            ToolDef {
                name: "place_order".into(),
                description: "place an order".into(),
                parameters: json!({ "type": "object" }),
            },
            ToolDef {
                name: "send_sms".into(),
                description: "send pickup code".into(),
                parameters: json!({ "type": "object" }),
            },
        ],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |tool_call_id, name| {
        let run = run.clone();
        async move {
            let content = match name.as_str() {
                "place_order" => "order ok",
                "send_sms" => "sent",
                other => panic!("unexpected tool {other}"),
            };
            submit_now(
                &run,
                ToolResultRequest {
                    tool_call_id,
                    content: content.into(),
                    is_error: false,
                },
            );
        }
    })
    .await;
    agent.await.unwrap().unwrap();

    let follow_up_pos = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::FollowUp),
                content,
                ..
            } if content.contains("send_sms")
        )
    });
    let sms_turn_pos = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Assistant),
                content,
                ..
            } if content == "sms sent"
        )
    });

    let follow_up_pos =
        follow_up_pos.expect("expected follow_up message.completed mentioning send_sms");
    let sms_turn_pos = sms_turn_pos.expect("expected assistant turn after follow-up");
    assert!(
        follow_up_pos < sms_turn_pos,
        "follow-up must be inserted before the next LLM turn; types={:?} sources={:?}",
        event_types(&events),
        sources(&events)
    );
    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished stop, got {other:?}"),
    }
}

#[tokio::test]
async fn llm_error_emits_error_event() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |_, _| async {}).await;
    let join = agent.await.unwrap();
    assert!(join.is_err(), "llm failure should return Err, got {join:?}");
    assert!(
        events.iter().any(|e| matches!(e, SseEvent::Error { .. })),
        "expected error event, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, SseEvent::RunStarted { .. })),
        "run.started should still be emitted before llm error"
    );
}

#[tokio::test]
async fn tool_wait_timeout_emits_error_event() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![MockTurn::WithToolCalls {
        content: "calling".into(),
        deltas: vec![],
        tool_calls: vec![ToolCall {
            id: "call_timeout".into(),
            name: "echo".into(),
            arguments: json!({}),
        }],
    }]));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![echo_tool()],
        llm,
        follow_up,
        sse,
        Duration::from_millis(50),
    ));

    let events = collect_events(&mut rx, |_id, _name| async {}).await;
    let join = agent.await.unwrap();
    assert!(
        join.is_err(),
        "tool timeout should return Err, got {join:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            SseEvent::Error { code: Some(code), .. } if code == "timeout"
        ) || matches!(e, SseEvent::Error { .. })),
        "expected error event on tool timeout, got {events:?}"
    );
}

#[tokio::test]
async fn follow_up_rounds_are_capped() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script_then_repeat(
        vec![],
        MockTurn::TextOnly {
            content: "ok".into(),
            deltas: vec![],
        },
    ));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(AlwaysFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = tokio::time::timeout(
        Duration::from_secs(2),
        collect_events(&mut rx, |_, _| async {}),
    )
    .await
    .expect("follow-up loop must terminate");
    let join = agent.await.unwrap();
    assert!(
        join.is_err(),
        "follow-up cap should return Err, got {join:?}"
    );

    let follow_up_count = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                SseEvent::MessageCompleted {
                    source: Some(MessageSource::FollowUp),
                    ..
                }
            )
        })
        .count();
    assert_eq!(
        follow_up_count,
        MAX_FOLLOW_UP_ROUNDS as usize,
        "expected {MAX_FOLLOW_UP_ROUNDS} follow-up rounds before cap, got {follow_up_count}; types={:?}",
        event_types(&events)
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            SseEvent::Error { code: Some(code), .. } if code == "follow_up_limit"
        )),
        "expected error code follow_up_limit, got {events:?}"
    );
}

#[tokio::test]
async fn example_order_follow_up_with_text_only_after_place_order_terminates() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script_then_repeat(
        vec![MockTurn::WithToolCalls {
            content: "placing".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_order".into(),
                name: "place_order".into(),
                arguments: json!({}),
            }],
        }],
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec![],
        },
    ));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(ExampleOrderFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("帮我点外卖")],
        vec![ToolDef {
            name: "place_order".into(),
            description: "place an order".into(),
            parameters: json!({ "type": "object" }),
        }],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = tokio::time::timeout(
        Duration::from_secs(2),
        collect_events(&mut rx, |tool_call_id, name| {
            let run = run.clone();
            async move {
                assert_eq!(name, "place_order");
                submit_now(
                    &run,
                    ToolResultRequest {
                        tool_call_id,
                        content: "order ok".into(),
                        is_error: false,
                    },
                );
            }
        }),
    )
    .await
    .expect("ExampleOrderFollowUp + text-only must not loop forever");
    let join = agent.await.unwrap();
    assert!(
        join.is_err(),
        "follow-up cap should return Err, got {join:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            SseEvent::Error { code: Some(code), .. } if code == "follow_up_limit"
        )),
        "expected follow_up_limit, got {events:?}"
    );
}

#[tokio::test]
async fn steer_after_text_only_turn_continues_inner_loop() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let resume = Arc::new(Notify::new());
    let llm: Arc<dyn LlmPort> = Arc::new(GatedScriptLlm::new(
        vec![
            text_turn("first", &["first"]),
            text_turn("after steer", &["after steer"]),
        ],
        Some(resume.clone()),
    ));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("hi")],
        vec![],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = tokio::time::timeout(Duration::from_secs(2), async {
        on_first_delta(&mut rx, || {
            run.enqueue_steer(vec![user_msg("改成微辣")])
                .expect("steer during first text-only stream");
            resume.notify_one();
        })
        .await
    })
    .await
    .expect("steer-after-text-only run must finish");
    agent.await.unwrap().unwrap();

    let steer_pos = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Steer),
                content,
                ..
            } if content == "改成微辣"
        )
    });
    let first_assistant = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Assistant),
                content,
                ..
            } if content == "first"
        )
    });
    let second_assistant = events.iter().position(|e| {
        matches!(
            e,
            SseEvent::MessageCompleted {
                source: Some(MessageSource::Assistant),
                content,
                ..
            } if content == "after steer"
        )
    });

    let first_assistant = first_assistant.expect("expected first assistant turn");
    let steer_pos = steer_pos.expect("expected source=steer after text-only turn");
    let second_assistant = second_assistant.expect("expected another assistant turn after steer");
    assert!(
        first_assistant < steer_pos && steer_pos < second_assistant,
        "expected assistant, then steer, then another assistant before finished; types={:?} sources={:?}",
        event_types(&events),
        sources(&events)
    );
    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "stop"),
        other => panic!("expected run.finished stop, got {other:?}"),
    }
}

#[tokio::test]
async fn tool_error_prefix_is_visible_to_next_llm_turn() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let mock = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "calling".into(),
            deltas: vec![],
            tool_calls: vec![ToolCall {
                id: "call_err".into(),
                name: "echo".into(),
                arguments: json!({}),
            }],
        },
        MockTurn::TextOnly {
            content: "failed".into(),
            deltas: vec!["failed".into()],
        },
    ]));
    let llm: Arc<dyn LlmPort> = mock.clone();
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("hi")],
        vec![echo_tool()],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = collect_events(&mut rx, |tool_call_id, _name| {
        let run = run.clone();
        async move {
            submit_now(
                &run,
                ToolResultRequest {
                    tool_call_id,
                    content: "boom".into(),
                    is_error: true,
                },
            );
        }
    })
    .await;
    agent.await.unwrap().unwrap();

    let contexts = mock.recorded_contexts();
    assert!(
        contexts.len() >= 2,
        "expected a second LLM turn after tool error, got {} contexts; events={:?}",
        contexts.len(),
        event_types(&events)
    );
    let tool_contents: Vec<&str> = contexts[1]
        .iter()
        .filter(|m| m.role == Role::Tool)
        .map(|m| m.content.as_str())
        .collect();
    assert!(
        tool_contents.iter().any(|c| c.starts_with("tool_error:")),
        "next LLM context should include tool_error: prefix, got {tool_contents:?}"
    );
}

#[tokio::test]
async fn cancel_during_text_only_stream_finishes_cancelled() {
    let registry = RunRegistry::new();
    let (_run_id, run) = registry.create();
    let resume = Arc::new(Notify::new());
    let llm: Arc<dyn LlmPort> = Arc::new(GatedScriptLlm::new(
        vec![text_turn("hello", &["hel", "lo"])],
        Some(resume.clone()),
    ));
    let follow_up: Arc<dyn FollowUpPolicy> = Arc::new(NoopFollowUp);
    let (sse, mut rx) = mpsc::channel(64);

    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("hi")],
        vec![],
        llm,
        follow_up,
        sse,
        Duration::from_secs(2),
    ));

    let events = tokio::time::timeout(Duration::from_secs(2), async {
        on_first_delta(&mut rx, || {
            run.cancel();
            resume.notify_one();
        })
        .await
    })
    .await
    .expect("cancel during text-only stream must terminate");
    let join = agent.await.unwrap();
    assert!(join.is_err(), "cancel should return Err, got {join:?}");
    match events.last() {
        Some(SseEvent::RunFinished { reason, .. }) => assert_eq!(reason, "cancelled"),
        other => panic!("expected run.finished cancelled, got {other:?} events={events:?}"),
    }
}
