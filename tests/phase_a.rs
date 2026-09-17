use std::sync::Arc;

use futures::StreamExt;
use serde_json::json;

use andromeda::{
    agent_loop, AgentContext, AgentEvent, AgentMessage, ContentPart, StopReason, StubBackend,
    StubTurn,
};

fn handle_messages_contain_tool_and_final(messages: &[AgentMessage]) -> bool {
    let has_tool = messages.iter().any(|m| {
        matches!(
            m,
            AgentMessage::ToolResult {
                tool_name,
                content,
                is_error: false,
                ..
            } if tool_name == "echo" && content == "ok"
        )
    });
    let has_final = messages.iter().any(|m| {
        matches!(
            m,
            AgentMessage::Assistant {
                parts,
                stop_reason: StopReason::Stop,
            } if parts.iter().any(|p| matches!(p, ContentPart::Text { text } if text == "done"))
        )
    });
    has_tool && has_final
}

fn is_tool_result_msg(m: &AgentMessage) -> bool {
    matches!(
        m,
        AgentMessage::ToolResult {
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_name == "echo" && content == "ok"
    )
}

fn is_final_assistant_msg(m: &AgentMessage) -> bool {
    matches!(
        m,
        AgentMessage::Assistant {
            parts,
            stop_reason: StopReason::Stop,
        } if parts.iter().any(|p| matches!(p, ContentPart::Text { text } if text == "done"))
    )
}

/// First index `i >= from` where `pred` holds.
fn find_from(events: &[AgentEvent], from: usize, pred: impl Fn(&AgentEvent) -> bool) -> Option<usize> {
    events.iter().enumerate().skip(from).find_map(|(i, e)| pred(e).then_some(i))
}

#[tokio::test]
async fn phase_a_tool_then_final_text_event_order() {
    let backend = StubBackend::script(vec![
        StubTurn::AssistantWithTool {
            name: "echo".into(),
            args: json!({"x": 1}),
        },
        StubTurn::AssistantText("done".into()),
    ]);
    let ctx = AgentContext::default();
    let stream = agent_loop(
        vec![AgentMessage::User {
            content: "hi".into(),
        }],
        ctx,
        Arc::new(backend),
        None,
    )
    .unwrap();
    let handle = stream.result_handle();
    let mut events = vec![];
    let mut s = stream;
    while let Some(e) = s.next().await {
        events.push(e);
    }
    let messages = handle.await.unwrap();

    assert!(matches!(events.first(), Some(AgentEvent::AgentStart)));
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd { .. })));

    // Relative order (design §2 / Phase A #4):
    // AgentStart → MessageUpdate → ToolExecutionStart → ToolExecutionEnd →
    // toolResult MessageStart/End → later assistant turn → AgentEnd
    let mut cursor = 0usize;
    let i_agent_start = find_from(&events, cursor, |e| matches!(e, AgentEvent::AgentStart))
        .expect("AgentStart");
    cursor = i_agent_start + 1;

    let i_msg_update = find_from(&events, cursor, |e| matches!(e, AgentEvent::MessageUpdate { .. }))
        .expect("MessageUpdate before tools");
    cursor = i_msg_update + 1;

    let i_tool_start =
        find_from(&events, cursor, |e| matches!(e, AgentEvent::ToolExecutionStart { .. }))
            .expect("ToolExecutionStart after MessageUpdate");
    cursor = i_tool_start + 1;

    let i_tool_end =
        find_from(&events, cursor, |e| matches!(e, AgentEvent::ToolExecutionEnd { .. }))
            .expect("ToolExecutionEnd after ToolExecutionStart");
    cursor = i_tool_end + 1;

    let i_tr_start = find_from(&events, cursor, |e| {
        matches!(e, AgentEvent::MessageStart { message } if is_tool_result_msg(message))
    })
    .expect("toolResult MessageStart after ToolExecutionEnd");
    cursor = i_tr_start + 1;

    let i_tr_end = find_from(&events, cursor, |e| {
        matches!(e, AgentEvent::MessageEnd { message } if is_tool_result_msg(message))
    })
    .expect("toolResult MessageEnd after MessageStart");
    cursor = i_tr_end + 1;

    let i_final_assistant = find_from(&events, cursor, |e| {
        matches!(
            e,
            AgentEvent::MessageStart { message }
                | AgentEvent::MessageUpdate { message }
                | AgentEvent::MessageEnd { message }
            if is_final_assistant_msg(message)
        )
    })
    .expect("final assistant turn after toolResult");
    cursor = i_final_assistant + 1;

    let i_agent_end = find_from(&events, cursor, |e| matches!(e, AgentEvent::AgentEnd { .. }))
        .expect("AgentEnd after final assistant");
    assert_eq!(i_agent_end, events.len() - 1);

    assert_eq!(handle_messages_contain_tool_and_final(&messages), true);

    // Design §4 Phase A #5: result() agrees with AgentEnd.messages
    match events.last() {
        Some(AgentEvent::AgentEnd {
            messages: end_messages,
        }) => {
            assert_eq!(&messages, end_messages);
        }
        other => panic!("expected AgentEnd last, got {other:?}"),
    }
}

#[tokio::test]
async fn phase_a_follow_up_restarts_outer_loop() {
    let backend = StubBackend::script(vec![
        StubTurn::AssistantText("first".into()),
        StubTurn::AssistantText("second".into()),
    ])
    .with_follow_up(AgentMessage::User {
        content: "more".into(),
    });
    let ctx = AgentContext::default();
    let stream = agent_loop(
        vec![AgentMessage::User {
            content: "hi".into(),
        }],
        ctx,
        Arc::new(backend),
        None,
    )
    .unwrap();
    let handle = stream.result_handle();
    let mut events = vec![];
    let mut s = stream;
    while let Some(e) = s.next().await {
        events.push(e);
    }
    let messages = handle.await.unwrap();

    assert!(matches!(events.first(), Some(AgentEvent::AgentStart)));
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd { .. })));

    let turn_starts = events
        .iter()
        .filter(|e| matches!(e, AgentEvent::TurnStart))
        .count();
    assert!(
        turn_starts >= 2,
        "follow-up should restart outer loop with another TurnStart, got {turn_starts}"
    );

    let assistant_texts: Vec<&str> = messages
        .iter()
        .filter_map(|m| match m {
            AgentMessage::Assistant {
                parts,
                stop_reason: StopReason::Stop,
            } => parts.iter().find_map(|p| match p {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .collect();
    assert_eq!(assistant_texts, vec!["first", "second"]);

    assert!(messages.iter().any(|m| {
        matches!(
            m,
            AgentMessage::User { content } if content == "more"
        )
    }));
}
