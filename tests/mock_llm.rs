use andromeda::llm::{LlmChunk, LlmPort, MockLlm, MockTurn, ToolCall};
use andromeda::wire::{Role, ToolDef, WireMessage};
use futures::StreamExt;
use serde_json::json;

fn user_msg(content: &str) -> WireMessage {
    WireMessage {
        role: Role::User,
        content: content.into(),
        tool_call_id: None,
        name: None,
    }
}

#[tokio::test]
async fn mock_yields_delta_then_completed_with_tool_call() {
    let llm = MockLlm::script(vec![MockTurn::WithToolCalls {
        content: "I'll call echo".into(),
        deltas: vec!["I'll ".into(), "call echo".into()],
        tool_calls: vec![ToolCall {
            id: "call_1".into(),
            name: "echo".into(),
            arguments: json!({ "msg": "hi" }),
        }],
    }]);

    let messages = [user_msg("hi")];
    let tools = [ToolDef {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({ "type": "object" }),
    }];

    let mut stream = llm.stream(&messages, &tools).await.unwrap();

    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        LlmChunk::TextDelta("I'll ".into())
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        LlmChunk::TextDelta("call echo".into())
    );
    match stream.next().await.unwrap().unwrap() {
        LlmChunk::Completed {
            content,
            tool_calls,
        } => {
            assert_eq!(content, "I'll call echo");
            assert_eq!(tool_calls.len(), 1);
            assert_eq!(tool_calls[0].id, "call_1");
            assert_eq!(tool_calls[0].name, "echo");
            assert_eq!(tool_calls[0].arguments, json!({ "msg": "hi" }));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
    assert!(stream.next().await.is_none());
}
