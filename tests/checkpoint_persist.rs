use std::sync::Arc;
use std::time::Duration;

use andromeda::agent::{NoopFollowUp, RunPersist, run_agent};
use andromeda::config::ContextConfig;
use andromeda::llm::{MockLlm, MockTurn, ToolCall};
use andromeda::protocol::{Role, ToolDef, ToolResultRequest, WireMessage};
use andromeda::runtime::RunRegistry;
use andromeda::store::{LocalFsRunStore, RunStatus, RunStore};
use serde_json::json;

fn user_msg(content: &str) -> WireMessage {
    WireMessage {
        role: Role::User,
        content: content.into(),
        tool_call_id: None,
        name: None,
        tool_calls: None,
    }
}

#[tokio::test]
async fn persists_waiting_tool_then_completed() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn RunStore> = Arc::new(LocalFsRunStore::new(dir.path()));
    let registry = RunRegistry::new();
    let (_id, run) = registry.create().await;
    run.set_persist(true).await;

    let llm = Arc::new(MockLlm::script(vec![
        MockTurn::WithToolCalls {
            content: "call".into(),
            deltas: vec!["call".into()],
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({"x": 1}),
            }],
        },
        MockTurn::TextOnly {
            content: "done".into(),
            deltas: vec!["done".into()],
        },
    ]));

    let mut rx = run.subscribe().await;
    let persist = Some(RunPersist {
        store: store.clone(),
        instance_id: "node-test".into(),
    });
    let tools = vec![ToolDef {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({"type": "object"}),
    }];
    let agent = tokio::spawn(run_agent(
        run.clone(),
        vec![user_msg("hi")],
        tools,
        llm,
        Arc::new(NoopFollowUp),
        Duration::from_secs(5),
        persist,
        ContextConfig::default(),
    ));

    // Wait for tool.request, then poll checkpoint (emit precedes persist by a tick).
    loop {
        let ev = rx.recv().await.expect("event");
        if matches!(ev, andromeda::protocol::SseEvent::ToolRequest { .. }) {
            break;
        }
    }

    let cp = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(cp) = store.load(&run.id().0).await.unwrap() {
                if cp.status == RunStatus::WaitingTool {
                    return cp;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout waiting for waiting_tool checkpoint");
    assert_eq!(cp.status, RunStatus::WaitingTool);
    assert_eq!(
        cp.pending_tool.as_ref().unwrap().tool_call_id,
        "call_1"
    );
    assert_eq!(cp.owner_id.as_deref(), Some("node-test"));
    assert!(cp.revision >= 1);

    run.submit_tool_result(ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .await
    .unwrap();

    while let Some(ev) = rx.recv().await {
        if matches!(
            ev,
            andromeda::protocol::SseEvent::RunFinished { .. }
                | andromeda::protocol::SseEvent::Error { .. }
        ) {
            break;
        }
    }
    agent.await.unwrap().unwrap();

    let final_cp = store.load(&run.id().0).await.unwrap().unwrap();
    assert_eq!(final_cp.status, RunStatus::Completed);
    assert!(final_cp.pending_tool.is_none());
}
