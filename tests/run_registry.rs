mod common;

use andromeda::protocol::{Role, ToolResultRequest};
use common::user_msg;

#[tokio::test]
async fn steer_queues_until_drained() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    h.enqueue_steer(vec![user_msg("改成微辣")]).await.unwrap();
    let drained = h.drain_steer().await;
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].role, Role::User);
    assert_eq!(drained[0].content, "改成微辣");
    assert!(h.drain_steer().await.is_empty());
}

#[tokio::test]
async fn tool_result_unblocks_waiter() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    let wait = tokio::spawn({
        let h = h.clone();
        async move {
            h.wait_tool("call_1".into(), std::time::Duration::from_secs(2))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    h.submit_tool_result(andromeda::protocol::ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .await
    .unwrap();
    let got = wait.await.unwrap().unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn submit_tool_result_conflicts_when_not_waiting_or_id_mismatch() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;

    let err = h
        .submit_tool_result(ToolResultRequest {
            tool_call_id: "call_1".into(),
            content: "early".into(),
            is_error: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err, andromeda::runtime::SubmitError::Conflict);

    let wait = tokio::spawn({
        let h = h.clone();
        async move {
            h.wait_tool("call_1".into(), std::time::Duration::from_secs(2))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let err = h
        .submit_tool_result(ToolResultRequest {
            tool_call_id: "call_other".into(),
            content: "nope".into(),
            is_error: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err, andromeda::runtime::SubmitError::Conflict);

    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .await
    .unwrap();
    let got = wait.await.unwrap().unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn registry_get_returns_cloned_handle() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (id, h) = reg.create().await;
    h.enqueue_steer(vec![user_msg("via create")]).await.unwrap();

    let got = reg.get(&id).await.expect("run should be registered");
    let drained = got.drain_steer().await;
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].content, "via create");
    assert!(h.drain_steer().await.is_empty());
}

#[tokio::test]
async fn parallel_waiters_resolve_by_tool_call_id() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    let rx_a = h.begin_wait_tool("a".into()).await.unwrap();
    let rx_b = h.begin_wait_tool("b".into()).await.unwrap();

    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "b".into(),
        content: "B".into(),
        is_error: false,
    })
    .await
    .unwrap();
    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "a".into(),
        content: "A".into(),
        is_error: false,
    })
    .await
    .unwrap();

    assert_eq!(rx_b.await.unwrap().content, "B");
    assert_eq!(rx_a.await.unwrap().content, "A");
}

#[tokio::test]
async fn cancel_waits_for_parent_task_drops_orphans() {
    use andromeda::store::PendingTool;
    use serde_json::json;

    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    h.upsert_pending_tool(PendingTool {
        tool_call_id: "echo_1".into(),
        name: "echo".into(),
        arguments: json!({}),
        agent_id: Some("sub-task1".into()),
        parent_task_id: Some("task1".into()),
    })
    .await;
    let rx = h.begin_wait_tool("echo_1".into()).await.unwrap();
    assert!(h.is_waiting_tool().await);

    h.cancel_waits_for_parent_task("task1").await;
    assert!(!h.is_waiting_tool().await);
    assert!(h.pending_tools().await.is_empty());
    assert!(rx.await.is_err());
}

#[tokio::test]
async fn pending_tools_tracks_multiple() {
    use andromeda::store::PendingTool;
    use serde_json::json;

    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    h.upsert_pending_tool(PendingTool {
        tool_call_id: "a".into(),
        name: "echo".into(),
        arguments: json!({}),
        agent_id: Some("sub-t1".into()),
        parent_task_id: Some("t1".into()),
    })
    .await;
    h.upsert_pending_tool(PendingTool {
        tool_call_id: "b".into(),
        name: "echo".into(),
        arguments: json!({}),
        agent_id: Some("sub-t2".into()),
        parent_task_id: Some("t2".into()),
    })
    .await;
    let all = h.pending_tools().await;
    assert_eq!(all.len(), 2);
    h.remove_pending_tool("a").await;
    assert_eq!(h.pending_tools().await.len(), 1);
}

#[tokio::test]
async fn begin_wait_tool_allows_submit_before_awaiting() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    let rx = h.begin_wait_tool("call_1".into()).await.unwrap();
    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .await
    .unwrap();
    let got = rx.await.unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn cancel_unblocks_tool_waiter() {
    let reg = andromeda::runtime::RunRegistry::new();
    let (_id, h) = reg.create().await;
    let wait = tokio::spawn({
        let h = h.clone();
        async move {
            h.wait_tool("call_1".into(), std::time::Duration::from_secs(2))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    h.cancel().await;
    let err = wait.await.unwrap().unwrap_err();
    assert_eq!(err, andromeda::runtime::WaitError::Cancelled);

    let err = h
        .enqueue_steer(vec![user_msg("too late")])
        .await
        .unwrap_err();
    assert_eq!(err, andromeda::runtime::SubmitError::Conflict);
}
