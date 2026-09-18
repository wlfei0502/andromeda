use andromeda::wire::{Role, ToolResultRequest, WireMessage};

fn user_msg(content: &str) -> WireMessage {
    WireMessage {
        role: Role::User,
        content: content.into(),
        tool_call_id: None,
        name: None,
    }
}

#[tokio::test]
async fn steer_queues_until_drained() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    h.enqueue_steer(vec![user_msg("改成微辣")]).unwrap();
    let drained = h.drain_steer();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].role, Role::User);
    assert_eq!(drained[0].content, "改成微辣");
    assert!(h.drain_steer().is_empty());
}

#[tokio::test]
async fn tool_result_unblocks_waiter() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    let wait = tokio::spawn({
        let h = h.clone();
        async move {
            h.wait_tool("call_1".into(), std::time::Duration::from_secs(2))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    h.submit_tool_result(andromeda::wire::ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .unwrap();
    let got = wait.await.unwrap().unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn submit_tool_result_conflicts_when_not_waiting_or_id_mismatch() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();

    let err = h
        .submit_tool_result(ToolResultRequest {
            tool_call_id: "call_1".into(),
            content: "early".into(),
            is_error: false,
        })
        .unwrap_err();
    assert_eq!(err, andromeda::run::SubmitError::Conflict);

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
        .unwrap_err();
    assert_eq!(err, andromeda::run::SubmitError::Conflict);

    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .unwrap();
    let got = wait.await.unwrap().unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn registry_get_returns_cloned_handle() {
    let reg = andromeda::run::RunRegistry::new();
    let (id, h) = reg.create();
    h.enqueue_steer(vec![user_msg("via create")]).unwrap();

    let got = reg.get(&id).expect("run should be registered");
    let drained = got.drain_steer();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].content, "via create");
    assert!(h.drain_steer().is_empty());
}

#[tokio::test]
async fn begin_wait_tool_allows_submit_before_awaiting() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    let rx = h.begin_wait_tool("call_1".into()).unwrap();
    h.submit_tool_result(ToolResultRequest {
        tool_call_id: "call_1".into(),
        content: "ok".into(),
        is_error: false,
    })
    .unwrap();
    let got = rx.await.unwrap();
    assert_eq!(got.content, "ok");
}

#[tokio::test]
async fn cancel_unblocks_tool_waiter() {
    let reg = andromeda::run::RunRegistry::new();
    let (_id, h) = reg.create();
    let wait = tokio::spawn({
        let h = h.clone();
        async move {
            h.wait_tool("call_1".into(), std::time::Duration::from_secs(2))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    h.cancel();
    let err = wait.await.unwrap().unwrap_err();
    assert_eq!(err, andromeda::run::WaitError::Cancelled);

    let err = h.enqueue_steer(vec![user_msg("too late")]).unwrap_err();
    assert_eq!(err, andromeda::run::SubmitError::Conflict);
}
