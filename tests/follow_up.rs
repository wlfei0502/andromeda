use andromeda::agent::{ExampleOrderFollowUp, FollowUpPolicy, NoopFollowUp, policy_from_name};
use andromeda::protocol::{Role, WireMessage};

fn tool_msg(name: &str, content: &str) -> WireMessage {
    WireMessage {
        role: Role::Tool,
        content: content.into(),
        tool_call_id: Some(format!("tc-{name}")),
        name: Some(name.into()),
        tool_calls: None,
        reasoning_content: None,
    }
}

#[test]
fn noop_returns_empty() {
    let policy = NoopFollowUp;
    let context = vec![tool_msg("place_order", "order ok")];
    assert!(policy.next(&context).is_empty());
}

#[test]
fn example_order_after_place_order_suggests_send_sms() {
    let policy = ExampleOrderFollowUp;
    let context = vec![tool_msg("place_order", "order ok")];
    let follow_ups = policy.next(&context);
    assert_eq!(follow_ups.len(), 1);
    assert!(
        matches!(follow_ups[0].role, Role::User | Role::System),
        "expected user or system follow-up, got {:?}",
        follow_ups[0].role
    );
    assert!(
        follow_ups[0].content.contains("send_sms"),
        "follow-up should mention send_sms: {}",
        follow_ups[0].content
    );
}

#[test]
fn example_order_skips_when_send_sms_already_succeeded() {
    let policy = ExampleOrderFollowUp;
    let context = vec![
        tool_msg("place_order", "order ok"),
        tool_msg("send_sms", "sent"),
    ];
    assert!(policy.next(&context).is_empty());
}

#[test]
fn example_order_skips_failed_place_order() {
    let policy = ExampleOrderFollowUp;
    let context = vec![tool_msg("place_order", "tool_error: declined")];
    assert!(policy.next(&context).is_empty());
}

#[test]
fn policy_from_name_selects_implementation() {
    assert!(policy_from_name("noop").next(&[]).is_empty());
    let ctx = vec![tool_msg("place_order", "ok")];
    assert!(!policy_from_name("example_order").next(&ctx).is_empty());
    assert!(policy_from_name("unknown").next(&ctx).is_empty());
}
