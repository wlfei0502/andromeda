use std::sync::Arc;

use crate::wire::{Role, WireMessage};

pub trait FollowUpPolicy: Send + Sync {
    fn next(&self, context: &[WireMessage]) -> Vec<WireMessage>;
}

pub struct NoopFollowUp;

impl FollowUpPolicy for NoopFollowUp {
    fn next(&self, _context: &[WireMessage]) -> Vec<WireMessage> {
        vec![]
    }
}

pub struct ExampleOrderFollowUp;

impl FollowUpPolicy for ExampleOrderFollowUp {
    fn next(&self, context: &[WireMessage]) -> Vec<WireMessage> {
        let place_order_ok = context.iter().any(|m| successful_tool(m, "place_order"));
        let send_sms_ok = context.iter().any(|m| successful_tool(m, "send_sms"));

        if place_order_ok && !send_sms_ok {
            vec![WireMessage {
                role: Role::System,
                content: "请调用 send_sms 发送取餐码".into(),
                tool_call_id: None,
                name: None,
            }]
        } else {
            vec![]
        }
    }
}

/// Tool results in run context use `tool_error:` prefix when the client reported `is_error`.
fn successful_tool(msg: &WireMessage, tool_name: &str) -> bool {
    msg.role == Role::Tool
        && msg.name.as_deref() == Some(tool_name)
        && !msg.content.starts_with("tool_error:")
}

pub fn policy_from_name(name: &str) -> Arc<dyn FollowUpPolicy> {
    match name {
        "example_order" => Arc::new(ExampleOrderFollowUp),
        _ => Arc::new(NoopFollowUp),
    }
}
