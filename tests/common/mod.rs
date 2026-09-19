//! Shared helpers for integration tests (`mod common;` in each test crate).
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use andromeda::agent::{NoopFollowUp, default_summarize_chain};
use andromeda::api::AppState;
use andromeda::config::ContextConfig;
use andromeda::llm::LlmPort;
use andromeda::protocol::{Role, SseEvent, ToolDef, WireMessage};
use andromeda::runtime::RunRegistry;
use andromeda::store::RunStore;
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use serde_json::json;

pub fn user_msg(content: &str) -> WireMessage {
    WireMessage {
        role: Role::User,
        content: content.into(),
        tool_call_id: None,
        name: None,
        tool_calls: None,
    }
}

pub fn echo_tool() -> ToolDef {
    ToolDef {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({ "type": "object" }),
    }
}

pub fn json_request(uri: &str, body: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

/// Alias used by checkpoint resume tests.
pub fn json_post(uri: &str, body: &impl serde::Serialize) -> Request<Body> {
    json_request(uri, body)
}

pub fn app_state(
    llm: Arc<dyn LlmPort>,
    store: Option<Arc<dyn RunStore>>,
    instance_id: &str,
    persist_enabled: bool,
    context: ContextConfig,
) -> AppState {
    AppState {
        registry: RunRegistry::new(),
        store,
        instance_id: instance_id.into(),
        persist_enabled,
        llm,
        follow_up: Arc::new(NoopFollowUp),
        tool_timeout: Duration::from_secs(if persist_enabled { 5 } else { 2 }),
        middlewares: default_summarize_chain(context),
    }
}

pub fn memory_state(llm: Arc<dyn LlmPort>) -> AppState {
    app_state(
        llm,
        None,
        "test-node",
        false,
        ContextConfig::default(),
    )
}

pub fn memory_state_with_context(llm: Arc<dyn LlmPort>, context: ContextConfig) -> AppState {
    app_state(llm, None, "test-node", false, context)
}

pub fn parse_sse_frame(frame: &str) -> Option<SseEvent> {
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&data)
                .unwrap_or_else(|err| panic!("invalid SSE data JSON: {err}; data={data:?}")),
        )
    }
}

pub async fn collect_sse_with<F, Fut>(body: Body, mut on_event: F) -> Vec<SseEvent>
where
    F: FnMut(SseEvent) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut body = body;
    let mut buf = String::new();
    let mut events = Vec::new();
    loop {
        let chunk = match body.frame().await {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => data,
                Err(_) => continue,
            },
            Some(Err(err)) => panic!("body error: {err}"),
            None => break,
        };
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = buf.find("\n\n") {
            let raw = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            if let Some(ev) = parse_sse_frame(&raw) {
                on_event(ev.clone()).await;
                let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
                events.push(ev);
                if terminal {
                    return events;
                }
            }
        }
    }
    events
}

pub async fn collect_sse(body: Body) -> Vec<SseEvent> {
    collect_sse_with(body, |_| async {}).await
}
