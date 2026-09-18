use crate::wire::SseEvent;

/// Channel the orchestrator uses to push wire events toward the HTTP SSE response.
///
/// Created with `tokio::sync::mpsc::channel` (bounded) or an equivalent `Sender`.
/// The HTTP layer holds the receiver and maps each `SseEvent` to an SSE frame.
pub type SseTx = tokio::sync::mpsc::Sender<SseEvent>;

pub fn sse_frame(event: &SseEvent) -> String {
    let data = serde_json::to_string(event).expect("SseEvent serializes to JSON");
    format!("event: {}\ndata: {}\n\n", event.event_name(), data)
}
