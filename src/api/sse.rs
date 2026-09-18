use crate::protocol::SseEvent;

pub fn sse_frame(event: &SseEvent) -> String {
    let data = serde_json::to_string(event).expect("SseEvent serializes to JSON");
    format!("event: {}\ndata: {}\n\n", event.event_name(), data)
}
