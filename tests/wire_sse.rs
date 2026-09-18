#[test]
fn sse_frame_includes_event_and_json_data() {
    let ev = andromeda::wire::SseEvent::RunStarted {
        run_id: "r1".into(),
    };
    let frame = andromeda::sse::sse_frame(&ev);
    assert!(frame.starts_with("event: run.started\n"));
    assert!(frame.contains("\"run_id\":\"r1\""));
    assert!(frame.ends_with("\n\n"));
}
