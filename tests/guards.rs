mod common;

use std::sync::Arc;
use std::time::Duration;

use andromeda::agent::{
    FollowUpPolicy, NoopFollowUp, OrchestratorError, REASON_LLM_ROUNDS, REASON_NOOP,
    REASON_TIMEOUT, default_summarize_chain, run_agent, run_agent_with_options,
};
use andromeda::config::{ContextConfig, GuardsConfig};
use andromeda::llm::{LlmPort, MockLlm, MockTurn};
use andromeda::protocol::{Role, SseEvent, WireMessage};
use andromeda::runtime::RunRegistry;
use andromeda::store::GuardsSnapshot;

use common::user_msg;

struct RepeatFollowUp;

impl FollowUpPolicy for RepeatFollowUp {
    fn next(&self, context: &[WireMessage]) -> Vec<WireMessage> {
        let assistants = context.iter().filter(|m| m.role == Role::Assistant).count();
        if assistants >= 6 {
            return vec![];
        }
        vec![WireMessage {
            role: Role::System,
            content: "continue".into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        }]
    }
}

fn tight(rounds: u32, wall: u64, follow: u32, noop: u32) -> GuardsConfig {
    GuardsConfig {
        max_llm_rounds: rounds,
        max_run_wall_secs: wall,
        max_follow_up_rounds: follow,
        max_noop_llm_rounds: noop,
    }
}

async fn collect(rx: &mut tokio::sync::mpsc::Receiver<SseEvent>) -> Vec<SseEvent> {
    let mut events = Vec::new();
    while let Some(ev) = rx.recv().await {
        let terminal = matches!(ev, SseEvent::RunFinished { .. } | SseEvent::Error { .. });
        events.push(ev);
        if terminal {
            break;
        }
    }
    events
}

#[tokio::test]
async fn llm_round_guard_finishes_before_next_call() {
    let registry = RunRegistry::new();
    let (_id, run) = registry.create().await;
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![MockTurn::TextOnly {
        content: "once".into(),
        deltas: vec!["once".into()],
    }]));
    let mut rx = run.subscribe().await;
    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        Arc::new(RepeatFollowUp),
        Duration::from_secs(2),
        None,
        default_summarize_chain(ContextConfig::default()),
        false,
        tight(1, 0, 8, 0),
    ));
    let events = collect(&mut rx).await;
    let err = agent.await.unwrap().unwrap_err();
    assert!(matches!(err, OrchestratorError::Guard(REASON_LLM_ROUNDS)));
    assert!(events.iter().any(|ev| matches!(
        ev,
        SseEvent::RunFinished { reason, .. } if reason == REASON_LLM_ROUNDS
    )));
}

#[tokio::test]
async fn wall_guard_stops_before_llm() {
    let registry = RunRegistry::new();
    let (_id, run) = registry.create().await;
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script(vec![]));
    let mut rx = run.subscribe().await;
    let mut guards = GuardsSnapshot::new_now();
    guards.started_at = "1".into();
    let agent = tokio::spawn(run_agent_with_options(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        Arc::new(NoopFollowUp),
        Duration::from_secs(2),
        None,
        false,
        default_summarize_chain(ContextConfig::default()),
        false,
        vec![],
        tight(0, 10, 8, 0),
        Some(guards),
    ));
    let events = collect(&mut rx).await;
    let err = agent.await.unwrap().unwrap_err();
    assert!(matches!(err, OrchestratorError::Guard(REASON_TIMEOUT)));
    assert!(events.iter().any(|ev| matches!(
        ev,
        SseEvent::RunFinished { reason, .. } if reason == REASON_TIMEOUT
    )));
}

#[tokio::test]
async fn noop_guard_stops_on_repeated_text() {
    let registry = RunRegistry::new();
    let (_id, run) = registry.create().await;
    let llm: Arc<dyn LlmPort> = Arc::new(MockLlm::script_then_repeat(
        vec![],
        MockTurn::TextOnly {
            content: "still working".into(),
            deltas: vec!["still working".into()],
        },
    ));
    let mut rx = run.subscribe().await;
    let agent = tokio::spawn(run_agent(
        run,
        vec![user_msg("hi")],
        vec![],
        llm,
        Arc::new(RepeatFollowUp),
        Duration::from_secs(2),
        None,
        default_summarize_chain(ContextConfig::default()),
        false,
        tight(0, 0, 8, 2),
    ));
    let events = collect(&mut rx).await;
    let err = agent.await.unwrap().unwrap_err();
    assert!(matches!(err, OrchestratorError::Guard(REASON_NOOP)));
    assert!(events.iter().any(|ev| matches!(
        ev,
        SseEvent::RunFinished { reason, .. } if reason == REASON_NOOP
    )));
}
