//! `before_llm` middleware chain (trait, runner, summarization, time context).

mod summarize;
mod time_context;

use std::sync::Arc;

use async_trait::async_trait;

pub use summarize::SummarizeMiddleware;
pub use time_context::{TIME_CONTEXT_PREFIX, TimeContextMiddleware, ensure_time_context};

use crate::agent::OrchestratorError;
use crate::config::ContextConfig;
use crate::llm::LlmPort;
use crate::protocol::{SseEvent, ToolDef, WireMessage};
use crate::store::PendingTool;

pub struct MwCtx<'a> {
    pub run_id: &'a str,
    pub context: &'a mut Vec<WireMessage>,
    pub tools: &'a [ToolDef],
    pub pending_tool: Option<&'a PendingTool>,
    pub llm: &'a dyn LlmPort,
}

pub struct MwEffect {
    pub events: Vec<SseEvent>,
    pub checkpoint: bool,
}

impl MwEffect {
    pub fn none() -> Self {
        Self {
            events: vec![],
            checkpoint: false,
        }
    }
}

pub enum MwAction {
    Continue(MwEffect),
}

#[async_trait]
pub trait AgentMiddleware: Send + Sync {
    fn name(&self) -> &'static str;

    async fn before_llm(&self, ctx: &mut MwCtx<'_>) -> Result<MwAction, OrchestratorError>;
}

pub async fn run_before_llm(
    chain: &[Arc<dyn AgentMiddleware>],
    ctx: &mut MwCtx<'_>,
) -> Result<MwEffect, OrchestratorError> {
    let mut merged = MwEffect::none();
    for mw in chain {
        match mw.before_llm(ctx).await? {
            MwAction::Continue(effect) => {
                merged.events.extend(effect.events);
                merged.checkpoint |= effect.checkpoint;
            }
        }
    }
    Ok(merged)
}

/// Default chain: summarize (if over threshold), then refresh wall-clock time.
pub fn default_summarize_chain(cfg: ContextConfig) -> Arc<[Arc<dyn AgentMiddleware>]> {
    Arc::from(vec![
        Arc::new(SummarizeMiddleware { config: cfg }) as Arc<dyn AgentMiddleware>,
        Arc::new(TimeContextMiddleware) as Arc<dyn AgentMiddleware>,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockLlm;

    struct RecordingMw;

    #[async_trait]
    impl AgentMiddleware for RecordingMw {
        fn name(&self) -> &'static str {
            "rec"
        }

        async fn before_llm(&self, _ctx: &mut MwCtx<'_>) -> Result<MwAction, OrchestratorError> {
            Ok(MwAction::Continue(MwEffect {
                events: vec![],
                checkpoint: true,
            }))
        }
    }

    #[tokio::test]
    async fn run_before_llm_merges_checkpoint() {
        let llm = MockLlm::script(vec![]);
        let mut context = vec![];
        let tools = &[] as &[ToolDef];
        let mut ctx = MwCtx {
            run_id: "run-1",
            context: &mut context,
            tools,
            pending_tool: None,
            llm: &llm,
        };
        let chain: Vec<Arc<dyn AgentMiddleware>> = vec![Arc::new(RecordingMw)];
        let effect = run_before_llm(&chain, &mut ctx).await.unwrap();
        assert!(effect.checkpoint);
        assert!(effect.events.is_empty());
    }
}
