//! Context summarization: engine + `SummarizeMiddleware` adapter.

pub mod engine;

use async_trait::async_trait;

use self::engine::{SummarizeError, SummarizeOutcome, maybe_summarize};
use super::{AgentMiddleware, MwAction, MwCtx, MwEffect};
use crate::agent::OrchestratorError;
use crate::config::ContextConfig;
use crate::protocol::SseEvent;

pub struct SummarizeMiddleware {
    pub config: ContextConfig,
}

#[async_trait]
impl AgentMiddleware for SummarizeMiddleware {
    fn name(&self) -> &'static str {
        "summarize"
    }

    async fn before_llm(&self, ctx: &mut MwCtx<'_>) -> Result<MwAction, OrchestratorError> {
        match maybe_summarize(ctx.context, &self.config, ctx.llm, ctx.pending_tool).await {
            Ok(SummarizeOutcome::Unchanged) => Ok(MwAction::Continue(MwEffect::none())),
            Ok(SummarizeOutcome::Summarized {
                before_tokens,
                after_tokens,
                kept_prefix,
                kept_suffix,
            }) => Ok(MwAction::Continue(MwEffect {
                events: vec![SseEvent::ContextSummarized {
                    run_id: ctx.run_id.to_string(),
                    before_tokens,
                    after_tokens,
                    kept_prefix,
                    kept_suffix,
                }],
                checkpoint: true,
            })),
            Err(SummarizeError::ContextOverflow { .. }) => Err(OrchestratorError::ContextOverflow),
        }
    }
}
