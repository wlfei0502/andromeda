use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::error::LoopError;
use crate::types::{AgentContext, AgentEvent, AgentMessage, ContentPart};

/// Sync emit callback wired to `EventStreamPusher::push`.
pub type Emit = Arc<dyn Fn(AgentEvent) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct TurnSnapshot {
    pub message: AgentMessage,
    pub tool_results: Vec<AgentMessage>,
    pub context_messages_len: usize,
    pub new_messages: Vec<AgentMessage>,
}

#[async_trait]
pub trait AgentBackend: Send + Sync {
    /// Stream one assistant turn, emitting message events via `emit`.
    ///
    /// Implementations **MUST** append the partial/final assistant into
    /// `ctx.messages` before returning `Ok` (pi-style). The agent loop must
    /// not push the assistant again on the Ok path; it only records the
    /// message in the run's `new_messages` return set.
    ///
    /// On `Err`, the loop may synthesize an Error/Aborted assistant and
    /// append it once itself.
    async fn stream_assistant(
        &self,
        ctx: &mut AgentContext,
        emit: Emit,
        cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError>;

    async fn execute_tool(
        &self,
        call: &ContentPart,
        emit: Emit,
        cancel: &CancellationToken,
    ) -> Result<AgentMessage, LoopError>;

    async fn get_steering(&self) -> Vec<AgentMessage> {
        vec![]
    }

    async fn get_follow_up(&self) -> Vec<AgentMessage> {
        vec![]
    }

    async fn should_stop_after_turn(&self, _turn: &TurnSnapshot) -> bool {
        false
    }
}
