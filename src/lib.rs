pub mod agent_loop;
pub mod backend;
pub mod error;
pub mod event_stream;
pub mod liter_backend;
pub mod stub_backend;
pub mod types;

pub use agent_loop::{agent_loop, agent_loop_continue};
pub use backend::{AgentBackend, Emit, TurnSnapshot};
pub use error::LoopError;
pub use event_stream::{EventStream, EventStreamPusher, ResultHandle};
pub use liter_backend::{
    LiterBackend, StreamAccumulator, ToolExecResult, ToolExecutor, echo_executor, echo_tool_def,
    to_llm_messages,
};
pub use stub_backend::{StubBackend, StubTurn};
pub use types::{
    AgentContext, AgentEvent, AgentMessage, AgentTool, ContentPart, StopReason, ToolCall,
};
