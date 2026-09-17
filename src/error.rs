#[derive(Debug, thiserror::Error)]
pub enum LoopError {
    #[error("invalid continue: {0}")]
    InvalidContinue(String),

    #[error("backend error: {0}")]
    Backend(String),
}
