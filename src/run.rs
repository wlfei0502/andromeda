use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::oneshot;

use crate::wire::{ToolResultRequest, WireMessage};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(pub String);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubmitError {
    #[error("run is not waiting for this tool result")]
    Conflict,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WaitError {
    #[error("tool wait timed out")]
    Timeout,
    #[error("run cancelled")]
    Cancelled,
}

struct ToolWaiter {
    tool_call_id: String,
    tx: oneshot::Sender<ToolResultRequest>,
}

struct RunInner {
    steer_queue: Vec<WireMessage>,
    waiter: Option<ToolWaiter>,
    cancelled: bool,
    finished: bool,
}

impl RunInner {
    fn new() -> Self {
        Self {
            steer_queue: Vec::new(),
            waiter: None,
            cancelled: false,
            finished: false,
        }
    }
}

#[derive(Clone)]
pub struct RunHandle {
    id: RunId,
    inner: Arc<TokioMutex<RunInner>>,
}

impl RunHandle {
    fn new(id: RunId) -> Self {
        Self {
            id,
            inner: Arc::new(TokioMutex::new(RunInner::new())),
        }
    }

    pub fn id(&self) -> &RunId {
        &self.id
    }

    fn lock(&self) -> tokio::sync::MutexGuard<'_, RunInner> {
        self.inner
            .try_lock()
            .expect("run inner mutex is never held across await")
    }

    pub fn enqueue_steer(&self, msgs: Vec<WireMessage>) -> Result<(), SubmitError> {
        let mut inner = self.lock();
        if inner.cancelled || inner.finished {
            return Err(SubmitError::Conflict);
        }
        inner.steer_queue.extend(msgs);
        Ok(())
    }

    pub fn drain_steer(&self) -> Vec<WireMessage> {
        let mut inner = self.lock();
        std::mem::take(&mut inner.steer_queue)
    }

    pub fn is_cancelled(&self) -> bool {
        self.lock().cancelled
    }

    /// Install the tool waiter immediately so `submit_tool_result` can succeed
    /// before the caller starts polling / emits `tool.request`.
    pub fn begin_wait_tool(
        &self,
        tool_call_id: String,
    ) -> Result<oneshot::Receiver<ToolResultRequest>, WaitError> {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(WaitError::Cancelled);
        }
        inner.waiter = Some(ToolWaiter { tool_call_id, tx });
        Ok(rx)
    }

    pub async fn recv_tool(
        &self,
        rx: oneshot::Receiver<ToolResultRequest>,
        timeout: Duration,
    ) -> Result<ToolResultRequest, WaitError> {
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(WaitError::Cancelled),
            Err(_) => {
                let mut inner = self.lock();
                inner.waiter = None;
                Err(WaitError::Timeout)
            }
        }
    }

    pub async fn wait_tool(
        &self,
        tool_call_id: String,
        timeout: Duration,
    ) -> Result<ToolResultRequest, WaitError> {
        let rx = self.begin_wait_tool(tool_call_id)?;
        self.recv_tool(rx, timeout).await
    }

    pub fn submit_tool_result(&self, result: ToolResultRequest) -> Result<(), SubmitError> {
        let mut inner = self.lock();
        match inner.waiter.take() {
            Some(waiter) if waiter.tool_call_id == result.tool_call_id => {
                waiter.tx.send(result).map_err(|_| SubmitError::Conflict)
            }
            Some(waiter) => {
                inner.waiter = Some(waiter);
                Err(SubmitError::Conflict)
            }
            None => Err(SubmitError::Conflict),
        }
    }

    pub fn cancel(&self) {
        let mut inner = self.lock();
        inner.cancelled = true;
        inner.steer_queue.clear();
        inner.waiter = None;
    }

    /// Mark the run terminal so later `steer` / mutations return conflict.
    pub fn finish(&self) {
        let mut inner = self.lock();
        inner.finished = true;
        inner.waiter = None;
    }
}

#[derive(Clone, Default)]
pub struct RunRegistry {
    runs: Arc<TokioMutex<HashMap<RunId, RunHandle>>>,
}

impl RunRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> tokio::sync::MutexGuard<'_, HashMap<RunId, RunHandle>> {
        self.runs
            .try_lock()
            .expect("run registry mutex is never held across await")
    }

    pub fn create(&self) -> (RunId, RunHandle) {
        let id = RunId(uuid::Uuid::new_v4().to_string());
        let handle = RunHandle::new(id.clone());
        self.lock().insert(id.clone(), handle.clone());
        (id, handle)
    }

    pub fn get(&self, id: &RunId) -> Option<RunHandle> {
        self.lock().get(id).cloned()
    }
}
