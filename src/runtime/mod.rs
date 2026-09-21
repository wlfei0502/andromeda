use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::protocol::{SseEvent, ToolResultRequest, WireMessage};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitOutcome {
    Delivered,
    DroppedNoSubscriber,
}

struct ToolWaiter {
    tool_call_id: String,
    tx: oneshot::Sender<ToolResultRequest>,
}

struct RunInner {
    steer_queue: Vec<WireMessage>,
    waiter: Option<ToolWaiter>,
    /// Snapshot of the outstanding client tool (for SSE re-emit on resume).
    pending_tool: Option<crate::store::PendingTool>,
    /// Current SSE subscriber (last `subscribe` wins).
    subscriber: Option<mpsc::Sender<SseEvent>>,
    /// Whether checkpoint persistence is enabled for this run (used by later LH tasks).
    persist: bool,
    cancelled: bool,
    finished: bool,
}

impl RunInner {
    fn new() -> Self {
        Self {
            steer_queue: Vec::new(),
            waiter: None,
            pending_tool: None,
            subscriber: None,
            persist: false,
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

    /// Rebuild a handle for a known run id (cold resume from checkpoint).
    pub fn reopen(id: RunId) -> Self {
        Self::new(id)
    }

    pub fn id(&self) -> &RunId {
        &self.id
    }

    async fn lock(&self) -> tokio::sync::MutexGuard<'_, RunInner> {
        self.inner.lock().await
    }

    pub async fn set_persist(&self, persist: bool) {
        self.lock().await.persist = persist;
    }

    pub async fn persist_enabled(&self) -> bool {
        self.lock().await.persist
    }

    /// Replace the SSE subscriber. Previous receiver will end (last subscriber wins).
    pub async fn subscribe(&self) -> mpsc::Receiver<SseEvent> {
        let (tx, rx) = mpsc::channel(64);
        let mut inner = self.lock().await;
        inner.subscriber = Some(tx);
        rx
    }

    /// Push an event to the current subscriber, if any.
    ///
    /// Missing or closed subscribers are **not** errors — the run continues.
    pub async fn emit_event(&self, event: SseEvent) -> EmitOutcome {
        let tx = {
            let inner = self.lock().await;
            inner.subscriber.clone()
        };
        let Some(tx) = tx else {
            return EmitOutcome::DroppedNoSubscriber;
        };
        match tx.send(event).await {
            Ok(()) => EmitOutcome::Delivered,
            Err(_) => {
                // Drop stale sender so the next subscribe installs cleanly.
                let mut inner = self.lock().await;
                inner.subscriber = None;
                EmitOutcome::DroppedNoSubscriber
            }
        }
    }

    pub async fn is_waiting_tool(&self) -> bool {
        self.lock().await.waiter.is_some()
    }

    pub async fn set_pending_tool(&self, tool: Option<crate::store::PendingTool>) {
        self.lock().await.pending_tool = tool;
    }

    pub async fn pending_tool(&self) -> Option<crate::store::PendingTool> {
        self.lock().await.pending_tool.clone()
    }

    pub async fn enqueue_steer(&self, msgs: Vec<WireMessage>) -> Result<(), SubmitError> {
        let mut inner = self.lock().await;
        if inner.cancelled || inner.finished {
            return Err(SubmitError::Conflict);
        }
        inner.steer_queue.extend(msgs);
        Ok(())
    }

    pub async fn drain_steer(&self) -> Vec<WireMessage> {
        let mut inner = self.lock().await;
        std::mem::take(&mut inner.steer_queue)
    }

    pub async fn is_cancelled(&self) -> bool {
        self.lock().await.cancelled
    }

    pub async fn is_finished(&self) -> bool {
        self.lock().await.finished
    }

    /// Install the tool waiter immediately so `submit_tool_result` can succeed
    /// before the caller starts polling / emits `tool.request`.
    pub async fn begin_wait_tool(
        &self,
        tool_call_id: String,
    ) -> Result<oneshot::Receiver<ToolResultRequest>, WaitError> {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.lock().await;
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
                let mut inner = self.lock().await;
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
        let rx = self.begin_wait_tool(tool_call_id).await?;
        self.recv_tool(rx, timeout).await
    }

    pub async fn submit_tool_result(&self, result: ToolResultRequest) -> Result<(), SubmitError> {
        let mut inner = self.lock().await;
        if inner.finished {
            return Err(SubmitError::Conflict);
        }
        match inner.waiter.take() {
            Some(waiter) if waiter.tool_call_id == result.tool_call_id => {
                inner.pending_tool = None;
                waiter.tx.send(result).map_err(|_| SubmitError::Conflict)
            }
            Some(waiter) => {
                inner.waiter = Some(waiter);
                Err(SubmitError::Conflict)
            }
            None => Err(SubmitError::Conflict),
        }
    }

    pub async fn cancel(&self) {
        let mut inner = self.lock().await;
        inner.cancelled = true;
        inner.steer_queue.clear();
        inner.waiter = None;
        inner.pending_tool = None;
    }

    /// Mark the run terminal so later `steer` / mutations return conflict.
    pub async fn finish(&self) {
        let mut inner = self.lock().await;
        if inner.finished {
            return;
        }
        inner.finished = true;
        inner.steer_queue.clear();
        inner.waiter = None;
        inner.pending_tool = None;
        // Drop subscriber so the SSE HTTP stream can end.
        inner.subscriber = None;
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

    async fn lock(&self) -> tokio::sync::MutexGuard<'_, HashMap<RunId, RunHandle>> {
        self.runs.lock().await
    }

    pub async fn create(&self) -> (RunId, RunHandle) {
        let id = RunId(uuid::Uuid::new_v4().to_string());
        let handle = RunHandle::new(id.clone());
        self.lock().await.insert(id.clone(), handle.clone());
        (id, handle)
    }

    /// Insert a pre-built handle (used when loading from store).
    pub async fn insert(&self, handle: RunHandle) {
        let id = handle.id().clone();
        self.lock().await.insert(id, handle);
    }

    pub async fn get(&self, id: &RunId) -> Option<RunHandle> {
        self.lock().await.get(id).cloned()
    }
}

#[cfg(test)]
mod hub_tests {
    use super::*;
    use crate::protocol::SseEvent;

    #[tokio::test]
    async fn emit_without_subscriber_is_ok() {
        let reg = RunRegistry::new();
        let (_id, run) = reg.create().await;
        let outcome = run
            .emit_event(SseEvent::RunStarted { run_id: "x".into() })
            .await;
        assert_eq!(outcome, EmitOutcome::DroppedNoSubscriber);
    }

    #[tokio::test]
    async fn last_subscriber_wins() {
        let reg = RunRegistry::new();
        let (_id, run) = reg.create().await;
        let mut rx1 = run.subscribe().await;
        let mut rx2 = run.subscribe().await;

        run.emit_event(SseEvent::RunStarted { run_id: "r".into() })
            .await;

        assert!(rx1.try_recv().is_err());
        let ev = rx2.recv().await.expect("second subscriber gets event");
        assert!(matches!(ev, SseEvent::RunStarted { .. }));
    }

    #[tokio::test]
    async fn drop_subscriber_then_resubscribe() {
        let reg = RunRegistry::new();
        let (_id, run) = reg.create().await;
        let rx = run.subscribe().await;
        drop(rx);

        let outcome = run
            .emit_event(SseEvent::RunStarted { run_id: "r".into() })
            .await;
        assert_eq!(outcome, EmitOutcome::DroppedNoSubscriber);

        let mut rx2 = run.subscribe().await;
        run.emit_event(SseEvent::RunFinished {
            run_id: "r".into(),
            reason: "stop".into(),
        })
        .await;
        let ev = rx2.recv().await.unwrap();
        assert!(matches!(ev, SseEvent::RunFinished { .. }));
    }
}
