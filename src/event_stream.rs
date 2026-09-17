use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};

struct ResultState<R> {
    value: Mutex<Option<R>>,
    waiter: Mutex<Option<oneshot::Sender<R>>>,
}

impl<R> ResultState<R> {
    fn resolve(&self, value: R) {
        if let Some(tx) = self.waiter.lock().unwrap().take() {
            let _ = tx.send(value);
            return;
        }
        *self.value.lock().unwrap() = Some(value);
    }
}

struct EventStreamInner<T, R> {
    tx: Mutex<Option<mpsc::UnboundedSender<T>>>,
    is_complete: Box<dyn Fn(&T) -> bool + Send + Sync>,
    extract: Box<dyn Fn(&T) -> R + Send + Sync>,
    result: ResultState<R>,
}

pub struct EventStream<T, R> {
    inner: Arc<EventStreamInner<T, R>>,
    rx: mpsc::UnboundedReceiver<T>,
}

/// Cloneable handle for pushing events while the stream is consumed elsewhere.
#[derive(Clone)]
pub struct EventStreamPusher<T, R> {
    inner: Arc<EventStreamInner<T, R>>,
}

impl<T: Send + 'static, R: Send + 'static> EventStreamPusher<T, R> {
    pub fn push(&self, event: T) {
        let is_complete = (self.inner.is_complete)(&event);
        if is_complete {
            let value = (self.inner.extract)(&event);
            self.inner.result.resolve(value);
        }
        if let Some(tx) = self.inner.tx.lock().unwrap().as_ref() {
            let _ = tx.send(event);
        }
        if is_complete {
            *self.inner.tx.lock().unwrap() = None;
        }
    }

    pub fn end(&self, result: Option<R>) {
        if let Some(value) = result {
            self.inner.result.resolve(value);
        }
        *self.inner.tx.lock().unwrap() = None;
    }
}

pub type ResultHandle<R> = Pin<Box<dyn std::future::Future<Output = Option<R>> + Send>>;

impl<T: Send + 'static, R: Send + 'static> EventStream<T, R> {
    pub fn new(
        is_complete: impl Fn(&T) -> bool + Send + Sync + 'static,
        extract: impl Fn(&T) -> R + Send + Sync + 'static,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(EventStreamInner {
                tx: Mutex::new(Some(tx)),
                is_complete: Box::new(is_complete),
                extract: Box::new(extract),
                result: ResultState {
                    value: Mutex::new(None),
                    waiter: Mutex::new(None),
                },
            }),
            rx,
        }
    }

    pub fn pusher(&self) -> EventStreamPusher<T, R> {
        EventStreamPusher {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn push(&self, event: T) {
        self.pusher().push(event);
    }

    pub fn end(&self, result: Option<R>) {
        self.pusher().end(result);
    }

    pub fn result_handle(&self) -> ResultHandle<R> {
        let inner = Arc::clone(&self.inner);
        let (tx, rx) = oneshot::channel();
        {
            if let Some(value) = inner.result.value.lock().unwrap().take() {
                let _ = tx.send(value);
            } else {
                *inner.result.waiter.lock().unwrap() = Some(tx);
            }
        }
        Box::pin(async move { rx.await.ok() })
    }
}

impl<T: Send + 'static, R> Stream for EventStream<T, R> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let rx = &mut self.get_mut().rx;
        match rx.try_recv() {
            Ok(value) => Poll::Ready(Some(value)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(TryRecvError::Empty) => rx.poll_recv(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EventStream;
    use futures::StreamExt;

    #[tokio::test]
    async fn push_complete_event_resolves_result_handle() {
        let stream = EventStream::new(
            |e: &i32| *e < 0,
            |e: &i32| e.abs(),
        );
        let handle = stream.result_handle();
        stream.push(1);
        stream.push(-7);
        assert_eq!(handle.await.unwrap(), 7);
        let mut s = stream;
        assert_eq!(s.next().await, Some(1));
        assert_eq!(s.next().await, Some(-7));
        assert_eq!(s.next().await, None);
    }

    #[tokio::test]
    async fn pusher_allows_concurrent_emit_while_consuming() {
        let stream = EventStream::new(
            |e: &i32| *e < 0,
            |e: &i32| e.abs(),
        );
        let handle = stream.result_handle();
        let pusher = stream.pusher();
        let consumer = tokio::spawn(async move {
            let mut events = Vec::new();
            let mut s = stream;
            while let Some(e) = s.next().await {
                events.push(e);
            }
            events
        });
        let producer = tokio::spawn(async move {
            pusher.push(1);
            pusher.push(-42);
        });
        producer.await.unwrap();
        assert_eq!(handle.await.unwrap(), 42);
        assert_eq!(consumer.await.unwrap(), vec![1, -42]);
    }
}
