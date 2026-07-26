use super::ArmedWaiterState;
use crate::mapping::MappedMemory;
use crate::ring::spsc::{IDLE, WAITING};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, MutexGuard, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

struct StoredNotificationStream<S> {
    stream: S,
    /// Keeps a registered consumer admitted for this stream's lifetime.
    _consumer_admission_permit: Option<OwnedSemaphorePermit>,
}

/// Owns the optional stream used for this ring's cross-process notifications.
pub(crate) struct NotificationStreamSlot<S> {
    notification_stream: Mutex<Option<StoredNotificationStream<S>>>,
    consumer_admission: Option<Arc<Semaphore>>,
    notification_stream_changed: Notify,
    mapped_memory: Arc<MappedMemory>,
}

impl<S> NotificationStreamSlot<S> {
    pub(crate) fn producer_waiting_for_notification_stream(
        mapped_memory: Arc<MappedMemory>,
    ) -> Arc<Self> {
        Arc::new(Self::new(
            None,
            Some(Arc::new(Semaphore::new(1))),
            mapped_memory,
        ))
    }

    pub(crate) fn consumer_with_notification_stream(
        notification_stream: S,
        mapped_memory: Arc<MappedMemory>,
    ) -> Arc<Self> {
        Arc::new(Self::new(
            Some(StoredNotificationStream {
                stream: notification_stream,
                _consumer_admission_permit: None,
            }),
            None,
            mapped_memory,
        ))
    }

    fn new(
        notification_stream: Option<StoredNotificationStream<S>>,
        consumer_admission: Option<Arc<Semaphore>>,
        mapped_memory: Arc<MappedMemory>,
    ) -> Self {
        Self {
            notification_stream: Mutex::new(notification_stream),
            consumer_admission,
            notification_stream_changed: Notify::new(),
            mapped_memory,
        }
    }

    /// Claims the registered ring until the permit or installed stream is dropped.
    pub(crate) fn try_claim_consumer(&self) -> io::Result<OwnedSemaphorePermit> {
        let admission = self
            .consumer_admission
            .as_ref()
            .expect("only producer notification slots admit consumers");
        match Arc::clone(admission).try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(TryAcquireError::NoPermits) => Err(crate::error::consumer_already_connected()),
            Err(TryAcquireError::Closed) => Err(crate::error::peer_disconnected()),
        }
    }

    /// Makes a handshaken stream available after sending one durable wake.
    pub(crate) async fn insert_notification_stream_after_client_ready(
        &self,
        mut notification_stream: S,
        consumer_admission: OwnedSemaphorePermit,
    ) -> io::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let mut stream_slot = self.notification_stream.lock().await;
        if stream_slot.is_some() {
            return Err(crate::error::consumer_already_connected());
        }
        let admission = self
            .consumer_admission
            .as_ref()
            .expect("only producer notification slots install streams");
        if admission.is_closed() {
            return Err(crate::error::peer_disconnected());
        }
        self.mapped_memory
            .header()
            .data_wait_state
            .store(IDLE, Ordering::Release);
        write_zero_byte_alert(&mut notification_stream).await?;
        if admission.is_closed() {
            return Err(crate::error::peer_disconnected());
        }
        *stream_slot = Some(StoredNotificationStream {
            stream: notification_stream,
            _consumer_admission_permit: Some(consumer_admission),
        });
        drop(stream_slot);
        self.notification_stream_changed.notify_one();
        Ok(())
    }

    /// Prevents new or replacement streams without disturbing an active one.
    pub(crate) fn close_consumer_admission(&self) {
        self.consumer_admission
            .as_ref()
            .expect("only producer notification slots admit consumers")
            .close();
        self.notification_stream_changed.notify_one();
    }

    pub(super) async fn wait_until_cursor_predicate_succeeds(
        &self,
        waiter_state: &AtomicU32,
        cursor_predicate: &mut impl FnMut() -> io::Result<bool>,
    ) -> io::Result<()>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            if cursor_predicate()? {
                return Ok(());
            }

            let mut locked_stream = self.wait_for_notification_stream_and_lock().await?;
            let _armed_waiter_state = ArmedWaiterState::new(waiter_state);
            if cursor_predicate()? {
                return Ok(());
            }
            locked_stream.read_zero_byte_alert().await?;
        }
    }

    /// Publishes under the stream lock whether or not a client has connected yet.
    pub(super) async fn publish_cursor_with_optional_notification_stream(
        &self,
        waiter_state: &AtomicU32,
        publish_cursor: impl FnOnce(),
    ) -> io::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let stream_slot = self.notification_stream.lock().await;
        if stream_slot.is_some() {
            return LockedNotificationStream::new(self, stream_slot)
                .publish_cursor_and_notify(waiter_state, publish_cursor)
                .await;
        }
        if self
            .consumer_admission
            .as_ref()
            .is_none_or(|admission| admission.is_closed())
        {
            return Err(crate::error::peer_disconnected());
        }
        publish_cursor();
        Ok(())
    }

    pub(super) async fn wait_for_notification_stream_and_lock(
        &self,
    ) -> io::Result<LockedNotificationStream<'_, S>> {
        loop {
            let stream_state_changed = self.notification_stream_changed.notified();
            let stream_slot = self.notification_stream.lock().await;
            if stream_slot.is_some() {
                return Ok(LockedNotificationStream::new(self, stream_slot));
            }
            if self
                .consumer_admission
                .as_ref()
                .is_none_or(|admission| admission.is_closed())
            {
                return Err(crate::error::peer_disconnected());
            }
            drop(stream_slot);
            stream_state_changed.await;
        }
    }

    fn record_notification_stream_disconnection(&self) {
        let header = self.mapped_memory.header();
        header.data_wait_state.store(IDLE, Ordering::Release);
        header.space_wait_state.store(IDLE, Ordering::Release);
        self.notification_stream_changed.notify_one();
    }
}

/// Locks a notification stream across one alert read or cursor-publication write.
pub(super) struct LockedNotificationStream<'a, S> {
    stream_slot: &'a NotificationStreamSlot<S>,
    notification_stream: MutexGuard<'a, Option<StoredNotificationStream<S>>>,
    remove_stream_on_drop: bool,
}

impl<'a, S> LockedNotificationStream<'a, S> {
    fn new(
        stream_slot: &'a NotificationStreamSlot<S>,
        notification_stream: MutexGuard<'a, Option<StoredNotificationStream<S>>>,
    ) -> Self {
        Self {
            stream_slot,
            notification_stream,
            remove_stream_on_drop: false,
        }
    }

    fn notification_stream_mut(&mut self) -> &mut S {
        &mut self
            .notification_stream
            .as_mut()
            .expect("notification stream disappeared while locked")
            .stream
    }

    async fn read_zero_byte_alert(&mut self) -> io::Result<()>
    where
        S: AsyncRead + Unpin,
    {
        let result = read_zero_byte_alert(self.notification_stream_mut()).await;
        if result.is_err() {
            self.remove_stream_on_drop = true;
        }
        result
    }

    pub(super) async fn publish_cursor_and_notify(
        mut self,
        waiter_state: &AtomicU32,
        publish_cursor: impl FnOnce(),
    ) -> io::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        publish_cursor();
        if waiter_state.swap(IDLE, Ordering::AcqRel) == WAITING {
            self.remove_stream_on_drop = true;
            write_zero_byte_alert(self.notification_stream_mut()).await?;
            self.remove_stream_on_drop = false;
        }
        Ok(())
    }
}

impl<S> Drop for LockedNotificationStream<'_, S> {
    fn drop(&mut self) {
        if self.remove_stream_on_drop {
            self.notification_stream.take();
            self.stream_slot.record_notification_stream_disconnection();
        }
    }
}

async fn read_zero_byte_alert<S>(notification_stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let byte = notification_stream
        .read_u8()
        .await
        .map_err(|_| crate::error::peer_disconnected())?;
    if byte == 0 {
        Ok(())
    } else {
        Err(crate::error::protocol("unknown control notification"))
    }
}

async fn write_zero_byte_alert<S>(notification_stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    notification_stream
        .write_u8(0)
        .await
        .map_err(|_| crate::error::peer_disconnected())
}

#[cfg(test)]
mod tests;
