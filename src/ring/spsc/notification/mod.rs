//! Delivers data and space notifications for one SPSC ring endpoint.
//!
//! Cross-process endpoints hold their full-duplex notification stream lock
//! across cursor publication and any resulting alert. Anonymous endpoints use
//! independent process-local wakes for data and space.

mod stream;

use super::{IDLE, WAITING};
use crate::local_socket::{ConsumerStream, ProducerStream};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;

pub(crate) use stream::NotificationStreamSlot;

/// Delivers one directional cursor notification over a stream or local wake.
pub(crate) enum Notification {
    ProducerNotificationStream(Arc<NotificationStreamSlot<ProducerStream>>),
    ConsumerNotificationStream(Arc<NotificationStreamSlot<ConsumerStream>>),
    ProcessLocal(Notify),
}

impl Notification {
    pub(crate) async fn wait_until(
        &self,
        waiter_state: &AtomicU32,
        mut cursor_predicate: impl FnMut() -> io::Result<bool>,
    ) -> io::Result<()> {
        match self {
            Self::ProducerNotificationStream(stream_slot) => {
                stream_slot
                    .wait_until_cursor_predicate_succeeds(waiter_state, &mut cursor_predicate)
                    .await
            }
            Self::ConsumerNotificationStream(stream_slot) => {
                stream_slot
                    .wait_until_cursor_predicate_succeeds(waiter_state, &mut cursor_predicate)
                    .await
            }
            Self::ProcessLocal(notification) => loop {
                if cursor_predicate()? {
                    return Ok(());
                }
                let _armed_waiter_state = ArmedWaiterState::new(waiter_state);
                if cursor_predicate()? {
                    return Ok(());
                }
                notification.notified().await;
            },
        }
    }

    pub(crate) async fn publish_cursor_and_notify(
        &self,
        waiter_state: &AtomicU32,
        publish_cursor: impl FnOnce(),
    ) -> io::Result<()> {
        match self {
            Self::ProducerNotificationStream(stream_slot) => {
                stream_slot
                    .publish_cursor_with_optional_notification_stream(waiter_state, publish_cursor)
                    .await
            }
            Self::ConsumerNotificationStream(stream_slot) => {
                stream_slot
                    .wait_for_notification_stream_and_lock()
                    .await?
                    .publish_cursor_and_notify(waiter_state, publish_cursor)
                    .await
            }
            Self::ProcessLocal(notification) => {
                publish_cursor();
                if waiter_state.swap(IDLE, Ordering::AcqRel) == WAITING {
                    notification.notify_one();
                }
                Ok(())
            }
        }
    }
}

/// Clears a waiter flag if its wait ends without a matching publication.
pub(super) struct ArmedWaiterState<'a>(&'a AtomicU32);

impl<'a> ArmedWaiterState<'a> {
    pub(super) fn new(waiter_state: &'a AtomicU32) -> Self {
        waiter_state.swap(WAITING, Ordering::AcqRel);
        Self(waiter_state)
    }
}

impl Drop for ArmedWaiterState<'_> {
    fn drop(&mut self) {
        let _ = self
            .0
            .compare_exchange(WAITING, IDLE, Ordering::AcqRel, Ordering::Acquire);
    }
}

#[cfg(test)]
mod tests;
