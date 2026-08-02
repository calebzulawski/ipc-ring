//! Wakes the two endpoints of an anonymous ring without an IPC stream.

use super::{ConsumerNotification, ProducerNotification};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Shared wakeups and closure state for an anonymous ring.
pub(in crate::ring) struct AnonymousNotifications {
    data_available: Notify,
    space_available: Notify,
    closed: AtomicBool,
}

impl AnonymousNotifications {
    fn new() -> Self {
        Self {
            data_available: Notify::new(),
            space_available: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    pub(super) fn notify_data(&self) -> io::Result<()> {
        self.wake(&self.data_available)
    }

    pub(super) async fn wait_for_data(&self) -> io::Result<()> {
        self.wait(&self.data_available).await
    }

    pub(super) fn notify_space(&self) -> io::Result<()> {
        self.wake(&self.space_available)
    }

    pub(super) async fn wait_for_space(&self) -> io::Result<()> {
        self.wait(&self.space_available).await
    }

    fn wake(&self, notification: &Notify) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        notification.notify_one();
        Ok(())
    }

    async fn wait(&self, notification: &Notify) -> io::Result<()> {
        let notified = notification.notified();
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        notified.await;
        if self.closed.load(Ordering::Acquire) {
            Err(crate::error::peer_disconnected())
        } else {
            Ok(())
        }
    }

    pub(super) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.data_available.notify_waiters();
            self.space_available.notify_waiters();
        }
    }
}

/// Creates both endpoints of an anonymous ring's wake channel.
pub(in crate::ring) fn pair() -> (ProducerNotification, ConsumerNotification) {
    let notifications = Arc::new(AnonymousNotifications::new());
    (
        ProducerNotification::Anonymous(Arc::clone(&notifications)),
        ConsumerNotification::Anonymous(notifications),
    )
}
