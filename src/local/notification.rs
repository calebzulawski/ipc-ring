//! Wakes the two endpoints of a local ring without an IPC stream.

use crate::ring::wake::{ConsumerWake, ProducerWake};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Shared wakeups and closure state for a local ring.
struct LocalNotifications {
    data_available: Notify,
    space_available: Notify,
    closed: AtomicBool,
}

impl LocalNotifications {
    fn new() -> Self {
        Self {
            data_available: Notify::new(),
            space_available: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.data_available.notify_waiters();
            self.space_available.notify_waiters();
        }
    }
}

/// Producer-side endpoint of one local reader's wake channel.
pub(crate) struct Producer(Arc<LocalNotifications>);

impl ProducerWake for Producer {
    fn notify_data(&self) -> io::Result<()> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        self.0.data_available.notify_one();
        Ok(())
    }

    async fn wait_for_space(&self) -> io::Result<()> {
        let notified = self.0.space_available.notified();
        if self.0.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        notified.await;
        if self.0.closed.load(Ordering::Acquire) {
            Err(crate::error::peer_disconnected())
        } else {
            Ok(())
        }
    }

    fn close(&self) {
        self.0.close();
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Consumer-side endpoint of one local reader's wake channel.
pub(crate) struct Consumer(Arc<LocalNotifications>);

impl ConsumerWake for Consumer {
    async fn wait_for_data(&mut self) -> io::Result<()> {
        let notified = self.0.data_available.notified();
        if self.0.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        notified.await;
        if self.0.closed.load(Ordering::Acquire) {
            Err(crate::error::peer_disconnected())
        } else {
            Ok(())
        }
    }

    fn notify_space(&self) -> io::Result<()> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        self.0.space_available.notify_one();
        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Creates both endpoints of a local ring's wake channel.
pub(crate) fn pair() -> (Producer, Consumer) {
    let notifications = Arc::new(LocalNotifications::new());
    (
        Producer(Arc::clone(&notifications)),
        Consumer(notifications),
    )
}
