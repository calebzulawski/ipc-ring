//! Wakes producers and consumers after shared ring positions change.
//!
//! IPC wakeups are a single zero byte; anonymous rings use Tokio notifications.

mod anonymous;
mod stream;

use crate::local_socket::ConsumerStream;
use std::io;
use std::sync::Arc;

pub(super) use anonymous::pair as anonymous_pair;
use stream::ConsumerIpcNotification;
pub(in crate::ring) use stream::ProducerIpcNotification;

/// Creates the two references needed while an IPC reader is attaching.
pub(in crate::ring) fn pending_ipc() -> (ProducerNotification, Arc<ProducerIpcNotification>) {
    let notification = Arc::new(ProducerIpcNotification::pending());
    (
        ProducerNotification::Ipc(Arc::clone(&notification)),
        notification,
    )
}

/// How the producer wakes or waits on one reader.
pub(super) enum ProducerNotification {
    Ipc(Arc<ProducerIpcNotification>),
    Anonymous(Arc<anonymous::AnonymousNotifications>),
}

impl ProducerNotification {
    pub(crate) fn notify_data(&self) -> io::Result<()> {
        match self {
            Self::Ipc(stream) => stream.try_wake(),
            Self::Anonymous(notifications) => notifications.notify_data(),
        }
    }

    pub(crate) async fn wait_for_space(&self) -> io::Result<()> {
        match self {
            Self::Ipc(stream) => stream.wait_for_wake().await,
            Self::Anonymous(notifications) => notifications.wait_for_space().await,
        }
    }

    /// Closes this reader's wake channel so pending operations observe removal.
    pub(crate) fn close(&self) {
        match self {
            Self::Ipc(stream) => stream.close(),
            Self::Anonymous(notifications) => notifications.close(),
        }
    }
}

impl Drop for ProducerNotification {
    fn drop(&mut self) {
        if let Self::Anonymous(notifications) = self {
            notifications.close();
        }
    }
}

/// How one reader waits for data or wakes the producer.
pub(super) enum ConsumerNotification {
    Ipc(ConsumerIpcNotification),
    Anonymous(Arc<anonymous::AnonymousNotifications>),
}

impl ConsumerNotification {
    pub(in crate::ring) fn ipc(stream: ConsumerStream) -> Self {
        Self::Ipc(ConsumerIpcNotification::connected(stream))
    }

    pub(crate) async fn wait_for_data(&mut self) -> io::Result<()> {
        match self {
            Self::Ipc(stream) => stream.wait_for_wake().await,
            Self::Anonymous(notifications) => notifications.wait_for_data().await,
        }
    }

    pub(crate) fn notify_space(&self) -> io::Result<()> {
        match self {
            Self::Ipc(stream) => stream.try_wake(),
            Self::Anonymous(notifications) => notifications.notify_space(),
        }
    }
}

impl Drop for ConsumerNotification {
    fn drop(&mut self) {
        if let Self::Anonymous(notifications) = self {
            notifications.close();
        }
    }
}
