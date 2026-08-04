//! Consumer endpoint for a server-registered ring.

use super::handshake;
use super::notification;
use super::socket::ConsumerStream;
use crate::View;
use crate::mapping::MappedMemory;
use crate::ring::consumer::{self as operations, ConsumerEndpoint};
use crate::ring::{MAX_READERS, PendingView};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Options for connecting to an IPC ring buffer.
#[derive(Clone, Copy, Debug)]
pub struct ConnectOptions {
    handshake_timeout: Duration,
}

impl ConnectOptions {
    /// Creates connection options with default settings.
    pub const fn new() -> Self {
        Self {
            handshake_timeout: handshake::DEFAULT_TIMEOUT,
        }
    }

    /// Sets how long a connection attempt may take.
    pub const fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Connects to `port` on the server at `path`.
    pub async fn connect(
        self,
        path: impl AsRef<Path>,
        port: impl Into<String>,
    ) -> io::Result<Consumer> {
        let path = path.as_ref().to_path_buf();
        let port = port.into();
        let (slot, stream, memory) = handshake::connect(path, port, self.handshake_timeout).await?;
        Consumer::from_attachment(slot, stream, memory)
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// One reader attached through a server.
pub struct Consumer {
    memory: Arc<MappedMemory>,
    slot: u8,
    notification: notification::Consumer,
    pending: Option<PendingView>,
}

impl Consumer {
    pub(crate) fn from_attachment(
        slot: u8,
        stream: ConsumerStream,
        memory: Arc<MappedMemory>,
    ) -> io::Result<Self> {
        if slot as usize >= MAX_READERS {
            return Err(crate::error::protocol("reader slot is out of range"));
        }
        Ok(Self {
            memory,
            slot,
            notification: notification::Consumer::connected(stream),
            pending: None,
        })
    }

    /// Requests `port` from the server at the supplied native socket or pipe path.
    pub async fn connect(path: impl AsRef<Path>, port: impl Into<String>) -> io::Result<Self> {
        ConnectOptions::new().connect(path, port).await
    }
}

impl ConsumerEndpoint for Consumer {
    type Notification = notification::Consumer;

    fn memory(&self) -> &Arc<MappedMemory> {
        &self.memory
    }

    fn slot(&self) -> u8 {
        self.slot
    }

    fn notification(&self) -> &Self::Notification {
        &self.notification
    }

    fn notification_mut(&mut self) -> &mut Self::Notification {
        &mut self.notification
    }

    fn pending(&self) -> Option<PendingView> {
        self.pending
    }

    fn pending_mut(&mut self) -> &mut Option<PendingView> {
        &mut self.pending
    }
}

impl View for Consumer {
    fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    fn try_reserve(&mut self, minimum: usize) -> io::Result<()> {
        operations::try_reserve(self, minimum)
    }

    async fn reserve(&mut self, minimum: usize) -> io::Result<()> {
        operations::reserve(self, minimum).await
    }

    fn view(&self) -> &[u8] {
        operations::view(self)
    }

    fn advance(&mut self, amount: usize) -> io::Result<()> {
        operations::advance(self, amount)
    }
}
