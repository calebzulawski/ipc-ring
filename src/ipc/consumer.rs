//! Consumer for a server-registered ring.

use super::handshake;
use super::notification;
use super::socket::ConsumerStream;
use crate::mapping::MappedMemory;
use crate::raw::{Cursor, Reservation};
use crate::ring::MAX_READERS;
use crate::ring::consumer::{self as operations, ConsumerState};
use crate::view::View;
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
    ) -> io::Result<View<Consumer>> {
        let path = path.as_ref().to_path_buf();
        let port = port.into();
        let (slot, stream, memory) = handshake::connect(path, port, self.handshake_timeout).await?;
        Ok(View::from_cursor(Consumer::from_attachment(
            slot, stream, memory,
        )?))
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw state for one reader attached through a server.
pub struct Consumer {
    memory: Arc<MappedMemory>,
    slot: u8,
    notification: notification::Consumer,
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
        })
    }
}

impl ConsumerState for Consumer {
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
}

// SAFETY: the attached mapping is double-mapped and remains alive with the
// cursor. The ring operations validate indexed reservations and synchronize
// shared cursors before returning or advancing them.
unsafe impl Cursor for Consumer {
    fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    fn position(&self) -> u64 {
        operations::position(self)
    }

    fn try_reserve_at(&mut self, position: u64, minimum: usize) -> io::Result<Reservation> {
        operations::try_reserve_at(self, position, minimum)
    }

    async fn reserve_at(&mut self, position: u64, minimum: usize) -> io::Result<Reservation> {
        operations::reserve_at(self, position, minimum).await
    }

    unsafe fn advance_to(&mut self, position: u64) -> io::Result<()> {
        // SAFETY: the caller supplies the raw-access and initialization
        // guarantees required by `Cursor::advance_to`.
        unsafe { operations::advance_to(self, position) }
    }
}
