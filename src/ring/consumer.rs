//! Consumer-side cursor management, waiting, and readable grants.

#[cfg(test)]
use super::Header;
use super::MAX_READERS;
use super::notification::ConsumerNotification;
use super::reader::LocalReaderGuard;
use super::state::{set_waiter_bit, used, valid_len};
use crate::error;
use crate::local_socket::ConsumerStream;
use crate::mapping::MappedMemory;
use std::io;
use std::path::Path;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Options for connecting a consumer to a registered ring.
#[derive(Clone, Copy, Debug)]
pub struct ConnectOptions {
    handshake_timeout: Duration,
}

impl ConnectOptions {
    /// Uses the crate's default timeout for the complete attachment handshake.
    pub const fn new() -> Self {
        Self {
            handshake_timeout: crate::handshake::DEFAULT_TIMEOUT,
        }
    }

    /// Limits the complete connection handshake without affecting later ring waits.
    pub const fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Connects to one ring port using these options.
    pub async fn connect(
        self,
        path: impl AsRef<Path>,
        port: impl Into<String>,
    ) -> io::Result<Consumer> {
        let path = path.as_ref().to_path_buf();
        let port = port.into();
        let (slot, stream, memory) =
            crate::handshake::connect(path, port, self.handshake_timeout).await?;
        Consumer::ipc(slot, stream, memory)
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// One reader attached to a shared ring.
pub struct Consumer {
    memory: Arc<MappedMemory>,
    slot: u8,
    notification: ConsumerNotification,
    /// Keeps the anonymous reader registered for this consumer's lifetime.
    _local_reader: Option<LocalReaderGuard>,
}

impl Consumer {
    /// Builds the consumer endpoint returned by a successful IPC handshake.
    pub(crate) fn ipc(
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
            notification: ConsumerNotification::ipc(stream),
            _local_reader: None,
        })
    }

    /// Builds slot zero of an anonymous in-process ring.
    pub(super) fn process_local(
        memory: Arc<MappedMemory>,
        notification: ConsumerNotification,
        local_reader: LocalReaderGuard,
    ) -> Self {
        Self {
            memory,
            slot: 0,
            notification,
            _local_reader: Some(local_reader),
        }
    }

    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
    }

    fn read_position(&self) -> u64 {
        self.memory.header().read_positions[self.slot as usize].load(Ordering::Relaxed)
    }

    /// Requests `port` from the router at the supplied native socket or pipe path.
    pub async fn connect(path: impl AsRef<Path>, port: impl Into<String>) -> io::Result<Self> {
        ConnectOptions::new().connect(path, port).await
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    /// Returns bytes currently published to this consumer but not yet released.
    pub fn readable_len(&self) -> io::Result<usize> {
        let read_position = self.read_position();
        let write_position = self.memory.header().write_position.load(Ordering::Acquire);
        used(write_position, read_position, self.capacity())
    }

    /// Grants `len` published bytes immediately or returns `WouldBlock`.
    pub fn try_inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.capacity())?;
        if self.readable_len()? < len {
            return Err(error::would_block());
        }
        Ok(self.grant(len))
    }

    /// Waits asynchronously until `len` published bytes can be inspected.
    pub async fn inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.capacity())?;
        self.wait_for_data(len).await?;
        Ok(self.grant(len))
    }

    /// Sets this reader's waiter bit, rechecks, then sleeps if data is still short.
    async fn wait_for_data(&mut self, minimum: usize) -> io::Result<()> {
        let bit = 1_u64 << self.slot;
        loop {
            if self.readable_len()? >= minimum {
                return Ok(());
            }
            let _waiter_bit = set_waiter_bit(&self.memory.header().data_waiters, bit);
            if self.readable_len()? >= minimum {
                return Ok(());
            }
            self.notification.wait_for_data().await?;
        }
    }

    fn grant(&mut self, len: usize) -> ReadGrant<'_> {
        let position = self.read_position();
        let offset = (position & (self.capacity() as u64 - 1)) as usize;
        ReadGrant {
            consumer: self,
            position,
            offset,
            len,
        }
    }

    /// Stores the new read position before waking a producer waiting on this slot.
    fn release_space(&self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        self.memory.header().read_positions[self.slot as usize]
            .store(position.wrapping_add(amount as u64), Ordering::Release);
        let bit = 1_u64 << self.slot;
        if self
            .memory
            .header()
            .space_waiters
            .fetch_and(!bit, Ordering::AcqRel)
            & bit
            != 0
        {
            self.notification.notify_space()?;
        }
        Ok(())
    }
}

/// A readable span whose cursor advances only when `release` succeeds.
pub struct ReadGrant<'a> {
    consumer: &'a mut Consumer,
    position: u64,
    offset: usize,
    len: usize,
}

impl ReadGrant<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the unique consumer borrow owns a published double-mapped span.
        unsafe { slice::from_raw_parts(self.consumer.memory.payload().add(self.offset), self.len) }
    }

    /// Makes the released space visible before waking the producer.
    ///
    /// A wakeup failure does not undo the release.
    pub fn release(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.consumer.release_space(self.position, amount)
    }
}
