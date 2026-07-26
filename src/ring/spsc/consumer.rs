use super::SharedRing;
use super::state::valid_len;
use crate::error;
use std::io;
use std::path::Path;
use std::slice;
use std::sync::Arc;
use std::time::Duration;

/// Options for connecting a consumer to a registered ring.
#[derive(Clone, Copy, Debug)]
pub struct ConnectOptions {
    handshake_timeout: Duration,
}

impl ConnectOptions {
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
        let (stream, memory) =
            crate::handshake::connect(path, port, self.handshake_timeout).await?;
        Ok(Consumer::new(Arc::new(SharedRing::consumer(
            stream, memory,
        ))))
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The sole reader for a shared ring; dropping it permits a named replacement.
pub struct Consumer {
    pub(super) ring: Arc<SharedRing>,
}

impl Consumer {
    pub(super) fn new(ring: Arc<SharedRing>) -> Self {
        Self { ring }
    }

    /// Requests `port` from the router at the supplied native socket or pipe path.
    pub async fn connect(path: impl AsRef<Path>, port: impl Into<String>) -> io::Result<Self> {
        ConnectOptions::new().connect(path, port).await
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    pub fn readable_len(&self) -> io::Result<usize> {
        self.ring.readable_len()
    }

    pub fn try_inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        if self.readable_len()? < len {
            return Err(error::would_block());
        }
        Ok(self.grant(len))
    }

    /// Waits asynchronously until `len` published bytes can be inspected.
    pub async fn inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        self.ring.wait_for_data(len).await?;
        Ok(self.grant(len))
    }

    fn grant(&mut self, len: usize) -> ReadGrant<'_> {
        let position = self.ring.read_position();
        let offset = (position & (self.ring.capacity() as u64 - 1)) as usize;
        ReadGrant {
            consumer: self,
            position,
            offset,
            len,
        }
    }

    async fn release(&mut self, position: u64, amount: usize) -> io::Result<()> {
        self.ring.release_space(position, amount).await
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
        unsafe { slice::from_raw_parts(self.consumer.ring.payload().add(self.offset), self.len) }
    }

    /// Cursor publication precedes notification; if notification fails, the
    /// released bytes have nevertheless been reclaimed.
    pub async fn release(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.consumer.release(self.position, amount).await
    }
}
