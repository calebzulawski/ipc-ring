//! Producer-side reservations, commits, and reader wakeups.

#[cfg(test)]
use super::Header;
use super::RegisteredRing;
use super::reader::{ActiveReaders, ReaderConnection, ReaderRegistry};
use super::state::{set_waiter_bit, used, valid_len};
use crate::error;
use crate::mapping::MappedMemory;
use arc_swap::{ArcSwap, Cache};
use std::io;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Whether a request fits or which reader can make it fit.
enum WritableState {
    Available(usize),
    BlockedByReader(Arc<ReaderConnection>),
}

/// The sole writer for a shared ring.
pub struct Producer {
    memory: Arc<MappedMemory>,
    registry: Arc<ReaderRegistry>,
    reader_cache: Cache<Arc<ArcSwap<ActiveReaders>>, Arc<ActiveReaders>>,
    /// Keeps a named ring registered for this producer's lifetime.
    _registration: Option<Arc<RegisteredRing>>,
}

impl Producer {
    /// Creates a producer whose lifetime is not tied to a server registration.
    pub(crate) fn unregistered(registry: Arc<ReaderRegistry>) -> Self {
        let memory = registry.memory();
        let reader_cache = Cache::new(registry.active_readers());
        Self {
            memory,
            registry,
            reader_cache,
            _registration: None,
        }
    }

    /// Creates a producer from one complete named-ring registration.
    pub(crate) fn registered(registration: Arc<RegisteredRing>) -> Self {
        let registry = Arc::clone(&registration.readers);
        let reader_cache = Cache::new(registry.active_readers());
        Self {
            memory: Arc::clone(&registration.memory),
            registry,
            reader_cache,
            _registration: Some(registration),
        }
    }

    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    fn write_position(&self) -> u64 {
        self.memory.header().write_position.load(Ordering::Relaxed)
    }

    /// Scans the current reader set, stopping at the first reader that blocks.
    fn scan_writable_state(
        memory: &MappedMemory,
        readers: &ActiveReaders,
        minimum: usize,
    ) -> io::Result<WritableState> {
        let capacity = memory.capacity();
        let write_position = memory.header().write_position.load(Ordering::Acquire);

        if readers.bitmap == 0 {
            return Ok(WritableState::Available(capacity));
        }

        let blocking_distance = capacity - minimum;
        let mut active = readers.bitmap;
        let mut greatest_buffered = 0;
        while active != 0 {
            let slot = active.trailing_zeros() as usize;
            active &= !(1_u64 << slot);
            let read_position = memory.header().read_positions[slot].load(Ordering::Acquire);
            let buffered = used(write_position, read_position, capacity)?;
            if buffered > blocking_distance {
                let reader = readers.connections[slot]
                    .as_ref()
                    .expect("active reader slot has a connection");
                return Ok(WritableState::BlockedByReader(Arc::clone(reader)));
            }
            greatest_buffered = greatest_buffered.max(buffered);
        }

        Ok(WritableState::Available(capacity - greatest_buffered))
    }

    /// Revalidates the cached membership before calculating writable space.
    fn writable_state(&mut self, minimum: usize) -> io::Result<WritableState> {
        let readers = self.reader_cache.load();
        if readers.bitmap == 0 && !self.registry.producer_survives_without_readers() {
            return Err(crate::error::peer_disconnected());
        }
        Self::scan_writable_state(&self.memory, readers, minimum)
    }

    /// Returns free capacity after checking the active reader cursors.
    pub fn writable_len(&mut self) -> io::Result<usize> {
        match self.writable_state(0)? {
            WritableState::Available(available) => Ok(available),
            WritableState::BlockedByReader(_) => unreachable!("zero bytes cannot be blocked"),
        }
    }

    /// Grants `len` writable bytes immediately or returns `WouldBlock`.
    pub fn try_reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.capacity())?;
        match self.writable_state(len)? {
            WritableState::Available(_) => Ok(self.grant(len)),
            WritableState::BlockedByReader(_) => Err(error::would_block()),
        }
    }

    /// Waits for `len` contiguous bytes, including across the ring's wrap boundary.
    pub async fn reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.capacity())?;
        self.wait_for_space(len).await?;
        Ok(self.grant(len))
    }

    /// Waits on one blocking reader and rechecks after its next notification.
    async fn wait_for_space(&mut self, minimum: usize) -> io::Result<()> {
        loop {
            let reader = match self.writable_state(minimum)? {
                WritableState::Available(_) => return Ok(()),
                WritableState::BlockedByReader(reader) => reader,
            };

            let registry = Arc::clone(&self.registry);
            let memory = Arc::clone(&self.memory);
            let bit = 1_u64 << reader.slot();
            let _waiter_bit = set_waiter_bit(&memory.header().space_waiters, bit);

            let still_blocking = match self.writable_state(minimum)? {
                WritableState::Available(_) => return Ok(()),
                WritableState::BlockedByReader(reader) => reader,
            };
            if !Arc::ptr_eq(&reader, &still_blocking) {
                continue;
            }

            if reader.notification.wait_for_space().await.is_err() {
                registry.disconnect(&reader);
            }
        }
    }

    fn grant(&mut self, len: usize) -> WriteGrant<'_> {
        let position = self.write_position();
        let offset = (position & (self.capacity() as u64 - 1)) as usize;
        WriteGrant {
            producer: self,
            position,
            offset,
            len,
        }
    }

    /// Makes committed bytes visible and wakes readers waiting for data.
    fn publish_data(&mut self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        if !self.registry.producer_survives_without_readers()
            && self.reader_cache.load().bitmap == 0
        {
            return Err(crate::error::peer_disconnected());
        }
        self.memory
            .header()
            .write_position
            .store(position.wrapping_add(amount as u64), Ordering::Release);

        let waiting = self.registry.take_data_waiters();
        if waiting != 0 {
            // Route captured bits through membership loaded after the take. A
            // bit set by a later attachment remains armed for the next write.
            let readers = self.reader_cache.load();
            Self::notify_data_waiters(&self.registry, readers, waiting);
        }
        Ok(())
    }

    /// Wakes only readers whose bits were set.
    fn notify_data_waiters(registry: &ReaderRegistry, readers: &ActiveReaders, mut waiting: u64) {
        let mut failed = Vec::new();
        while waiting != 0 {
            let slot = waiting.trailing_zeros() as usize;
            waiting &= !(1_u64 << slot);
            let Some(reader) = readers.connections[slot].as_ref().map(Arc::clone) else {
                continue;
            };
            if reader.notification.notify_data().is_err() {
                failed.push(reader);
            }
        }
        for reader in failed {
            registry.disconnect(&reader);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_positions_for_test(&mut self, position: u64) {
        self.memory
            .header()
            .write_position
            .store(position, Ordering::Relaxed);
        let mut active = self.reader_cache.load().bitmap;
        while active != 0 {
            let slot = active.trailing_zeros() as usize;
            active &= !(1_u64 << slot);
            self.memory.header().read_positions[slot].store(position, Ordering::Relaxed);
        }
    }
}

/// A writable span whose cursor advances only when `commit` succeeds.
pub struct WriteGrant<'a> {
    producer: &'a mut Producer,
    position: u64,
    offset: usize,
    len: usize,
}

impl WriteGrant<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the unique producer borrow owns a free, double-mapped span.
        unsafe {
            slice::from_raw_parts_mut(self.producer.memory.payload().add(self.offset), self.len)
        }
    }

    /// Makes the committed bytes visible before waking readers.
    ///
    /// A wakeup failure does not undo the commit.
    pub fn commit(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.producer.publish_data(self.position, amount)
    }
}
