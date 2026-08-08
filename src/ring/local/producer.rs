//! Producer for a local ring.

use super::notification;
use crate::cursor::{Cursor, CursorMut, Reservation};
#[cfg(test)]
use crate::ring::Header;
use crate::ring::mapping::MappedMemory;
use crate::ring::producer::{self as operations, ProducerState, ReaderCache};
use crate::ring::reader::ReaderRegistry;
use arc_swap::Cache;
use std::io;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::Ordering;

/// Raw state for the write side of a local ring buffer.
pub struct Producer {
    memory: Arc<MappedMemory>,
    registry: Arc<ReaderRegistry<notification::Producer>>,
    reader_cache: ReaderCache<notification::Producer>,
}

impl Producer {
    pub(super) fn new(registry: Arc<ReaderRegistry<notification::Producer>>) -> Self {
        let memory = registry.memory();
        let reader_cache = Cache::new(registry.active_readers());
        Self {
            memory,
            registry,
            reader_cache,
        }
    }
}

impl ProducerState for Producer {
    type Notification = notification::Producer;
    const SURVIVES_WITHOUT_READERS: bool = false;

    fn memory(&self) -> &Arc<MappedMemory> {
        &self.memory
    }

    fn registry(&self) -> &Arc<ReaderRegistry<Self::Notification>> {
        &self.registry
    }

    fn reader_cache(&mut self) -> &mut ReaderCache<Self::Notification> {
        &mut self.reader_cache
    }
}

// SAFETY: the local mapping is double-mapped and remains alive with the
// cursor. The single producer owns the writable cursor, while the ring
// operations validate reservations against every active reader.
unsafe impl Cursor for Producer {
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

// SAFETY: producer reservations exclude all published bytes visible to active
// readers, and this is the unique producer that may write unpublished reservations.
unsafe impl CursorMut for Producer {}

impl crate::view::View<Producer> {
    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.cursor().memory.header()
    }

    #[cfg(test)]
    pub(crate) fn set_positions_for_test(&mut self, position: u64) {
        let cursor = self.cursor_mut();
        cursor
            .memory
            .header()
            .write_position
            .store(position, Ordering::Relaxed);
        let readers = cursor.reader_cache.load().clone();
        let mut active = readers.bitmap;
        while active != 0 {
            let slot = active.trailing_zeros() as usize;
            active &= !(1_u64 << slot);
            cursor.memory.header().read_positions[slot].store(position, Ordering::Relaxed);
        }
    }
}
