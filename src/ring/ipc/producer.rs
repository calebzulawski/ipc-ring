//! Producer for a server-registered ring.

use super::notification;
use super::registered::RegisteredRing;
use crate::cursor::{Cursor, CursorMut, Reservation};
use crate::ring::mapping::MappedMemory;
use crate::ring::producer::{self as operations, ProducerState, ReaderCache};
use crate::ring::reader::ReaderRegistry;
use arc_swap::Cache;
use std::io;
use std::sync::Arc;

type ProducerNotification = Arc<notification::Producer>;

/// Raw state for the write side of a server-registered ring.
pub struct Producer {
    memory: Arc<MappedMemory>,
    registry: Arc<ReaderRegistry<ProducerNotification>>,
    reader_cache: ReaderCache<ProducerNotification>,
    _registration: Arc<RegisteredRing>,
}

impl Producer {
    pub(crate) fn registered(registration: Arc<RegisteredRing>) -> Self {
        let registry = Arc::clone(&registration.readers);
        let reader_cache = Cache::new(registry.active_readers());
        Self {
            memory: Arc::clone(&registration.memory),
            registry,
            reader_cache,
            _registration: registration,
        }
    }
}

impl ProducerState for Producer {
    type Notification = ProducerNotification;
    const SURVIVES_WITHOUT_READERS: bool = true;

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

// SAFETY: the registered mapping is double-mapped and remains alive with the
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
