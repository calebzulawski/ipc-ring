//! Producer endpoint for a local ring.

use super::notification;
use crate::mapping::MappedMemory;
#[cfg(test)]
use crate::ring::Header;
use crate::ring::PendingView;
use crate::ring::producer::{self as operations, ProducerEndpoint, ReaderCache};
use crate::ring::reader::ReaderRegistry;
use crate::{View, ViewMut};
use arc_swap::Cache;
use std::io;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::Ordering;

/// The write side of a local ring buffer.
pub struct Producer {
    memory: Arc<MappedMemory>,
    registry: Arc<ReaderRegistry<notification::Producer>>,
    reader_cache: ReaderCache<notification::Producer>,
    pending: Option<PendingView>,
}

impl Producer {
    pub(super) fn new(registry: Arc<ReaderRegistry<notification::Producer>>) -> Self {
        let memory = registry.memory();
        let reader_cache = Cache::new(registry.active_readers());
        Self {
            memory,
            registry,
            reader_cache,
            pending: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
    }

    #[cfg(test)]
    pub(crate) fn set_positions_for_test(&mut self, position: u64) {
        self.memory
            .header()
            .write_position
            .store(position, Ordering::Relaxed);
        let readers = self.reader_cache.load().clone();
        let mut active = readers.bitmap;
        while active != 0 {
            let slot = active.trailing_zeros() as usize;
            active &= !(1_u64 << slot);
            self.memory.header().read_positions[slot].store(position, Ordering::Relaxed);
        }
    }
}

impl ProducerEndpoint for Producer {
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

    fn pending(&self) -> Option<PendingView> {
        self.pending
    }

    fn pending_mut(&mut self) -> &mut Option<PendingView> {
        &mut self.pending
    }
}

impl View for Producer {
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

impl ViewMut for Producer {
    fn view_mut(&mut self) -> &mut [u8] {
        operations::view_mut(self)
    }
}
