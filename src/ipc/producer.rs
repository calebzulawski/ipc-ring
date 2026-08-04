//! Producer endpoint for a server-registered ring.

use super::notification;
use super::registered::RegisteredRing;
use crate::mapping::MappedMemory;
use crate::ring::PendingView;
use crate::ring::producer::{self as operations, ProducerEndpoint, ReaderCache};
use crate::ring::reader::ReaderRegistry;
use crate::{View, ViewMut};
use arc_swap::Cache;
use std::io;
use std::sync::Arc;

type ProducerNotification = Arc<notification::Producer>;

/// The write side of an IPC ring buffer.
pub struct Producer {
    memory: Arc<MappedMemory>,
    registry: Arc<ReaderRegistry<ProducerNotification>>,
    reader_cache: ReaderCache<ProducerNotification>,
    _registration: Arc<RegisteredRing>,
    pending: Option<PendingView>,
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
            pending: None,
        }
    }
}

impl ProducerEndpoint for Producer {
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
