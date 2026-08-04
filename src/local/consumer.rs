//! Consumer endpoint for a local ring.

use super::notification;
use super::reader;
use crate::View;
use crate::mapping::MappedMemory;
#[cfg(test)]
use crate::ring::Header;
use crate::ring::PendingView;
use crate::ring::consumer::{self as operations, ConsumerEndpoint};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// One reader of a local in-process ring.
pub struct Consumer {
    memory: Arc<MappedMemory>,
    slot: u8,
    notification: notification::Consumer,
    local_reader: reader::Guard,
    pending: Option<PendingView>,
}

impl Consumer {
    pub(super) fn new(
        memory: Arc<MappedMemory>,
        slot: u8,
        notification: notification::Consumer,
        local_reader: reader::Guard,
    ) -> Self {
        Self {
            memory,
            slot,
            notification,
            local_reader,
            pending: None,
        }
    }

    /// Creates an independent reader beginning at this reader's current cursor.
    pub fn try_clone(&self) -> io::Result<Self> {
        let (producer_notification, notification) = notification::pair();
        let read_position =
            self.memory.header().read_positions[self.slot as usize].load(Ordering::Acquire);
        let (slot, local_reader) = self
            .local_reader
            .claim_sibling(read_position, producer_notification)?;
        Ok(Self {
            memory: Arc::clone(&self.memory),
            slot,
            notification,
            local_reader,
            pending: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
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
