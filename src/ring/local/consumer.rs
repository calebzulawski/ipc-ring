//! Consumer for a local ring.

use super::notification;
use super::reader;
use crate::cursor::{Cursor, Reservation, TryFork};
#[cfg(test)]
use crate::ring::Header;
use crate::ring::consumer::{self as operations, ConsumerState};
use crate::ring::mapping::MappedMemory;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Raw state for one reader of a local in-process ring.
pub struct Consumer {
    memory: Arc<MappedMemory>,
    slot: u8,
    notification: notification::Consumer,
    local_reader: reader::Guard,
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
        }
    }

    fn fork_inner(&self) -> io::Result<Self> {
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
        })
    }
}

// SAFETY: each fork claims and publishes a distinct reader slot at the source
// consumer's acquired read position. The producer retains bytes until every
// active slot advances, so one fork cannot invalidate another fork's eligible
// reservations.
unsafe impl TryFork for Consumer {
    fn try_fork(&self) -> io::Result<Self> {
        self.fork_inner()
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

// SAFETY: the local mapping is double-mapped and remains alive with the
// cursor. The ring operations validate indexed reservations and synchronize the
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

impl crate::view::View<Consumer> {
    #[cfg(test)]
    pub(crate) fn header(&self) -> &Header {
        self.cursor().memory.header()
    }
}
