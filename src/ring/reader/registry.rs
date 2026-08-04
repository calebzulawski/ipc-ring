//! Reserves reader slots during attachment and publishes completed connections.

use super::{ReaderConnection, SlotLease};
use crate::mapping::MappedMemory;
use crate::ring::MAX_READERS;
use crate::ring::wake::ProducerWake;
use arc_swap::ArcSwap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One coherent set of readers used by the producer without locking.
pub(crate) struct ActiveReaders<N> {
    pub(crate) bitmap: u64,
    pub(crate) connections: [Option<Arc<ReaderConnection<N>>>; MAX_READERS],
}

impl<N> Clone for ActiveReaders<N> {
    fn clone(&self) -> Self {
        Self {
            bitmap: self.bitmap,
            connections: self.connections.clone(),
        }
    }
}

impl<N> Default for ActiveReaders<N> {
    fn default() -> Self {
        Self {
            bitmap: 0,
            connections: std::array::from_fn(|_| None),
        }
    }
}

/// Tracks which connection owns each reader slot.
pub(crate) struct ReaderRegistry<N: ProducerWake> {
    memory: Arc<MappedMemory>,
    reservations: Arc<AtomicU64>,
    active_readers: Arc<ArcSwap<ActiveReaders<N>>>,
}

impl<N: ProducerWake> ReaderRegistry<N> {
    pub(crate) fn new(memory: Arc<MappedMemory>) -> Arc<Self> {
        Arc::new(Self {
            memory,
            reservations: Arc::new(AtomicU64::new(0)),
            active_readers: Arc::new(ArcSwap::from_pointee(ActiveReaders::default())),
        })
    }

    /// Clones the mapping whose reader slots this registry manages.
    pub(crate) fn memory(&self) -> Arc<MappedMemory> {
        Arc::clone(&self.memory)
    }

    /// Returns the producer's current write cursor.
    pub(crate) fn write_position(&self) -> u64 {
        self.memory.header().write_position.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn reserved_slots(&self) -> u64 {
        self.reservations.load(Ordering::Acquire)
    }

    pub(crate) fn reserve_reader(
        self: &Arc<Self>,
        notification: N,
    ) -> io::Result<Arc<ReaderConnection<N>>> {
        let lease = SlotLease::reserve(Arc::clone(&self.reservations))?;
        Ok(Arc::new(ReaderConnection {
            lease,
            notification,
        }))
    }

    /// Publishes a completed reader at the supplied cursor.
    pub(crate) fn activate_at(&self, reader: &Arc<ReaderConnection<N>>, read_position: u64) {
        let slot = reader.slot();
        let bit = 1_u64 << slot;
        self.memory.header().read_positions[slot].store(read_position, Ordering::Release);
        self.memory
            .header()
            .data_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.memory
            .header()
            .space_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.active_readers.rcu(|current| {
            let mut updated = (**current).clone();
            assert!(
                updated.connections[slot].is_none(),
                "reserved reader slot was already active"
            );
            updated.connections[slot] = Some(Arc::clone(reader));
            updated.bitmap |= bit;
            Arc::new(updated)
        });
    }

    /// Removes this exact connection without disturbing a reused slot.
    pub(crate) fn disconnect(&self, reader: &Arc<ReaderConnection<N>>) {
        let slot = reader.slot();
        let bit = 1_u64 << slot;
        reader.notification.close();
        let previous = self.active_readers.rcu(|current| {
            let matches = current.connections[slot]
                .as_ref()
                .is_some_and(|stored| Arc::ptr_eq(stored, reader));
            if !matches {
                return Arc::clone(current);
            }

            let mut updated = (**current).clone();
            updated.connections[slot] = None;
            updated.bitmap &= !bit;
            Arc::new(updated)
        });
        let removed = previous.connections[slot]
            .as_ref()
            .is_some_and(|stored| Arc::ptr_eq(stored, reader));
        if !removed {
            return;
        }

        self.memory
            .header()
            .data_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.memory
            .header()
            .space_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        reader.release_slot();
    }

    /// Clones the shared membership source used by a producer-owned cache.
    pub(crate) fn active_readers(&self) -> Arc<ArcSwap<ActiveReaders<N>>> {
        Arc::clone(&self.active_readers)
    }

    /// Takes the reader bits armed before the latest publication.
    pub(crate) fn take_data_waiters(&self) -> u64 {
        self.memory.header().data_waiters.swap(0, Ordering::AcqRel)
    }
}
