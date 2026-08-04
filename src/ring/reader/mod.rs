//! Owns reader admission and the producer-side connection for each slot.

mod registry;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(crate) use registry::{ActiveReaders, ReaderRegistry};

/// Exclusive ownership of one process-local reader slot reservation.
struct SlotLease {
    reservations: Arc<AtomicU64>,
    bit: u64,
    released: AtomicBool,
}

impl SlotLease {
    fn reserve(reservations: Arc<AtomicU64>) -> io::Result<Self> {
        let previous = reservations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                let free = !reserved;
                (free != 0).then(|| reserved | (1_u64 << free.trailing_zeros()))
            })
            .map_err(|_| crate::error::reader_capacity_exhausted())?;
        let bit = 1_u64 << (!previous).trailing_zeros();
        Ok(Self::claimed(reservations, bit))
    }

    fn claimed(reservations: Arc<AtomicU64>, bit: u64) -> Self {
        debug_assert!(bit.is_power_of_two());
        debug_assert_ne!(reservations.load(Ordering::Acquire) & bit, 0);
        Self {
            reservations,
            bit,
            released: AtomicBool::new(false),
        }
    }

    fn slot(&self) -> usize {
        self.bit.trailing_zeros() as usize
    }

    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.reservations.fetch_and(!self.bit, Ordering::AcqRel);
        }
    }
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        self.release();
    }
}

/// Producer-side state for one reader slot.
pub(crate) struct ReaderConnection<N> {
    /// Keeps this connection's shared read-cursor index reserved.
    lease: SlotLease,
    /// Channel used to wake this reader or wait for it to release space.
    pub(crate) notification: N,
}

impl<N> ReaderConnection<N> {
    pub(crate) fn slot(&self) -> usize {
        self.lease.slot()
    }

    pub(crate) fn release_slot(&self) {
        self.lease.release();
    }
}
