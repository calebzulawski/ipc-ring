//! Defines the fixed shared-memory prefix read by every attached process.

use std::mem::{offset_of, size_of};
use std::sync::atomic::AtomicU64;

/// Version written into each shared-memory header.
pub(crate) const ABI_VERSION: u64 = 1;
/// Each bit in a waiter mask represents one reader slot.
pub(crate) const MAX_READERS: usize = u64::BITS as usize;

/// Shared cursors and waiter masks stored immediately before the payload.
#[repr(C)]
pub(crate) struct Header {
    /// Written last so readers cannot attach to a partly initialized mapping.
    pub(crate) version: AtomicU64,
    /// Payload size in bytes; always a power of two.
    pub(crate) capacity: u64,
    /// Wrapping byte position written by the sole producer.
    pub(crate) write_position: AtomicU64,
    /// One bit per reader currently waiting for data.
    pub(crate) data_waiters: AtomicU64,
    /// One bit for the reader the producer is waiting on, or zero.
    pub(crate) space_waiters: AtomicU64,
    /// Wrapping byte position published separately by each reader.
    pub(crate) read_positions: [AtomicU64; MAX_READERS],
}

impl Header {
    /// Creates a zeroed header; mapping creation writes `version` afterward.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            version: AtomicU64::new(0),
            capacity: capacity as u64,
            write_position: AtomicU64::new(0),
            data_waiters: AtomicU64::new(0),
            space_waiters: AtomicU64::new(0),
            read_positions: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

const _: () = {
    assert!(offset_of!(Header, version) == 0);
    assert!(offset_of!(Header, capacity) == 8);
    assert!(offset_of!(Header, write_position) == 16);
    assert!(offset_of!(Header, data_waiters) == 24);
    assert!(offset_of!(Header, space_waiters) == 32);
    assert!(offset_of!(Header, read_positions) == 40);
    assert!(size_of::<Header>() == 40 + MAX_READERS * 8);
};
