//! Validates grant lengths and distances between wrapping shared cursors.

use crate::error;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sets one waiter bit and clears it when the surrounding wait scope exits.
pub(super) fn set_waiter_bit(waiters: &AtomicU64, bit: u64) -> impl Drop + '_ {
    waiters.fetch_or(bit, Ordering::AcqRel);
    scopeguard::guard((waiters, bit), |(waiters, bit)| {
        waiters.fetch_and(!bit, Ordering::AcqRel);
    })
}

/// Rejects spans larger than the double-mapped payload.
pub(super) fn valid_len(len: usize, capacity: usize) -> io::Result<()> {
    if len > capacity {
        Err(error::invalid_length())
    } else {
        Ok(())
    }
}

/// Returns buffered bytes, rejecting cursor distances beyond one ring capacity.
pub(super) fn used(write: u64, read: u64, capacity: usize) -> io::Result<usize> {
    let len = write.wrapping_sub(read);
    if len > capacity as u64 {
        Err(error::corrupt_state())
    } else {
        Ok(len as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::used;
    use std::io::ErrorKind;

    #[test]
    fn cursor_distances_cover_empty_full_corrupt_and_wrapping_states() {
        let capacity = 8;
        assert_eq!(used(5, 5, capacity).unwrap(), 0);
        assert_eq!(used(13, 5, capacity).unwrap(), capacity);
        assert_eq!(used(3, u64::MAX - 4, capacity).unwrap(), capacity);
        assert_eq!(used(14, 5, capacity).unwrap_err().kind(), ErrorKind::Other);
        assert_eq!(
            used(3, u64::MAX - 5, capacity).unwrap_err().kind(),
            ErrorKind::Other
        );
    }
}
