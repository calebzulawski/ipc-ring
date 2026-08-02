#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use unix as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

use crate::error;
use crate::ring::Header;
use std::io;
use std::sync::atomic::Ordering;

pub(crate) use implementation::{MappedMemory, SharedMemory};

pub(crate) fn create(minimum_capacity: usize) -> io::Result<(SharedMemory, MappedMemory)> {
    implementation::create(minimum_capacity)
}

pub(crate) unsafe fn attach(shared_memory: &SharedMemory) -> io::Result<MappedMemory> {
    // SAFETY: the caller received this object from the trusted local server.
    unsafe { implementation::attach(shared_memory) }
}

#[cfg(test)]
pub(crate) fn minimum_capacity() -> usize {
    implementation::minimum_capacity()
}

fn capacity_for_minimum(minimum_capacity: usize) -> io::Result<usize> {
    let granularity = implementation::minimum_capacity();
    assert!(
        granularity != 0 && granularity.is_power_of_two(),
        "mapping granularity must be a nonzero power of two"
    );

    let capacity = minimum_capacity
        .max(granularity)
        .checked_next_power_of_two()
        .ok_or_else(|| error::invalid_input("minimum capacity is too large"))?;
    if capacity as u128 > 1_u128 << 63 {
        return Err(error::invalid_input("capacity exceeds 2^63"));
    }
    capacity
        .checked_mul(2)
        .ok_or_else(|| error::invalid_input("double-mapped capacity overflows address space"))?;
    Ok(capacity)
}

fn read_version(header: &Header) -> io::Result<u64> {
    let version = header.version.load(Ordering::Acquire);
    if version == 0 {
        Err(error::invalid_layout("header initialization is incomplete"))
    } else {
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::{capacity_for_minimum, minimum_capacity};

    #[test]
    fn capacity_rounding_uses_the_next_valid_power_of_two() {
        let granularity = minimum_capacity();
        let largest_double_mappable_power = 1_usize << (usize::BITS - 2);
        let cases = [
            (0, Some(granularity)),
            (1, Some(granularity)),
            (granularity - 1, Some(granularity)),
            (granularity, Some(granularity)),
            (granularity + 1, Some(granularity * 2)),
            (granularity * 2, Some(granularity * 2)),
            (granularity * 2 + 1, Some(granularity * 4)),
            (
                largest_double_mappable_power,
                Some(largest_double_mappable_power),
            ),
            (largest_double_mappable_power + 1, None),
            (usize::MAX, None),
        ];
        for (minimum, expected) in cases {
            assert_eq!(capacity_for_minimum(minimum).ok(), expected);
        }
    }
}
