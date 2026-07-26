#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use unix as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

use crate::error;
use crate::ring::spsc::Header;
use std::io;
use std::sync::atomic::Ordering;

pub(crate) use implementation::{MappedMemory, SharedMemory};

pub(crate) fn create(minimum_capacity: usize) -> io::Result<(SharedMemory, MappedMemory)> {
    implementation::create(minimum_capacity)
}

pub(crate) unsafe fn attach(shared_memory: &SharedMemory) -> io::Result<MappedMemory> {
    // SAFETY: the caller obtained the object through a trusted local handoff.
    unsafe { implementation::attach(shared_memory) }
}

#[cfg(test)]
pub(crate) fn minimum_capacity() -> usize {
    implementation::minimum_capacity()
}

fn capacity_for_minimum(minimum_capacity: usize, granularity: usize) -> io::Result<usize> {
    if minimum_capacity == 0 {
        return Err(error::invalid_input(
            "minimum capacity must be greater than zero",
        ));
    }
    if granularity == 0 || !granularity.is_power_of_two() {
        return Err(error::platform_invariant(
            "mapping granularity must be a nonzero power of two",
        ));
    }

    let capacity = minimum_capacity
        .max(granularity)
        .checked_next_power_of_two()
        .ok_or_else(|| error::invalid_input("minimum capacity is too large"))?;
    if capacity as u128 > 1_u128 << 63 {
        return Err(error::invalid_input("capacity exceeds 2^63"));
    }
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
    use super::capacity_for_minimum;

    #[test]
    fn capacity_rounding_uses_the_next_valid_power_of_two() {
        assert!(capacity_for_minimum(0, 4096).is_err());
        assert_eq!(capacity_for_minimum(1, 4096).unwrap(), 4096);
        assert_eq!(capacity_for_minimum(4096, 4096).unwrap(), 4096);
        assert_eq!(capacity_for_minimum(4097, 4096).unwrap(), 8192);
        assert!(capacity_for_minimum(1, 6144).is_err());
    }
}
