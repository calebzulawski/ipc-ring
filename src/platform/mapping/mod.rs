#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use unix as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

use crate::error;
use crate::platform::connection::{Connection, InitializationDeadline};
use crate::platform::notification::Notification;
use crate::ring::spsc::Header;
use std::io;
use std::sync::atomic::Ordering;

pub(crate) struct MappedRing {
    connection: Connection,
    memory: implementation::MappedMemory,
}

impl MappedRing {
    pub(crate) fn create_anonymous(
        minimum_capacity: usize,
        consumer_claim: u64,
    ) -> io::Result<Self> {
        let (connection, memory) =
            implementation::create_anonymous(minimum_capacity, consumer_claim)?;
        Ok(Self { connection, memory })
    }

    pub(crate) fn bind(
        name: &str,
        minimum_capacity: usize,
        consumer_claim: u64,
    ) -> io::Result<Self> {
        let (connection, memory) = implementation::bind(name, minimum_capacity, consumer_claim)?;
        Ok(Self { connection, memory })
    }

    pub(crate) fn connect(name: &str) -> io::Result<Self> {
        let (connection, memory) = implementation::connect(name)?;
        Ok(Self { connection, memory })
    }

    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
    }

    pub(crate) fn payload(&self) -> *mut u8 {
        self.memory.payload()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    pub(crate) fn data_notification(&self) -> Notification<'_> {
        self.connection.notifications(self.header()).data()
    }

    pub(crate) fn space_notification(&self) -> Notification<'_> {
        self.connection.notifications(self.header()).space()
    }

    #[cfg(test)]
    pub(crate) fn minimum_capacity() -> usize {
        implementation::minimum_capacity()
    }

    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn truncate_shared_memory_for_test(&self, len: usize) -> std::io::Result<()> {
        self.connection.truncate_shared_memory_for_test(len)
    }

    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn reinitialize_header_for_test(&self, consumer_claim: u64) {
        self.memory.reinitialize_header_for_test(consumer_claim);
    }
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

fn wait_for_version(header: &Header, deadline: InitializationDeadline) -> io::Result<u64> {
    loop {
        let version = header.version.load(Ordering::Acquire);
        if version != 0 {
            return Ok(version);
        }
        deadline.wait()?;
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
