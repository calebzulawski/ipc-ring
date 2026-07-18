use std::sync::atomic::AtomicU64;

pub(crate) const ABI_VERSION: u64 = 1;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) const IDLE: u64 = 0;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) const WAITING: u64 = 1;
pub(crate) const CONSUMER_FREE: u64 = 0;
pub(crate) const CONSUMER_CLAIMED: u64 = 1;

#[repr(C)]
pub(crate) struct Header {
    pub(crate) version: AtomicU64,
    pub(crate) capacity: u64,
    pub(crate) write_position: AtomicU64,
    pub(crate) read_position: AtomicU64,
    pub(crate) consumer_claim: AtomicU64,
    #[cfg(target_os = "linux")]
    pub(crate) data_wait_state: std::sync::atomic::AtomicU32,
    #[cfg(target_os = "linux")]
    pub(crate) space_wait_state: std::sync::atomic::AtomicU32,
    #[cfg(target_os = "macos")]
    pub(crate) data_wait_state: AtomicU64,
    #[cfg(target_os = "macos")]
    pub(crate) space_wait_state: AtomicU64,
}

impl Header {
    pub(crate) fn new(capacity: usize, consumer_claim: u64) -> Self {
        Self {
            version: AtomicU64::new(0),
            capacity: capacity as u64,
            write_position: AtomicU64::new(0),
            read_position: AtomicU64::new(0),
            consumer_claim: AtomicU64::new(consumer_claim),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            data_wait_state: Default::default(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            space_wait_state: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{offset_of, size_of};

    #[test]
    fn offsets_and_values_match_v1() {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert_eq!(IDLE, 0);
            assert_eq!(WAITING, 1);
        }
        assert_eq!(CONSUMER_FREE, 0);
        assert_eq!(CONSUMER_CLAIMED, 1);
        assert_eq!(offset_of!(Header, version), 0);
        assert_eq!(offset_of!(Header, capacity), 8);
        assert_eq!(offset_of!(Header, write_position), 16);
        assert_eq!(offset_of!(Header, read_position), 24);
        assert_eq!(offset_of!(Header, consumer_claim), 32);
        #[cfg(target_os = "linux")]
        {
            assert_eq!(offset_of!(Header, data_wait_state), 40);
            assert_eq!(offset_of!(Header, space_wait_state), 44);
            assert_eq!(size_of::<Header>(), 48);
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(offset_of!(Header, data_wait_state), 40);
            assert_eq!(offset_of!(Header, space_wait_state), 48);
            assert_eq!(size_of::<Header>(), 56);
        }
        #[cfg(target_os = "windows")]
        assert_eq!(size_of::<Header>(), 40);
    }
}
