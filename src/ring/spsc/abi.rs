use std::sync::atomic::AtomicU64;

pub(crate) const ABI_VERSION: u64 = 1;
pub(crate) const IDLE: u32 = 0;
pub(crate) const WAITING: u32 = 1;

#[repr(C)]
pub(crate) struct Header {
    pub(crate) version: AtomicU64,
    pub(crate) capacity: u64,
    pub(crate) write_position: AtomicU64,
    pub(crate) read_position: AtomicU64,
    pub(crate) data_wait_state: std::sync::atomic::AtomicU32,
    pub(crate) space_wait_state: std::sync::atomic::AtomicU32,
}

impl Header {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            version: AtomicU64::new(0),
            capacity: capacity as u64,
            write_position: AtomicU64::new(0),
            read_position: AtomicU64::new(0),
            data_wait_state: Default::default(),
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
        assert_eq!(IDLE, 0);
        assert_eq!(WAITING, 1);
        assert_eq!(offset_of!(Header, version), 0);
        assert_eq!(offset_of!(Header, capacity), 8);
        assert_eq!(offset_of!(Header, write_position), 16);
        assert_eq!(offset_of!(Header, read_position), 24);
        assert_eq!(offset_of!(Header, data_wait_state), 32);
        assert_eq!(offset_of!(Header, space_wait_state), 36);
        assert_eq!(size_of::<Header>(), 40);
    }
}
