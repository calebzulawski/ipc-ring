use crate::error;
use std::io;

pub(super) fn valid_len(len: usize, capacity: usize) -> io::Result<()> {
    if len == 0 || len > capacity {
        Err(error::invalid_length())
    } else {
        Ok(())
    }
}

pub(super) fn used(write: u64, read: u64, capacity: usize) -> io::Result<usize> {
    let len = write.wrapping_sub(read);
    if len > capacity as u64 {
        Err(error::corrupt_state())
    } else {
        Ok(len as usize)
    }
}
