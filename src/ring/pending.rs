//! Pending span metadata retained by an endpoint between operations.

/// One validated span retained by an endpoint until its next operation.
#[derive(Clone, Copy)]
pub(crate) struct PendingView {
    pub(crate) position: u64,
    pub(crate) offset: usize,
    pub(crate) len: usize,
}

impl PendingView {
    pub(crate) fn new(position: u64, len: usize, capacity: usize) -> Self {
        let offset = (position & (capacity as u64 - 1)) as usize;
        Self {
            position,
            offset,
            len,
        }
    }
}
