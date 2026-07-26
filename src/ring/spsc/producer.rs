use super::state::valid_len;
use super::{RegisteredRing, SharedRing};
use crate::error;
use std::io;
use std::slice;
use std::sync::Arc;

/// The sole writer for a shared ring.
pub struct Producer {
    pub(super) ring: Arc<SharedRing>,
    /// Keeps a named ring registered for this producer's lifetime.
    _registration: Option<Arc<RegisteredRing>>,
}

impl Producer {
    pub(crate) fn new(ring: Arc<SharedRing>, registration: Option<Arc<RegisteredRing>>) -> Self {
        Self {
            ring,
            _registration: registration,
        }
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    pub fn writable_len(&self) -> io::Result<usize> {
        self.ring.writable_len()
    }

    pub fn try_reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        if self.writable_len()? < len {
            return Err(error::would_block());
        }
        Ok(self.grant(len))
    }

    /// Waits asynchronously until `len` contiguous aliased bytes can be granted.
    pub async fn reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        self.ring.wait_for_space(len).await?;
        Ok(self.grant(len))
    }

    fn grant(&mut self, len: usize) -> WriteGrant<'_> {
        let position = self.ring.write_position();
        let offset = (position & (self.ring.capacity() as u64 - 1)) as usize;
        WriteGrant {
            producer: self,
            position,
            offset,
            len,
        }
    }

    async fn commit(&mut self, position: u64, amount: usize) -> io::Result<()> {
        self.ring.publish_data(position, amount).await
    }
}

/// A writable span whose cursor advances only when `commit` succeeds.
pub struct WriteGrant<'a> {
    producer: &'a mut Producer,
    position: u64,
    offset: usize,
    len: usize,
}

impl WriteGrant<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the unique producer borrow owns a free, double-mapped span.
        unsafe {
            slice::from_raw_parts_mut(self.producer.ring.payload().add(self.offset), self.len)
        }
    }

    /// Cursor publication precedes notification; if notification fails, the
    /// committed bytes are nevertheless visible.
    pub async fn commit(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.producer.commit(self.position, amount).await
    }
}
