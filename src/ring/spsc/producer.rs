use super::CONSUMER_FREE;
use super::state::{used, valid_len};
use crate::error;
use crate::platform::MappedRing;
use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct Producer {
    pub(super) ring: Arc<MappedRing>,
    _not_sync: PhantomData<Cell<()>>,
}

impl Producer {
    pub(super) fn new(ring: Arc<MappedRing>) -> Self {
        Self {
            ring,
            _not_sync: PhantomData,
        }
    }

    /// Creates a named ring with at least the requested payload capacity and returns
    /// its sole producer endpoint.
    pub fn bind(name: &str, minimum_capacity: usize) -> io::Result<Self> {
        Ok(Self::new(Arc::new(MappedRing::bind(
            name,
            minimum_capacity,
            CONSUMER_FREE,
        )?)))
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    pub fn writable_len(&self) -> io::Result<usize> {
        let header = self.ring.header();
        let write = header.write_position.load(Ordering::Relaxed);
        let read = header.read_position.load(Ordering::Acquire);
        Ok(self.ring.capacity() - used(write, read, self.ring.capacity())?)
    }

    pub fn try_reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        if self.writable_len()? < len {
            return Err(error::would_block());
        }
        Ok(self.grant(len))
    }

    pub fn reserve(&mut self, len: usize) -> io::Result<WriteGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        loop {
            if self.writable_len()? >= len {
                return Ok(self.grant(len));
            }

            let notification = self.ring.space_notification();
            let registration = notification.register();
            if self.writable_len()? >= len {
                continue;
            }
            registration.wait()?;
        }
    }

    fn grant(&mut self, len: usize) -> WriteGrant<'_> {
        let position = self.ring.header().write_position.load(Ordering::Relaxed);
        let offset = (position & (self.ring.capacity() as u64 - 1)) as usize;
        WriteGrant {
            producer: self,
            position,
            offset,
            len,
        }
    }

    fn commit(&mut self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        self.ring
            .header()
            .write_position
            .store(position.wrapping_add(amount as u64), Ordering::Release);
        self.ring.data_notification().notify()?;
        Ok(())
    }
}

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
    pub fn commit(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.producer.commit(self.position, amount)
    }
}

impl Drop for WriteGrant<'_> {
    fn drop(&mut self) {
        // Abandoning a grant intentionally leaves the write cursor unchanged.
    }
}
