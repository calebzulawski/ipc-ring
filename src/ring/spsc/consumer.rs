use super::state::{used, valid_len};
use super::{CONSUMER_CLAIMED, CONSUMER_FREE};
use crate::error;
use crate::platform::MappedRing;
use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct Consumer {
    pub(super) ring: Arc<MappedRing>,
    _not_sync: PhantomData<Cell<()>>,
}

impl Consumer {
    pub(super) fn new(ring: Arc<MappedRing>) -> Self {
        Self {
            ring,
            _not_sync: PhantomData,
        }
    }

    /// Opens a named ring and atomically claims its sole consumer endpoint.
    pub fn connect(name: &str) -> io::Result<Self> {
        let ring = Arc::new(MappedRing::connect(name)?);
        match ring.header().consumer_claim.compare_exchange(
            CONSUMER_FREE,
            CONSUMER_CLAIMED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(Self::new(ring)),
            Err(CONSUMER_CLAIMED) => Err(error::consumer_already_connected()),
            Err(_) => Err(error::corrupt_state()),
        }
    }

    /// Returns the actual payload capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    pub fn readable_len(&self) -> io::Result<usize> {
        let header = self.ring.header();
        let read = header.read_position.load(Ordering::Relaxed);
        let write = header.write_position.load(Ordering::Acquire);
        used(write, read, self.ring.capacity())
    }

    pub fn try_inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        if self.readable_len()? < len {
            return Err(error::would_block());
        }
        Ok(self.grant(len))
    }

    pub fn inspect(&mut self, len: usize) -> io::Result<ReadGrant<'_>> {
        valid_len(len, self.ring.capacity())?;
        loop {
            if self.readable_len()? >= len {
                return Ok(self.grant(len));
            }

            let notification = self.ring.data_notification();
            let registration = notification.register();
            if self.readable_len()? >= len {
                continue;
            }
            registration.wait()?;
        }
    }

    fn grant(&mut self, len: usize) -> ReadGrant<'_> {
        let position = self.ring.header().read_position.load(Ordering::Relaxed);
        let offset = (position & (self.ring.capacity() as u64 - 1)) as usize;
        ReadGrant {
            consumer: self,
            position,
            offset,
            len,
        }
    }

    fn release(&mut self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        self.ring
            .header()
            .read_position
            .store(position.wrapping_add(amount as u64), Ordering::Release);
        self.ring.space_notification().notify()?;
        Ok(())
    }
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.ring
            .header()
            .consumer_claim
            .store(CONSUMER_FREE, Ordering::Release);
    }
}

pub struct ReadGrant<'a> {
    consumer: &'a mut Consumer,
    position: u64,
    offset: usize,
    len: usize,
}

impl ReadGrant<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the unique consumer borrow owns a published double-mapped span.
        unsafe { slice::from_raw_parts(self.consumer.ring.payload().add(self.offset), self.len) }
    }

    /// Cursor publication precedes notification; if notification fails, the
    /// released bytes have nevertheless been reclaimed.
    pub fn release(self, amount: usize) -> io::Result<()> {
        if amount > self.len {
            return Err(error::invalid_length());
        }
        self.consumer.release(self.position, amount)
    }
}

impl Drop for ReadGrant<'_> {
    fn drop(&mut self) {
        // Abandoning a grant intentionally leaves the read cursor unchanged.
    }
}
