//! Safe stateful access to indexed byte cursors.

use crate::raw::{Cursor, CursorMut, Reservation};
use std::io;
use std::slice;

/// Retains one safe reservation over an indexed byte cursor.
///
/// Each operation follows the same cycle:
///
/// 1. Call [`try_reserve`](Self::try_reserve) or [`reserve`](Self::reserve) with
///    the minimum number of bytes needed.
/// 2. Access the reserved bytes through [`view`](Self::view), or
///    [`view_mut`](Self::view_mut) when `C` implements [`CursorMut`].
/// 3. Call [`advance`](Self::advance) with the number of bytes that were read or
///    written.
///
/// A successful reservation may provide more bytes than requested. The view
/// does not change until it is advanced or a new reservation begins. Advancing
/// a producer makes written bytes available to consumers; advancing a consumer
/// makes read bytes available for writing again.
///
/// ```
/// fn send_one_message() -> std::io::Result<()> {
///     let (mut producer, mut consumer) = ipc_ring::local::create(1)?;
///
///     producer.try_reserve(4)?;
///     producer.view_mut()[..4].copy_from_slice(b"ping");
///     producer.advance(4)?;
///
///     consumer.try_reserve(1)?;
///     assert_eq!(&consumer.view()[..4], b"ping");
///     consumer.advance(4)?;
///     Ok(())
/// }
/// ```
pub struct View<C: Cursor> {
    cursor: C,
    pending: Option<Reservation>,
}

impl<C: Cursor> View<C> {
    pub(crate) fn from_cursor(cursor: C) -> Self {
        Self {
            cursor,
            pending: None,
        }
    }

    pub(crate) fn cursor(&self) -> &C {
        &self.cursor
    }

    pub(crate) fn cursor_mut(&mut self) -> &mut C {
        &mut self.cursor
    }

    /// Returns the buffer capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.cursor.capacity()
    }

    /// Immediately reserves at least `minimum` bytes or returns `WouldBlock`.
    ///
    /// Calling this method clears any previous reservation, including when the
    /// new request fails. Use a minimum of zero to reserve however many bytes
    /// are currently available without waiting.
    pub fn try_reserve(&mut self, minimum: usize) -> io::Result<()> {
        self.pending = None;
        let position = self.cursor.position();
        self.pending = Some(self.cursor_mut().try_reserve_at(position, minimum)?);
        Ok(())
    }

    /// Waits until at least `minimum` bytes can be reserved.
    ///
    /// The previous reservation is cleared when the returned future is first
    /// polled. Merely creating and dropping the future leaves the previous
    /// reservation unchanged.
    pub async fn reserve(&mut self, minimum: usize) -> io::Result<()> {
        self.pending = None;
        let position = self.cursor.position();
        self.pending = Some(self.cursor_mut().reserve_at(position, minimum).await?);
        Ok(())
    }

    /// Returns the reserved bytes.
    ///
    /// The slice is empty when there is no reservation or no bytes were
    /// available for a zero-minimum reservation.
    pub fn view(&self) -> &[u8] {
        let Some(reservation) = self.pending else {
            return &[];
        };
        // SAFETY: `Cursor` guarantees that a returned reservation is valid
        // until advancement passes its position. This type does not expose its
        // cursor and clears `pending` before advancing.
        unsafe { slice::from_raw_parts(reservation.ptr, reservation.len) }
    }

    /// Marks the first `amount` reserved bytes as complete.
    ///
    /// An amount larger than `view().len()`, or a positive amount without a
    /// reservation, returns `InvalidInput` and does not advance the ring.
    /// Advancing zero bytes always succeeds. Every call clears the current
    /// reservation.
    pub fn advance(&mut self, amount: usize) -> io::Result<()> {
        let pending = self.pending.take();
        if amount == 0 {
            return Ok(());
        }
        let Some(reservation) = pending.filter(|reservation| amount <= reservation.len) else {
            return Err(crate::error::invalid_length());
        };
        let position = reservation.position.wrapping_add(amount as u64);
        // SAFETY: `amount` lies within a readable reservation returned by the
        // cursor, and no slice can remain borrowed across this mutable call.
        // Consequently, the range is initialized and no safe access retained
        // by this view is invalidated.
        unsafe { self.cursor_mut().advance_to(position) }
    }
}

impl<C: CursorMut> View<C> {
    /// Returns the reserved bytes for in-place modification.
    ///
    /// Consumer views do not provide mutable access:
    ///
    /// ```compile_fail
    /// let (_, mut consumer) = ipc_ring::local::create(1).unwrap();
    /// consumer.view_mut();
    /// ```
    pub fn view_mut(&mut self) -> &mut [u8] {
        let Some(reservation) = self.pending else {
            return &mut [];
        };
        // SAFETY: `CursorMut` guarantees that reservations are uniquely
        // writable while the cursor and this safe view are exclusively borrowed.
        unsafe { slice::from_raw_parts_mut(reservation.ptr.cast_mut(), reservation.len) }
    }
}
