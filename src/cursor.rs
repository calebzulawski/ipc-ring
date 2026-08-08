//! Unsafe indexed byte cursors over stable storage.

use std::io;

/// An unchecked descriptor produced by a successful cursor reservation.
///
/// Constructing, copying, or moving this value does not reserve storage,
/// extend its validity, or access the referenced memory. A reservation
/// returned by [`Cursor`] may only be dereferenced while that cursor's safety
/// contract remains satisfied.
#[derive(Clone, Copy, Debug)]
pub struct Reservation {
    /// The wrapping logical position of the first byte.
    pub position: u64,
    /// A pointer to the first byte.
    pub ptr: *const u8,
    /// The number of contiguous bytes beginning at `ptr`.
    pub len: usize,
}

// SAFETY: `Reservation` is inert metadata and provides no operation that accesses
// the pointed-to memory. Callers must separately uphold the unsafe rules for
// dereferencing `ptr` after moving or sharing the descriptor.
unsafe impl Send for Reservation {}
// SAFETY: Sharing a `Reservation` only shares inert pointer metadata. It does not
// grant shared access to the pointed-to memory.
unsafe impl Sync for Reservation {}

/// Provides forward-moving indexed access to stable byte storage.
///
/// Unlike [`crate::view::View`], reservations are not retained by the cursor,
/// and later reservations do not invalidate earlier reservations. Positions use
/// wrapping `u64` arithmetic and may be at most one cursor capacity ahead of
/// [`position`](Self::position).
///
/// # Safety
///
/// Implementations must return non-null pointers that are valid to read for
/// the reported length and remain address-valid while the cursor is alive.
/// A returned reservation must remain eligible for access across later reservations
/// and across advancement up to its starting position. Once
/// [`advance_to`](Self::advance_to) moves beyond that starting position, the
/// reservation must be treated as invalid because its bytes may be reused or
/// accessed through another cursor.
///
/// Implementations must also enforce the documented capacity, availability,
/// cursor-ordering, and memory-ordering rules. Safe views rely on these
/// guarantees when constructing slices from returned reservations.
#[allow(async_fn_in_trait)]
pub unsafe trait Cursor {
    /// Returns the buffer capacity in bytes.
    fn capacity(&self) -> usize;

    /// Returns the current published or released cursor position.
    fn position(&self) -> u64;

    /// Immediately reserves at least `minimum` bytes beginning at `position`.
    ///
    /// A successful reservation returns all bytes currently available from
    /// `position`, not only the requested minimum. `WouldBlock` is returned
    /// when the requested end position is valid but not yet available.
    fn try_reserve_at(&mut self, position: u64, minimum: usize) -> io::Result<Reservation>;

    /// Waits until at least `minimum` bytes can be reserved at `position`.
    async fn reserve_at(&mut self, position: u64, minimum: usize) -> io::Result<Reservation>;

    /// Commits or retires every byte before `position`.
    ///
    /// The implementation still validates that the target is forward and
    /// within the currently available extent.
    ///
    /// # Safety
    ///
    /// The caller must ensure that bytes being committed for reading have been
    /// initialized and that moving the cursor does not invalidate any raw
    /// access that will subsequently be used. Ring producers use advancement
    /// to publish bytes, while ring consumers use it to release bytes.
    unsafe fn advance_to(&mut self, position: u64) -> io::Result<()>;
}

/// Marks a cursor that can create an independent cursor at its current position.
///
/// A fork begins at the source cursor's current logical position. Advancing or
/// dropping either cursor must not advance, invalidate, or otherwise change the
/// other cursor. Both cursors must continue to observe the same future input,
/// subject to the underlying source's documented retention and backpressure
/// behavior.
///
/// Forking is fallible because it may require an operating-system resource, a
/// reader slot, or other bounded storage. Types that cannot provide independent
/// cursors do not implement this trait; runtime failures are returned as errors.
///
/// # Safety
///
/// Implementations must ensure that each successful fork independently upholds
/// the full [`Cursor`] reservation-lifetime contract. In particular, advancing
/// one fork must not permit storage to be changed or reused while a reservation
/// belonging to another fork remains eligible for access.
pub unsafe trait TryFork: Cursor + Sized {
    /// Creates an independent cursor beginning at this cursor's current position.
    fn try_fork(&self) -> io::Result<Self>;
}

/// Marks a cursor whose reservations may be changed in place.
///
/// # Safety
///
/// Every reservation returned by [`Cursor`] must be valid for unique mutable access
/// while the cursor is exclusively borrowed and the reservation remains eligible
/// for access. No reader, writer, or independently shared view may access
/// the same bytes during that mutable access.
pub unsafe trait CursorMut: Cursor {}

#[cfg(test)]
mod tests {
    use super::{Cursor, CursorMut, Reservation, TryFork};
    use crate::ring::{ipc, local};

    fn assert_copy_send_sync<T: Copy + Send + Sync>() {}
    fn assert_cursor<T: Cursor>() {}
    fn assert_cursor_mut<T: CursorMut>() {}
    fn assert_try_fork<T: TryFork>() {}

    #[test]
    fn reservation_is_inert_copyable_thread_safe_metadata() {
        assert_copy_send_sync::<Reservation>();
        let byte = 0_u8;
        let reservation = Reservation {
            position: 7,
            ptr: &byte,
            len: 1,
        };
        let copied = reservation;
        assert_eq!(copied.position, 7);
        assert_eq!(copied.ptr, &byte);
        assert_eq!(copied.len, 1);
    }

    #[test]
    fn cursor_capabilities_match_their_ring_roles() {
        assert_cursor::<local::Consumer>();
        assert_cursor::<ipc::Consumer>();
        assert_try_fork::<local::Consumer>();
        assert_cursor_mut::<local::Producer>();
        assert_cursor_mut::<ipc::Producer>();
    }
}
