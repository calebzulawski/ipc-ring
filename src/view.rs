//! Byte views shared by ring producers and consumers.

use std::io;

/// Reserves and advances bytes on one side of a ring.
///
/// Each operation follows the same cycle:
///
/// 1. Call [`try_reserve`](Self::try_reserve) or [`reserve`](Self::reserve) with
///    the minimum number of bytes needed.
/// 2. Access the reserved bytes through [`view`](Self::view), or
///    [`ViewMut::view_mut`] when writing.
/// 3. Call [`advance`](Self::advance) with the number of bytes that were read or
///    written.
///
/// A successful reservation may provide more bytes than requested. The view
/// does not change until it is advanced or a new reservation begins. Advancing
/// a producer makes written bytes available to consumers; advancing a consumer
/// makes read bytes available for writing again.
///
/// ```
/// use ipc_ring::{View, ViewMut};
///
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
#[allow(async_fn_in_trait)]
pub trait View {
    /// Returns the buffer capacity in bytes.
    fn capacity(&self) -> usize;

    /// Immediately reserves at least `minimum` bytes or returns `WouldBlock`.
    ///
    /// Calling this method clears any previous reservation, including when the
    /// new request fails. Use a minimum of zero to reserve however many bytes
    /// are currently available without waiting.
    fn try_reserve(&mut self, minimum: usize) -> io::Result<()>;

    /// Waits until at least `minimum` bytes can be reserved.
    ///
    /// The previous reservation is cleared when the returned future is first
    /// polled. Merely creating and dropping the future leaves the previous
    /// reservation unchanged.
    async fn reserve(&mut self, minimum: usize) -> io::Result<()>;

    /// Returns the reserved bytes.
    ///
    /// The slice is empty when there is no reservation or no bytes were
    /// available for a zero-minimum reservation.
    fn view(&self) -> &[u8];

    /// Marks the first `amount` reserved bytes as complete.
    ///
    /// An amount larger than `view().len()`, or a positive amount without a
    /// reservation, returns `InvalidInput` and does not advance the ring.
    /// Advancing zero bytes always succeeds. Every call clears the current
    /// reservation.
    fn advance(&mut self, amount: usize) -> io::Result<()>;
}

/// A view that allows changing reserved bytes before advancing.
pub trait ViewMut: View {
    /// Returns the reserved bytes for writing.
    fn view_mut(&mut self) -> &mut [u8];
}
