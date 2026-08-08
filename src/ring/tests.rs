use crate::cursor::Cursor;
use crate::ring::local;
use crate::ring::mapping;
use std::io::ErrorKind;
use std::sync::atomic::Ordering;

#[test]
fn indexed_consumer_reservations_are_independent() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();
    producer.try_reserve(6).unwrap();
    producer.view_mut()[..6].copy_from_slice(b"abcdef");
    producer.advance(6).unwrap();

    let cursor = consumer.cursor_mut();
    let start = cursor.position();
    let first = cursor.try_reserve_at(start, 1).unwrap();
    let later = cursor.try_reserve_at(start.wrapping_add(2), 2).unwrap();
    assert_eq!(first.len, 6);
    assert_eq!(later.len, 4);
    // SAFETY: neither reservation has been passed by the consumer cursor.
    unsafe {
        assert_eq!(std::slice::from_raw_parts(first.ptr, first.len), b"abcdef");
        assert_eq!(std::slice::from_raw_parts(later.ptr, later.len), b"cdef");
    }

    // Re-reservation and a no-op advancement do not disturb either pointer.
    assert_eq!(cursor.try_reserve_at(start, 6).unwrap().ptr, first.ptr);
    // SAFETY: advancing to the current position invalidates no reservation.
    unsafe { cursor.advance_to(start).unwrap() };
    // SAFETY: the cursor still has not passed `later.position`.
    unsafe {
        assert_eq!(std::slice::from_raw_parts(later.ptr, later.len), b"cdef");
    }

    // Advancing to a reservation's start releases only the preceding bytes.
    // SAFETY: `first` is not used again, and advancement stops at `later`.
    unsafe { cursor.advance_to(later.position).unwrap() };
    assert_eq!(cursor.position(), later.position);
    // SAFETY: advancement reached, but did not pass, `later.position`.
    unsafe {
        assert_eq!(std::slice::from_raw_parts(later.ptr, later.len), b"cdef");
    }

    assert_eq!(
        cursor.try_reserve_at(start, 0).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        // SAFETY: the invalid target is rejected without changing the cursor.
        unsafe { cursor.advance_to(start.wrapping_add(7)) }
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );

    let end = start.wrapping_add(6);
    let empty = cursor.try_reserve_at(end, 0).unwrap();
    assert_eq!(empty.position, end);
    assert_eq!(empty.len, 0);
    assert_eq!(
        cursor
            .try_reserve_at(end.wrapping_add(1), 0)
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
}

#[test]
fn indexed_producer_reservations_can_be_published_together() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();
    let cursor = producer.cursor_mut();
    let start = cursor.position();
    let prefix = cursor.try_reserve_at(start, 2).unwrap();
    let payload = cursor.try_reserve_at(start.wrapping_add(2), 3).unwrap();

    // SAFETY: these reservations belong to the unique producer, are disjoint
    // for the written lengths, and have not yet been published.
    unsafe {
        std::ptr::copy_nonoverlapping(b"--".as_ptr(), prefix.ptr.cast_mut(), 2);
        std::ptr::copy_nonoverlapping(b"raw".as_ptr(), payload.ptr.cast_mut(), 3);
        cursor.advance_to(start.wrapping_add(5)).unwrap();
    }

    consumer.try_reserve(5).unwrap();
    assert_eq!(&consumer.view()[..5], b"--raw");
    consumer.advance(5).unwrap();
}

#[tokio::test]
async fn indexed_cursor_wait_includes_the_distance_to_its_start() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();
    let start = consumer.cursor().position().wrapping_add(2);

    let read = async {
        let reservation = consumer.cursor_mut().reserve_at(start, 2).await.unwrap();
        assert_eq!(reservation.position, start);
        // SAFETY: the consumer cursor has not advanced, and the producer has
        // published every byte in this reservation.
        unsafe { assert_eq!(std::slice::from_raw_parts(reservation.ptr, 2), b"it") };
    };
    let write = async {
        tokio::task::yield_now().await;
        producer.try_reserve(4).unwrap();
        producer.view_mut()[..4].copy_from_slice(b"wait");
        producer.advance(4).unwrap();
    };
    tokio::join!(read, write);
}

#[test]
fn indexed_cursor_positions_validate_capacity_and_wraparound() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();
    let capacity = producer.capacity();
    let near_wrap = u64::MAX - 2;
    producer.set_positions_for_test(near_wrap);

    let cursor = producer.cursor_mut();
    let later = near_wrap.wrapping_add(2);
    let reservation = cursor.try_reserve_at(later, 1).unwrap();
    assert_eq!(reservation.position, later);
    assert_eq!(reservation.len, capacity - 2);
    assert_eq!(
        cursor
            .try_reserve_at(near_wrap.wrapping_add(capacity as u64), 1)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        cursor
            .try_reserve_at(near_wrap.wrapping_sub(1), 0)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        // SAFETY: the invalid target is rejected without publishing data.
        unsafe { cursor.advance_to(near_wrap.wrapping_add(capacity as u64 + 1)) }
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );

    // Keep the consumer alive through all producer checks.
    assert_eq!(consumer.cursor_mut().position(), near_wrap);
}

#[tokio::test]
async fn wrapping_cursors_and_nonblocking_availability() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();
    let capacity = producer.capacity();
    let near_wrap = u64::MAX - 3;
    producer.set_positions_for_test(near_wrap);
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    assert_eq!(
        producer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
    consumer.reserve(capacity).await.unwrap();
    consumer.advance(capacity).unwrap();
    assert_eq!(
        consumer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn cancelling_waits_clears_their_bitmap_bits() {
    let (mut producer, mut consumer) = local::create(mapping::minimum_capacity()).unwrap();

    let mut data_wait = Box::pin(consumer.reserve(1));
    tokio::select! {
        biased;
        _ = &mut data_wait => panic!("empty ring unexpectedly had data"),
        _ = tokio::task::yield_now() => {}
    }
    drop(data_wait);
    assert_eq!(consumer.header().data_waiters.load(Ordering::Acquire), 0);

    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    let mut space_wait = Box::pin(producer.reserve(1));
    tokio::select! {
        biased;
        _ = &mut space_wait => panic!("full ring unexpectedly had space"),
        _ = tokio::task::yield_now() => {}
    }
    drop(space_wait);
    assert_eq!(producer.header().space_waiters.load(Ordering::Acquire), 0);
}
