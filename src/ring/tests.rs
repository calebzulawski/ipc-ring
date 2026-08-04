use crate::View;
use crate::local;
use crate::mapping;
use std::io::ErrorKind;
use std::sync::atomic::Ordering;

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
