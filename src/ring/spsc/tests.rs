use super::anonymous;
use crate::mapping;
use std::io::ErrorKind;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn wrapping_cursors_and_nonblocking_availability() {
    let (mut producer, mut consumer) = anonymous(mapping::minimum_capacity()).unwrap();
    let capacity = producer.capacity();
    let near_wrap = u64::MAX - 3;
    producer
        .ring
        .header()
        .write_position
        .store(near_wrap, Ordering::Relaxed);
    producer
        .ring
        .header()
        .read_position
        .store(near_wrap, Ordering::Relaxed);
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();
    assert_eq!(
        producer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
    consumer
        .inspect(capacity)
        .await
        .unwrap()
        .release(capacity)
        .await
        .unwrap();
    assert_eq!(
        consumer.try_inspect(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
}
