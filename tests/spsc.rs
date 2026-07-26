use ipc_ring::spsc::anonymous;
use std::io::ErrorKind;
use std::time::Duration;

#[test]
fn creation_rounds_up_minimum_capacity() {
    let (producer, consumer) = anonymous(1).unwrap();
    let actual = producer.capacity();
    assert!(actual >= 1);
    assert!(actual.is_power_of_two());
    assert_eq!(consumer.capacity(), actual);
    assert_eq!(anonymous(0).err().unwrap().kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn alias_boundary_and_partial_grants() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    let capacity = producer.capacity();
    producer
        .reserve(capacity - 4)
        .await
        .unwrap()
        .commit(capacity - 4)
        .await
        .unwrap();
    consumer
        .inspect(capacity - 4)
        .await
        .unwrap()
        .release(capacity - 4)
        .await
        .unwrap();

    let mut grant = producer.reserve(8).await.unwrap();
    grant.as_mut_slice().copy_from_slice(b"abcdefgh");
    grant.commit(8).await.unwrap();
    let grant = consumer.inspect(8).await.unwrap();
    assert_eq!(grant.as_slice(), b"abcdefgh");
    grant.release(3).await.unwrap();
    assert_eq!(consumer.readable_len().unwrap(), 5);
}

#[tokio::test]
async fn anonymous_wait_wakes() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    let read = async {
        let grant = consumer.inspect(4).await.unwrap();
        assert_eq!(grant.as_slice(), b"wake");
        grant.release(4).await.unwrap();
    };
    let write = async {
        tokio::task::yield_now().await;
        let mut grant = producer.reserve(4).await.unwrap();
        grant.as_mut_slice().copy_from_slice(b"wake");
        grant.commit(4).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(read, write);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn full_producer_wakes_when_space_is_released() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();

    let write = async {
        producer.reserve(1).await.unwrap().commit(1).await.unwrap();
    };
    let read = async {
        tokio::task::yield_now().await;
        consumer.inspect(1).await.unwrap().release(1).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(write, read);
    })
    .await
    .unwrap();
}

#[test]
fn span_validation_errors_are_invalid_input() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    assert_eq!(
        producer.try_reserve(0).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    let invalid = consumer.capacity() + 1;
    assert_eq!(
        consumer.try_inspect(invalid).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
}
