use ipc_ring::ring::anonymous;
use std::io::ErrorKind;
use std::time::Duration;

#[tokio::test]
async fn zero_length_grants_are_empty_noops() {
    let (mut producer, mut consumer) = anonymous(0).unwrap();
    let capacity = producer.capacity();

    let write = producer.try_reserve(0).unwrap();
    assert!(write.is_empty());
    write.commit(0).unwrap();
    let write = producer.reserve(0).await.unwrap();
    assert!(write.is_empty());
    write.commit(0).unwrap();

    let read = consumer.try_inspect(0).unwrap();
    assert!(read.is_empty());
    read.release(0).unwrap();
    let read = consumer.inspect(0).await.unwrap();
    assert!(read.is_empty());
    read.release(0).unwrap();

    assert_eq!(producer.writable_len().unwrap(), capacity);
    assert_eq!(consumer.readable_len().unwrap(), 0);
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
        .unwrap();
    consumer
        .inspect(capacity - 4)
        .await
        .unwrap()
        .release(capacity - 4)
        .unwrap();

    let mut grant = producer.reserve(8).await.unwrap();
    grant.as_mut_slice().copy_from_slice(b"abcdefgh");
    grant.commit(6).unwrap();
    let grant = consumer.inspect(6).await.unwrap();
    assert_eq!(grant.as_slice(), b"abcdef");
    grant.release(3).unwrap();
    assert_eq!(consumer.readable_len().unwrap(), 3);
}

#[tokio::test]
async fn anonymous_wait_wakes() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    let read = async {
        let grant = consumer.inspect(4).await.unwrap();
        assert_eq!(grant.as_slice(), b"wake");
        grant.release(4).unwrap();
    };
    let write = async {
        tokio::task::yield_now().await;
        let mut grant = producer.reserve(4).await.unwrap();
        grant.as_mut_slice().copy_from_slice(b"wake");
        grant.commit(4).unwrap();
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
        .unwrap();

    let write = async {
        producer.reserve(1).await.unwrap().commit(1).unwrap();
    };
    let read = async {
        tokio::task::yield_now().await;
        consumer.inspect(1).await.unwrap().release(1).unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(write, read);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_anonymous_consumer_wakes_the_producer() {
    let (mut producer, consumer) = anonymous(1).unwrap();
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .unwrap();

    let wait = producer.reserve(1);
    tokio::pin!(wait);
    tokio::select! {
        biased;
        _ = &mut wait => panic!("full producer unexpectedly completed"),
        _ = tokio::task::yield_now() => {}
    }

    drop(consumer);
    let error = match tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap()
    {
        Ok(_) => panic!("producer reserved space after its consumer was dropped"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn dropping_anonymous_producer_wakes_the_consumer() {
    let (producer, mut consumer) = anonymous(1).unwrap();
    let wait = consumer.inspect(1);
    tokio::pin!(wait);
    tokio::select! {
        biased;
        _ = &mut wait => panic!("empty consumer unexpectedly completed"),
        _ = tokio::task::yield_now() => {}
    }

    drop(producer);
    let error = match tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap()
    {
        Ok(_) => panic!("consumer inspected data after its producer was dropped"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn span_validation_errors_are_invalid_input() {
    let (mut producer, mut consumer) = anonymous(1).unwrap();
    let invalid = producer.capacity() + 1;
    assert_eq!(
        producer.try_reserve(invalid).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        producer.reserve(invalid).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        consumer.try_inspect(invalid).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        consumer.inspect(invalid).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );

    assert_eq!(
        producer
            .try_reserve(1)
            .unwrap()
            .commit(2)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    producer.try_reserve(1).unwrap().commit(1).unwrap();
    assert_eq!(
        consumer
            .try_inspect(1)
            .unwrap()
            .release(2)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(consumer.readable_len().unwrap(), 1);
    consumer.try_inspect(1).unwrap().release(1).unwrap();
}
