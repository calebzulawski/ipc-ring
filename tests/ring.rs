use ipc_ring::cursor::{Cursor, CursorMut};
use ipc_ring::ring::local;
use ipc_ring::view::View;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

async fn copy_reserved<S: Cursor, D: CursorMut>(
    source: &mut View<S>,
    destination: &mut View<D>,
    minimum: usize,
) -> io::Result<()> {
    source.reserve(minimum).await?;
    destination.reserve(minimum).await?;
    let amount = source.view().len().min(destination.view().len());
    destination.view_mut()[..amount].copy_from_slice(&source.view()[..amount]);
    source.advance(amount)?;
    destination.advance(amount)
}

fn snapshot_len<C: Cursor>(view: &mut View<C>) -> usize {
    view.try_reserve(0).unwrap();
    let len = view.view().len();
    view.advance(0).unwrap();
    len
}

#[tokio::test]
async fn zero_minimum_reservations_snapshot_current_availability() {
    let (mut producer, mut consumer) = local::create(0).unwrap();
    let capacity = producer.capacity();

    producer.try_reserve(0).unwrap();
    assert_eq!(producer.view_mut().len(), capacity);
    producer.advance(0).unwrap();
    producer.reserve(0).await.unwrap();
    assert_eq!(producer.view_mut().len(), capacity);
    producer.advance(0).unwrap();

    consumer.try_reserve(0).unwrap();
    assert!(consumer.view().is_empty());
    consumer.advance(0).unwrap();
    consumer.reserve(0).await.unwrap();
    assert!(consumer.view().is_empty());
    consumer.advance(0).unwrap();

    assert_eq!(snapshot_len(&mut producer), capacity);
    assert_eq!(snapshot_len(&mut consumer), 0);
}

#[tokio::test]
async fn alias_boundary_and_partial_views() {
    let (mut producer, mut consumer) = local::create(1).unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity - 4).await.unwrap();
    producer.advance(capacity - 4).unwrap();
    consumer.reserve(capacity - 4).await.unwrap();
    consumer.advance(capacity - 4).unwrap();

    producer.reserve(8).await.unwrap();
    producer.view_mut()[..8].copy_from_slice(b"abcdefgh");
    producer.advance(6).unwrap();
    consumer.reserve(6).await.unwrap();
    assert_eq!(&consumer.view()[..6], b"abcdef");
    consumer.advance(3).unwrap();
    assert!(consumer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 3);
}

#[tokio::test]
async fn generic_views_form_a_processing_chain() {
    let (mut input, mut source) = local::create(1).unwrap();
    let (mut destination, mut output) = local::create(1).unwrap();

    input.reserve(5).await.unwrap();
    input.view_mut()[..5].copy_from_slice(b"chain");
    input.advance(5).unwrap();

    copy_reserved(&mut source, &mut destination, 5)
        .await
        .unwrap();
    output.reserve(5).await.unwrap();
    assert_eq!(&output.view()[..5], b"chain");
    output.advance(5).unwrap();
}

#[tokio::test]
async fn pending_views_are_consumed_by_every_advance_attempt() {
    let (mut producer, mut consumer) = local::create(1).unwrap();

    producer.try_reserve(2).unwrap();
    producer.view_mut()[..2].copy_from_slice(b"no");
    let oversized = producer.view().len() + 1;
    assert_eq!(
        producer.advance(oversized).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(producer.view().is_empty());
    assert_eq!(
        producer.advance(1).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(snapshot_len(&mut consumer), 0);

    producer.try_reserve(3).unwrap();
    producer.view_mut()[..3].copy_from_slice(b"yes");
    producer.advance(2).unwrap();
    assert!(producer.view().is_empty());
    assert_eq!(
        producer.advance(1).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    consumer.reserve(2).await.unwrap();
    assert_eq!(&consumer.view()[..2], b"ye");
    assert_eq!(
        consumer.advance(3).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(consumer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 2);

    consumer.reserve(2).await.unwrap();
    consumer.advance(1).unwrap();
    assert!(consumer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 1);
}

#[tokio::test]
async fn zero_advance_clears_pending_and_never_requires_one() {
    let (mut producer, mut consumer) = local::create(1).unwrap();

    producer.advance(0).unwrap();
    producer.try_reserve(4).unwrap();
    producer.advance(0).unwrap();
    assert!(producer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 0);

    consumer.advance(0).unwrap();
    producer.try_reserve(1).unwrap();
    producer.advance(1).unwrap();
    consumer.try_reserve(1).unwrap();
    consumer.advance(0).unwrap();
    assert!(consumer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 1);
}

#[tokio::test]
async fn reservation_start_and_poll_abandon_previous_views() {
    let (mut producer, mut consumer) = local::create(1).unwrap();

    producer.try_reserve(2).unwrap();
    producer.view_mut()[..2].copy_from_slice(b"ok");
    let unpolled = producer.reserve(1);
    drop(unpolled);
    assert_eq!(&producer.view()[..2], b"ok");

    assert_eq!(
        producer
            .try_reserve(producer.capacity() + 1)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(producer.view().is_empty());

    producer.try_reserve(1).unwrap();
    producer.view_mut()[0] = b'x';
    producer.advance(1).unwrap();
    consumer.try_reserve(1).unwrap();
    assert_eq!(&consumer.view()[..1], b"x");

    let mut blocked = Box::pin(consumer.reserve(2));
    tokio::select! {
        biased;
        result = &mut blocked => panic!("reservation unexpectedly completed: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    drop(blocked);
    assert!(consumer.view().is_empty());
    assert_eq!(snapshot_len(&mut consumer), 1);
}

#[tokio::test]
async fn capacity_view_access_and_forking_preserve_pending_state() {
    let (mut producer, mut consumer) = local::create(1).unwrap();
    producer.try_reserve(4).unwrap();
    producer.view_mut()[..4].copy_from_slice(b"data");
    let _ = producer.capacity();
    assert_eq!(&producer.view()[..4], b"data");
    producer.advance(4).unwrap();

    consumer.try_reserve(4).unwrap();
    let _ = consumer.capacity();
    let mut fork = consumer.try_fork().unwrap();
    assert_eq!(&consumer.view()[..4], b"data");
    fork.try_reserve(0).unwrap();
    assert_eq!(fork.view().len(), 4);
    consumer.advance(4).unwrap();
    fork.reserve(4).await.unwrap();
    assert_eq!(&fork.view()[..4], b"data");
    fork.advance(4).unwrap();
}

#[tokio::test]
async fn local_forks_inherit_unread_data_and_receive_future_data_independently() {
    let (mut producer, mut first) = local::create(1).unwrap();
    producer.reserve(4).await.unwrap();
    producer.view_mut()[..4].copy_from_slice(b"past");
    producer.advance(4).unwrap();

    first.reserve(2).await.unwrap();
    assert_eq!(first.view().len(), 4);
    assert_eq!(&first.view()[..2], b"pa");
    first.advance(2).unwrap();
    let mut second = first.try_fork().unwrap();

    first.reserve(2).await.unwrap();
    second.reserve(2).await.unwrap();
    assert_eq!(&first.view()[..2], b"st");
    assert_eq!(&second.view()[..2], b"st");
    first.advance(2).unwrap();
    second.advance(2).unwrap();

    producer.reserve(3).await.unwrap();
    producer.view_mut()[..3].copy_from_slice(b"new");
    producer.advance(3).unwrap();
    first.reserve(3).await.unwrap();
    second.reserve(3).await.unwrap();
    assert_eq!(&first.view()[..3], b"new");
    assert_eq!(&second.view()[..3], b"new");
    first.advance(3).unwrap();
    second.advance(3).unwrap();
}

#[tokio::test]
async fn slowest_local_fork_controls_backpressure() {
    let (mut producer, mut first) = local::create(1).unwrap();
    let mut second = first.try_fork().unwrap();
    let capacity = producer.capacity();

    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    first.reserve(capacity).await.unwrap();
    first.advance(capacity).unwrap();
    assert_eq!(
        producer.try_reserve(1).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );

    second.reserve(1).await.unwrap();
    second.advance(1).unwrap();
    producer.try_reserve(1).unwrap();
    producer.advance(1).unwrap();
}

#[test]
fn local_reader_admission_is_concurrent_bounded_and_reusable() {
    let (mut producer, first) = local::create(1).unwrap();
    let first = Arc::new(first);
    let mut readers = std::thread::scope(|scope| {
        let handles = (0..16)
            .map(|_| {
                let first = Arc::clone(&first);
                scope.spawn(move || first.try_fork().unwrap())
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    while readers.len() < 63 {
        readers.push(first.try_fork().unwrap());
    }
    assert_eq!(
        first.try_fork().err().unwrap().kind(),
        ErrorKind::ResourceBusy
    );

    drop(readers.pop());
    readers.push(first.try_fork().unwrap());
    assert!(producer.try_reserve(1).is_ok());
}

#[tokio::test]
async fn local_reader_removal_is_independent_and_final() {
    let (mut producer, first) = local::create(1).unwrap();
    let second = first.try_fork().unwrap();
    drop(second);
    producer.try_reserve(1).unwrap();
    producer.advance(1).unwrap();
    drop(first);
    assert_eq!(
        producer.try_reserve(1).unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );

    let (producer, first) = local::create(1).unwrap();
    let mut second = first.try_fork().unwrap();
    drop(first);
    drop(producer);
    assert_eq!(
        second.reserve(1).await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
}

#[test]
fn forking_reports_a_disconnected_local_registry() {
    let (producer, consumer) = local::create(1).unwrap();
    drop(producer);
    assert_eq!(
        consumer.try_fork().err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn local_wait_wakes() {
    let (mut producer, mut consumer) = local::create(1).unwrap();
    let read = async {
        consumer.reserve(4).await.unwrap();
        assert_eq!(&consumer.view()[..4], b"wake");
        consumer.advance(4).unwrap();
    };
    let write = async {
        tokio::task::yield_now().await;
        producer.reserve(4).await.unwrap();
        producer.view_mut()[..4].copy_from_slice(b"wake");
        producer.advance(4).unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(read, write);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn full_producer_wakes_when_space_is_released() {
    let (mut producer, mut consumer) = local::create(1).unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();

    let write = async {
        producer.reserve(1).await.unwrap();
        producer.advance(1).unwrap();
    };
    let read = async {
        tokio::task::yield_now().await;
        consumer.reserve(1).await.unwrap();
        consumer.advance(1).unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(write, read);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_local_consumer_wakes_the_producer() {
    let (mut producer, consumer) = local::create(1).unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();

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
async fn dropping_local_producer_wakes_the_consumer() {
    let (producer, mut consumer) = local::create(1).unwrap();
    let wait = consumer.reserve(1);
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
async fn reservation_validation_errors_are_invalid_input() {
    let (mut producer, mut consumer) = local::create(1).unwrap();
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
        consumer.try_reserve(invalid).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        consumer.reserve(invalid).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );

    producer.try_reserve(1).unwrap();
    let oversized = producer.view().len() + 1;
    assert_eq!(
        producer.advance(oversized).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    producer.try_reserve(1).unwrap();
    producer.advance(1).unwrap();
    consumer.try_reserve(1).unwrap();
    assert_eq!(
        consumer.advance(2).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(snapshot_len(&mut consumer), 1);
    consumer.try_reserve(1).unwrap();
    consumer.advance(1).unwrap();
}
