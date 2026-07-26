use super::{NotificationStreamSlot, read_zero_byte_alert, write_zero_byte_alert};
use crate::mapping::{self, MappedMemory};
use crate::ring::spsc::{IDLE, WAITING};
use std::cell::Cell;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;

fn mapped_memory() -> Arc<MappedMemory> {
    let (_mapping, memory) = mapping::create(mapping::minimum_capacity()).unwrap();
    Arc::new(memory)
}

fn producer_notification_stream_slot() -> Arc<NotificationStreamSlot<DuplexStream>> {
    NotificationStreamSlot::producer_waiting_for_notification_stream(mapped_memory())
}

async fn blocked_notification_stream_insertion(
    stream_slot: &Arc<NotificationStreamSlot<DuplexStream>>,
) -> (JoinHandle<io::Result<()>>, DuplexStream) {
    let (mut stream, peer) = tokio::io::duplex(1);
    stream.write_u8(u8::MAX).await.unwrap();
    let consumer_admission = stream_slot.try_claim_consumer().unwrap();
    let insert_stream = {
        let stream_slot = Arc::clone(stream_slot);
        tokio::spawn(async move {
            stream_slot
                .insert_notification_stream_after_client_ready(stream, consumer_admission)
                .await
        })
    };

    for _ in 0..100 {
        if stream_slot.notification_stream.try_lock().is_err() {
            return (insert_stream, peer);
        }
        tokio::task::yield_now().await;
    }
    panic!("notification stream insertion did not start");
}

#[tokio::test]
async fn alert_io_encodes_validates_and_normalizes_stream_closure() {
    let (mut reader, mut writer) = tokio::io::duplex(1);
    write_zero_byte_alert(&mut writer).await.unwrap();
    assert_eq!(reader.read_u8().await.unwrap(), 0);
    writer.write_all(&[0]).await.unwrap();
    read_zero_byte_alert(&mut reader).await.unwrap();
    writer.write_all(&[u8::MAX]).await.unwrap();
    assert_eq!(
        read_zero_byte_alert(&mut reader).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    drop(writer);
    assert_eq!(
        read_zero_byte_alert(&mut reader).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn connection_wake_covers_waiters_armed_before_and_during_insertion() {
    let stream_slot = producer_notification_stream_slot();
    let waiter_state = &stream_slot.mapped_memory.header().data_wait_state;
    waiter_state.store(WAITING, Ordering::Release);
    let (stream, mut peer) = tokio::io::duplex(1);
    let consumer_admission = stream_slot.try_claim_consumer().unwrap();

    stream_slot
        .insert_notification_stream_after_client_ready(stream, consumer_admission)
        .await
        .unwrap();
    assert_eq!(waiter_state.load(Ordering::Acquire), IDLE);
    assert_eq!(peer.read_u8().await.unwrap(), 0);

    let stream_slot = producer_notification_stream_slot();
    let waiter_state = &stream_slot.mapped_memory.header().data_wait_state;
    waiter_state.store(WAITING, Ordering::Release);
    let (mut stream, mut peer) = tokio::io::duplex(1);
    stream.write_u8(u8::MAX).await.unwrap();
    let consumer_admission = stream_slot.try_claim_consumer().unwrap();
    let insertion_stream_slot = Arc::clone(&stream_slot);
    let insertion = tokio::spawn(async move {
        insertion_stream_slot
            .insert_notification_stream_after_client_ready(stream, consumer_admission)
            .await
    });

    while waiter_state.load(Ordering::Acquire) != IDLE {
        tokio::task::yield_now().await;
    }
    waiter_state.store(WAITING, Ordering::Release);
    assert_eq!(peer.read_u8().await.unwrap(), u8::MAX);
    assert_eq!(peer.read_u8().await.unwrap(), 0);
    insertion.await.unwrap().unwrap();
    assert_eq!(waiter_state.load(Ordering::Acquire), WAITING);
}

#[tokio::test]
async fn cancelling_a_pending_send_disconnects_its_lease() {
    let (mut stream, _peer) = tokio::io::duplex(1);
    stream.write_all(&[0]).await.unwrap();
    let stream_slot =
        NotificationStreamSlot::consumer_with_notification_stream(stream, mapped_memory());
    let locked_stream = stream_slot
        .wait_for_notification_stream_and_lock()
        .await
        .unwrap();
    let state = AtomicU32::new(WAITING);
    let published = Cell::new(false);

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            locked_stream.publish_cursor_and_notify(&state, || published.set(true)),
        )
        .await
        .is_err()
    );

    assert!(published.get());
    let error = match stream_slot.wait_for_notification_stream_and_lock().await {
        Ok(_) => panic!("cancelled send left its notification stream available"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn publication_waits_for_notification_stream_insertion_and_uses_the_stream() {
    let stream_slot = producer_notification_stream_slot();
    let (insert_stream, mut peer) = blocked_notification_stream_insertion(&stream_slot).await;

    let published = Arc::new(AtomicBool::new(false));
    let publish = {
        let stream_slot = Arc::clone(&stream_slot);
        let published = Arc::clone(&published);
        tokio::spawn(async move {
            stream_slot
                .publish_cursor_with_optional_notification_stream(&AtomicU32::new(IDLE), || {
                    published.store(true, Ordering::Release)
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!published.load(Ordering::Acquire));
    assert!(!publish.is_finished());

    assert_eq!(peer.read_u8().await.unwrap(), u8::MAX);
    assert_eq!(peer.read_u8().await.unwrap(), 0);
    insert_stream.await.unwrap().unwrap();
    publish.await.unwrap().unwrap();
    assert!(published.load(Ordering::Acquire));
}

#[tokio::test]
async fn cancelling_notification_stream_insertion_allows_preconnection_publication() {
    let stream_slot = producer_notification_stream_slot();
    let (insert_stream, _peer) = blocked_notification_stream_insertion(&stream_slot).await;
    insert_stream.abort();
    assert!(insert_stream.await.unwrap_err().is_cancelled());
    drop(stream_slot.try_claim_consumer().unwrap());

    let called = AtomicBool::new(false);
    stream_slot
        .publish_cursor_with_optional_notification_stream(&AtomicU32::new(IDLE), || {
            called.store(true, Ordering::Release)
        })
        .await
        .unwrap();
    assert!(called.load(Ordering::Acquire));
}

#[tokio::test]
async fn stopping_during_notification_stream_insertion_wakes_a_blocked_publication() {
    let stream_slot = producer_notification_stream_slot();
    let (insert_stream, _peer) = blocked_notification_stream_insertion(&stream_slot).await;

    let publish = {
        let stream_slot = Arc::clone(&stream_slot);
        tokio::spawn(async move {
            stream_slot
                .publish_cursor_with_optional_notification_stream(&AtomicU32::new(IDLE), || {})
                .await
        })
    };
    tokio::task::yield_now().await;
    stream_slot.close_consumer_admission();
    insert_stream.abort();
    assert!(insert_stream.await.unwrap_err().is_cancelled());

    assert_eq!(
        publish.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn failed_notification_stream_insertion_allows_preconnection_publication() {
    let stream_slot = producer_notification_stream_slot();
    let (stream, peer) = tokio::io::duplex(1);
    drop(peer);
    let consumer_admission = stream_slot.try_claim_consumer().unwrap();

    assert_eq!(
        stream_slot
            .insert_notification_stream_after_client_ready(stream, consumer_admission)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(stream_slot.try_claim_consumer().unwrap());

    let called = Cell::new(false);
    stream_slot
        .publish_cursor_with_optional_notification_stream(&AtomicU32::new(IDLE), || {
            called.set(true)
        })
        .await
        .unwrap();
    assert!(called.get());
}
