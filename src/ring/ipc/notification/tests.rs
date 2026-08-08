use super::{Producer, WakeStream, read_wake_byte, try_write_wake};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
#[cfg(unix)]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

#[derive(Clone, Copy)]
enum WakeOutcome {
    Bytes(usize),
    WouldBlock,
    Failure,
}

struct ScriptedWake {
    outcome: WakeOutcome,
}

impl WakeStream for ScriptedWake {
    fn try_write_wake_byte(&self) -> io::Result<usize> {
        match self.outcome {
            WakeOutcome::Bytes(amount) => Ok(amount),
            WakeOutcome::WouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            WakeOutcome::Failure => Err(io::Error::from(io::ErrorKind::ConnectionReset)),
        }
    }
}

impl AsyncRead for ScriptedWake {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for ScriptedWake {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn wake_byte_io_validates_zero_and_reports_eof_as_broken_pipe() {
    let (mut reader, mut writer) = tokio::io::duplex(1);
    writer.write_all(&[0]).await.unwrap();
    read_wake_byte(&mut reader).await.unwrap();
    writer.write_all(&[u8::MAX]).await.unwrap();
    assert_eq!(
        read_wake_byte(&mut reader).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    drop(writer);
    assert_eq!(
        read_wake_byte(&mut reader).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
#[cfg(unix)]
async fn installation_sends_one_reconciliation_wake() {
    let notification = Producer::pending();
    let (mut stream, mut peer) = crate::ring::ipc::socket::pair().unwrap();
    stream.write_u8(0).await.unwrap();
    assert_eq!(peer.read_u8().await.unwrap(), 0);
    notification.try_wake().unwrap();

    notification.install(stream).unwrap();
    assert_eq!(peer.read_u8().await.unwrap(), 0);
}

#[test]
fn wake_write_results_are_classified() {
    let cases = [
        (WakeOutcome::Bytes(1), true),
        (WakeOutcome::WouldBlock, true),
        (WakeOutcome::Bytes(0), false),
        (WakeOutcome::Failure, false),
    ];

    for (outcome, succeeds) in cases {
        let stream = ScriptedWake { outcome };
        let result = try_write_wake(&stream);
        if succeeds {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        }
    }
}

#[tokio::test]
async fn closing_a_pending_producer_notification_wakes_its_reader() {
    let notification = Arc::new(Producer::pending());
    let read = {
        let notification = Arc::clone(&notification);
        tokio::spawn(async move { notification.wait_for_wake().await })
    };
    tokio::task::yield_now().await;
    notification.close();
    assert_eq!(
        read.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
#[cfg(unix)]
async fn a_closed_notification_rejects_stream_installation() {
    let notification = Producer::pending();
    notification.close();
    let (stream, _peer) = crate::ring::ipc::socket::pair().unwrap();

    assert_eq!(
        notification.install(stream).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
#[cfg(unix)]
async fn closing_an_installed_producer_notification_interrupts_its_read() {
    let notification = Arc::new(Producer::pending());
    let (stream, _peer) = crate::ring::ipc::socket::pair().unwrap();
    notification.install(stream).unwrap();

    let read = {
        let notification = Arc::clone(&notification);
        tokio::spawn(async move { notification.wait_for_wake().await })
    };
    for _ in 0..100 {
        if notification.stream.get().unwrap().try_lock().is_err() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(notification.stream.get().unwrap().try_lock().is_err());

    notification.close();
    assert_eq!(
        read.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}
