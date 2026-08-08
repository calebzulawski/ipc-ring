//! Installs and uses the socket or pipe that wakes one IPC reader.

use super::socket::{ConsumerStream, ProducerStream};
use crate::ring::wake::{ConsumerWake, ProducerWake};
use std::io;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::{Mutex, Notify};

/// Stream operations needed to send wakeups without blocking the producer.
pub(crate) trait WakeStream: AsyncRead + AsyncWrite + Unpin {
    fn try_write_wake_byte(&self) -> io::Result<usize>;
}

#[cfg(unix)]
impl WakeStream for tokio::net::UnixStream {
    fn try_write_wake_byte(&self) -> io::Result<usize> {
        self.try_write(&[0])
    }
}

#[cfg(windows)]
impl WakeStream for ProducerStream {
    fn try_write_wake_byte(&self) -> io::Result<usize> {
        self.try_write(&[0])
    }
}

#[cfg(windows)]
impl WakeStream for ConsumerStream {
    fn try_write_wake_byte(&self) -> io::Result<usize> {
        self.try_write(&[0])
    }
}

/// Producer-side IPC wakeup stream, which may still be in its handshake.
pub(crate) struct Producer {
    stream: OnceLock<Mutex<Option<ProducerStream>>>,
    state_changed: Notify,
    closed: AtomicBool,
}

impl Producer {
    pub(crate) fn pending() -> Self {
        Self {
            stream: OnceLock::new(),
            state_changed: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Reconciles the attachment gap, then publishes the stream exactly once.
    pub(crate) fn install(&self, stream: ProducerStream) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) || self.stream.get().is_some() {
            return Err(crate::error::peer_disconnected());
        }
        try_write_wake(&stream)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }

        if self.stream.set(Mutex::new(Some(stream))).is_err() {
            return Err(crate::error::peer_disconnected());
        }
        if self.closed.load(Ordering::Acquire) {
            self.close();
            return Err(crate::error::peer_disconnected());
        }

        self.state_changed.notify_one();
        Ok(())
    }

    /// Makes future I/O fail and interrupts installation or a blocked read.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(stream) = self.stream.get()
            && let Ok(mut stream) = stream.try_lock()
        {
            stream.take();
        }
        self.state_changed.notify_one();
    }

    fn record_failure(&self, stream: &mut Option<ProducerStream>) {
        stream.take();
        self.closed.store(true, Ordering::Release);
        self.state_changed.notify_one();
    }

    /// Writes one zero byte; `WouldBlock` means a wakeup is already waiting.
    pub(crate) fn try_wake(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::error::peer_disconnected());
        }
        let Some(stream) = self.stream.get() else {
            return Ok(());
        };
        let Ok(mut stored) = stream.try_lock() else {
            return Ok(());
        };
        if self.closed.load(Ordering::Acquire) {
            self.record_failure(&mut stored);
            return Err(crate::error::peer_disconnected());
        }
        let Some(stream) = stored.as_ref() else {
            return Err(crate::error::peer_disconnected());
        };
        let result = try_write_wake(stream);
        if result.is_err() {
            self.record_failure(&mut stored);
        }
        result
    }

    /// Waits for stream installation if needed, then consumes one wakeup byte.
    pub(crate) async fn wait_for_wake(&self) -> io::Result<()> {
        loop {
            let state_changed = self.state_changed.notified();
            if self.closed.load(Ordering::Acquire) {
                return Err(crate::error::peer_disconnected());
            }
            let Some(stream) = self.stream.get() else {
                state_changed.await;
                continue;
            };
            let mut stored = stream.lock().await;
            if self.closed.load(Ordering::Acquire) {
                self.record_failure(&mut stored);
                return Err(crate::error::peer_disconnected());
            }
            if let Some(stream) = stored.as_mut() {
                let result = tokio::select! {
                    result = read_wake_byte(stream) => result,
                    _ = state_changed => {
                        if self.closed.load(Ordering::Acquire) {
                            Err(crate::error::peer_disconnected())
                        } else {
                            continue;
                        }
                    }
                };
                if result.is_err() {
                    self.record_failure(&mut stored);
                }
                return result;
            }
            return Err(crate::error::peer_disconnected());
        }
    }
}

/// Consumer-side IPC wakeup stream, which is connected before construction.
pub(crate) struct Consumer {
    stream: ConsumerStream,
}

impl Consumer {
    pub(crate) fn connected(stream: ConsumerStream) -> Self {
        Self { stream }
    }

    pub(crate) async fn wait_for_wake(&mut self) -> io::Result<()> {
        read_wake_byte(&mut self.stream).await
    }

    pub(crate) fn try_wake(&self) -> io::Result<()> {
        try_write_wake(&self.stream)
    }
}

impl ProducerWake for Arc<Producer> {
    fn notify_data(&self) -> io::Result<()> {
        self.try_wake()
    }

    async fn wait_for_space(&self) -> io::Result<()> {
        self.wait_for_wake().await
    }

    fn close(&self) {
        Producer::close(self);
    }
}

impl ConsumerWake for Consumer {
    async fn wait_for_data(&mut self) -> io::Result<()> {
        self.wait_for_wake().await
    }

    fn notify_space(&self) -> io::Result<()> {
        self.try_wake()
    }
}

fn try_write_wake<S>(stream: &S) -> io::Result<()>
where
    S: WakeStream,
{
    match stream.try_write_wake_byte() {
        Ok(1) => Ok(()),
        Ok(_) => Err(crate::error::peer_disconnected()),
        Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(_) => Err(crate::error::peer_disconnected()),
    }
}

/// Reads the zero byte that tells a peer to recheck the shared ring positions.
pub(super) async fn read_wake_byte<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let byte = stream
        .read_u8()
        .await
        .map_err(|_| crate::error::peer_disconnected())?;
    if byte == 0 {
        Ok(())
    } else {
        Err(crate::error::protocol("unknown control notification"))
    }
}

#[cfg(test)]
#[path = "notification/tests.rs"]
mod tests;
