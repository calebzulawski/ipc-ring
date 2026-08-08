//! Implements the Windows control connection with Tokio byte-mode named pipes.

use crate::sys::windows::owned;
use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle, RawHandle};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
use windows::Win32::Foundation::HANDLE;
use windows::Win32::System::Pipes::GetNamedPipeClientProcessId;
use windows::Win32::System::Threading::{OpenProcess, PROCESS_DUP_HANDLE};

pub(crate) type ConsumerStream = NamedPipeClient;
pub(crate) type ProducerStream = NamedPipeServer;

/// Keeps an accepted pipe and its client process alive through mapping duplication.
pub(crate) struct HandshakeStream {
    inner: NamedPipeServer,
    client_process: OwnedHandle,
}

impl HandshakeStream {
    /// Separates the connected pipe from the process pinned for handle transfer.
    pub(crate) fn into_parts(self) -> (ProducerStream, OwnedHandle) {
        (self.inner, self.client_process)
    }
}

impl AsyncRead for HandshakeStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for HandshakeStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

impl AsRawHandle for HandshakeStream {
    fn as_raw_handle(&self) -> RawHandle {
        self.inner.as_raw_handle()
    }
}

/// Retains the pipe path needed to create each replacement server instance.
pub(crate) struct Listener {
    path: PathBuf,
    pending: NamedPipeServer,
}

pub(crate) fn bind(path: &Path) -> io::Result<Listener> {
    let path = path.to_path_buf();
    let pending = create_server(&path, true)?;
    Ok(Listener { path, pending })
}

pub(crate) async fn accept(listener: &mut Listener) -> io::Result<HandshakeStream> {
    listener.pending.connect().await?;
    let replacement = create_server(&listener.path, false)?;
    let connected = std::mem::replace(&mut listener.pending, replacement);
    let mut pid = 0;
    let pipe = HANDLE(connected.as_raw_handle());
    unsafe { GetNamedPipeClientProcessId(pipe, &mut pid) }
        .map_err(crate::sys::windows::windows_error)?;
    let client_process = owned(unsafe { OpenProcess(PROCESS_DUP_HANDLE, false, pid) })?;
    Ok(HandshakeStream {
        inner: connected,
        client_process,
    })
}

pub(crate) async fn connect(path: &Path) -> io::Result<ConsumerStream> {
    ClientOptions::new().open(path.as_os_str())
}

fn create_server(path: &Path, first: bool) -> io::Result<NamedPipeServer> {
    let mut options = ServerOptions::new();
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    options.create(path.as_os_str())
}
