use std::io;
use std::path::Path;

pub(crate) type ConsumerStream = tokio::net::UnixStream;
pub(crate) type ProducerStream = tokio::net::UnixStream;
pub(crate) type HandshakeStream = tokio::net::UnixStream;
pub(crate) type Listener = tokio::net::UnixListener;

pub(crate) fn bind(path: &Path) -> io::Result<Listener> {
    Listener::bind(path)
}

pub(crate) async fn accept(listener: &mut Listener) -> io::Result<HandshakeStream> {
    listener.accept().await.map(|(stream, _)| stream)
}

/// Opens the exact socket pathname supplied by the caller.
pub(crate) async fn connect(path: &Path) -> io::Result<ConsumerStream> {
    tokio::net::UnixStream::connect(path).await
}

#[cfg(test)]
pub(crate) fn pair() -> io::Result<(ProducerStream, ConsumerStream)> {
    tokio::net::UnixStream::pair()
}
