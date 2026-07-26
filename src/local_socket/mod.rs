//! Opens Tokio local endpoints and transfers the shared-memory mapping between peers.
//!
//! Unix streams and listeners are exposed directly. Windows retains one handshake wrapper because
//! mapping transfer must keep the connected client process pinned.

mod mapping_handle;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub(crate) use mapping_handle::{receive_mapping_handle, send_mapping_handle};
#[cfg(all(test, unix))]
pub(crate) use unix::pair;
#[cfg(unix)]
pub(crate) use unix::{
    ConsumerStream, HandshakeStream, Listener, ProducerStream, accept, bind, connect,
};
#[cfg(windows)]
pub(crate) use windows::{
    ConsumerStream, HandshakeStream, Listener, ProducerStream, accept, bind, connect,
};
