//! Zero-copy shared-memory rings for interprocess communication.
#![deny(unsafe_op_in_unsafe_fn)]

mod error;
mod handshake;
mod local_socket;
mod mapping;
pub mod ring;
mod server;
mod sys;

pub use server::{Server, ServerOptions};
