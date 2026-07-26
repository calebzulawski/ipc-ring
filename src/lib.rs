//! Zero-copy shared-memory rings for interprocess communication.
#![deny(unsafe_op_in_unsafe_fn)]

mod error;
mod handshake;
mod local_socket;
mod mapping;
mod ring;
mod server;
mod sys;

pub use ring::spsc;
pub use server::{RingBuilder, Server, ServerOptions};
