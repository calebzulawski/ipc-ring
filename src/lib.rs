//! Zero-copy shared-memory rings for interprocess communication.
#![deny(unsafe_op_in_unsafe_fn)]

mod error;
mod platform;
mod ring;

pub use ring::spsc;
