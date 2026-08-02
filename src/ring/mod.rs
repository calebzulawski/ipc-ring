//! A single-producer shared-memory byte ring.

mod abi;
mod anonymous;
mod consumer;
pub(crate) mod notification;
mod producer;
mod reader;
mod registered;
mod state;

#[cfg(test)]
mod tests;

pub use anonymous::anonymous;
pub use consumer::{ConnectOptions, Consumer, ReadGrant};
pub use producer::{Producer, WriteGrant};

pub(crate) use abi::{ABI_VERSION, Header, MAX_READERS};
pub(crate) use reader::ReaderRegistry;
pub(crate) use registered::RegisteredRing;
