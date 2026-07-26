//! A single-producer, single-consumer shared-memory ring.

mod abi;
mod anonymous;
mod consumer;
pub(crate) mod notification;
mod producer;
mod shared;
mod state;

#[cfg(test)]
mod tests;

pub use anonymous::anonymous;
pub use consumer::{ConnectOptions, Consumer, ReadGrant};
pub use producer::{Producer, WriteGrant};

pub(crate) use abi::{ABI_VERSION, Header, IDLE, WAITING};
pub(crate) use shared::{RegisteredRing, SharedRing};
