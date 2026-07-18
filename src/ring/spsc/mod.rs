//! A single-producer, single-consumer shared-memory ring.

mod abi;
mod anonymous;
mod consumer;
mod producer;
mod state;

#[cfg(test)]
mod tests;

pub use anonymous::anonymous;
pub use consumer::{Consumer, ReadGrant};
pub use producer::{Producer, WriteGrant};

pub(crate) use abi::{ABI_VERSION, CONSUMER_CLAIMED, CONSUMER_FREE, Header};
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) use abi::{IDLE, WAITING};
