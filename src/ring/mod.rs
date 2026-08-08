//! A single-producer shared-memory byte ring.

pub mod ipc;

pub mod local;

mod abi;
pub(crate) mod consumer;
mod mapping;
pub(crate) mod producer;
pub(crate) mod reader;
mod state;
pub(crate) mod wake;

#[cfg(test)]
mod tests;

pub(crate) use abi::{ABI_VERSION, Header, MAX_READERS};
