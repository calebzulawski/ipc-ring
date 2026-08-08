//! Implements the IPC routing and mapping-attachment exchange.

use std::time::Duration;

mod client;
mod protocol;
mod server;

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) use client::connect;
pub(crate) use protocol::validate_port;
pub(crate) use server::route;
