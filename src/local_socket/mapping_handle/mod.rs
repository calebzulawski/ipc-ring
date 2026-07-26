//! Transfers only the anonymous shared-memory mapping used during attachment.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

pub(crate) use implementation::{receive_mapping_handle, send_mapping_handle};
