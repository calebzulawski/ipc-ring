//! Ring buffers for in-process and interprocess communication.
//!
//! - Copy-free reads and writes through contiguous byte views, even across wraparound.
//! - Attach up to 64 parallel readers to a single ring buffer.
//! - Asynchronous waiting with slowest-consumer backpressure.
#![deny(unsafe_op_in_unsafe_fn)]

pub mod cursor;
mod error;
pub mod ring;
mod sys;
pub mod view;
