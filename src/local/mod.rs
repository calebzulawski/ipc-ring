//! Ring buffers for in-process communication.
//!
//! A local ring is created as a producer and consumer pair.
//!
//! # Example
//!
//! ```rust,no_run
//! use ipc_ring::local;
//!
//! # fn setup() -> std::io::Result<()> {
//! let (producer, consumer) = local::create(64 * 1024)?;
//! let second_consumer = consumer.try_clone()?;
//! # let _ = (producer, consumer, second_consumer);
//! # Ok(())
//! # }
//! ```

mod consumer;
mod notification;
mod producer;
mod reader;

pub use consumer::Consumer;
pub use producer::Producer;

use crate::mapping;
use std::io;
use std::sync::Arc;

/// Creates a local ring with at least the requested payload capacity.
pub fn create(minimum_capacity: usize) -> io::Result<(Producer, Consumer)> {
    let (_shared_memory, memory) = mapping::create(minimum_capacity)?;
    let memory = Arc::new(memory);
    let (producer_notification, consumer_notification) = notification::pair();
    let (readers, slot, local_reader) =
        reader::create_registry(Arc::clone(&memory), producer_notification)?;
    let producer = Producer::new(readers);
    let consumer = Consumer::new(memory, slot, consumer_notification, local_reader);
    Ok((producer, consumer))
}
