use super::notification::anonymous_pair;
use super::{Consumer, Producer, ReaderRegistry};
use crate::mapping;
use std::io;
use std::sync::Arc;

/// Creates an anonymous ring with at least the requested payload capacity.
pub fn anonymous(minimum_capacity: usize) -> io::Result<(Producer, Consumer)> {
    let (_shared_memory, memory) = mapping::create(minimum_capacity)?;
    let memory = Arc::new(memory);
    let (producer_notification, consumer_notification) = anonymous_pair();
    let (readers, local_reader) =
        ReaderRegistry::process_local(Arc::clone(&memory), producer_notification);
    let producer = Producer::unregistered(readers);
    let consumer = Consumer::process_local(memory, consumer_notification, local_reader);
    Ok((producer, consumer))
}
