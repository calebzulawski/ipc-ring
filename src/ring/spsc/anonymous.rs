use super::SharedRing;
use super::{Consumer, Producer};
use std::io;
use std::sync::Arc;

/// Creates an anonymous ring with at least the requested payload capacity.
pub fn anonymous(minimum_capacity: usize) -> io::Result<(Producer, Consumer)> {
    let ring = Arc::new(SharedRing::create_anonymous(minimum_capacity)?);
    Ok((Producer::new(Arc::clone(&ring), None), Consumer::new(ring)))
}
