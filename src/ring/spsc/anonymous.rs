use super::{Consumer, Producer};
use crate::platform::MappedRing;
use crate::ring::spsc::CONSUMER_CLAIMED;
use std::io;
use std::sync::Arc;

/// Creates an anonymous ring with at least the requested payload capacity and
/// returns both SPSC endpoints already claimed.
pub fn anonymous(minimum_capacity: usize) -> io::Result<(Producer, Consumer)> {
    let ring = Arc::new(MappedRing::create_anonymous(
        minimum_capacity,
        CONSUMER_CLAIMED,
    )?);
    Ok((Producer::new(Arc::clone(&ring)), Consumer::new(ring)))
}
