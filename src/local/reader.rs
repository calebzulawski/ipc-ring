//! Local reader admission and lifetime guards.

use super::notification;
use crate::mapping::MappedMemory;
use crate::ring::reader::{ReaderConnection, ReaderRegistry};
use std::io;
use std::sync::{Arc, Weak};

type Registry = ReaderRegistry<notification::Producer>;

/// Creates a registry containing the local ring's first reader.
pub(super) fn create_registry(
    memory: Arc<MappedMemory>,
    notification: notification::Producer,
) -> io::Result<(Arc<Registry>, u8, Guard)> {
    let registry = ReaderRegistry::new(memory);
    let (slot, guard) = claim(&registry, 0, notification)?;
    Ok((registry, slot, guard))
}

fn claim(
    registry: &Arc<Registry>,
    read_position: u64,
    notification: notification::Producer,
) -> io::Result<(u8, Guard)> {
    let reader = registry.reserve_reader(notification)?;
    let slot = reader.slot() as u8;
    registry.activate_at(&reader, read_position);
    let guard = Guard {
        registry: Arc::downgrade(registry),
        reader: Arc::downgrade(&reader),
    };
    Ok((slot, guard))
}

/// Removes a local reader from its registry when the consumer is dropped.
pub(super) struct Guard {
    registry: Weak<Registry>,
    reader: Weak<ReaderConnection<notification::Producer>>,
}

impl Guard {
    pub(super) fn claim_sibling(
        &self,
        read_position: u64,
        notification: notification::Producer,
    ) -> io::Result<(u8, Self)> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(crate::error::peer_disconnected)?;
        claim(&registry, read_position, notification)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let (Some(registry), Some(reader)) = (self.registry.upgrade(), self.reader.upgrade()) {
            registry.disconnect(&reader);
        }
    }
}
