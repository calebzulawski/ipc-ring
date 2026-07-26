use crate::ring::spsc::{Producer, RegisteredRing, SharedRing};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, Weak};

/// Registers rings while the listener task remains alive.
#[derive(Clone)]
pub struct Server {
    pub(super) registry: Weak<ServerRegistry>,
}

impl Server {
    /// Starts a builder for one case-sensitive UTF-8 port.
    pub fn register(&self, port: impl Into<String>) -> RingBuilder {
        RingBuilder {
            registry: self.registry.clone(),
            port: port.into(),
        }
    }
}

/// Creates one ring and publishes it in the server's routing table.
pub struct RingBuilder {
    registry: Weak<ServerRegistry>,
    port: String,
}

impl RingBuilder {
    /// Allocates an anonymous mapping and registers its sole producer.
    pub fn spsc(self, minimum_capacity: usize) -> io::Result<Producer> {
        self.registry
            .upgrade()
            .ok_or_else(crate::error::peer_disconnected)?
            .register_spsc(self.port, minimum_capacity)
    }
}

pub(crate) struct ServerRegistry {
    rings_by_port: Mutex<HashMap<String, Weak<RegisteredRing>>>,
}

impl ServerRegistry {
    pub(super) fn new() -> Self {
        Self {
            rings_by_port: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn find(&self, port: &str) -> Option<Arc<RegisteredRing>> {
        self.rings_by_port
            .lock()
            .expect("server registry mutex poisoned")
            .get(port)
            .and_then(Weak::upgrade)
    }

    fn register_spsc(&self, port: String, minimum_capacity: usize) -> io::Result<Producer> {
        crate::handshake::validate_port(&port)?;
        let registered_ring = RegisteredRing::create(minimum_capacity)?;
        let mut rings_by_port = self
            .rings_by_port
            .lock()
            .expect("server registry mutex poisoned");
        if rings_by_port.get(&port).and_then(Weak::upgrade).is_some() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        rings_by_port.insert(port, Arc::downgrade(&registered_ring));
        drop(rings_by_port);

        let ring = Arc::new(SharedRing::registered(&registered_ring));
        Ok(Producer::new(ring, Some(registered_ring)))
    }
}

impl Drop for ServerRegistry {
    fn drop(&mut self) {
        let rings_by_port = self
            .rings_by_port
            .get_mut()
            .unwrap_or_else(|cause| cause.into_inner());
        for registration in rings_by_port.values().filter_map(Weak::upgrade) {
            registration.stop_accepting_consumers();
        }
    }
}
