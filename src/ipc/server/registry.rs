use crate::ipc::Producer;
use crate::ipc::handshake;
use crate::ipc::registered::RegisteredRing;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, Weak};

/// Registers rings while the listener task remains alive.
#[derive(Clone)]
pub struct Server {
    pub(super) registry: Weak<ServerRegistry>,
}

impl Server {
    /// Creates a ring buffer named `port` with at least `minimum_capacity` bytes.
    pub fn register(
        &self,
        port: impl Into<String>,
        minimum_capacity: usize,
    ) -> io::Result<Producer> {
        self.registry
            .upgrade()
            .ok_or_else(crate::error::peer_disconnected)?
            .register_ring(port.into(), minimum_capacity)
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

    fn register_ring(&self, port: String, minimum_capacity: usize) -> io::Result<Producer> {
        handshake::validate_port(&port)?;
        let mut rings_by_port = self
            .rings_by_port
            .lock()
            .expect("server registry mutex poisoned");
        if rings_by_port.get(&port).and_then(Weak::upgrade).is_some() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }

        let registered_ring = RegisteredRing::create(minimum_capacity)?;
        rings_by_port.insert(port, Arc::downgrade(&registered_ring));
        drop(rings_by_port);

        Ok(Producer::registered(registered_ring))
    }
}
