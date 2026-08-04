use super::notification;
use super::reader::ReaderClaim;
use crate::mapping::{self, MappedMemory, SharedMemory};
use crate::ring::reader::ReaderRegistry;
use std::io;
use std::sync::Arc;

/// Keeps a named mapping and its reader slots registered with the server.
pub(crate) struct RegisteredRing {
    shared_memory: SharedMemory,
    pub(super) memory: Arc<MappedMemory>,
    pub(super) readers: Arc<ReaderRegistry<Arc<notification::Producer>>>,
}

impl RegisteredRing {
    /// Allocates the mapping and registers it to accept readers.
    pub(crate) fn create(minimum_capacity: usize) -> io::Result<Arc<Self>> {
        let (shared_memory, memory) = mapping::create(minimum_capacity)?;
        let memory = Arc::new(memory);
        let readers = ReaderRegistry::new(Arc::clone(&memory));
        Ok(Arc::new(Self {
            shared_memory,
            memory,
            readers,
        }))
    }

    /// Returns the platform mapping handle transferred during attachment.
    pub(crate) fn shared_memory(&self) -> &SharedMemory {
        &self.shared_memory
    }

    /// Reserves one reader slot for an attachment handled by the server.
    pub(crate) fn claim_reader(self: &Arc<Self>) -> io::Result<ReaderClaim> {
        self.readers.claim()
    }
}
