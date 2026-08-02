//! Reserves reader slots during attachment and publishes completed connections.

use super::{ReaderConnection, SlotLease};
use crate::local_socket::ProducerStream;
use crate::mapping::MappedMemory;
use crate::ring::MAX_READERS;
use crate::ring::notification::{ProducerIpcNotification, ProducerNotification, pending_ipc};
use arc_swap::ArcSwap;
use scopeguard::ScopeGuard;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// One coherent set of readers used by the producer without locking.
#[derive(Clone)]
pub(in crate::ring) struct ActiveReaders {
    pub(in crate::ring) bitmap: u64,
    pub(in crate::ring) connections: [Option<Arc<ReaderConnection>>; MAX_READERS],
}

impl Default for ActiveReaders {
    fn default() -> Self {
        Self {
            bitmap: 0,
            connections: std::array::from_fn(|_| None),
        }
    }
}

/// Tracks which connection owns each reader slot.
pub(crate) struct ReaderRegistry {
    memory: Arc<MappedMemory>,
    reservations: Arc<AtomicU64>,
    active_readers: Arc<ArcSwap<ActiveReaders>>,
    /// Named rings remain writable without readers; anonymous rings do not.
    producer_survives_without_readers: bool,
}

impl ReaderRegistry {
    /// Creates a registry for a ring published by the IPC server.
    pub(crate) fn named(memory: Arc<MappedMemory>) -> Arc<Self> {
        Arc::new(Self {
            memory,
            reservations: Arc::new(AtomicU64::new(0)),
            active_readers: Arc::new(ArcSwap::from_pointee(ActiveReaders::default())),
            producer_survives_without_readers: true,
        })
    }

    /// Creates a registry containing the anonymous ring's sole reader.
    pub(in crate::ring) fn process_local(
        memory: Arc<MappedMemory>,
        notification: ProducerNotification,
    ) -> (Arc<Self>, LocalReaderGuard) {
        let reservations = Arc::new(AtomicU64::new(1));
        let reader = Arc::new(ReaderConnection {
            lease: SlotLease::claimed(Arc::clone(&reservations), 1),
            notification,
        });
        let active_readers = ActiveReaders {
            bitmap: 1,
            connections: std::array::from_fn(|slot| (slot == 0).then(|| Arc::clone(&reader))),
        };
        let registry = Arc::new(Self {
            memory,
            reservations,
            active_readers: Arc::new(ArcSwap::from_pointee(active_readers)),
            producer_survives_without_readers: false,
        });
        let guard = LocalReaderGuard {
            registry: Arc::downgrade(&registry),
            reader: Arc::downgrade(&reader),
        };
        (registry, guard)
    }

    /// Clones the mapping whose reader slots this registry manages.
    pub(in crate::ring) fn memory(&self) -> Arc<MappedMemory> {
        Arc::clone(&self.memory)
    }

    /// Reserves a slot while the client maps and validates the shared memory.
    pub(crate) fn claim(self: &Arc<Self>) -> io::Result<ReaderClaim> {
        let lease = SlotLease::reserve(Arc::clone(&self.reservations))?;
        let (notification, pending_notification) = pending_ipc();
        let claimed = ClaimedReader {
            registry: Arc::clone(self),
            reader: Arc::new(ReaderConnection {
                lease,
                notification,
            }),
        };
        Ok(ReaderClaim {
            claimed: scopeguard::guard(claimed, rollback_claim as fn(ClaimedReader)),
            pending_notification,
        })
    }

    /// Publishes a completed reader starting at the current write cursor.
    fn activate(&self, reader: &Arc<ReaderConnection>) {
        let slot = reader.slot();
        let bit = 1_u64 << slot;
        let write_position = self.memory.header().write_position.load(Ordering::Acquire);
        self.memory.header().read_positions[slot].store(write_position, Ordering::Release);
        self.memory
            .header()
            .data_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.memory
            .header()
            .space_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.active_readers.rcu(|current| {
            let mut updated = (**current).clone();
            assert!(
                updated.connections[slot].is_none(),
                "reserved reader slot was already active"
            );
            updated.connections[slot] = Some(Arc::clone(reader));
            updated.bitmap |= bit;
            Arc::new(updated)
        });
    }

    /// Removes this exact connection without disturbing a reused slot.
    pub(in crate::ring) fn disconnect(&self, reader: &Arc<ReaderConnection>) {
        let slot = reader.slot();
        let bit = 1_u64 << slot;
        reader.notification.close();
        let previous = self.active_readers.rcu(|current| {
            let matches = current.connections[slot]
                .as_ref()
                .is_some_and(|stored| Arc::ptr_eq(stored, reader));
            if !matches {
                return Arc::clone(current);
            }

            let mut updated = (**current).clone();
            updated.connections[slot] = None;
            updated.bitmap &= !bit;
            Arc::new(updated)
        });
        let removed = previous.connections[slot]
            .as_ref()
            .is_some_and(|stored| Arc::ptr_eq(stored, reader));
        if !removed {
            return;
        }

        self.memory
            .header()
            .data_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        self.memory
            .header()
            .space_waiters
            .fetch_and(!bit, Ordering::AcqRel);
        reader.release_slot();
    }

    /// Clones the shared membership source used by a producer-owned cache.
    pub(in crate::ring) fn active_readers(&self) -> Arc<ArcSwap<ActiveReaders>> {
        Arc::clone(&self.active_readers)
    }

    pub(in crate::ring) fn producer_survives_without_readers(&self) -> bool {
        self.producer_survives_without_readers
    }

    /// Takes the reader bits armed before the latest publication.
    pub(crate) fn take_data_waiters(&self) -> u64 {
        self.memory.header().data_waiters.swap(0, Ordering::AcqRel)
    }
}

/// Removes an anonymous reader from its registry when the consumer is dropped.
pub(crate) struct LocalReaderGuard {
    registry: Weak<ReaderRegistry>,
    reader: Weak<ReaderConnection>,
}

impl Drop for LocalReaderGuard {
    fn drop(&mut self) {
        if let (Some(registry), Some(reader)) = (self.registry.upgrade(), self.reader.upgrade()) {
            registry.disconnect(&reader);
        }
    }
}

struct ClaimedReader {
    registry: Arc<ReaderRegistry>,
    reader: Arc<ReaderConnection>,
}

fn rollback_claim(claimed: ClaimedReader) {
    claimed.registry.disconnect(&claimed.reader);
}

type ClaimedReaderGuard = ScopeGuard<ClaimedReader, fn(ClaimedReader)>;

/// Reserved reader slot that is disconnected unless its stream is installed.
pub(crate) struct ReaderClaim {
    claimed: ClaimedReaderGuard,
    pending_notification: Arc<ProducerIpcNotification>,
}

impl ReaderClaim {
    /// Returns the slot sent in the successful attachment handshake.
    pub(crate) fn slot(&self) -> u8 {
        self.claimed.reader.slot() as u8
    }

    /// Activates this reader immediately before the final handshake message.
    pub(crate) fn activate(&mut self) {
        self.claimed.registry.activate(&self.claimed.reader);
    }

    /// Stores the acknowledged stream and makes this reader permanent.
    pub(crate) fn finish(self, stream: ProducerStream) -> io::Result<()> {
        self.pending_notification.install(stream)?;
        // Stream installation commits the claim; the registry now owns the reader.
        drop(ScopeGuard::into_inner(self.claimed));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ReaderRegistry;
    use crate::mapping;
    use crate::ring::MAX_READERS;
    use std::io::ErrorKind;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn registry() -> Arc<ReaderRegistry> {
        let (_shared_memory, memory) = mapping::create(mapping::minimum_capacity()).unwrap();
        ReaderRegistry::named(Arc::new(memory))
    }

    #[test]
    fn incomplete_claims_reserve_and_release_slots_without_changing_membership() {
        let registry = registry();
        let first = registry.claim().unwrap();
        let second = registry.claim().unwrap();
        assert_eq!(first.slot(), 0);
        assert_eq!(second.slot(), 1);

        drop(first);
        assert_eq!(registry.claim().unwrap().slot(), 0);
    }

    #[test]
    fn active_claims_roll_back_when_installation_does_not_finish() {
        let registry = registry();
        let mut claim = registry.claim().unwrap();
        claim.activate();
        drop(claim);

        assert_eq!(registry.claim().unwrap().slot(), 0);
    }

    #[test]
    fn concurrent_claims_reserve_every_slot_exactly_once() {
        let registry = registry();
        let start = Barrier::new(MAX_READERS);
        let claims = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(MAX_READERS);
            for _ in 0..MAX_READERS {
                let registry = Arc::clone(&registry);
                let start = &start;
                handles.push(scope.spawn(move || {
                    start.wait();
                    registry.claim().unwrap()
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let mut slots = claims.iter().map(|claim| claim.slot()).collect::<Vec<_>>();
        slots.sort_unstable();
        assert_eq!(slots, (0..MAX_READERS as u8).collect::<Vec<_>>());
        assert_eq!(
            registry.claim().err().unwrap().kind(),
            ErrorKind::ResourceBusy
        );

        drop(claims);
        assert_eq!(registry.reservations.load(Ordering::Acquire), 0);
    }

    #[test]
    fn concurrent_membership_updates_do_not_lose_other_slots() {
        let registry = registry();
        let claims = (0..MAX_READERS)
            .map(|_| registry.claim().unwrap())
            .collect::<Vec<_>>();
        let claims = thread::scope(|scope| {
            let handles = claims
                .into_iter()
                .map(|mut claim| {
                    scope.spawn(move || {
                        claim.activate();
                        claim
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let active = registry.active_readers.load_full();
        assert_eq!(active.bitmap, u64::MAX);
        assert!(active.connections.iter().all(Option::is_some));

        thread::scope(|scope| {
            for claim in claims {
                scope.spawn(move || drop(claim));
            }
        });
        let active = registry.active_readers.load_full();
        assert_eq!(active.bitmap, 0);
        assert!(active.connections.iter().all(Option::is_none));
        assert_eq!(registry.reservations.load(Ordering::Acquire), 0);
    }

    #[test]
    fn stale_disconnect_cannot_disturb_a_reused_slot() {
        let registry = registry();
        let mut old_claim = registry.claim().unwrap();
        let old_reader = Arc::clone(&old_claim.claimed.reader);
        old_claim.activate();
        registry.disconnect(&old_reader);

        let mut replacement = registry.claim().unwrap();
        assert_eq!(replacement.slot(), 0);
        let replacement_reader = Arc::clone(&replacement.claimed.reader);
        replacement.activate();

        drop(old_claim);
        registry
            .memory
            .header()
            .data_waiters
            .store(1, Ordering::Release);
        registry
            .memory
            .header()
            .space_waiters
            .store(1, Ordering::Release);
        registry.disconnect(&old_reader);

        let active = registry.active_readers.load_full();
        assert_eq!(active.bitmap & 1, 1);
        assert!(
            active.connections[0]
                .as_ref()
                .is_some_and(|stored| Arc::ptr_eq(stored, &replacement_reader))
        );
        assert_eq!(
            registry
                .memory
                .header()
                .data_waiters
                .load(Ordering::Acquire),
            1
        );
        assert_eq!(
            registry
                .memory
                .header()
                .space_waiters
                .load(Ordering::Acquire),
            1
        );

        let remaining = (1..MAX_READERS)
            .map(|_| registry.claim().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            registry.claim().err().unwrap().kind(),
            ErrorKind::ResourceBusy
        );
        drop(remaining);
        drop(replacement);
    }
}
