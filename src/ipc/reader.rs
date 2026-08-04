//! IPC reader admission across the attachment handshake.

use super::notification;
use super::socket::ProducerStream;
use crate::ring::reader::{ReaderConnection, ReaderRegistry};
use scopeguard::ScopeGuard;
use std::io;
use std::sync::Arc;

type Registry = ReaderRegistry<Arc<notification::Producer>>;

impl Registry {
    /// Reserves a slot while the client maps and validates the shared memory.
    pub(crate) fn claim(self: &Arc<Self>) -> io::Result<ReaderClaim> {
        let pending_notification = Arc::new(notification::Producer::pending());
        let reader = self.reserve_reader(Arc::clone(&pending_notification))?;
        let claimed = ClaimedReader {
            registry: Arc::clone(self),
            reader,
        };
        Ok(ReaderClaim {
            claimed: scopeguard::guard(claimed, rollback_claim as fn(ClaimedReader)),
            pending_notification,
        })
    }
}

struct ClaimedReader {
    registry: Arc<Registry>,
    reader: Arc<ReaderConnection<Arc<notification::Producer>>>,
}

fn rollback_claim(claimed: ClaimedReader) {
    claimed.registry.disconnect(&claimed.reader);
}

type ClaimedReaderGuard = ScopeGuard<ClaimedReader, fn(ClaimedReader)>;

/// Reserved reader slot that is disconnected unless its stream is installed.
pub(crate) struct ReaderClaim {
    claimed: ClaimedReaderGuard,
    pending_notification: Arc<notification::Producer>,
}

impl ReaderClaim {
    /// Returns the slot sent in the successful attachment handshake.
    pub(crate) fn slot(&self) -> u8 {
        self.claimed.reader.slot() as u8
    }

    /// Activates this reader immediately before the final handshake message.
    pub(crate) fn activate(&mut self) {
        let write_position = self.claimed.registry.write_position();
        self.claimed
            .registry
            .activate_at(&self.claimed.reader, write_position);
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
    use super::Registry;
    use crate::mapping;
    use crate::ring::MAX_READERS;
    use std::io::ErrorKind;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn registry() -> Arc<Registry> {
        let (_shared_memory, memory) = mapping::create(mapping::minimum_capacity()).unwrap();
        crate::ring::reader::ReaderRegistry::new(Arc::new(memory))
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
        assert_eq!(registry.reserved_slots(), 0);
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

        let active = registry.active_readers().load_full();
        assert_eq!(active.bitmap, u64::MAX);
        assert!(active.connections.iter().all(Option::is_some));

        thread::scope(|scope| {
            for claim in claims {
                scope.spawn(move || drop(claim));
            }
        });
        let active = registry.active_readers().load_full();
        assert_eq!(active.bitmap, 0);
        assert!(active.connections.iter().all(Option::is_none));
        assert_eq!(registry.reserved_slots(), 0);
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
            .memory()
            .header()
            .data_waiters
            .store(1, Ordering::Release);
        registry
            .memory()
            .header()
            .space_waiters
            .store(1, Ordering::Release);
        registry.disconnect(&old_reader);

        let active = registry.active_readers().load_full();
        assert_eq!(active.bitmap & 1, 1);
        assert!(
            active.connections[0]
                .as_ref()
                .is_some_and(|stored| Arc::ptr_eq(stored, &replacement_reader))
        );
        assert_eq!(
            registry
                .memory()
                .header()
                .data_waiters
                .load(Ordering::Acquire),
            1
        );
        assert_eq!(
            registry
                .memory()
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
