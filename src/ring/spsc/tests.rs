#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::WAITING;
use super::{ABI_VERSION, CONSUMER_CLAIMED, CONSUMER_FREE, Consumer, Header, Producer, anonymous};
use crate::platform;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn unique_name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{label}{:x}{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn creation_rounds_up_minimum_capacity() {
    let granularity = platform::minimum_capacity();

    let (producer, consumer) = anonymous(1).unwrap();
    assert_eq!(producer.capacity(), granularity);
    assert_eq!(consumer.capacity(), granularity);

    let requested = granularity + 1;
    let name = unique_name("capacity");
    let producer = Producer::bind(&name, requested).unwrap();
    let consumer = Consumer::connect(&name).unwrap();
    let actual = producer.capacity();
    assert_eq!(producer.ring.header().capacity as usize, actual);
    assert!(actual >= requested);
    assert!(actual.is_power_of_two());
    assert_eq!(actual % granularity, 0);
    assert_eq!(consumer.capacity(), actual);

    assert_eq!(anonymous(0).err().unwrap().kind(), ErrorKind::InvalidInput);
}

#[test]
fn anonymous_claims_both_endpoints() {
    let (producer, consumer) = anonymous(platform::minimum_capacity()).unwrap();
    assert_eq!(producer.capacity(), consumer.capacity());
    assert_eq!(
        producer
            .ring
            .header()
            .consumer_claim
            .load(Ordering::Acquire),
        CONSUMER_CLAIMED
    );
    drop(consumer);
    assert_eq!(
        producer
            .ring
            .header()
            .consumer_claim
            .load(Ordering::Acquire),
        CONSUMER_FREE
    );
}

#[test]
fn alias_boundary_and_partial_grants() {
    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    let cap = producer.capacity();
    producer.reserve(cap - 4).unwrap().commit(cap - 4).unwrap();
    consumer.inspect(cap - 4).unwrap().release(cap - 4).unwrap();
    drop(producer.reserve(8).unwrap());
    assert_eq!(consumer.readable_len().unwrap(), 0);

    let mut grant = producer.reserve(8).unwrap();
    grant.as_mut_slice().copy_from_slice(b"abcdefgh");
    grant.commit(8).unwrap();
    let grant = consumer.inspect(8).unwrap();
    assert_eq!(grant.as_slice(), b"abcdefgh");
    grant.release(3).unwrap();
    assert_eq!(consumer.readable_len().unwrap(), 5);
}

#[test]
fn blocking_wait_wakes() {
    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    let child = thread::spawn(move || {
        let grant = consumer.inspect(4).unwrap();
        assert_eq!(grant.as_slice(), b"wake");
        grant.release(4).unwrap();
    });

    let mut grant = producer.reserve(4).unwrap();
    grant.as_mut_slice().copy_from_slice(b"wake");
    grant.commit(4).unwrap();
    child.join().unwrap();
}

#[test]
fn blocked_producer_wakes_when_space_is_released() {
    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    let cap = producer.capacity();
    producer.reserve(cap).unwrap().commit(cap).unwrap();

    let child = thread::spawn(move || {
        producer.reserve(1).unwrap().commit(1).unwrap();
    });

    #[cfg(target_os = "linux")]
    while consumer
        .ring
        .header()
        .space_wait_state
        .load(Ordering::Acquire)
        != WAITING as u32
    {
        thread::yield_now();
    }
    #[cfg(target_os = "macos")]
    while consumer
        .ring
        .header()
        .space_wait_state
        .load(Ordering::Acquire)
        != WAITING
    {
        thread::yield_now();
    }

    consumer.inspect(1).unwrap().release(1).unwrap();
    child.join().unwrap();
    assert_eq!(consumer.readable_len().unwrap(), cap);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn named_connection_rejects_nonexact_shared_memory_length() {
    let name = unique_name("len");
    let producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    let wrong_length = platform::minimum_capacity() + producer.capacity() * 2;
    producer
        .ring
        .truncate_shared_memory_for_test(wrong_length)
        .unwrap();

    let error = crate::platform::MappedRing::connect(&name)
        .err()
        .expect("nonexact shared-memory object must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn named_connection_waits_for_shared_memory_sizing() {
    let name = unique_name("size");
    let producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    producer.ring.header().version.store(0, Ordering::Release);
    producer.ring.truncate_shared_memory_for_test(0).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let child_barrier = Arc::clone(&barrier);
    let child_name = name.clone();
    let child = thread::spawn(move || {
        child_barrier.wait();
        Consumer::connect(&child_name)
    });
    barrier.wait();
    thread::sleep(Duration::from_millis(10));
    assert!(!child.is_finished());

    let shared_memory_len = platform::minimum_capacity() + producer.capacity();
    producer
        .ring
        .truncate_shared_memory_for_test(shared_memory_len)
        .unwrap();
    producer.ring.reinitialize_header_for_test(CONSUMER_FREE);

    let consumer = child.join().unwrap().unwrap();
    assert_eq!(consumer.capacity(), producer.capacity());
}

#[test]
fn missing_primary_name_is_not_found() {
    let error = Consumer::connect(&unique_name("missing")).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
}

#[test]
fn initialization_and_exact_version() {
    let fresh = Header::new(platform::minimum_capacity(), CONSUMER_FREE);
    assert_eq!(fresh.version.load(Ordering::Relaxed), 0);
    assert_eq!(fresh.consumer_claim.load(Ordering::Relaxed), CONSUMER_FREE);

    let name = unique_name("ver");
    let producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    assert_eq!(
        producer.ring.header().version.load(Ordering::Acquire),
        ABI_VERSION
    );

    producer.ring.header().version.store(0, Ordering::Release);
    let barrier = Arc::new(Barrier::new(2));
    let child_barrier = Arc::clone(&barrier);
    let child_name = name.clone();
    let child = thread::spawn(move || {
        child_barrier.wait();
        Consumer::connect(&child_name)
    });
    barrier.wait();
    thread::sleep(Duration::from_millis(10));
    assert!(!child.is_finished());
    producer
        .ring
        .header()
        .version
        .store(ABI_VERSION, Ordering::Release);
    drop(child.join().unwrap().unwrap());

    producer.ring.header().version.store(0, Ordering::Release);
    let error = crate::platform::MappedRing::connect(&name)
        .err()
        .expect("version zero must time out");
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert_eq!(
        error.to_string(),
        "ring initialization did not complete within 100 ms"
    );

    producer
        .ring
        .header()
        .version
        .store(ABI_VERSION + 1, Ordering::Release);
    let error = crate::platform::MappedRing::connect(&name)
        .err()
        .expect("unknown version must be rejected");
    assert_eq!(error.kind(), ErrorKind::Other);
    assert_eq!(error.to_string(), "unsupported IPC ring ABI version 2");
    producer
        .ring
        .header()
        .version
        .store(ABI_VERSION, Ordering::Release);
}

#[test]
fn consumer_claim_is_exclusive_and_reusable() {
    let name = unique_name("claim");
    let producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    assert_eq!(
        producer
            .ring
            .header()
            .consumer_claim
            .load(Ordering::Acquire),
        CONSUMER_FREE
    );

    let consumer = Consumer::connect(&name).unwrap();
    let error = Consumer::connect(&name).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::ResourceBusy);
    assert_eq!(error.to_string(), "a consumer is already connected");
    drop(consumer);

    let replacement = Consumer::connect(&name).unwrap();
    assert_eq!(replacement.capacity(), producer.capacity());
}

#[test]
fn concurrent_consumers_have_one_winner() {
    let name = unique_name("race");
    let _producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let name = name.clone();
            thread::spawn(move || Consumer::connect(&name))
        })
        .collect();

    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.to_string() == "a consumer is already connected")
            })
            .count(),
        7
    );
}

#[test]
fn duplicate_producer_bind_fails() {
    let name = unique_name("bind");
    let _producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    let error = Producer::bind(&name, platform::minimum_capacity())
        .err()
        .expect("duplicate bind must fail");
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert!(error.raw_os_error().is_some());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn producer_drop_unlinks_without_invalidating_consumer() {
    let name = unique_name("drop");
    let mut producer = Producer::bind(&name, platform::minimum_capacity()).unwrap();
    let mut consumer = Consumer::connect(&name).unwrap();

    let mut grant = producer.reserve(5).unwrap();
    grant.as_mut_slice().copy_from_slice(b"alive");
    grant.commit(5).unwrap();
    drop(producer);

    let grant = consumer.inspect(5).unwrap();
    assert_eq!(grant.as_slice(), b"alive");
    grant.release(5).unwrap();

    let error = Consumer::connect(&name)
        .err()
        .expect("producer drop must remove POSIX discovery name");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.raw_os_error().is_some());
}

#[test]
fn empty_full_abandoned_and_wrapping_cursors() {
    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    let cap = producer.capacity();

    assert_eq!(producer.writable_len().unwrap(), cap);
    assert_eq!(consumer.readable_len().unwrap(), 0);
    drop(producer.reserve(cap).unwrap());
    assert_eq!(consumer.readable_len().unwrap(), 0);

    let near_wrap = u64::MAX - 3;
    producer
        .ring
        .header()
        .write_position
        .store(near_wrap, Ordering::Relaxed);
    producer
        .ring
        .header()
        .read_position
        .store(near_wrap, Ordering::Relaxed);

    producer.reserve(cap).unwrap().commit(cap).unwrap();
    assert_eq!(producer.writable_len().unwrap(), 0);
    assert_eq!(consumer.readable_len().unwrap(), cap);
    consumer.inspect(cap).unwrap().release(cap).unwrap();
    assert_eq!(producer.writable_len().unwrap(), cap);
    assert_eq!(consumer.readable_len().unwrap(), 0);
}

#[test]
fn corrupt_cursor_distance_is_rejected() {
    let (producer, consumer) = anonymous(platform::minimum_capacity()).unwrap();
    let cap = producer.capacity();
    producer
        .ring
        .header()
        .write_position
        .store(cap as u64 + 1, Ordering::Relaxed);
    assert_eq!(
        consumer.readable_len().err().unwrap().kind(),
        ErrorKind::Other
    );
}

#[test]
fn nonblocking_availability_is_would_block() {
    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    assert_eq!(
        consumer.try_inspect(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );

    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .unwrap()
        .commit(capacity)
        .unwrap();
    assert_eq!(
        producer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
}

#[test]
fn validation_errors_are_invalid_input() {
    assert_eq!(
        Producer::bind("", platform::minimum_capacity())
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );

    let (mut producer, mut consumer) = anonymous(platform::minimum_capacity()).unwrap();
    assert_eq!(
        producer.try_reserve(0).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        consumer
            .try_inspect(consumer.capacity() + 1)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn wait_handshake_all_orderings() {
    #[derive(Clone, Copy, Debug, Default)]
    struct Model {
        predicate: bool,
        waiting: bool,
        asleep: bool,
        done: bool,
    }

    fn explore(waiter_step: u8, publisher_step: u8, state: Model, paths: &mut usize) {
        if waiter_step == 5 && publisher_step == 2 {
            *paths += 1;
            assert!(state.predicate && state.done && !state.asleep, "{state:?}");
            return;
        }

        if waiter_step < 5 && !state.asleep {
            let mut next = state;
            match waiter_step {
                0 => next.done = next.predicate,
                1 if !next.done => next.waiting = true,
                2 if !next.done && next.predicate => {
                    next.waiting = false;
                    next.done = true;
                }
                3 if !next.done => next.asleep = next.waiting,
                4 if !next.done => {
                    next.waiting = false;
                    next.done = next.predicate;
                }
                _ => {}
            }
            explore(waiter_step + 1, publisher_step, next, paths);
        }

        if publisher_step < 2 {
            let mut next = state;
            if publisher_step == 0 {
                next.predicate = true;
            } else {
                let observed_waiter = next.waiting;
                next.waiting = false;
                if observed_waiter {
                    next.asleep = false;
                }
            }
            explore(waiter_step, publisher_step + 1, next, paths);
        }
    }

    let mut paths = 0;
    explore(0, 0, Model::default(), &mut paths);
    assert!(paths > 0);

    let mut spurious = Model {
        waiting: true,
        ..Model::default()
    };
    spurious.waiting = false;
    assert!(!spurious.predicate && !spurious.done);

    let mut repeated = Model {
        waiting: true,
        asleep: true,
        ..Model::default()
    };
    repeated.predicate = true;
    repeated.waiting = false;
    repeated.asleep = false;
    repeated.waiting = false;
    assert!(repeated.predicate && !repeated.asleep);
}
