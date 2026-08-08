//! Producer-side raw reservations, cursor publication, and reader wakeups.

use super::reader::{ActiveReaders, ReaderConnection, ReaderRegistry};
use super::state::{input_distance, set_waiter_bit, valid_len};
use super::wake::ProducerWake;
use crate::cursor::Reservation;
use crate::error;
use crate::ring::mapping::MappedMemory;
use arc_swap::{ArcSwap, Cache};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) type ReaderCache<N> = Cache<Arc<ArcSwap<ActiveReaders<N>>>, Arc<ActiveReaders<N>>>;

/// Whether a request fits or which reader can make it fit.
enum WritableState<N> {
    Available(usize),
    BlockedByReader(Arc<ReaderConnection<N>>),
}

pub(crate) trait ProducerState {
    type Notification: ProducerWake;
    const SURVIVES_WITHOUT_READERS: bool;

    fn memory(&self) -> &Arc<MappedMemory>;
    fn registry(&self) -> &Arc<ReaderRegistry<Self::Notification>>;
    fn reader_cache(&mut self) -> &mut ReaderCache<Self::Notification>;
}

pub(crate) fn position<P: ProducerState>(producer: &P) -> u64 {
    producer
        .memory()
        .header()
        .write_position
        .load(Ordering::Relaxed)
}

/// Scans the current reader set, stopping at the first reader that blocks.
fn scan_writable_state<N: ProducerWake>(
    memory: &MappedMemory,
    readers: &ActiveReaders<N>,
    minimum: usize,
) -> io::Result<WritableState<N>> {
    let capacity = memory.capacity();
    let write_position = memory.header().write_position.load(Ordering::Acquire);

    if readers.bitmap == 0 {
        return Ok(WritableState::Available(capacity));
    }

    let blocking_distance = capacity - minimum;
    let mut active = readers.bitmap;
    let mut greatest_buffered = 0;
    while active != 0 {
        let slot = active.trailing_zeros() as usize;
        active &= !(1_u64 << slot);
        let read_position = memory.header().read_positions[slot].load(Ordering::Acquire);
        let buffered = super::state::used(write_position, read_position, capacity)?;
        if buffered > blocking_distance {
            let reader = readers.connections[slot]
                .as_ref()
                .expect("active reader slot has a connection");
            return Ok(WritableState::BlockedByReader(Arc::clone(reader)));
        }
        greatest_buffered = greatest_buffered.max(buffered);
    }

    Ok(WritableState::Available(capacity - greatest_buffered))
}

fn writable_state<P: ProducerState>(
    producer: &mut P,
    minimum: usize,
) -> io::Result<WritableState<P::Notification>> {
    let readers = producer.reader_cache().load().clone();
    if readers.bitmap == 0 && !P::SURVIVES_WITHOUT_READERS {
        return Err(crate::error::peer_disconnected());
    }
    scan_writable_state(producer.memory(), &readers, minimum)
}

fn requested_len<P: ProducerState>(
    producer: &P,
    start: u64,
    minimum: usize,
) -> io::Result<(usize, usize)> {
    let capacity = producer.memory().capacity();
    valid_len(minimum, capacity)?;
    let offset = input_distance(start, position(producer), capacity)?;
    let required = offset
        .checked_add(minimum)
        .filter(|required| *required <= capacity)
        .ok_or_else(error::invalid_length)?;
    Ok((offset, required))
}

fn reservation<P: ProducerState>(producer: &P, start: u64, len: usize) -> Reservation {
    let capacity = producer.memory().capacity();
    let offset = (start & (capacity as u64 - 1)) as usize;
    // SAFETY: mappings contain two adjacent payload views, and `len` was
    // validated to remain within one capacity from `start`.
    let ptr = unsafe { producer.memory().payload().add(offset) } as *const u8;
    Reservation {
        position: start,
        ptr,
        len,
    }
}

pub(crate) fn try_reserve_at<P: ProducerState>(
    producer: &mut P,
    start: u64,
    minimum: usize,
) -> io::Result<Reservation> {
    let (offset, required) = requested_len(producer, start, minimum)?;
    match writable_state(producer, required)? {
        WritableState::Available(available) => Ok(reservation(producer, start, available - offset)),
        WritableState::BlockedByReader(_) => Err(error::would_block()),
    }
}

pub(crate) async fn reserve_at<P: ProducerState>(
    producer: &mut P,
    start: u64,
    minimum: usize,
) -> io::Result<Reservation> {
    let (offset, required) = requested_len(producer, start, minimum)?;
    let available = wait_for_space(producer, required).await?;
    Ok(reservation(producer, start, available - offset))
}

/// Waits on one blocking reader and rechecks after its next notification.
async fn wait_for_space<P: ProducerState>(producer: &mut P, required: usize) -> io::Result<usize> {
    loop {
        let reader = match writable_state(producer, required)? {
            WritableState::Available(available) => return Ok(available),
            WritableState::BlockedByReader(reader) => reader,
        };

        let registry = Arc::clone(producer.registry());
        let memory = Arc::clone(producer.memory());
        let bit = 1_u64 << reader.slot();
        let _waiter_bit = set_waiter_bit(&memory.header().space_waiters, bit);

        let still_blocking = match writable_state(producer, required)? {
            WritableState::Available(available) => return Ok(available),
            WritableState::BlockedByReader(reader) => reader,
        };
        if !Arc::ptr_eq(&reader, &still_blocking) {
            continue;
        }

        if reader.notification.wait_for_space().await.is_err() {
            registry.disconnect(&reader);
        }
    }
}

pub(crate) unsafe fn advance_to<P: ProducerState>(producer: &mut P, target: u64) -> io::Result<()> {
    let current = position(producer);
    let amount = input_distance(target, current, producer.memory().capacity())?;
    if amount == 0 {
        return Ok(());
    }
    if matches!(
        writable_state(producer, amount)?,
        WritableState::BlockedByReader(_)
    ) {
        return Err(error::invalid_input(
            "advance position exceeds the writable extent",
        ));
    }
    publish_data(producer, target)
}

/// Makes advanced bytes visible and wakes readers waiting for data.
fn publish_data<P: ProducerState>(producer: &mut P, target: u64) -> io::Result<()> {
    let readers = producer.reader_cache().load().clone();
    if !P::SURVIVES_WITHOUT_READERS && readers.bitmap == 0 {
        return Err(crate::error::peer_disconnected());
    }
    producer
        .memory()
        .header()
        .write_position
        .store(target, Ordering::Release);

    let waiting = producer.registry().take_data_waiters();
    if waiting != 0 {
        // Route captured bits through membership loaded after the take. A bit
        // set by a later attachment remains armed for the next write.
        let readers = producer.reader_cache().load().clone();
        notify_data_waiters(producer.registry(), &readers, waiting);
    }
    Ok(())
}

fn notify_data_waiters<N: ProducerWake>(
    registry: &ReaderRegistry<N>,
    readers: &ActiveReaders<N>,
    mut waiting: u64,
) {
    let mut failed = Vec::new();
    while waiting != 0 {
        let slot = waiting.trailing_zeros() as usize;
        waiting &= !(1_u64 << slot);
        let Some(reader) = readers.connections[slot].as_ref().map(Arc::clone) else {
            continue;
        };
        if reader.notification.notify_data().is_err() {
            failed.push(reader);
        }
    }
    for reader in failed {
        registry.disconnect(&reader);
    }
}
