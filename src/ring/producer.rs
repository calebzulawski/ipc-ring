//! Producer-side reservations, cursor publication, and reader wakeups.

use super::PendingView;
use super::reader::{ActiveReaders, ReaderConnection, ReaderRegistry};
use super::state::{set_waiter_bit, used, valid_len};
use super::wake::ProducerWake;
use crate::error;
use crate::mapping::MappedMemory;
use arc_swap::{ArcSwap, Cache};
use std::io;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) type ReaderCache<N> = Cache<Arc<ArcSwap<ActiveReaders<N>>>, Arc<ActiveReaders<N>>>;

/// Whether a request fits or which reader can make it fit.
enum WritableState<N> {
    Available(usize),
    BlockedByReader(Arc<ReaderConnection<N>>),
}

pub(crate) trait ProducerEndpoint {
    type Notification: ProducerWake;
    const SURVIVES_WITHOUT_READERS: bool;

    fn memory(&self) -> &Arc<MappedMemory>;
    fn registry(&self) -> &Arc<ReaderRegistry<Self::Notification>>;
    fn reader_cache(&mut self) -> &mut ReaderCache<Self::Notification>;
    fn pending(&self) -> Option<PendingView>;
    fn pending_mut(&mut self) -> &mut Option<PendingView>;
}

fn write_position<P: ProducerEndpoint>(producer: &P) -> u64 {
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
        let buffered = used(write_position, read_position, capacity)?;
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

fn writable_state<P: ProducerEndpoint>(
    producer: &mut P,
    minimum: usize,
) -> io::Result<WritableState<P::Notification>> {
    let readers = producer.reader_cache().load().clone();
    if readers.bitmap == 0 && !P::SURVIVES_WITHOUT_READERS {
        return Err(crate::error::peer_disconnected());
    }
    scan_writable_state(producer.memory(), &readers, minimum)
}

fn install_pending<P: ProducerEndpoint>(producer: &mut P, available: usize) {
    let capacity = producer.memory().capacity();
    let pending = PendingView::new(write_position(producer), available, capacity);
    *producer.pending_mut() = Some(pending);
}

pub(crate) fn try_reserve<P: ProducerEndpoint>(producer: &mut P, minimum: usize) -> io::Result<()> {
    *producer.pending_mut() = None;
    valid_len(minimum, producer.memory().capacity())?;
    match writable_state(producer, minimum)? {
        WritableState::Available(available) => {
            install_pending(producer, available);
            Ok(())
        }
        WritableState::BlockedByReader(_) => Err(error::would_block()),
    }
}

pub(crate) async fn reserve<P: ProducerEndpoint>(
    producer: &mut P,
    minimum: usize,
) -> io::Result<()> {
    *producer.pending_mut() = None;
    valid_len(minimum, producer.memory().capacity())?;
    let available = wait_for_space(producer, minimum).await?;
    install_pending(producer, available);
    Ok(())
}

/// Waits on one blocking reader and rechecks after its next notification.
async fn wait_for_space<P: ProducerEndpoint>(
    producer: &mut P,
    minimum: usize,
) -> io::Result<usize> {
    loop {
        let reader = match writable_state(producer, minimum)? {
            WritableState::Available(available) => return Ok(available),
            WritableState::BlockedByReader(reader) => reader,
        };

        let registry = Arc::clone(producer.registry());
        let memory = Arc::clone(producer.memory());
        let bit = 1_u64 << reader.slot();
        let _waiter_bit = set_waiter_bit(&memory.header().space_waiters, bit);

        let still_blocking = match writable_state(producer, minimum)? {
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

pub(crate) fn view<P: ProducerEndpoint>(producer: &P) -> &[u8] {
    let Some(pending) = producer.pending() else {
        return &[];
    };
    // SAFETY: the pending span was validated against all active read cursors.
    unsafe { slice::from_raw_parts(producer.memory().payload().add(pending.offset), pending.len) }
}

pub(crate) fn view_mut<P: ProducerEndpoint>(producer: &mut P) -> &mut [u8] {
    let Some(pending) = producer.pending() else {
        return &mut [];
    };
    // SAFETY: the unique producer borrow owns the validated double-mapped span.
    unsafe {
        slice::from_raw_parts_mut(producer.memory().payload().add(pending.offset), pending.len)
    }
}

pub(crate) fn advance<P: ProducerEndpoint>(producer: &mut P, amount: usize) -> io::Result<()> {
    let pending = producer.pending_mut().take();
    if amount == 0 {
        return Ok(());
    }
    let Some(pending) = pending.filter(|pending| amount <= pending.len) else {
        return Err(error::invalid_length());
    };
    publish_data(producer, pending.position, amount)
}

/// Makes advanced bytes visible and wakes readers waiting for data.
fn publish_data<P: ProducerEndpoint>(
    producer: &mut P,
    position: u64,
    amount: usize,
) -> io::Result<()> {
    let readers = producer.reader_cache().load().clone();
    if !P::SURVIVES_WITHOUT_READERS && readers.bitmap == 0 {
        return Err(crate::error::peer_disconnected());
    }
    producer
        .memory()
        .header()
        .write_position
        .store(position.wrapping_add(amount as u64), Ordering::Release);

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
