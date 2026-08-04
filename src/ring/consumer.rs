//! Consumer-side cursor management, waiting, and readable views.

use super::PendingView;
use super::state::{set_waiter_bit, used, valid_len};
use super::wake::ConsumerWake;
use crate::error;
use crate::mapping::MappedMemory;
use std::io;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;
pub(crate) trait ConsumerEndpoint {
    type Notification: ConsumerWake;

    fn memory(&self) -> &Arc<MappedMemory>;
    fn slot(&self) -> u8;
    fn notification(&self) -> &Self::Notification;
    fn notification_mut(&mut self) -> &mut Self::Notification;
    fn pending(&self) -> Option<PendingView>;
    fn pending_mut(&mut self) -> &mut Option<PendingView>;
}

fn read_position<C: ConsumerEndpoint>(consumer: &C) -> u64 {
    consumer.memory().header().read_positions[consumer.slot() as usize].load(Ordering::Acquire)
}

fn readable_len<C: ConsumerEndpoint>(consumer: &C) -> io::Result<usize> {
    let read_position = read_position(consumer);
    let write_position = consumer
        .memory()
        .header()
        .write_position
        .load(Ordering::Acquire);
    used(write_position, read_position, consumer.memory().capacity())
}

fn install_pending<C: ConsumerEndpoint>(consumer: &mut C, available: usize) {
    let capacity = consumer.memory().capacity();
    let pending = PendingView::new(read_position(consumer), available, capacity);
    *consumer.pending_mut() = Some(pending);
}

pub(crate) fn try_reserve<C: ConsumerEndpoint>(consumer: &mut C, minimum: usize) -> io::Result<()> {
    *consumer.pending_mut() = None;
    valid_len(minimum, consumer.memory().capacity())?;
    let available = readable_len(consumer)?;
    if available < minimum {
        return Err(error::would_block());
    }
    install_pending(consumer, available);
    Ok(())
}

pub(crate) async fn reserve<C: ConsumerEndpoint>(
    consumer: &mut C,
    minimum: usize,
) -> io::Result<()> {
    *consumer.pending_mut() = None;
    valid_len(minimum, consumer.memory().capacity())?;
    let available = wait_for_data(consumer, minimum).await?;
    install_pending(consumer, available);
    Ok(())
}

/// Sets this reader's waiter bit, rechecks, then sleeps if data is still short.
async fn wait_for_data<C: ConsumerEndpoint>(consumer: &mut C, minimum: usize) -> io::Result<usize> {
    let bit = 1_u64 << consumer.slot();
    loop {
        let available = readable_len(consumer)?;
        if available >= minimum {
            return Ok(available);
        }
        let memory = Arc::clone(consumer.memory());
        let _waiter_bit = set_waiter_bit(&memory.header().data_waiters, bit);
        let available = readable_len(consumer)?;
        if available >= minimum {
            return Ok(available);
        }
        consumer.notification_mut().wait_for_data().await?;
    }
}

pub(crate) fn view<C: ConsumerEndpoint>(consumer: &C) -> &[u8] {
    let Some(pending) = consumer.pending() else {
        return &[];
    };
    // SAFETY: the pending span is published and protected by this reader cursor.
    unsafe { slice::from_raw_parts(consumer.memory().payload().add(pending.offset), pending.len) }
}

pub(crate) fn advance<C: ConsumerEndpoint>(consumer: &mut C, amount: usize) -> io::Result<()> {
    let pending = consumer.pending_mut().take();
    if amount == 0 {
        return Ok(());
    }
    let Some(pending) = pending.filter(|pending| amount <= pending.len) else {
        return Err(error::invalid_length());
    };
    release_space(consumer, pending.position, amount)
}

/// Stores the new read position before waking a producer waiting on this slot.
fn release_space<C: ConsumerEndpoint>(
    consumer: &C,
    position: u64,
    amount: usize,
) -> io::Result<()> {
    consumer.memory().header().read_positions[consumer.slot() as usize]
        .store(position.wrapping_add(amount as u64), Ordering::Release);
    let bit = 1_u64 << consumer.slot();
    if consumer
        .memory()
        .header()
        .space_waiters
        .fetch_and(!bit, Ordering::AcqRel)
        & bit
        != 0
    {
        consumer.notification().notify_space()?;
    }
    Ok(())
}
