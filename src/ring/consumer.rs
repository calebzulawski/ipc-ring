//! Consumer-side cursor management, waiting, and raw readable reservations.

use super::state::{input_distance, set_waiter_bit, used, valid_len};
use super::wake::ConsumerWake;
use crate::cursor::Reservation;
use crate::error;
use crate::ring::mapping::MappedMemory;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) trait ConsumerState {
    type Notification: ConsumerWake;

    fn memory(&self) -> &Arc<MappedMemory>;
    fn slot(&self) -> u8;
    fn notification(&self) -> &Self::Notification;
    fn notification_mut(&mut self) -> &mut Self::Notification;
}

pub(crate) fn position<C: ConsumerState>(consumer: &C) -> u64 {
    consumer.memory().header().read_positions[consumer.slot() as usize].load(Ordering::Acquire)
}

fn readable_len<C: ConsumerState>(consumer: &C) -> io::Result<usize> {
    let read_position = position(consumer);
    let write_position = consumer
        .memory()
        .header()
        .write_position
        .load(Ordering::Acquire);
    used(write_position, read_position, consumer.memory().capacity())
}

fn requested_len<C: ConsumerState>(
    consumer: &C,
    start: u64,
    minimum: usize,
) -> io::Result<(usize, usize)> {
    let capacity = consumer.memory().capacity();
    valid_len(minimum, capacity)?;
    let offset = input_distance(start, position(consumer), capacity)?;
    let required = offset
        .checked_add(minimum)
        .filter(|required| *required <= capacity)
        .ok_or_else(error::invalid_length)?;
    Ok((offset, required))
}

fn reservation<C: ConsumerState>(consumer: &C, start: u64, len: usize) -> Reservation {
    let capacity = consumer.memory().capacity();
    let offset = (start & (capacity as u64 - 1)) as usize;
    // SAFETY: mappings contain two adjacent payload views, and `len` was
    // validated to remain within one capacity from `start`.
    let ptr = unsafe { consumer.memory().payload().add(offset) } as *const u8;
    Reservation {
        position: start,
        ptr,
        len,
    }
}

pub(crate) fn try_reserve_at<C: ConsumerState>(
    consumer: &mut C,
    start: u64,
    minimum: usize,
) -> io::Result<Reservation> {
    let (offset, required) = requested_len(consumer, start, minimum)?;
    let available = readable_len(consumer)?;
    if available < required {
        return Err(error::would_block());
    }
    Ok(reservation(consumer, start, available - offset))
}

pub(crate) async fn reserve_at<C: ConsumerState>(
    consumer: &mut C,
    start: u64,
    minimum: usize,
) -> io::Result<Reservation> {
    let (offset, required) = requested_len(consumer, start, minimum)?;
    let available = wait_for_data(consumer, required).await?;
    Ok(reservation(consumer, start, available - offset))
}

/// Sets this reader's waiter bit, rechecks, then sleeps if data is still short.
async fn wait_for_data<C: ConsumerState>(consumer: &mut C, required: usize) -> io::Result<usize> {
    let bit = 1_u64 << consumer.slot();
    loop {
        let available = readable_len(consumer)?;
        if available >= required {
            return Ok(available);
        }
        let memory = Arc::clone(consumer.memory());
        let _waiter_bit = set_waiter_bit(&memory.header().data_waiters, bit);
        let available = readable_len(consumer)?;
        if available >= required {
            return Ok(available);
        }
        consumer.notification_mut().wait_for_data().await?;
    }
}

pub(crate) unsafe fn advance_to<C: ConsumerState>(consumer: &mut C, target: u64) -> io::Result<()> {
    let current = position(consumer);
    let amount = input_distance(target, current, consumer.memory().capacity())?;
    if amount == 0 {
        return Ok(());
    }
    if amount > readable_len(consumer)? {
        return Err(error::invalid_input(
            "advance position exceeds the readable extent",
        ));
    }

    consumer.memory().header().read_positions[consumer.slot() as usize]
        .store(target, Ordering::Release);
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
