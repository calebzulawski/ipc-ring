use super::Header;
use super::notification::{Notification, NotificationStreamSlot};
use super::state::used;
use crate::local_socket::{ConsumerStream, ProducerStream};
use crate::mapping::{self, MappedMemory, SharedMemory};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{Notify, OwnedSemaphorePermit};

/// Owns the resources needed to attach and replace a registered consumer.
pub(crate) struct RegisteredRing {
    shared_memory: SharedMemory,
    memory: Arc<MappedMemory>,
    notification_stream_slot: Arc<NotificationStreamSlot<ProducerStream>>,
}

impl RegisteredRing {
    pub(crate) fn create(minimum_capacity: usize) -> io::Result<Arc<Self>> {
        let (shared_memory, memory) = mapping::create(minimum_capacity)?;
        let memory = Arc::new(memory);
        Ok(Arc::new(Self {
            shared_memory,
            notification_stream_slot:
                NotificationStreamSlot::producer_waiting_for_notification_stream(Arc::clone(
                    &memory,
                )),
            memory,
        }))
    }

    pub(crate) fn shared_memory(&self) -> &SharedMemory {
        &self.shared_memory
    }

    pub(crate) fn claim_consumer(&self) -> io::Result<OwnedSemaphorePermit> {
        self.notification_stream_slot.try_claim_consumer()
    }

    pub(crate) async fn insert_notification_stream(
        &self,
        stream: ProducerStream,
        consumer_admission: OwnedSemaphorePermit,
    ) -> io::Result<()> {
        self.notification_stream_slot
            .insert_notification_stream_after_client_ready(stream, consumer_admission)
            .await
    }

    pub(crate) fn stop_accepting_consumers(&self) {
        self.notification_stream_slot.close_consumer_admission();
    }
}

pub(crate) struct SharedRing {
    // Notifications drop first, closing their shared stream before the mapping disappears.
    consumer_data_available_notification: Notification,
    producer_space_available_notification: Notification,
    memory: Arc<MappedMemory>,
}

impl SharedRing {
    pub(crate) fn create_anonymous(minimum_capacity: usize) -> io::Result<Self> {
        let (_shared_memory, memory) = mapping::create(minimum_capacity)?;
        Ok(Self {
            consumer_data_available_notification: Notification::ProcessLocal(Notify::new()),
            producer_space_available_notification: Notification::ProcessLocal(Notify::new()),
            memory: Arc::new(memory),
        })
    }

    pub(crate) fn registered(registered_ring: &RegisteredRing) -> Self {
        let memory = Arc::clone(&registered_ring.memory);
        let stream_slot = Arc::clone(&registered_ring.notification_stream_slot);
        Self {
            consumer_data_available_notification: Notification::ProducerNotificationStream(
                Arc::clone(&stream_slot),
            ),
            producer_space_available_notification: Notification::ProducerNotificationStream(
                stream_slot,
            ),
            memory,
        }
    }

    pub(crate) fn consumer(stream: ConsumerStream, memory: Arc<MappedMemory>) -> Self {
        let stream_slot =
            NotificationStreamSlot::consumer_with_notification_stream(stream, Arc::clone(&memory));
        Self {
            consumer_data_available_notification: Notification::ConsumerNotificationStream(
                Arc::clone(&stream_slot),
            ),
            producer_space_available_notification: Notification::ConsumerNotificationStream(
                stream_slot,
            ),
            memory,
        }
    }

    pub(crate) fn header(&self) -> &Header {
        self.memory.header()
    }

    pub(crate) fn payload(&self) -> *mut u8 {
        self.memory.payload()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    pub(crate) fn write_position(&self) -> u64 {
        self.header().write_position.load(Ordering::Relaxed)
    }

    pub(crate) fn read_position(&self) -> u64 {
        self.header().read_position.load(Ordering::Relaxed)
    }

    pub(crate) fn readable_len(&self) -> io::Result<usize> {
        let header = self.header();
        let read = header.read_position.load(Ordering::Relaxed);
        let write = header.write_position.load(Ordering::Acquire);
        used(write, read, self.capacity())
    }

    pub(crate) fn writable_len(&self) -> io::Result<usize> {
        let header = self.header();
        let write = header.write_position.load(Ordering::Relaxed);
        let read = header.read_position.load(Ordering::Acquire);
        Ok(self.capacity() - used(write, read, self.capacity())?)
    }

    pub(crate) async fn wait_for_data(&self, minimum: usize) -> io::Result<()> {
        self.consumer_data_available_notification
            .wait_until(&self.header().data_wait_state, || {
                Ok(self.readable_len()? >= minimum)
            })
            .await
    }

    pub(crate) async fn wait_for_space(&self, minimum: usize) -> io::Result<()> {
        self.producer_space_available_notification
            .wait_until(&self.header().space_wait_state, || {
                Ok(self.writable_len()? >= minimum)
            })
            .await
    }

    pub(crate) async fn publish_data(&self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        let header = self.header();
        self.consumer_data_available_notification
            .publish_cursor_and_notify(&header.data_wait_state, || {
                header
                    .write_position
                    .store(position.wrapping_add(amount as u64), Ordering::Release);
            })
            .await
    }

    pub(crate) async fn release_space(&self, position: u64, amount: usize) -> io::Result<()> {
        if amount == 0 {
            return Ok(());
        }
        let header = self.header();
        self.producer_space_available_notification
            .publish_cursor_and_notify(&header.space_wait_state, || {
                header
                    .read_position
                    .store(position.wrapping_add(amount as u64), Ordering::Release);
            })
            .await
    }
}
