use crate::ring::spsc::Header;
use crate::ring::spsc::{IDLE, WAITING};
use std::io;

pub(super) struct NotificationResources;

impl NotificationResources {
    pub(super) fn anonymous() -> io::Result<Self> {
        Ok(Self)
    }

    pub(super) fn bind(_name: &str) -> io::Result<Self> {
        Ok(Self)
    }

    pub(super) fn connect(_name: &str) -> io::Result<Self> {
        Ok(Self)
    }
}
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Notification<'a> {
    state: &'a AtomicU64,
}

impl<'a> Notification<'a> {
    pub(super) fn for_data(_resources: &'a NotificationResources, header: &'a Header) -> Self {
        Self {
            state: &header.data_wait_state,
        }
    }

    pub(super) fn for_space(_resources: &'a NotificationResources, header: &'a Header) -> Self {
        Self {
            state: &header.space_wait_state,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(state: &'a AtomicU64) -> Self {
        Self { state }
    }

    pub(super) fn register(&self) -> Registration<'a> {
        self.state.swap(WAITING, Ordering::AcqRel);
        Registration { state: self.state }
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        if self.state.swap(IDLE, Ordering::AcqRel) == WAITING {
            os_wake(self.state)?;
        }
        Ok(())
    }
}

pub(super) struct Registration<'a> {
    state: &'a AtomicU64,
}

impl Registration<'_> {
    pub(super) fn wait(&self) -> io::Result<()> {
        os_wait(self.state)
    }

    pub(super) fn clear(&mut self) {
        self.state.swap(IDLE, Ordering::AcqRel);
    }
}

fn os_wait(state: &AtomicU64) -> io::Result<()> {
    // SAFETY: aligned shared u64 and the size matches the atomic.
    let result = unsafe {
        libc::os_sync_wait_on_address(
            state as *const _ as *mut _,
            WAITING,
            size_of::<u64>(),
            libc::OS_SYNC_WAIT_ON_ADDRESS_SHARED,
        )
    };
    if result >= 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::EINTR || code == libc::EFAULT => Ok(()),
        _ => Err(error),
    }
}

fn os_wake(state: &AtomicU64) -> io::Result<()> {
    // SAFETY: address and size match the wait operation.
    let result = unsafe {
        libc::os_sync_wake_by_address_any(
            state as *const _ as *mut _,
            size_of::<u64>(),
            libc::OS_SYNC_WAKE_BY_ADDRESS_SHARED,
        )
    };
    if result >= 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(error)
    }
}
