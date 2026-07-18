use crate::ring::spsc::Header;
use crate::ring::spsc::{IDLE, WAITING};
use rustix::thread::futex;
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
use std::sync::atomic::{AtomicU32, Ordering};

pub(super) struct Notification<'a> {
    state: &'a AtomicU32,
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
    pub(super) fn for_test(state: &'a AtomicU32) -> Self {
        Self { state }
    }

    pub(super) fn register(&self) -> Registration<'a> {
        self.state.swap(WAITING as u32, Ordering::AcqRel);
        Registration { state: self.state }
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        if self.state.swap(IDLE as u32, Ordering::AcqRel) == WAITING as u32 {
            futex_wake(self.state)?;
        }
        Ok(())
    }
}

pub(super) struct Registration<'a> {
    state: &'a AtomicU32,
}

impl Registration<'_> {
    pub(super) fn wait(&self) -> io::Result<()> {
        futex_wait(self.state)
    }

    pub(super) fn clear(&mut self) {
        self.state.swap(IDLE as u32, Ordering::AcqRel);
    }
}

fn futex_wait(state: &AtomicU32) -> io::Result<()> {
    match futex::wait(state, futex::Flags::empty(), WAITING as u32, None) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn futex_wake(state: &AtomicU32) -> io::Result<()> {
    futex::wake(state, futex::Flags::empty(), 1)
        .map(|_| ())
        .map_err(Into::into)
}
