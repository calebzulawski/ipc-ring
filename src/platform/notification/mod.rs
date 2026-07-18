#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as implementation;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

use crate::ring::spsc::Header;
use std::io;

pub(crate) struct NotificationResources {
    inner: implementation::NotificationResources,
}

impl NotificationResources {
    pub(crate) fn anonymous() -> io::Result<Self> {
        Ok(Self {
            inner: implementation::NotificationResources::anonymous()?,
        })
    }

    pub(crate) fn bind(name: &str) -> io::Result<Self> {
        Ok(Self {
            inner: implementation::NotificationResources::bind(name)?,
        })
    }

    pub(crate) fn connect(name: &str) -> io::Result<Self> {
        Ok(Self {
            inner: implementation::NotificationResources::connect(name)?,
        })
    }
}

pub(crate) struct Notifications<'a> {
    data: Notification<'a>,
    space: Notification<'a>,
}

impl<'a> Notifications<'a> {
    pub(crate) fn new(resources: &'a NotificationResources, header: &'a Header) -> Self {
        Self {
            data: Notification::for_data(resources, header),
            space: Notification::for_space(resources, header),
        }
    }

    pub(crate) fn data(self) -> Notification<'a> {
        self.data
    }

    pub(crate) fn space(self) -> Notification<'a> {
        self.space
    }
}

pub(crate) struct Notification<'a> {
    inner: implementation::Notification<'a>,
}

impl<'a> Notification<'a> {
    pub(crate) fn for_data(resources: &'a NotificationResources, header: &'a Header) -> Self {
        Self {
            inner: implementation::Notification::for_data(&resources.inner, header),
        }
    }

    pub(crate) fn for_space(resources: &'a NotificationResources, header: &'a Header) -> Self {
        Self {
            inner: implementation::Notification::for_space(&resources.inner, header),
        }
    }

    pub(crate) fn register(&self) -> WaitRegistration<'a> {
        WaitRegistration {
            inner: self.inner.register(),
        }
    }

    pub(crate) fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

pub(crate) struct WaitRegistration<'a> {
    inner: implementation::Registration<'a>,
}

impl WaitRegistration<'_> {
    pub(crate) fn wait(&self) -> io::Result<()> {
        self.inner.wait()
    }
}

impl Drop for WaitRegistration<'_> {
    fn drop(&mut self) {
        self.inner.clear();
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::ring::spsc::{IDLE, WAITING};
    use std::sync::atomic::Ordering;

    #[test]
    fn dropping_registration_clears_wait_state() {
        #[cfg(target_os = "linux")]
        let state = std::sync::atomic::AtomicU32::new(IDLE as u32);
        #[cfg(target_os = "macos")]
        let state = std::sync::atomic::AtomicU64::new(IDLE);

        let notification = Notification {
            inner: implementation::Notification::for_test(&state),
        };
        let registration = notification.register();
        #[cfg(target_os = "linux")]
        assert_eq!(state.load(Ordering::Relaxed), WAITING as u32);
        #[cfg(target_os = "macos")]
        assert_eq!(state.load(Ordering::Relaxed), WAITING);

        drop(registration);
        #[cfg(target_os = "linux")]
        assert_eq!(state.load(Ordering::Relaxed), IDLE as u32);
        #[cfg(target_os = "macos")]
        assert_eq!(state.load(Ordering::Relaxed), IDLE);
    }
}
