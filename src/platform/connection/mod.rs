#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use unix as implementation;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as implementation;

use crate::error;
use crate::platform::notification::{NotificationResources, Notifications};
use crate::ring::spsc::Header;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

pub(crate) use implementation::SharedMemoryObject;

pub(crate) struct Connection {
    shared_memory: SharedMemoryObject,
    notifications: NotificationResources,
}

impl Connection {
    pub(crate) fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        let shared_memory = SharedMemoryObject::anonymous(shared_memory_len)?;
        let notifications = NotificationResources::anonymous()?;
        Ok(Self {
            shared_memory,
            notifications,
        })
    }

    pub(crate) fn bind(name: &str, shared_memory_len: u64) -> io::Result<Self> {
        validate_name(name)?;
        let shared_memory = SharedMemoryObject::bind(name, shared_memory_len)?;
        let notifications = NotificationResources::bind(name)?;
        Ok(Self {
            shared_memory,
            notifications,
        })
    }

    pub(crate) fn connect(name: &str) -> io::Result<(Self, InitializationDeadline)> {
        validate_name(name)?;
        // Opening the primary object is not retried: a missing name means no
        // producer has made this ring discoverable.
        let shared_memory = SharedMemoryObject::connect(name)?;
        let deadline = InitializationDeadline::start();
        let notifications =
            retry_initialization_not_found(deadline, || NotificationResources::connect(name))?;
        Ok((
            Self {
                shared_memory,
                notifications,
            },
            deadline,
        ))
    }

    pub(crate) fn shared_memory(&self) -> &SharedMemoryObject {
        &self.shared_memory
    }

    pub(crate) fn notifications<'a>(&'a self, header: &'a Header) -> Notifications<'a> {
        Notifications::new(&self.notifications, header)
    }

    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn truncate_shared_memory_for_test(&self, len: usize) -> std::io::Result<()> {
        self.shared_memory.truncate_for_test(len)
    }
}

fn retry_initialization_not_found<T>(
    deadline: InitializationDeadline,
    mut operation: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == io::ErrorKind::NotFound => deadline.wait()?,
            Err(error) => return Err(error),
        }
    }
}

const MAX_LOGICAL_NAME_LEN: usize = 20;
const INITIALIZATION_TIMEOUT: Duration = Duration::from_millis(100);
const INITIALIZATION_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
pub(crate) struct InitializationDeadline {
    deadline: Instant,
}

impl InitializationDeadline {
    fn start() -> Self {
        Self {
            deadline: Instant::now() + INITIALIZATION_TIMEOUT,
        }
    }

    pub(crate) fn wait(self) -> io::Result<()> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(error::initialization_timed_out(INITIALIZATION_TIMEOUT));
        }
        thread::sleep(remaining.min(INITIALIZATION_POLL_INTERVAL));
        Ok(())
    }
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > MAX_LOGICAL_NAME_LEN
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(error::invalid_name());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{InitializationDeadline, retry_initialization_not_found};
    use std::io;

    #[test]
    fn partial_notification_creation_retries_not_found() {
        let deadline = InitializationDeadline::start();
        let mut attempts = 0;
        retry_initialization_not_found(deadline, || {
            attempts += 1;
            if attempts < 3 {
                Err(io::Error::from(io::ErrorKind::NotFound))
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[test]
    fn notification_errors_other_than_not_found_are_immediate() {
        let deadline = InitializationDeadline::start();
        let mut attempts = 0;
        let error = retry_initialization_not_found(deadline, || {
            attempts += 1;
            Err::<(), _>(io::Error::from(io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(attempts, 1);
    }
}
