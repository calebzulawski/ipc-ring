use crate::platform::sys::windows::{owned, raw, windows_error};
use crate::ring::spsc::Header;
use std::io;
use std::os::windows::io::OwnedHandle;
use windows::Win32::Foundation::{
    ERROR_ALREADY_EXISTS, ERROR_SUCCESS, GetLastError, SetLastError, WAIT_FAILED,
};
use windows::Win32::System::Threading::{
    CreateEventW, EVENT_ALL_ACCESS, INFINITE, OpenEventW, SetEvent, WaitForSingleObject,
};
use windows::core::{HSTRING, PCWSTR};

pub(super) struct NotificationResources {
    data_event: OwnedHandle,
    space_event: OwnedHandle,
}

impl NotificationResources {
    pub(super) fn anonymous() -> io::Result<Self> {
        Ok(Self {
            data_event: create_event()?,
            space_event: create_event()?,
        })
    }

    pub(super) fn bind(name: &str) -> io::Result<Self> {
        Ok(Self {
            data_event: create_new_event(&object_name(name, "data"))?,
            space_event: create_new_event(&object_name(name, "space"))?,
        })
    }

    pub(super) fn connect(name: &str) -> io::Result<Self> {
        let data_name = object_name(name, "data");
        let space_name = object_name(name, "space");
        // SAFETY: valid null-terminated names and requested access masks.
        let data_event = owned(unsafe { OpenEventW(EVENT_ALL_ACCESS, false, &data_name) })?;
        // SAFETY: valid null-terminated names and requested access masks.
        let space_event = owned(unsafe { OpenEventW(EVENT_ALL_ACCESS, false, &space_name) })?;
        Ok(Self {
            data_event,
            space_event,
        })
    }
}

fn object_name(name: &str, component: &str) -> HSTRING {
    format!("Local\\ipc-ring-{name}-{component}").into()
}

fn create_event() -> io::Result<OwnedHandle> {
    // SAFETY: auto-reset, initially nonsignaled event with optional valid name.
    owned(unsafe { CreateEventW(None, false, false, PCWSTR::null()) })
}

fn create_new_event(name: &HSTRING) -> io::Result<OwnedHandle> {
    // SAFETY: resets this thread's status before the create-new probe.
    unsafe { SetLastError(ERROR_SUCCESS) };
    // SAFETY: valid name and auto-reset event arguments.
    let raw_event = unsafe { CreateEventW(None, false, false, name) };
    // SAFETY: immediately observes the status of CreateEventW.
    let status = unsafe { GetLastError() };
    let event = owned(raw_event)?;
    if status == ERROR_ALREADY_EXISTS {
        Err(io::Error::from(io::ErrorKind::AlreadyExists))
    } else {
        Ok(event)
    }
}

pub(super) struct Notification<'a> {
    event: &'a OwnedHandle,
}

impl<'a> Notification<'a> {
    pub(super) fn for_data(resources: &'a NotificationResources, _header: &'a Header) -> Self {
        Self {
            event: &resources.data_event,
        }
    }

    pub(super) fn for_space(resources: &'a NotificationResources, _header: &'a Header) -> Self {
        Self {
            event: &resources.space_event,
        }
    }

    pub(super) fn register(&self) -> Registration<'a> {
        Registration { event: self.event }
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        // SAFETY: valid event handle.
        unsafe { SetEvent(raw(self.event)) }.map_err(windows_error)
    }
}

pub(super) struct Registration<'a> {
    event: &'a OwnedHandle,
}

impl Registration<'_> {
    pub(super) fn wait(&self) -> io::Result<()> {
        // SAFETY: valid event handle.
        if unsafe { WaitForSingleObject(raw(self.event), INFINITE) } == WAIT_FAILED {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub(super) fn clear(&mut self) {
        // Windows auto-reset events require no explicit waiter-state cleanup.
    }
}
