use crate::platform::sys::windows::{owned, raw};
use std::io;
use std::os::windows::io::OwnedHandle;
use windows::Win32::Foundation::{
    ERROR_ALREADY_EXISTS, ERROR_SUCCESS, GetLastError, HANDLE, INVALID_HANDLE_VALUE, SetLastError,
};
use windows::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_ALL_ACCESS, OpenFileMappingW, PAGE_READWRITE,
};
use windows::core::{HSTRING, PCWSTR};

pub(crate) struct SharedMemoryObject {
    handle: OwnedHandle,
}

impl SharedMemoryObject {
    pub(crate) fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        let handle = create_mapping(shared_memory_len)?;
        Ok(Self { handle })
    }

    pub(crate) fn bind(name: &str, shared_memory_len: u64) -> io::Result<Self> {
        let mapping_name = object_name(name, "mapping");
        let handle = create_new_mapping(shared_memory_len, &mapping_name)?;
        Ok(Self { handle })
    }

    pub(crate) fn connect(name: &str) -> io::Result<Self> {
        let mapping_name = object_name(name, "mapping");
        // SAFETY: valid null-terminated names and requested access masks.
        let handle =
            owned(unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS.0, false, &mapping_name) })?;
        Ok(Self { handle })
    }

    pub(crate) fn handle(&self) -> HANDLE {
        raw(&self.handle)
    }
}

fn object_name(name: &str, component: &str) -> HSTRING {
    format!("Local\\ipc-ring-{name}-{component}").into()
}

fn create_mapping(shared_memory_len: u64) -> io::Result<OwnedHandle> {
    // SAFETY: pagefile mapping with checked exact size and optional valid name.
    owned(unsafe {
        CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            None,
            PAGE_READWRITE,
            (shared_memory_len >> 32) as u32,
            shared_memory_len as u32,
            PCWSTR::null(),
        )
    })
}

fn create_new_mapping(shared_memory_len: u64, name: &HSTRING) -> io::Result<OwnedHandle> {
    // SAFETY: resets this thread's status before the create-new probe.
    unsafe { SetLastError(ERROR_SUCCESS) };
    // GetLastError must be sampled before any other system call.
    let raw_mapping = unsafe {
        CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            None,
            PAGE_READWRITE,
            (shared_memory_len >> 32) as u32,
            shared_memory_len as u32,
            name,
        )
    };
    // SAFETY: immediately observes the status of CreateFileMappingW.
    let status = unsafe { GetLastError() };
    let handle = owned(raw_mapping)?;
    if status == ERROR_ALREADY_EXISTS {
        Err(io::Error::from(io::ErrorKind::AlreadyExists))
    } else {
        Ok(handle)
    }
}
