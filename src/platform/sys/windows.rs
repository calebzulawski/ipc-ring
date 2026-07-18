use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows::Win32::Foundation::HANDLE;

pub(crate) fn raw(handle: &OwnedHandle) -> HANDLE {
    HANDLE(handle.as_raw_handle())
}

pub(crate) fn owned(handle: windows::core::Result<HANDLE>) -> io::Result<OwnedHandle> {
    let handle = handle.map_err(windows_error)?;
    // SAFETY: the successful Win32 call returned a newly owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
}

pub(crate) fn windows_error(error: windows::core::Error) -> io::Error {
    let hresult = error.code().0 as u32;
    if hresult & 0xffff_0000 == 0x8007_0000 {
        io::Error::from_raw_os_error((hresult & 0xffff) as i32)
    } else {
        io::Error::other(error)
    }
}
