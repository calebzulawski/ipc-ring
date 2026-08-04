use crate::ipc::socket::{ConsumerStream, HandshakeStream, ProducerStream};
use scopeguard::ScopeGuard;
use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::ptr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use windows::Win32::Foundation::{
    DUPLICATE_CLOSE_SOURCE, DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE,
};
use windows::Win32::System::Threading::GetCurrentProcess;

/// Duplicates a mapping into the accepted client and sends its handle value.
pub(crate) async fn send_mapping_handle(
    stream: HandshakeStream,
    mapping: &impl AsRawHandle,
) -> io::Result<ProducerStream> {
    let (mut stream, client_process) = stream.into_parts();
    let mapping = duplicate_mapping(
        mapping.as_raw_handle(),
        crate::sys::windows::raw(&client_process),
    )?;
    let mapping = scopeguard::guard((client_process, mapping), |(client_process, mapping)| {
        // SAFETY: mapping is a handle in the pinned client process. A null target
        // plus DUPLICATE_CLOSE_SOURCE closes it without creating another handle.
        let _ = unsafe {
            DuplicateHandle(
                crate::sys::windows::raw(&client_process),
                HANDLE(mapping as usize as *mut c_void),
                HANDLE::default(),
                ptr::null_mut(),
                0,
                false,
                DUPLICATE_CLOSE_SOURCE,
            )
        };
    });
    let bytes = mapping.1.to_le_bytes();
    stream.write_all(&bytes).await?;
    // The complete handle value is the ownership handoff to the client.
    drop(ScopeGuard::into_inner(mapping));
    Ok(stream)
}

/// Receives the handle value already duplicated into this process.
pub(crate) async fn receive_mapping_handle(stream: &mut ConsumerStream) -> io::Result<OwnedHandle> {
    let mut bytes = [0_u8; 8];
    stream.read_exact(&mut bytes).await?;
    let value = u64::from_le_bytes(bytes);
    if value == 0 || value == u64::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mapping transfer contains an invalid handle",
        ));
    }
    // SAFETY: the server duplicated this numeric handle into the current process.
    Ok(unsafe { OwnedHandle::from_raw_handle(value as usize as *mut c_void) })
}

fn duplicate_mapping(mapping: RawHandle, target: HANDLE) -> io::Result<u64> {
    let mut duplicated = HANDLE::default();
    // SAFETY: mapping is live for the call and target is the pinned client process.
    unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            HANDLE(mapping),
            target,
            &mut duplicated,
            0,
            false,
            DUPLICATE_SAME_ACCESS,
        )
    }
    .map_err(crate::sys::windows::windows_error)?;
    Ok(duplicated.0 as usize as u64)
}
