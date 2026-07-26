use crate::local_socket::{ConsumerStream, HandshakeStream, ProducerStream};
use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use windows::Win32::Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE};
use windows::Win32::System::Threading::GetCurrentProcess;

/// Duplicates a mapping into the accepted client and sends its handle value.
pub(crate) async fn send_mapping_handle(
    stream: HandshakeStream,
    mapping: &impl AsRawHandle,
) -> io::Result<ProducerStream> {
    let client_process = stream.client_process();
    let mapping = duplicate_mapping(
        mapping.as_raw_handle(),
        crate::sys::windows::raw(client_process),
    )?;
    let mut stream = stream.into_producer_stream();
    // The client owns the duplicate even if delivery subsequently fails.
    stream.write_all(&mapping.to_le_bytes()).await?;
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
