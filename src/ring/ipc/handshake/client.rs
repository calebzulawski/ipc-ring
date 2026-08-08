use super::protocol;
use crate::ring::ipc::socket::{self, ConsumerStream};
use crate::ring::mapping::{self, MappedMemory, SharedMemory};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(crate) async fn connect(
    path: PathBuf,
    port: String,
    handshake_timeout: Duration,
) -> io::Result<(u8, ConsumerStream, Arc<MappedMemory>)> {
    tokio::time::timeout(handshake_timeout, connect_and_attach(path, port))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

async fn connect_and_attach(
    path: PathBuf,
    port: String,
) -> io::Result<(u8, ConsumerStream, Arc<MappedMemory>)> {
    let mut stream = socket::connect(&path).await?;
    protocol::send_request(&mut stream, &port).await?;
    let reader_slot = protocol::receive_status(&mut stream).await?;
    let handle = socket::receive_mapping_handle(&mut stream).await?;
    let shared_memory = SharedMemory::from_handle(handle);
    // SAFETY: attach validates the complete shared layout before Ready is sent.
    let memory = Arc::new(unsafe { mapping::attach(&shared_memory)? });
    drop(shared_memory);
    protocol::send_ready(&mut stream).await?;
    protocol::receive_attached(&mut stream).await?;
    Ok((reader_slot, stream, memory))
}
