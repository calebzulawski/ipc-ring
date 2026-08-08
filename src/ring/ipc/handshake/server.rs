use super::protocol;
use crate::ring::ipc::server::ServerRegistry;
use crate::ring::ipc::socket::{self, HandshakeStream};
use std::io;
use std::sync::Weak;
use std::time::Duration;

pub(crate) async fn route(
    stream: HandshakeStream,
    server_registry: Weak<ServerRegistry>,
    handshake_timeout: Duration,
) -> io::Result<()> {
    tokio::time::timeout(handshake_timeout, route_and_attach(stream, server_registry))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

async fn route_and_attach(
    mut stream: HandshakeStream,
    server_registry: Weak<ServerRegistry>,
) -> io::Result<()> {
    let port = protocol::receive_request(&mut stream).await?;
    let registered_ring = {
        let server_registry = server_registry
            .upgrade()
            .ok_or_else(crate::error::peer_disconnected)?;
        server_registry.find(&port)
    };
    let ring = match registered_ring {
        Some(ring) => ring,
        None => {
            protocol::send_status(&mut stream, protocol::Status::NotFound, None).await?;
            return Err(crate::error::port_not_found());
        }
    };
    let mut reader_claim = match ring.claim_reader() {
        Ok(reader_claim) => reader_claim,
        Err(cause) if cause.kind() == io::ErrorKind::ResourceBusy => {
            protocol::send_status(&mut stream, protocol::Status::Busy, None).await?;
            return Err(cause);
        }
        Err(cause) => return Err(cause),
    };

    protocol::send_status(&mut stream, protocol::Status::Ok, Some(reader_claim.slot())).await?;
    let mut stream = socket::send_mapping_handle(stream, ring.shared_memory()).await?;
    protocol::receive_ready(&mut stream).await?;
    reader_claim.activate();
    protocol::send_attached(&mut stream).await?;
    reader_claim.finish(stream)?;
    Ok(())
}
