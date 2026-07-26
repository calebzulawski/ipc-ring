//! Sends and receives the meaningful messages in the handshake protocol.

use super::stream_io;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

const VERSION: u8 = 1;
const READY: [u8; 8] = *b"IPCRDY01";

pub(crate) fn validate_port(port: &str) -> io::Result<()> {
    if port.is_empty() {
        return Err(crate::error::invalid_input("port must not be empty"));
    }
    if port.len() > u8::MAX as usize {
        return Err(crate::error::invalid_input(
            "port must be at most 255 UTF-8 bytes",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Status {
    Ok = 0,
    Busy = 1,
    NotFound = 2,
    Invalid = 3,
    Incompatible = 4,
}

pub(super) async fn send_request<S>(stream: &mut S, port: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    validate_port(port)?;
    stream.write_all(&[VERSION, port.len() as u8]).await?;
    stream.write_all(port.as_bytes()).await
}

pub(super) async fn receive_request<S>(stream: &mut S) -> io::Result<String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let [version, length] = stream_io::read_exact(stream).await?;
    if version != VERSION {
        send_status(stream, Status::Incompatible).await?;
        return Err(crate::error::incompatible_protocol());
    }
    if length == 0 {
        send_status(stream, Status::Invalid).await?;
        return Err(crate::error::protocol("port must not be empty"));
    }

    let bytes = stream_io::read_bytes(stream, length as usize).await?;
    match String::from_utf8(bytes) {
        Ok(port) => Ok(port),
        Err(_) => {
            send_status(stream, Status::Invalid).await?;
            Err(crate::error::protocol("port is not valid UTF-8"))
        }
    }
}

pub(super) async fn send_status<S>(stream: &mut S, status: Status) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_all(&response(status)).await
}

pub(super) async fn receive_status<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    validate_response(stream_io::read_exact(stream).await?)
}

pub(super) async fn send_ready<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_all(&READY).await
}

pub(super) async fn receive_ready<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    if stream_io::read_exact::<{ READY.len() }, _>(stream).await? == READY {
        Ok(())
    } else {
        Err(crate::error::protocol(
            "client did not confirm mapping attachment",
        ))
    }
}

fn response(status: Status) -> [u8; 2] {
    [VERSION, status as u8]
}

fn validate_response([version, status]: [u8; 2]) -> io::Result<()> {
    if version != VERSION {
        return Err(crate::error::incompatible_protocol());
    }
    match status {
        value if value == Status::Ok as u8 => {}
        value if value == Status::Busy as u8 => {
            return Err(crate::error::consumer_already_connected());
        }
        value if value == Status::NotFound as u8 => return Err(crate::error::port_not_found()),
        value if value == Status::Invalid as u8 => {
            return Err(crate::error::protocol("server rejected the port"));
        }
        value if value == Status::Incompatible as u8 => {
            return Err(crate::error::incompatible_protocol());
        }
        _ => return Err(crate::error::protocol("unknown routing status")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Status, response, validate_response};
    use std::io::ErrorKind;

    #[test]
    fn routing_statuses_map_to_public_error_kinds() {
        validate_response(response(Status::Ok)).unwrap();
        assert_eq!(
            validate_response(response(Status::Busy))
                .unwrap_err()
                .kind(),
            ErrorKind::ResourceBusy
        );
        assert_eq!(
            validate_response(response(Status::NotFound))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            validate_response(response(Status::Invalid))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            validate_response(response(Status::Incompatible))
                .unwrap_err()
                .to_string(),
            "server uses an unsupported control protocol version"
        );
    }

    #[test]
    fn unknown_status_and_version_are_rejected() {
        assert_eq!(
            validate_response([1, u8::MAX]).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            validate_response([2, Status::Ok as u8])
                .unwrap_err()
                .to_string(),
            "server uses an unsupported control protocol version"
        );
    }
}
