//! Sends and receives the meaningful messages in the handshake protocol.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const VERSION: u8 = 1;
const READY: [u8; 8] = *b"IPCRDY01";
const ATTACHED: [u8; 1] = [0];

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

const _: () = {
    assert!(VERSION == 1);
    assert!(Status::Ok as u8 == 0);
    assert!(Status::Busy as u8 == 1);
    assert!(Status::NotFound as u8 == 2);
    assert!(Status::Invalid as u8 == 3);
    assert!(Status::Incompatible as u8 == 4);
};

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
    let [version, length] = read_exact(stream).await?;
    if version != VERSION {
        send_status(stream, Status::Incompatible, None).await?;
        return Err(crate::error::incompatible_protocol());
    }
    if length == 0 {
        send_status(stream, Status::Invalid, None).await?;
        return Err(crate::error::protocol("port must not be empty"));
    }

    let bytes = read_bytes(stream, length as usize).await?;
    match String::from_utf8(bytes) {
        Ok(port) => Ok(port),
        Err(_) => {
            send_status(stream, Status::Invalid, None).await?;
            Err(crate::error::protocol("port is not valid UTF-8"))
        }
    }
}

pub(super) async fn send_status<S>(
    stream: &mut S,
    status: Status,
    reader_slot: Option<u8>,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_all(&response(status)).await?;
    if let Some(reader_slot) = reader_slot {
        stream.write_all(&[reader_slot]).await?;
    }
    Ok(())
}

pub(super) async fn receive_status<S>(stream: &mut S) -> io::Result<u8>
where
    S: AsyncRead + Unpin,
{
    validate_response(read_exact(stream).await?)?;
    read_exact::<1, _>(stream).await.map(|[slot]| slot)
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
    if read_exact::<{ READY.len() }, _>(stream).await? == READY {
        Ok(())
    } else {
        Err(crate::error::protocol(
            "client did not confirm mapping attachment",
        ))
    }
}

pub(super) async fn send_attached<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_all(&ATTACHED).await
}

pub(super) async fn receive_attached<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    if read_exact::<{ ATTACHED.len() }, _>(stream).await? == ATTACHED {
        Ok(())
    } else {
        Err(crate::error::protocol(
            "server sent an invalid attachment acknowledgement",
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
            return Err(crate::error::reader_capacity_exhausted());
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

async fn read_exact<const N: usize, S>(stream: &mut S) -> io::Result<[u8; N]>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = [0; N];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn read_bytes<S>(stream: &mut S, length: usize) -> io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{Status, VERSION, receive_attached, validate_port, validate_response};
    use std::io::ErrorKind;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn unknown_status_and_version_are_rejected() {
        assert_eq!(
            validate_response([VERSION, u8::MAX]).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            validate_response([VERSION + 1, Status::Ok as u8])
                .unwrap_err()
                .to_string(),
            "server uses an unsupported control protocol version"
        );
    }

    #[tokio::test]
    async fn attachment_acknowledgements_are_validated() {
        let (mut client, mut server) = tokio::io::duplex(1);
        server.write_u8(0).await.unwrap();
        receive_attached(&mut client).await.unwrap();

        server.write_u8(u8::MAX).await.unwrap();
        assert_eq!(
            receive_attached(&mut client).await.unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        drop(server);
        assert_eq!(
            receive_attached(&mut client).await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn port_lengths_use_utf8_bytes_and_cover_protocol_boundaries() {
        validate_port("a").unwrap();
        validate_port(&"x".repeat(255)).unwrap();
        validate_port(&("é".repeat(127) + "a")).unwrap();

        assert_eq!(
            validate_port("").unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_port(&"x".repeat(256)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_port(&"é".repeat(128)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}
