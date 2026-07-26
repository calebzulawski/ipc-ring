//! Reads fixed and length-prefixed byte sequences without interpreting them.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};

pub(super) async fn read_exact<const N: usize, S>(stream: &mut S) -> io::Result<[u8; N]>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = [0_u8; N];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub(super) async fn read_bytes<S>(stream: &mut S, length: usize) -> io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}
