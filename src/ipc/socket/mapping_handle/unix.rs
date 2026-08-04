use crate::ipc::socket::{ConsumerStream, HandshakeStream, ProducerStream};
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};

/// Sends one mapping descriptor with the carrier byte required by `SCM_RIGHTS`.
pub(crate) async fn send_mapping_handle(
    stream: HandshakeStream,
    mapping: &impl AsFd,
) -> io::Result<ProducerStream> {
    let carrier = [0_u8];
    let sent = stream
        .async_io(tokio::io::Interest::WRITABLE, || {
            let data = [IoSlice::new(&carrier)];
            let descriptors = [mapping.as_fd()];
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
            let mut ancillary = rustix::net::SendAncillaryBuffer::new(&mut space);
            assert!(
                ancillary.push(rustix::net::SendAncillaryMessage::ScmRights(&descriptors)),
                "statically sized descriptor ancillary buffer is too small"
            );
            rustix::net::sendmsg(
                &stream,
                &data,
                &mut ancillary,
                rustix::net::SendFlags::empty(),
            )
            .map_err(io::Error::from)
        })
        .await?;
    if sent == 0 {
        Err(io::Error::from(io::ErrorKind::WriteZero))
    } else {
        Ok(stream)
    }
}

/// Receives exactly one close-on-exec mapping descriptor.
pub(crate) async fn receive_mapping_handle(stream: &mut ConsumerStream) -> io::Result<OwnedFd> {
    let mut carrier = [0_u8];
    let (received, truncated, descriptors) = stream
        .async_io(tokio::io::Interest::READABLE, || {
            let mut data = [IoSliceMut::new(&mut carrier)];
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
            let mut ancillary = rustix::net::RecvAncillaryBuffer::new(&mut space);
            let message = rustix::net::recvmsg(
                &*stream,
                &mut data,
                &mut ancillary,
                rustix::net::RecvFlags::empty(),
            )
            .map_err(io::Error::from)?;
            let mut descriptors = Vec::new();
            for message in ancillary.drain() {
                if let rustix::net::RecvAncillaryMessage::ScmRights(values) = message {
                    descriptors.extend(values);
                }
            }
            Ok((
                message.bytes,
                message.flags.contains(rustix::net::ReturnFlags::CTRUNC),
                descriptors,
            ))
        })
        .await?;
    if received == 0 {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
    }
    validate_received_descriptor(truncated, descriptors)
}

fn validate_received_descriptor(
    truncated: bool,
    mut descriptors: Vec<OwnedFd>,
) -> io::Result<OwnedFd> {
    if truncated || descriptors.len() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mapping transfer must contain exactly one descriptor",
        ));
    }
    let descriptor = descriptors.pop().expect("validated descriptor count");
    rustix::io::fcntl_setfd(&descriptor, rustix::io::FdFlags::CLOEXEC).map_err(io::Error::from)?;
    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn mapping() -> crate::mapping::SharedMemory {
        crate::mapping::create(crate::mapping::minimum_capacity())
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn received_mapping_descriptor_is_close_on_exec() {
        let mapping = mapping();
        let (sender, mut receiver) = crate::ipc::socket::pair().unwrap();
        let _sender = send_mapping_handle(sender, &mapping).await.unwrap();
        let received = receive_mapping_handle(&mut receiver).await.unwrap();

        let flags = rustix::io::fcntl_getfd(&received).unwrap();
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
    }

    #[tokio::test]
    async fn extra_mapping_descriptors_are_rejected() {
        let mapping = mapping();
        let first = mapping.duplicate_descriptor().unwrap();
        let second = mapping.duplicate_descriptor().unwrap();
        let (sender, mut receiver) = crate::ipc::socket::pair().unwrap();
        let marker = [0_u8];
        sender
            .async_io(tokio::io::Interest::WRITABLE, || {
                let data = [IoSlice::new(&marker)];
                let descriptors = [first.as_fd(), second.as_fd()];
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
                let mut ancillary = rustix::net::SendAncillaryBuffer::new(&mut space);
                assert!(ancillary.push(rustix::net::SendAncillaryMessage::ScmRights(&descriptors)));
                rustix::net::sendmsg(
                    &sender,
                    &data,
                    &mut ancillary,
                    rustix::net::SendFlags::empty(),
                )
                .map_err(io::Error::from)
            })
            .await
            .unwrap();
        let cause = receive_mapping_handle(&mut receiver).await.unwrap_err();

        assert_eq!(cause.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn missing_mapping_descriptor_is_rejected() {
        let (mut sender, mut receiver) = crate::ipc::socket::pair().unwrap();
        sender.write_all(&[0]).await.unwrap();
        let cause = receive_mapping_handle(&mut receiver).await.unwrap_err();

        assert_eq!(cause.kind(), io::ErrorKind::InvalidData);
    }
}
