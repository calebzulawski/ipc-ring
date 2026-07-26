use std::fmt;
use std::io;

#[derive(Debug)]
enum Detail {
    ConsumerAlreadyConnected,
    PortNotFound,
    IncompatibleProtocol,
    UnsupportedVersion(u64),
    InvalidLayout(&'static str),
    CorruptState,
    InvalidLength,
    InvalidInput(&'static str),
    Protocol(&'static str),
    PlatformInvariant(&'static str),
    PeerDisconnected,
    WouldBlock,
}

impl fmt::Display for Detail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsumerAlreadyConnected => f.write_str("a consumer is already connected"),
            Self::PortNotFound => f.write_str("the requested ring port is not registered"),
            Self::IncompatibleProtocol => {
                f.write_str("server uses an unsupported control protocol version")
            }
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported IPC ring ABI version {version}")
            }
            Self::InvalidLayout(reason) => write!(f, "invalid IPC ring layout: {reason}"),
            Self::CorruptState => f.write_str("shared ring state is corrupt"),
            Self::InvalidLength => f.write_str("length must be between 1 and capacity"),
            Self::InvalidInput(reason) => write!(f, "invalid input: {reason}"),
            Self::Protocol(reason) => write!(f, "invalid control protocol: {reason}"),
            Self::PlatformInvariant(reason) => {
                write!(f, "platform mapping invariant failed: {reason}")
            }
            Self::PeerDisconnected => f.write_str("ring peer disconnected"),
            Self::WouldBlock => f.write_str("requested span is unavailable"),
        }
    }
}

impl std::error::Error for Detail {}

fn new(kind: io::ErrorKind, detail: Detail) -> io::Error {
    io::Error::new(kind, detail)
}

pub(crate) fn consumer_already_connected() -> io::Error {
    new(
        io::ErrorKind::ResourceBusy,
        Detail::ConsumerAlreadyConnected,
    )
}

pub(crate) fn port_not_found() -> io::Error {
    new(io::ErrorKind::NotFound, Detail::PortNotFound)
}

pub(crate) fn incompatible_protocol() -> io::Error {
    new(io::ErrorKind::Other, Detail::IncompatibleProtocol)
}

pub(crate) fn unsupported_version(version: u64) -> io::Error {
    new(io::ErrorKind::Other, Detail::UnsupportedVersion(version))
}

pub(crate) fn invalid_layout(reason: &'static str) -> io::Error {
    new(io::ErrorKind::InvalidData, Detail::InvalidLayout(reason))
}

pub(crate) fn corrupt_state() -> io::Error {
    new(io::ErrorKind::Other, Detail::CorruptState)
}

pub(crate) fn invalid_length() -> io::Error {
    new(io::ErrorKind::InvalidInput, Detail::InvalidLength)
}

pub(crate) fn invalid_input(reason: &'static str) -> io::Error {
    new(io::ErrorKind::InvalidInput, Detail::InvalidInput(reason))
}

pub(crate) fn platform_invariant(reason: &'static str) -> io::Error {
    new(io::ErrorKind::Other, Detail::PlatformInvariant(reason))
}

pub(crate) fn protocol(reason: &'static str) -> io::Error {
    new(io::ErrorKind::InvalidData, Detail::Protocol(reason))
}

pub(crate) fn peer_disconnected() -> io::Error {
    new(io::ErrorKind::BrokenPipe, Detail::PeerDisconnected)
}

pub(crate) fn would_block() -> io::Error {
    new(io::ErrorKind::WouldBlock, Detail::WouldBlock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn library_errors_have_standard_kinds_and_details() {
        let cases = [
            (consumer_already_connected(), io::ErrorKind::ResourceBusy),
            (port_not_found(), io::ErrorKind::NotFound),
            (incompatible_protocol(), io::ErrorKind::Other),
            (unsupported_version(2), io::ErrorKind::Other),
            (invalid_layout("bad"), io::ErrorKind::InvalidData),
            (corrupt_state(), io::ErrorKind::Other),
            (invalid_length(), io::ErrorKind::InvalidInput),
            (platform_invariant("bad"), io::ErrorKind::Other),
            (protocol("bad"), io::ErrorKind::InvalidData),
            (peer_disconnected(), io::ErrorKind::BrokenPipe),
            (would_block(), io::ErrorKind::WouldBlock),
        ];

        for (error, kind) in cases {
            assert_eq!(error.kind(), kind);
            assert!(error.get_ref().is_some());
        }
    }
}
