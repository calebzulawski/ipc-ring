use std::fmt;
use std::io;
use std::time::Duration;

#[derive(Debug)]
enum Detail {
    InvalidName,
    ConsumerAlreadyConnected,
    InitializationTimedOut(Duration),
    UnsupportedVersion(u64),
    InvalidLayout(&'static str),
    CorruptState,
    InvalidLength,
    InvalidInput(&'static str),
    PlatformInvariant(&'static str),
    WouldBlock,
}

impl fmt::Display for Detail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => {
                f.write_str("name must be 1-20 ASCII letters, digits, '.', '_', or '-'")
            }
            Self::ConsumerAlreadyConnected => f.write_str("a consumer is already connected"),
            Self::InitializationTimedOut(timeout) => {
                write!(
                    f,
                    "ring initialization did not complete within {} ms",
                    timeout.as_millis()
                )
            }
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported IPC ring ABI version {version}")
            }
            Self::InvalidLayout(reason) => write!(f, "invalid IPC ring layout: {reason}"),
            Self::CorruptState => f.write_str("shared ring state is corrupt"),
            Self::InvalidLength => f.write_str("length must be between 1 and capacity"),
            Self::InvalidInput(reason) => write!(f, "invalid input: {reason}"),
            Self::PlatformInvariant(reason) => {
                write!(f, "platform mapping invariant failed: {reason}")
            }
            Self::WouldBlock => f.write_str("requested span is unavailable"),
        }
    }
}

impl std::error::Error for Detail {}

fn new(kind: io::ErrorKind, detail: Detail) -> io::Error {
    io::Error::new(kind, detail)
}

pub(crate) fn invalid_name() -> io::Error {
    new(io::ErrorKind::InvalidInput, Detail::InvalidName)
}

pub(crate) fn consumer_already_connected() -> io::Error {
    new(
        io::ErrorKind::ResourceBusy,
        Detail::ConsumerAlreadyConnected,
    )
}

pub(crate) fn initialization_timed_out(timeout: Duration) -> io::Error {
    new(
        io::ErrorKind::TimedOut,
        Detail::InitializationTimedOut(timeout),
    )
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

pub(crate) fn would_block() -> io::Error {
    new(io::ErrorKind::WouldBlock, Detail::WouldBlock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_errors_have_standard_kinds_and_details() {
        let cases = [
            (invalid_name(), io::ErrorKind::InvalidInput),
            (consumer_already_connected(), io::ErrorKind::ResourceBusy),
            (
                initialization_timed_out(Duration::from_millis(100)),
                io::ErrorKind::TimedOut,
            ),
            (unsupported_version(2), io::ErrorKind::Other),
            (invalid_layout("bad"), io::ErrorKind::InvalidData),
            (corrupt_state(), io::ErrorKind::Other),
            (invalid_length(), io::ErrorKind::InvalidInput),
            (platform_invariant("bad"), io::ErrorKind::Other),
            (would_block(), io::ErrorKind::WouldBlock),
        ];

        for (error, kind) in cases {
            assert_eq!(error.kind(), kind);
            assert!(error.get_ref().is_some());
        }
    }
}
