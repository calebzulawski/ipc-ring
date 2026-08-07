use std::io;

pub(crate) fn reader_capacity_exhausted() -> io::Error {
    io::Error::new(
        io::ErrorKind::ResourceBusy,
        "the ring's reader capacity is exhausted",
    )
}

pub(crate) fn port_not_found() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        "the requested ring port is not registered",
    )
}

pub(crate) fn incompatible_protocol() -> io::Error {
    io::Error::other("server uses an unsupported control protocol version")
}

pub(crate) fn unsupported_version(version: u64) -> io::Error {
    io::Error::other(format!("unsupported IPC ring ABI version {version}"))
}

pub(crate) fn invalid_layout(reason: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid IPC ring layout: {reason}"),
    )
}

pub(crate) fn corrupt_state() -> io::Error {
    io::Error::other("shared ring state is corrupt")
}

pub(crate) fn invalid_length() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "length exceeds the ring capacity or reservation",
    )
}

pub(crate) fn invalid_input(reason: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid input: {reason}"),
    )
}

pub(crate) fn platform_invariant(reason: &'static str) -> io::Error {
    io::Error::other(format!("platform mapping invariant failed: {reason}"))
}

pub(crate) fn protocol(reason: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid control protocol: {reason}"),
    )
}

pub(crate) fn peer_disconnected() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "ring peer disconnected")
}

pub(crate) fn would_block() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "requested reservation is unavailable",
    )
}
