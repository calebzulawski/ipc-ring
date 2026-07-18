mod connection;
mod mapping;
mod notification;
mod sys;

pub(crate) use mapping::MappedRing;

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
compile_error!("ipc-ring v1 supports Linux, macOS, and Windows only");

#[cfg(test)]
pub(crate) fn minimum_capacity() -> usize {
    MappedRing::minimum_capacity()
}
