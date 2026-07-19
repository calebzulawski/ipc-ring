use std::ffi::CString;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};

pub(crate) struct SharedMemoryObject {
    descriptor: OwnedFd,
    owned_name: Option<CString>,
}

impl SharedMemoryObject {
    pub(crate) fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        let descriptor = shm_open_anonymous::shm_open_anonymous();
        if descriptor == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: shm_open_anonymous returned a new, owned descriptor.
        let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let result = Self {
            descriptor,
            owned_name: None,
        };
        result.set_len(shared_memory_len)?;
        Ok(result)
    }

    pub(crate) fn bind(name: &str, shared_memory_len: u64) -> io::Result<Self> {
        let name = os_name(name);
        let result = Self {
            descriptor: open_new(&name)?,
            owned_name: Some(name),
        };
        result.set_len(shared_memory_len)?;
        Ok(result)
    }

    pub(crate) fn connect(name: &str) -> io::Result<Self> {
        let name = os_name(name);
        let descriptor = rustix::shm::open(
            name.as_c_str(),
            rustix::shm::OFlags::RDWR,
            rustix::shm::Mode::empty(),
        )
        .map_err(io::Error::from)?;
        Ok(Self {
            descriptor,
            owned_name: None,
        })
    }

    pub(crate) fn descriptor(&self) -> &OwnedFd {
        &self.descriptor
    }

    #[cfg(test)]
    pub(crate) fn truncate_for_test(&self, len: usize) -> io::Result<()> {
        rustix::fs::ftruncate(&self.descriptor, len as u64).map_err(Into::into)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        rustix::fs::ftruncate(&self.descriptor, len).map_err(Into::into)
    }
}

impl Drop for SharedMemoryObject {
    fn drop(&mut self) {
        if let Some(name) = &self.owned_name {
            // Best-effort cleanup. Existing mappings remain valid after unlink.
            let _ = rustix::shm::unlink(name.as_c_str());
        }
    }
}

fn os_name(name: &str) -> CString {
    CString::new(format!("/ipc-ring-{name}")).expect("validated logical name")
}

fn open_new(name: &CString) -> io::Result<OwnedFd> {
    rustix::shm::open(
        name.as_c_str(),
        rustix::shm::OFlags::RDWR | rustix::shm::OFlags::CREATE | rustix::shm::OFlags::EXCL,
        rustix::shm::Mode::RUSR | rustix::shm::Mode::WUSR,
    )
    .map_err(Into::into)
}
