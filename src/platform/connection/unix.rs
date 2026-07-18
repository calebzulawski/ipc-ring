use std::ffi::CString;
use std::io;
use std::os::fd::OwnedFd;

pub(crate) struct SharedMemoryObject {
    descriptor: OwnedFd,
    owned_name: Option<CString>,
}

impl SharedMemoryObject {
    pub(crate) fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let descriptor = rustix::fs::memfd_create("ipc-ring", rustix::fs::MemfdFlags::CLOEXEC)
                .map_err(io::Error::from)?;
            let result = Self {
                descriptor,
                owned_name: None,
            };
            result.set_len(shared_memory_len)?;
            Ok(result)
        }

        #[cfg(target_os = "macos")]
        {
            use std::sync::atomic::{AtomicU64, Ordering};

            static NEXT: AtomicU64 = AtomicU64::new(0);
            for _ in 0..128 {
                let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
                let pid = std::process::id();
                let name = CString::new(format!("/ipc-ring-{pid}-{sequence}")).unwrap();
                match open_new(&name) {
                    Ok(descriptor) => {
                        // Unlinking retains the open object.
                        let _ = rustix::shm::unlink(name.as_c_str());
                        let result = Self {
                            descriptor,
                            owned_name: None,
                        };
                        result.set_len(shared_memory_len)?;
                        return Ok(result);
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "shared-memory name exhaustion",
            ))
        }
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
