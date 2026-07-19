use super::{capacity_for_minimum, wait_for_version};
use crate::error;
use crate::platform::connection::{Connection, InitializationDeadline, SharedMemoryObject};
use crate::ring::spsc::{ABI_VERSION, Header};
use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::os::fd::OwnedFd;
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;

pub(super) struct Layout {
    page_size: usize,
    capacity: usize,
    shared_memory_len: u64,
}

impl Layout {
    fn for_minimum_capacity(minimum_capacity: usize) -> io::Result<Self> {
        let page = page_size()?;
        let capacity = capacity_for_minimum(minimum_capacity, page)?;
        validate_capacity(capacity, page)?;
        let shared_memory_len = page
            .checked_add(capacity)
            .ok_or_else(|| error::invalid_input("shared-memory object length overflows usize"))?
            .try_into()
            .map_err(|_| error::invalid_input("shared-memory object length exceeds u64"))?;
        Ok(Self {
            page_size: page,
            capacity,
            shared_memory_len,
        })
    }
}

pub(super) struct MappedMemory {
    header: NonNull<Header>,
    payload: NonNull<u8>,
    page_size: usize,
    capacity: usize,
}

// SAFETY: mappings are stable; mutable access is controlled by endpoint roles.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

impl MappedMemory {
    pub(crate) fn create(
        shared_memory: &SharedMemoryObject,
        layout: Layout,
        consumer_claim: u64,
    ) -> io::Result<Self> {
        let fd = shared_memory.descriptor();
        // SAFETY: this freshly sized object is exclusively initialized here.
        let memory = unsafe { Self::map(fd, layout.page_size, layout.capacity)? };
        // SAFETY: header is aligned, writable, and currently private to creator.
        unsafe {
            ptr::write(
                memory.header.as_ptr(),
                Header::new(layout.capacity, consumer_claim),
            )
        };
        memory
            .header()
            .version
            .store(ABI_VERSION, Ordering::Release);
        Ok(memory)
    }

    pub(crate) unsafe fn attach(
        shared_memory: &SharedMemoryObject,
        deadline: InitializationDeadline,
    ) -> io::Result<Self> {
        let fd = shared_memory.descriptor();
        let page = page_size()?;
        wait_for_header_size(fd, page, deadline)?;
        let header = map_header(fd, page)?;
        // SAFETY: caller supplies a trusted attachment; a native page covers Header.
        let value = unsafe { header.as_ref() };
        let version = match wait_for_version(value, deadline) {
            Ok(version) => version,
            Err(error) => {
                // SAFETY: exact mapping returned by map_header.
                let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
                return Err(error);
            }
        };
        if version != ABI_VERSION {
            // SAFETY: exact mapping returned by map_header.
            let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
            return Err(error::unsupported_version(version));
        }

        let capacity: usize = match value.capacity.try_into() {
            Ok(value) => value,
            Err(_) => {
                // SAFETY: exact mapping returned by map_header.
                let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
                return Err(error::invalid_layout("capacity exceeds usize"));
            }
        };
        if let Err(error) = validate_capacity(capacity, page)
            .and_then(|_| validate_shared_memory(fd, page, capacity))
        {
            // SAFETY: exact mapping returned by map_header.
            let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
            return Err(error);
        }

        // SAFETY: validated descriptor, offset, and capacity.
        let payload = match unsafe { map_payload(fd, page, capacity) } {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: exact mapping returned by map_header.
                let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
                return Err(error);
            }
        };
        Ok(Self {
            header,
            payload,
            page_size: page,
            capacity,
        })
    }

    unsafe fn map(fd: &OwnedFd, page: usize, capacity: usize) -> io::Result<Self> {
        validate_shared_memory(fd, page, capacity)?;
        let header = map_header(fd, page)?;
        // SAFETY: validated descriptor, offset, and capacity.
        let payload = match unsafe { map_payload(fd, page, capacity) } {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: exact mapping returned by map_header.
                let _ = unsafe { rustix::mm::munmap(header.as_ptr().cast(), page) };
                return Err(error);
            }
        };
        Ok(Self {
            header,
            payload,
            page_size: page,
            capacity,
        })
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: mapping lives for self and its header was validated during attachment.
        unsafe { self.header.as_ref() }
    }

    pub(crate) fn payload(&self) -> *mut u8 {
        self.payload.as_ptr()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(crate) fn reinitialize_header_for_test(&self, consumer_claim: u64) {
        // SAFETY: the test has restored the complete backing length and the
        // creator is exclusively republishing the zeroed header.
        unsafe {
            ptr::write(
                self.header.as_ptr(),
                Header::new(self.capacity, consumer_claim),
            )
        };
        self.header().version.store(ABI_VERSION, Ordering::Release);
    }
}

pub(super) fn create_anonymous(
    minimum_capacity: usize,
    consumer_claim: u64,
) -> io::Result<(Connection, MappedMemory)> {
    let layout = Layout::for_minimum_capacity(minimum_capacity)?;
    let connection = Connection::anonymous(layout.shared_memory_len)?;
    let memory = MappedMemory::create(connection.shared_memory(), layout, consumer_claim)?;
    Ok((connection, memory))
}

pub(super) fn bind(
    name: &str,
    minimum_capacity: usize,
    consumer_claim: u64,
) -> io::Result<(Connection, MappedMemory)> {
    let layout = Layout::for_minimum_capacity(minimum_capacity)?;
    let connection = Connection::bind(name, layout.shared_memory_len)?;
    let memory = MappedMemory::create(connection.shared_memory(), layout, consumer_claim)?;
    Ok((connection, memory))
}

pub(super) fn connect(name: &str) -> io::Result<(Connection, MappedMemory)> {
    let (connection, deadline) = Connection::connect(name)?;
    // SAFETY: the library opened this named object and validates its v1 layout.
    let memory = unsafe { MappedMemory::attach(connection.shared_memory(), deadline)? };
    Ok((connection, memory))
}

#[cfg(test)]
pub(super) fn minimum_capacity() -> usize {
    page_size().expect("page size")
}

impl Drop for MappedMemory {
    fn drop(&mut self) {
        // SAFETY: these exact mappings are uniquely owned by self.
        unsafe {
            let _ = rustix::mm::munmap(self.header.as_ptr().cast(), self.page_size);
            let _ = rustix::mm::munmap(self.payload.as_ptr().cast(), self.capacity * 2);
        }
    }
}

fn validate_capacity(capacity: usize, page: usize) -> io::Result<()> {
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(error::invalid_layout(
            "capacity must be a nonzero power of two",
        ));
    }
    if capacity < page || !capacity.is_multiple_of(page) {
        return Err(error::invalid_layout("capacity must be page aligned"));
    }
    if capacity as u128 > 1_u128 << 63 {
        return Err(error::invalid_layout("capacity exceeds 2^63"));
    }
    capacity
        .checked_mul(2)
        .ok_or_else(|| error::invalid_layout("double mapping length overflows"))?;
    if size_of::<Header>() > page {
        return Err(error::platform_invariant("header exceeds one page"));
    }
    Ok(())
}

fn validate_shared_memory(fd: &OwnedFd, page: usize, capacity: usize) -> io::Result<()> {
    let stat = rustix::fs::fstat(fd).map_err(io::Error::from)?;
    let expected = page
        .checked_add(capacity)
        .ok_or_else(|| error::invalid_layout("shared-memory object length overflows"))?;
    if stat.st_size < 0 || stat.st_size as u128 != expected as u128 {
        return Err(error::invalid_layout(
            "shared-memory object length does not match capacity",
        ));
    }
    Ok(())
}

fn wait_for_header_size(
    fd: &OwnedFd,
    page: usize,
    deadline: InitializationDeadline,
) -> io::Result<()> {
    loop {
        let stat = rustix::fs::fstat(fd).map_err(io::Error::from)?;
        if stat.st_size < 0 {
            return Err(error::invalid_layout(
                "shared-memory object has a negative length",
            ));
        }
        if stat.st_size as u128 >= page as u128 {
            return Ok(());
        }
        deadline.wait()?;
    }
}

pub(crate) fn page_size() -> io::Result<usize> {
    Ok(rustix::param::page_size())
}

fn map_header(fd: &OwnedFd, page: usize) -> io::Result<NonNull<Header>> {
    let pointer = mmap_shared(ptr::null_mut(), page, fd, 0, false)?;
    NonNull::new(pointer.cast()).ok_or_else(|| error::platform_invariant("null header mapping"))
}

unsafe fn map_payload(fd: &OwnedFd, offset: usize, capacity: usize) -> io::Result<NonNull<u8>> {
    let total = capacity
        .checked_mul(2)
        .ok_or_else(|| error::platform_invariant("double mapping overflow"))?;
    // SAFETY: reserves a new inaccessible anonymous range.
    let base = unsafe {
        rustix::mm::mmap_anonymous(
            ptr::null_mut(),
            total,
            rustix::mm::ProtFlags::empty(),
            rustix::mm::MapFlags::PRIVATE,
        )
    }
    .map_err(io::Error::from)?;
    if let Err(error) = mmap_shared(base, capacity, fd, offset, true) {
        // SAFETY: exact reservation above.
        let _ = unsafe { rustix::mm::munmap(base, total) };
        return Err(error);
    }
    // SAFETY: second half lies within reservation.
    let second = unsafe { base.cast::<u8>().add(capacity).cast() };
    if let Err(error) = mmap_shared(second, capacity, fd, offset, true) {
        // SAFETY: unmaps mapped first half and remaining reservation.
        let _ = unsafe { rustix::mm::munmap(base, total) };
        return Err(error);
    }
    NonNull::new(base.cast()).ok_or_else(|| error::platform_invariant("null payload mapping"))
}

fn mmap_shared(
    address: *mut c_void,
    len: usize,
    fd: &OwnedFd,
    offset: usize,
    fixed: bool,
) -> io::Result<*mut c_void> {
    let offset: u64 = offset
        .try_into()
        .map_err(|_| error::platform_invariant("mapping offset exceeds u64"))?;
    let flags = rustix::mm::MapFlags::SHARED
        | if fixed {
            rustix::mm::MapFlags::FIXED
        } else {
            rustix::mm::MapFlags::empty()
        };
    // SAFETY: checked page-aligned ranges; fixed addresses are owned reservations.
    let pointer = unsafe {
        rustix::mm::mmap(
            address,
            len,
            rustix::mm::ProtFlags::READ | rustix::mm::ProtFlags::WRITE,
            flags,
            fd,
            offset,
        )
    }
    .map_err(io::Error::from)?;
    if fixed && pointer != address {
        Err(error::platform_invariant(
            "MAP_FIXED returned another address",
        ))
    } else {
        Ok(pointer)
    }
}
