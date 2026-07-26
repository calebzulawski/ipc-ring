use super::{capacity_for_minimum, read_version};
use crate::error;
use crate::ring::spsc::{ABI_VERSION, Header};
use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;

struct Layout {
    page_size: usize,
    capacity: usize,
    shared_memory_len: u64,
}

/// Owns a header mapping until it becomes part of `MappedMemory`.
struct HeaderMapping {
    pointer: NonNull<Header>,
    page_size: usize,
}

impl HeaderMapping {
    fn map(fd: &OwnedFd, page_size: usize) -> io::Result<Self> {
        let pointer = mmap_shared(ptr::null_mut(), page_size, fd, 0, false)?;
        let pointer = NonNull::new(pointer.cast())
            .ok_or_else(|| error::platform_invariant("null header mapping"))?;
        Ok(Self { pointer, page_size })
    }

    fn header(&self) -> &Header {
        // SAFETY: this owner retains a live page containing the header.
        unsafe { self.pointer.as_ref() }
    }

    fn into_pointer(self) -> NonNull<Header> {
        let pointer = self.pointer;
        std::mem::forget(self);
        pointer
    }
}

impl Drop for HeaderMapping {
    fn drop(&mut self) {
        // SAFETY: this owner still holds the exact mapping returned by mmap.
        let _ = unsafe { rustix::mm::munmap(self.pointer.as_ptr().cast(), self.page_size) };
    }
}

/// Owns an anonymous shared-memory object independently of any mapped views.
pub(crate) struct SharedMemory {
    descriptor: OwnedFd,
}

impl SharedMemory {
    fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        let descriptor = shm_open_anonymous::shm_open_anonymous();
        if descriptor == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: shm_open_anonymous returned a new, owned descriptor.
        let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
        rustix::fs::ftruncate(&descriptor, shared_memory_len).map_err(io::Error::from)?;
        Ok(Self { descriptor })
    }

    pub(crate) fn from_handle(descriptor: OwnedFd) -> Self {
        Self { descriptor }
    }

    fn descriptor(&self) -> &OwnedFd {
        &self.descriptor
    }

    #[cfg(test)]
    pub(crate) fn duplicate_descriptor(&self) -> io::Result<OwnedFd> {
        rustix::io::fcntl_dupfd_cloexec(&self.descriptor, 0).map_err(Into::into)
    }
}

impl AsFd for SharedMemory {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }
}

impl Layout {
    fn for_minimum_capacity(minimum_capacity: usize) -> io::Result<Self> {
        let page = page_size();
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

pub(crate) struct MappedMemory {
    header: NonNull<Header>,
    payload: NonNull<u8>,
    page_size: usize,
    capacity: usize,
}

// SAFETY: mappings are stable; mutable access is controlled by endpoint roles.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

impl MappedMemory {
    fn create(shared_memory: &SharedMemory, layout: &Layout) -> io::Result<Self> {
        let fd = shared_memory.descriptor();
        // SAFETY: this freshly sized object is exclusively initialized here.
        let memory = unsafe { Self::map(fd, layout.page_size, layout.capacity)? };
        // SAFETY: header is aligned, writable, and currently private to creator.
        unsafe { ptr::write(memory.header.as_ptr(), Header::new(layout.capacity)) };
        memory
            .header()
            .version
            .store(ABI_VERSION, Ordering::Release);
        Ok(memory)
    }

    unsafe fn attach(shared_memory: &SharedMemory) -> io::Result<Self> {
        let fd = shared_memory.descriptor();
        let page = page_size();
        validate_header_size(fd, page)?;
        let header = HeaderMapping::map(fd, page)?;
        // SAFETY: caller supplies a trusted attachment; a native page covers Header.
        let value = header.header();
        let version = read_version(value)?;
        if version != ABI_VERSION {
            return Err(error::unsupported_version(version));
        }

        let capacity: usize = value
            .capacity
            .try_into()
            .map_err(|_| error::invalid_layout("capacity exceeds usize"))?;
        validate_capacity(capacity, page)?;
        validate_shared_memory(fd, page, capacity)?;

        // SAFETY: validated descriptor, offset, and capacity.
        let payload = unsafe { map_payload(fd, page, capacity)? };
        Ok(Self {
            header: header.into_pointer(),
            payload,
            page_size: page,
            capacity,
        })
    }

    unsafe fn map(fd: &OwnedFd, page: usize, capacity: usize) -> io::Result<Self> {
        validate_shared_memory(fd, page, capacity)?;
        let header = HeaderMapping::map(fd, page)?;
        // SAFETY: validated descriptor, offset, and capacity.
        let payload = unsafe { map_payload(fd, page, capacity)? };
        Ok(Self {
            header: header.into_pointer(),
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
}

pub(super) fn create(minimum_capacity: usize) -> io::Result<(SharedMemory, MappedMemory)> {
    let layout = Layout::for_minimum_capacity(minimum_capacity)?;
    let shared_memory = SharedMemory::anonymous(layout.shared_memory_len)?;
    let memory = MappedMemory::create(&shared_memory, &layout)?;
    Ok((shared_memory, memory))
}

pub(super) unsafe fn attach(shared_memory: &SharedMemory) -> io::Result<MappedMemory> {
    // SAFETY: the descriptor arrived through the negotiated local control channel;
    // attach validates the complete shared ABI layout.
    unsafe { MappedMemory::attach(shared_memory) }
}

#[cfg(test)]
pub(super) fn minimum_capacity() -> usize {
    page_size()
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

fn validate_header_size(fd: &OwnedFd, page: usize) -> io::Result<()> {
    let stat = rustix::fs::fstat(fd).map_err(io::Error::from)?;
    if stat.st_size < 0 {
        return Err(error::invalid_layout(
            "shared-memory object has a negative length",
        ));
    }
    if (stat.st_size as u128) < page as u128 {
        return Err(error::invalid_layout(
            "shared-memory object is shorter than its header",
        ));
    }
    Ok(())
}

fn page_size() -> usize {
    rustix::param::page_size()
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
