use super::{capacity_for_minimum, read_version};
use crate::error;
use crate::ring::{ABI_VERSION, Header};
use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::{self, NonNull};
use std::sync::OnceLock;
use std::sync::atomic::Ordering;

struct Layout {
    page_size: usize,
    capacity: usize,
    shared_memory_len: u64,
}

/// Unmaps one provisional region unless ownership moves into `MappedMemory`.
struct MappedRegion<T> {
    pointer: NonNull<T>,
    len: usize,
}

impl<T> MappedRegion<T> {
    unsafe fn from_mmap(pointer: *mut c_void, len: usize) -> io::Result<Self> {
        let Some(pointer) = NonNull::new(pointer.cast()) else {
            // SAFETY: the caller supplied the exact range returned by mmap.
            let _ = unsafe { rustix::mm::munmap(pointer, len) };
            return Err(error::platform_invariant("null mapping"));
        };
        Ok(Self { pointer, len })
    }

    fn as_ptr(&self) -> *mut T {
        self.pointer.as_ptr()
    }
}

impl MappedRegion<Header> {
    fn header(&self) -> &Header {
        // SAFETY: this owner retains a live page containing the header.
        unsafe { self.pointer.as_ref() }
    }
}

impl<T> Drop for MappedRegion<T> {
    fn drop(&mut self) {
        // SAFETY: this owner still holds the exact mapping returned by mmap.
        let _ = unsafe { rustix::mm::munmap(self.pointer.as_ptr().cast(), self.len) };
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
        let capacity = capacity_for_minimum(minimum_capacity)?;
        Self::new(page, capacity)
    }

    fn new(page_size: usize, capacity: usize) -> io::Result<Self> {
        validate_capacity(capacity, page_size)?;
        let shared_memory_len = page_size
            .checked_add(capacity)
            .ok_or_else(|| error::invalid_layout("shared-memory object length overflows"))?
            .try_into()
            .map_err(|_| error::invalid_layout("shared-memory object length exceeds u64"))?;
        Ok(Self {
            page_size,
            capacity,
            shared_memory_len,
        })
    }
}

pub(crate) struct MappedMemory {
    header: MappedRegion<Header>,
    payload: MappedRegion<u8>,
    capacity: usize,
}

// SAFETY: mappings are stable; mutable access is controlled by endpoint roles.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

impl MappedMemory {
    fn create(shared_memory: &SharedMemory, layout: &Layout) -> io::Result<Self> {
        let fd = shared_memory.descriptor();
        // SAFETY: this freshly sized object is exclusively initialized here.
        let memory = unsafe { Self::map(fd, layout)? };
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
        let shared_memory_len = shared_memory_len(fd)?;
        if shared_memory_len < page as u64 {
            return Err(error::invalid_layout(
                "shared-memory object is shorter than its header",
            ));
        }
        let header = map_header(fd, page)?;
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
        let layout = Layout::new(page, capacity)?;
        validate_shared_memory_len(shared_memory_len, &layout)?;

        // SAFETY: validated descriptor, offset, and capacity.
        let payload = unsafe { map_payload(fd, &layout)? };
        Ok(Self {
            header,
            payload,
            capacity: layout.capacity,
        })
    }

    unsafe fn map(fd: &OwnedFd, layout: &Layout) -> io::Result<Self> {
        validate_shared_memory_len(shared_memory_len(fd)?, layout)?;
        let header = map_header(fd, layout.page_size)?;
        // SAFETY: validated descriptor, offset, and capacity.
        let payload = unsafe { map_payload(fd, layout)? };
        Ok(Self {
            header,
            payload,
            capacity: layout.capacity,
        })
    }

    pub(crate) fn header(&self) -> &Header {
        self.header.header()
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

pub(super) fn minimum_capacity() -> usize {
    page_size()
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
    assert!(
        size_of::<Header>() <= page,
        "IPC ring header must fit in one mapping page"
    );
    Ok(())
}

fn shared_memory_len(fd: &OwnedFd) -> io::Result<u64> {
    let stat = rustix::fs::fstat(fd).map_err(io::Error::from)?;
    stat.st_size
        .try_into()
        .map_err(|_| error::invalid_layout("shared-memory object has a negative length"))
}

fn validate_shared_memory_len(shared_memory_len: u64, layout: &Layout) -> io::Result<()> {
    if shared_memory_len < layout.shared_memory_len {
        return Err(error::invalid_layout(
            "shared-memory object is shorter than its declared capacity",
        ));
    }
    Ok(())
}

fn page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    *PAGE_SIZE.get_or_init(rustix::param::page_size)
}

fn map_header(fd: &OwnedFd, page_size: usize) -> io::Result<MappedRegion<Header>> {
    let pointer = mmap_shared(ptr::null_mut(), page_size, fd, 0, false)?;
    // SAFETY: pointer and length are the exact successful mmap result.
    unsafe { MappedRegion::from_mmap(pointer, page_size) }
}

unsafe fn map_payload(fd: &OwnedFd, layout: &Layout) -> io::Result<MappedRegion<u8>> {
    let mapping_len = layout
        .capacity
        .checked_mul(2)
        .ok_or_else(|| error::invalid_layout("double mapping length overflows"))?;
    // SAFETY: reserves a new inaccessible anonymous range.
    let base = unsafe {
        rustix::mm::mmap_anonymous(
            ptr::null_mut(),
            mapping_len,
            rustix::mm::ProtFlags::empty(),
            rustix::mm::MapFlags::PRIVATE,
        )
    }
    .map_err(io::Error::from)?;
    // SAFETY: base and length are the exact successful mmap result.
    let payload: MappedRegion<u8> = unsafe { MappedRegion::from_mmap(base, mapping_len)? };
    mmap_shared(
        payload.as_ptr().cast(),
        layout.capacity,
        fd,
        layout.page_size,
        true,
    )?;
    // SAFETY: second half lies within reservation.
    let second = unsafe { payload.as_ptr().add(layout.capacity).cast() };
    mmap_shared(second, layout.capacity, fd, layout.page_size, true)?;
    Ok(payload)
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
        // SAFETY: this unexpected mapping is still owned by this call.
        let _ = unsafe { rustix::mm::munmap(pointer, len) };
        Err(error::platform_invariant(
            "MAP_FIXED returned another address",
        ))
    } else {
        Ok(pointer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_accepts_trailing_shared_memory() {
        let (shared_memory, memory) = create(minimum_capacity()).unwrap();
        let expanded = shared_memory_len(shared_memory.descriptor()).unwrap() + page_size() as u64;
        rustix::fs::ftruncate(shared_memory.descriptor(), expanded).unwrap();

        // SAFETY: the test supplies the mapping created and initialized above.
        let attached = unsafe { attach(&shared_memory) }.unwrap();
        assert_eq!(attached.capacity(), memory.capacity());
    }
}
