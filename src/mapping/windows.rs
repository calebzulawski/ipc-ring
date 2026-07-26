use super::{capacity_for_minimum, read_version};
use crate::error;
use crate::ring::spsc::{ABI_VERSION, Header};
use crate::sys::windows::{owned, windows_error};
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, OwnedHandle, RawHandle};
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_ALL_ACCESS, MEM_PRESERVE_PLACEHOLDER, MEM_RELEASE,
    MEM_REPLACE_PLACEHOLDER, MEM_RESERVE, MEM_RESERVE_PLACEHOLDER, MEMORY_BASIC_INFORMATION,
    MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile, MapViewOfFile3, PAGE_NOACCESS, PAGE_READWRITE,
    UnmapViewOfFile, VIRTUAL_FREE_TYPE, VirtualAlloc2, VirtualFree, VirtualQuery,
};
use windows::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

struct Layout {
    page_size: usize,
    capacity: usize,
    shared_memory_len: u64,
}

/// Owns a header view until it becomes part of `MappedMemory`.
struct HeaderMapping {
    pointer: NonNull<Header>,
}

impl HeaderMapping {
    unsafe fn map(mapping: HANDLE, page_size: usize) -> io::Result<Self> {
        // SAFETY: the caller provides a live mapping handle and page-sized view.
        let pointer = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, page_size).Value };
        let pointer = NonNull::new(pointer.cast()).ok_or_else(io::Error::last_os_error)?;
        Ok(Self { pointer })
    }

    fn header(&self) -> &Header {
        // SAFETY: this owner retains a live view containing the header.
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
        // SAFETY: this owner still holds the exact view returned by MapViewOfFile.
        unsafe { unmap_view(self.pointer.as_ptr().cast()) };
    }
}

/// Owns an anonymous page-file mapping independently of any mapped views.
pub(crate) struct SharedMemory {
    handle: OwnedHandle,
}

impl SharedMemory {
    fn anonymous(shared_memory_len: u64) -> io::Result<Self> {
        // SAFETY: pagefile mapping with checked exact size and no public name.
        let handle = owned(unsafe {
            CreateFileMappingW(
                windows::Win32::Foundation::INVALID_HANDLE_VALUE,
                None,
                PAGE_READWRITE,
                (shared_memory_len >> 32) as u32,
                shared_memory_len as u32,
                windows::core::PCWSTR::null(),
            )
        })?;
        Ok(Self { handle })
    }

    pub(crate) fn from_handle(handle: OwnedHandle) -> Self {
        Self { handle }
    }

    pub(crate) fn handle(&self) -> HANDLE {
        crate::sys::windows::raw(&self.handle)
    }
}

impl AsRawHandle for SharedMemory {
    fn as_raw_handle(&self) -> RawHandle {
        self.handle.as_raw_handle()
    }
}

impl Layout {
    fn for_minimum_capacity(minimum_capacity: usize) -> io::Result<Self> {
        let (page, granularity) = system_sizes();
        let capacity = capacity_for_minimum(minimum_capacity, granularity)?;
        validate_capacity(capacity, page, granularity)?;
        let shared_memory_len = page
            .checked_add(capacity)
            .ok_or_else(|| error::invalid_input("shared-memory object length overflows"))?
            as u64;
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
    capacity: usize,
}

// SAFETY: mappings are stable; mutable access is controlled by endpoint roles.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

impl MappedMemory {
    fn create(shared_memory: &SharedMemory, layout: &Layout) -> io::Result<Self> {
        // SAFETY: freshly created section and validated layout.
        let memory = unsafe { Self::map(shared_memory.handle(), layout)? };
        // SAFETY: fresh, aligned writable header.
        unsafe { ptr::write(memory.header.as_ptr(), Header::new(layout.capacity)) };
        memory
            .header()
            .version
            .store(ABI_VERSION, Ordering::Release);
        Ok(memory)
    }

    unsafe fn attach(shared_memory: &SharedMemory) -> io::Result<Self> {
        let mapping = shared_memory.handle();
        let (page, granularity) = system_sizes();
        // SAFETY: attachment contract provides a readable/writable mapping.
        let header = unsafe { HeaderMapping::map(mapping, page)? };
        // SAFETY: trusted attachment and one-page mapping cover Header.
        let value = header.header();
        let version = read_version(value)?;
        if version != ABI_VERSION {
            return Err(error::unsupported_version(version));
        }
        let capacity: usize = value
            .capacity
            .try_into()
            .map_err(|_| error::invalid_layout("capacity exceeds usize"))?;
        validate_capacity(capacity, page, granularity)?;
        validate_shared_memory(mapping, page, capacity)?;
        // SAFETY: validated section offset and capacity.
        let payload = unsafe { map_payload(mapping, page, capacity)? };
        Ok(Self {
            header: header.into_pointer(),
            payload,
            capacity,
        })
    }

    unsafe fn map(mapping: HANDLE, layout: &Layout) -> io::Result<Self> {
        // SAFETY: valid fresh section.
        let header = unsafe { HeaderMapping::map(mapping, layout.page_size)? };
        // SAFETY: validated fresh section.
        let payload = unsafe { map_payload(mapping, layout.page_size, layout.capacity)? };
        Ok(Self {
            header: header.into_pointer(),
            payload,
            capacity: layout.capacity,
        })
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: the mapped header remains valid for self's lifetime.
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
    // SAFETY: the handle arrived through the negotiated local control channel;
    // attach validates the complete shared ABI layout.
    unsafe { MappedMemory::attach(shared_memory) }
}

#[cfg(test)]
pub(super) fn minimum_capacity() -> usize {
    allocation_granularity()
}

impl Drop for MappedMemory {
    fn drop(&mut self) {
        // SAFETY: exact views uniquely owned by self.
        unsafe {
            unmap_view(self.header.as_ptr().cast());
            unmap_view(self.payload.as_ptr().cast());
            unmap_view(self.payload.as_ptr().add(self.capacity).cast());
        }
    }
}

#[cfg(test)]
fn allocation_granularity() -> usize {
    system_sizes().1
}

fn validate_capacity(capacity: usize, page: usize, granularity: usize) -> io::Result<()> {
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(error::invalid_layout(
            "capacity must be a nonzero power of two",
        ));
    }
    if capacity < granularity || !capacity.is_multiple_of(granularity) {
        return Err(error::invalid_layout(
            "capacity must match allocation granularity",
        ));
    }
    if capacity as u128 > 1_u128 << 63 {
        return Err(error::invalid_layout("capacity exceeds 2^63"));
    }
    capacity
        .checked_mul(2)
        .ok_or_else(|| error::invalid_layout("double mapping overflow"))?;
    if size_of::<Header>() > page {
        return Err(error::platform_invariant("header exceeds one page"));
    }
    Ok(())
}

fn validate_shared_memory(mapping: HANDLE, page: usize, capacity: usize) -> io::Result<()> {
    let expected = page
        .checked_add(capacity)
        .ok_or_else(|| error::invalid_layout("shared-memory object length overflows"))?;
    // SAFETY: zero length requests a view of the complete section.
    let view = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, 0).Value };
    if view.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: zeroed storage is valid for this output-only structure.
    let mut information = MEMORY_BASIC_INFORMATION::default();
    // SAFETY: the view and output buffer are valid for the stated sizes.
    let queried = unsafe {
        VirtualQuery(
            Some(view.cast_const()),
            &mut information,
            size_of::<MEMORY_BASIC_INFORMATION>(),
        )
    };
    // SAFETY: exact temporary view returned above.
    unsafe { unmap_view(view) };

    if queried != size_of::<MEMORY_BASIC_INFORMATION>()
        || information.BaseAddress != view
        || information.AllocationBase != view
        || information.RegionSize != expected
    {
        return Err(error::invalid_layout(
            "shared-memory object length does not match capacity",
        ));
    }
    Ok(())
}

fn system_sizes() -> (usize, usize) {
    // SAFETY: zeroed structure is valid output storage.
    let mut information = SYSTEM_INFO::default();
    // SAFETY: valid output pointer.
    unsafe { GetSystemInfo(&mut information) };
    (
        information.dwPageSize as usize,
        information.dwAllocationGranularity as usize,
    )
}

unsafe fn map_payload(mapping: HANDLE, offset: usize, capacity: usize) -> io::Result<NonNull<u8>> {
    let total = capacity
        .checked_mul(2)
        .ok_or_else(|| error::platform_invariant("double mapping overflow"))?;
    // SAFETY: reserves an inaccessible placeholder.
    let base = unsafe {
        VirtualAlloc2(
            None,
            None,
            total,
            MEM_RESERVE | MEM_RESERVE_PLACEHOLDER,
            PAGE_NOACCESS.0,
            None,
        )
    };
    if base.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: splits the placeholder exactly in half.
    let split_flags = VIRTUAL_FREE_TYPE(MEM_RELEASE.0 | MEM_PRESERVE_PLACEHOLDER.0);
    if let Err(error) = unsafe { VirtualFree(base, capacity, split_flags) } {
        // SAFETY: exact placeholder reservation above.
        let _ = unsafe { VirtualFree(base, 0, MEM_RELEASE) };
        return Err(windows_error(error));
    }
    // SAFETY: replaces first placeholder with the payload section view.
    let first = unsafe {
        MapViewOfFile3(
            mapping,
            None,
            Some(base.cast_const()),
            offset as u64,
            capacity,
            MEM_REPLACE_PLACEHOLDER,
            PAGE_READWRITE.0,
            None,
        )
    };
    if first.Value.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: both exact placeholders remain reserved.
        unsafe {
            let _ = VirtualFree(base, 0, MEM_RELEASE);
            let _ = VirtualFree(base.add(capacity), 0, MEM_RELEASE);
        }
        return Err(error);
    }
    // SAFETY: second address is the adjacent placeholder.
    let second_address = unsafe { base.add(capacity) };
    let second = unsafe {
        MapViewOfFile3(
            mapping,
            None,
            Some(second_address.cast_const()),
            offset as u64,
            capacity,
            MEM_REPLACE_PLACEHOLDER,
            PAGE_READWRITE.0,
            None,
        )
    };
    if second.Value.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: first is a mapped view and second is the remaining placeholder.
        unsafe {
            unmap_view(first.Value);
            let _ = VirtualFree(second_address, 0, MEM_RELEASE);
        }
        return Err(error);
    }
    NonNull::new(first.Value.cast())
        .ok_or_else(|| error::platform_invariant("null payload mapping"))
}

unsafe fn unmap_view(pointer: *mut std::ffi::c_void) {
    // SAFETY: caller provides the base of a live mapped view.
    let _ = unsafe { UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: pointer }) };
}
