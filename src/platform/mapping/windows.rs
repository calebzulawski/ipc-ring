use super::{capacity_for_minimum, wait_for_version};
use crate::error;
use crate::platform::connection::{Connection, InitializationDeadline, SharedMemoryObject};
use crate::platform::sys::windows::windows_error;
use crate::ring::spsc::{ABI_VERSION, Header};
use std::io;
use std::mem::size_of;
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::System::Memory::{
    FILE_MAP_ALL_ACCESS, MEM_PRESERVE_PLACEHOLDER, MEM_RELEASE, MEM_REPLACE_PLACEHOLDER,
    MEM_RESERVE, MEM_RESERVE_PLACEHOLDER, MEMORY_BASIC_INFORMATION, MEMORY_MAPPED_VIEW_ADDRESS,
    MapViewOfFile, MapViewOfFile3, PAGE_NOACCESS, PAGE_READWRITE, UnmapViewOfFile,
    VIRTUAL_FREE_TYPE, VirtualAlloc2, VirtualFree, VirtualQuery,
};
use windows::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

pub(super) struct Layout {
    page_size: usize,
    capacity: usize,
    shared_memory_len: u64,
}

impl Layout {
    pub(super) fn for_minimum_capacity(minimum_capacity: usize) -> io::Result<Self> {
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

    pub(super) fn shared_memory_len(&self) -> u64 {
        self.shared_memory_len
    }
}

pub(super) struct MappedMemory {
    header: NonNull<Header>,
    payload: NonNull<u8>,
    capacity: usize,
}

// SAFETY: mappings are stable; mutable access is controlled by endpoint roles.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

impl MappedMemory {
    pub(super) fn create(
        shared_memory: &SharedMemoryObject,
        layout: Layout,
        consumer_claim: u64,
    ) -> io::Result<Self> {
        // SAFETY: freshly created section and validated layout.
        let memory = unsafe { Self::map(shared_memory.handle(), &layout)? };
        // SAFETY: fresh, aligned writable header.
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

    pub(super) unsafe fn attach(
        shared_memory: &SharedMemoryObject,
        deadline: InitializationDeadline,
    ) -> io::Result<Self> {
        let mapping = shared_memory.handle();
        let (page, granularity) = system_sizes();
        // SAFETY: attachment contract provides a readable/writable mapping.
        let pointer = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, page).Value };
        let header: NonNull<Header> =
            NonNull::new(pointer.cast()).ok_or_else(io::Error::last_os_error)?;
        // SAFETY: trusted attachment and one-page mapping cover Header.
        let value = unsafe { header.as_ref() };
        let version = match wait_for_version(value, deadline) {
            Ok(version) => version,
            Err(error) => {
                // SAFETY: exact view returned above.
                unsafe { unmap_view(pointer) };
                return Err(error);
            }
        };
        if version != ABI_VERSION {
            // SAFETY: exact view returned above.
            unsafe { unmap_view(pointer) };
            return Err(error::unsupported_version(version));
        }
        let capacity: usize = match value.capacity.try_into() {
            Ok(value) => value,
            Err(_) => {
                // SAFETY: exact view returned above.
                unsafe { unmap_view(pointer) };
                return Err(error::invalid_layout("capacity exceeds usize"));
            }
        };
        if let Err(error) = validate_capacity(capacity, page, granularity)
            .and_then(|_| validate_shared_memory(mapping, page, capacity))
        {
            // SAFETY: exact view returned above.
            unsafe { unmap_view(pointer) };
            return Err(error);
        }
        // SAFETY: validated section offset and capacity.
        let payload = match unsafe { map_payload(mapping, page, capacity) } {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: exact view returned above.
                unsafe { unmap_view(pointer) };
                return Err(error);
            }
        };
        Ok(Self {
            header,
            payload,
            capacity,
        })
    }

    unsafe fn map(mapping: HANDLE, layout: &Layout) -> io::Result<Self> {
        // SAFETY: valid fresh section.
        let pointer =
            unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, layout.page_size).Value };
        let header = NonNull::new(pointer.cast()).ok_or_else(io::Error::last_os_error)?;
        // SAFETY: validated fresh section.
        let payload = match unsafe { map_payload(mapping, layout.page_size, layout.capacity) } {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: exact view returned above.
                unsafe { unmap_view(pointer) };
                return Err(error);
            }
        };
        Ok(Self {
            header,
            payload,
            capacity: layout.capacity,
        })
    }

    pub(super) fn header(&self) -> &Header {
        // SAFETY: the mapped header remains valid for self's lifetime.
        unsafe { self.header.as_ref() }
    }

    pub(super) fn payload(&self) -> *mut u8 {
        self.payload.as_ptr()
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }
}

pub(super) fn create_anonymous(
    minimum_capacity: usize,
    consumer_claim: u64,
) -> io::Result<(Connection, MappedMemory)> {
    let layout = Layout::for_minimum_capacity(minimum_capacity)?;
    let connection = Connection::anonymous(layout.shared_memory_len())?;
    let memory = MappedMemory::create(connection.shared_memory(), layout, consumer_claim)?;
    Ok((connection, memory))
}

pub(super) fn bind(
    name: &str,
    minimum_capacity: usize,
    consumer_claim: u64,
) -> io::Result<(Connection, MappedMemory)> {
    let layout = Layout::for_minimum_capacity(minimum_capacity)?;
    let connection = Connection::bind(name, layout.shared_memory_len())?;
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
pub(super) fn allocation_granularity() -> usize {
    system_sizes().1
}

fn validate_capacity(capacity: usize, page: usize, granularity: usize) -> io::Result<()> {
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(error::invalid_layout(
            "capacity must be a nonzero power of two",
        ));
    }
    if capacity < granularity || capacity % granularity != 0 {
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
