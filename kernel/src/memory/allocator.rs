use std::alloc::{self, GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::Mutex;

use allocator_api2::alloc::{AllocError, Allocator};

use super::{MemoryError, MemoryResult};

/// The allocator used by Kernel's participating metadata storage.
///
/// Every copy uses the same counters and optional limit, including across threads and FFI calls.
/// Storage calls this [`GlobalAlloc`] backend explicitly. It forwards to the process's existing
/// allocator, so it must not itself be installed with `#[global_allocator]`.
/// Use fallible collection growth (`try_reserve`): infallible growth can invoke Rust's
/// allocation-error handler when this allocator returns null.
#[derive(Clone, Copy, Debug)]
pub struct KernelAllocator {
    _private: (),
}

/// Shared allocator instance for allocation-heavy Kernel metadata.
///
/// Tracking starts with the first allocation. Enforcement is disabled until
/// [`KernelAllocator::set_limit`] succeeds.
pub static KERNEL_ALLOCATOR: KernelAllocator = KernelAllocator { _private: () };

/// A coherent observation of the shared allocator's requested-byte counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryUsage {
    /// Configured cap, or `None` when enforcement is disabled.
    pub limit: Option<usize>,
    /// Outstanding requested bytes, including allocations currently in flight.
    pub reserved: usize,
    /// Largest reserved count since initialization, including old-plus-new growth capacity.
    pub peak: usize,
    /// Requests rejected before calling the backing allocator.
    pub rejected: usize,
}

static STATE: Mutex<MemoryUsage> = Mutex::new(MemoryUsage {
    limit: None,
    reserved: 0,
    peak: 0,
    rejected: 0,
});

impl KernelAllocator {
    /// Set or clear the shared limit without resetting usage or historical counters.
    ///
    /// The first call initializes and tracks shared canonical schemas before applying the limit.
    /// A limit below current usage is rejected without changing the previous limit. Configuration
    /// and allocation reservations are serialized, so successful updates also cover allocations
    /// already in flight. `None` disables enforcement while retaining tracking.
    pub fn set_limit(&self, limit: Option<usize>) -> MemoryResult<()> {
        super::initialize_schemas();
        let mut state = STATE.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(limit) = limit
            && limit < state.reserved
        {
            return Err(MemoryError::LimitBelowUsage);
        }
        state.limit = limit;
        Ok(())
    }

    /// Read the shared counters, including usage allocated before a limit was configured.
    pub fn usage(&self) -> MemoryUsage {
        *STATE.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn allocation_error(&self) -> MemoryError {
        MemoryError::AllocationFailed {
            limit: self.usage().limit.unwrap_or(usize::MAX),
        }
    }

    fn reserve(&self, bytes: usize) -> Option<Reservation> {
        let mut state = STATE.lock().unwrap_or_else(|error| error.into_inner());
        let next = state.reserved.checked_add(bytes);
        match next {
            Some(next) if state.limit.is_none_or(|limit| next <= limit) => {
                state.reserved = next;
                state.peak = state.peak.max(next);
                Some(Reservation { bytes })
            }
            _ => {
                state.rejected = state.rejected.saturating_add(1);
                None
            }
        }
    }

    fn release(&self, bytes: usize) {
        let mut state = STATE.lock().unwrap_or_else(|error| error.into_inner());
        state.reserved -= bytes;
    }

    pub(super) fn allocate_with(
        &self,
        layout: Layout,
        backing: impl FnOnce(Layout) -> *mut u8,
    ) -> *mut u8 {
        let Some(reservation) = self.reserve(layout.size()) else {
            return std::ptr::null_mut();
        };
        let allocation = backing(layout);
        if !allocation.is_null() {
            std::mem::forget(reservation);
        }
        allocation
    }
}

// SAFETY: The host allocation functions supply aligned storage and receive the original layouts.
// Reservation updates finish before the backing call; a failed backing call rolls them back.
// This backend is invoked explicitly and is never registered as the process allocator.
unsafe impl GlobalAlloc for KernelAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocate_with(layout, |layout| unsafe { alloc::alloc(layout) })
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocate_with(layout, |layout| unsafe { alloc::alloc_zeroed(layout) })
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The caller supplies a live allocation from this allocator and a fitting layout.
        unsafe { alloc::dealloc(ptr, layout) };
        self.release(layout.size());
    }
}

/// Adapts the stable backend to fallible containers without using std's allocator API.
#[derive(Clone, Copy, Debug)]
pub(super) struct ContainerAllocator;

impl ContainerAllocator {
    fn allocate_with(&self, layout: Layout, zeroed: bool) -> Result<NonNull<[u8]>, AllocError> {
        let pointer = if layout.size() == 0 {
            std::ptr::without_provenance_mut(layout.align())
        } else if zeroed {
            // SAFETY: The layout is nonzero and the backend forwards its alignment unchanged.
            unsafe { KERNEL_ALLOCATOR.alloc_zeroed(layout) }
        } else {
            // SAFETY: The layout is nonzero and the backend forwards its alignment unchanged.
            unsafe { KERNEL_ALLOCATOR.alloc(layout) }
        };
        NonNull::new(pointer)
            .map(|pointer| NonNull::slice_from_raw_parts(pointer, layout.size()))
            .ok_or(AllocError)
    }
}

// SAFETY: Each block retains this allocator through its owner. Zero-size blocks use an aligned
// sentinel. Default grow/shrink allocate before freeing, preserving the old block on failure and
// charging overlapping storage until the backing allocation is actually released.
unsafe impl Allocator for ContainerAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, false)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, true)
    }

    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            // SAFETY: Nonzero blocks come from this backend and retain their original layout.
            unsafe { KERNEL_ALLOCATOR.dealloc(pointer.as_ptr(), layout) };
        }
    }
}

struct Reservation {
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        KERNEL_ALLOCATOR.release(self.bytes);
    }
}
