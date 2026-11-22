use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::Mutex;

use super::{MemoryError, MemoryResult};

/// The allocator used by Kernel's participating metadata storage.
///
/// Every copy uses the same counters and optional limit, including across threads and FFI calls.
/// This is an explicit [`Allocator`]; it does not replace the process's global allocator.
/// Use fallible collection growth (`try_reserve`): infallible growth can invoke Rust's
/// allocation-error handler when this allocator returns [`AllocError`].
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

    fn reserve(&self, bytes: usize) -> Result<Reservation, AllocError> {
        let mut state = STATE.lock().unwrap_or_else(|error| error.into_inner());
        let next = state.reserved.checked_add(bytes);
        match next {
            Some(next) if state.limit.is_none_or(|limit| next <= limit) => {
                state.reserved = next;
                state.peak = state.peak.max(next);
                Ok(Reservation { bytes })
            }
            _ => {
                state.rejected = state.rejected.saturating_add(1);
                Err(AllocError)
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
        backing: impl FnOnce(Layout) -> Result<NonNull<[u8]>, AllocError>,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let reservation = self.reserve(layout.size())?;
        let allocation = backing(layout)?;
        // Expose exactly the charged length, so every fitting layout frees that same charge.
        let allocation = NonNull::slice_from_raw_parts(allocation.cast(), layout.size());
        std::mem::forget(reservation);
        Ok(allocation)
    }
}

// SAFETY: Global supplies aligned storage; the exposed block size is exactly the accounted size.
// All copies share one state. Default grow/shrink allocate the replacement before freeing the
// original block, so both blocks count toward the limit and failure preserves the original block.
unsafe impl Allocator for KernelAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, |layout| Global.allocate(layout))
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, |layout| Global.allocate_zeroed(layout))
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: The caller supplies a live allocation from this allocator and a fitting layout.
        unsafe { Global.deallocate(ptr, layout) };
        self.release(layout.size());
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
