//! Shared allocation tracking and optional enforcement for Kernel metadata storage.

use delta_kernel::memory::{KERNEL_ALLOCATOR, MemoryUsage};

use crate::OptionalValue;

/// Requested-byte counters for participating Kernel metadata allocations.
///
/// This excludes engine-owned buffers and allocations using other allocators. The peak is
/// historical and can exceed a limit enabled after that peak occurred.
#[repr(C)]
#[derive(Debug)]
pub struct FfiMemoryUsage {
    /// Configured limit, or `None` when enforcement is disabled.
    pub limit: OptionalValue<usize>,
    /// Outstanding requested bytes, including in-flight reservations.
    pub reserved: usize,
    /// Maximum reserved bytes since initialization.
    pub peak: usize,
    /// Requests rejected by the configured limit.
    pub rejected: usize,
}

impl From<MemoryUsage> for FfiMemoryUsage {
    fn from(usage: MemoryUsage) -> Self {
        Self {
            limit: usage.limit.into(),
            reserved: usage.reserved,
            peak: usage.peak,
            rejected: usage.rejected,
        }
    }
}

/// Set or clear the shared metadata allocation limit.
///
/// The first call initializes and counts fixed canonical schemas. Tracking is always enabled.
/// `None` disables enforcement. A limit below current usage returns
/// false and preserves the previous limit. Successful updates apply to all threads and existing
/// allocations without resetting counters. This does not bound total native memory or RSS.
#[unsafe(no_mangle)]
pub extern "C" fn kernel_memory_set_limit(limit: OptionalValue<usize>) -> bool {
    KERNEL_ALLOCATOR.set_limit(limit.into()).is_ok()
}

/// Copy the shared allocator counters to `out`, returning false for a null pointer.
///
/// # Safety
///
/// A non-null `out` must be aligned, writable storage for one `FfiMemoryUsage`, exclusively
/// borrowed for the call. No caller memory is retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kernel_memory_usage(out: *mut FfiMemoryUsage) -> bool {
    if out.is_null() {
        return false;
    }
    unsafe { out.write(KERNEL_ALLOCATOR.usage().into()) };
    true
}
