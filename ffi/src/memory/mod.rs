//! Shared global Rust allocation tracking and optional enforcement.

use delta_kernel::memory::{KERNEL_ALLOCATOR, MemoryUsage};

use crate::OptionalValue;

/// Requested-byte counters for allocations through the installed Kernel global allocator.
///
/// This includes standard Rust containers and Rust-owned dependency buffers. Direct libc
/// allocations, memory mappings, and allocator overhead are excluded. Historical peaks can exceed
/// limits enabled after those peaks occurred.
#[repr(C)]
#[derive(Debug)]
pub struct FfiMemoryUsage {
    /// Configured limit, or `None` when enforcement is disabled.
    pub limit: OptionalValue<usize>,
    /// Outstanding requested bytes, including in-flight reservations.
    pub reserved: usize,
    /// Maximum reserved bytes since initialization or peak reset.
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

/// Set or clear the shared global Rust allocation limit.
///
/// The default `global-allocator` feature installs the hook. Rust applications that disable it
/// must install `KernelAllocator` themselves. The first call initializes fixed canonical schemas.
/// `None` disables enforcement. A limit below current usage returns
/// false and preserves the previous limit. Successful updates apply to all threads and existing
/// allocations without resetting counters. Fallible allocation APIs return errors on rejection;
/// infallible paths can invoke Rust's allocation-error handler. This does not bound total RSS.
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
