//! FFI access to the installed global Rust allocator's requested-byte counters.
//!
//! Direct libc allocations, memory mappings, and allocator overhead are excluded. Counters and
//! limits are shared across threads; resetting the peak does not isolate a particular request.

#[cfg(feature = "global-allocator")]
use delta_kernel::memory::KERNEL_ALLOCATOR;
#[cfg(feature = "global-allocator")]
use delta_kernel::memory::KernelAllocator;

#[cfg(feature = "global-allocator")]
#[global_allocator]
static GLOBAL_ALLOC: KernelAllocator = KernelAllocator::new();

/// Whether this library was built with allocation tracking.
///
/// The `*_native_bytes` getters return zero when this is false.
#[unsafe(no_mangle)]
pub extern "C" fn alloc_tracking_enabled() -> bool {
    cfg!(feature = "global-allocator")
}

/// Peak requested-byte reservations since the library was loaded or reset.
///
/// This includes in-flight requests and conservative old-plus-new reallocation reservations.
/// Values are shared across threads and exclude direct libc allocations and memory mappings.
/// Returns zero when built without `global-allocator`.
#[unsafe(no_mangle)]
pub extern "C" fn peak_native_bytes() -> u64 {
    cfg_select! {
        feature = "global-allocator" => KERNEL_ALLOCATOR.usage().peak as u64,
        _ => 0,
    }
}

/// Current requested Rust bytes, including allocation reservations still in flight.
///
/// This is shared across threads and excludes allocator overhead. Returns zero when
/// built without `global-allocator`.
#[unsafe(no_mangle)]
pub extern "C" fn current_native_bytes() -> u64 {
    cfg_select! {
        feature = "global-allocator" => KERNEL_ALLOCATOR.usage().reserved as u64,
        _ => 0,
    }
}

/// Reset the historical requested-byte peak to current usage and return the previous peak.
///
/// The reset excludes new reservations; concurrent frees can only reduce usage. Values are shared.
/// Returns zero when this library does not install the global allocator.
#[unsafe(no_mangle)]
pub extern "C" fn reset_peak_native_bytes() -> u64 {
    cfg_select! {
        feature = "global-allocator" => KERNEL_ALLOCATOR.reset_peak() as u64,
        _ => 0,
    }
}

#[cfg(all(test, not(feature = "global-allocator")))]
mod disabled_tests {
    use super::{
        alloc_tracking_enabled, current_native_bytes, peak_native_bytes, reset_peak_native_bytes,
    };

    #[test]
    fn getters_report_disabled_tracking() {
        assert!(!alloc_tracking_enabled());
        assert_eq!(peak_native_bytes(), 0);
        assert_eq!(current_native_bytes(), 0);
        assert_eq!(reset_peak_native_bytes(), 0);
    }
}

#[cfg(all(test, feature = "global-allocator"))]
mod global_allocator_tests {
    use super::{
        alloc_tracking_enabled, current_native_bytes, peak_native_bytes, reset_peak_native_bytes,
    };

    // Far above incidental harness allocation, so the bounds below cannot be met by noise.
    const N: usize = 8 * 1024 * 1024;

    #[test]
    fn installed_global_allocator_accounts_a_large_allocation() {
        assert!(alloc_tracking_enabled());
        let _ = reset_peak_native_bytes();
        let before = current_native_bytes();

        let buf = vec![0u8; N];
        let during = current_native_bytes();
        assert!(
            during >= before + N as u64,
            "alloc not tracked: {before} -> {during}"
        );
        assert!(peak_native_bytes() >= during);

        drop(buf);
        let previous_peak = reset_peak_native_bytes();
        assert!(previous_peak >= during);
    }
}
