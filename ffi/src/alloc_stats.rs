//! FFI access to `peak_alloc`'s process-wide Rust allocation counters.
//!
//! # Scope
//!
//! Counters record requested allocation sizes only, so they are a lower bound on process RSS:
//! C/`mmap` allocations and allocator overhead are not included. Values are process-global and
//! advisory, rather than measurements for a single operation. `peak_alloc` uses relaxed atomic
//! operations to update its counters. Its peak reset is not coordinated with allocation, so callers
//! must serialize a reset against native work when they require a meaningful post-reset peak.

#[cfg(feature = "alloc-tracking")]
use peak_alloc::PeakAlloc;

#[cfg(feature = "alloc-tracking")]
#[global_allocator]
static GLOBAL_ALLOC: PeakAlloc = PeakAlloc;

/// Whether this library was built with allocation tracking.
///
/// The `*_native_bytes` getters return zero when this is false.
#[no_mangle]
pub extern "C" fn alloc_tracking_enabled() -> bool {
    cfg!(feature = "alloc-tracking")
}

/// Reported peak simultaneously live native bytes since the library was loaded or reset.
///
/// The value is process-wide, not per-operation, and counts requested Rust allocation sizes only.
/// Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn peak_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.peak_usage() as u64
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Native bytes currently live (allocated but not yet freed).
///
/// The value is process-wide and counts requested Rust allocation sizes only. Returns zero when
/// built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn current_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.current_usage() as u64
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Resets the peak to the current live total and returns the peak sampled immediately before reset.
///
/// This is process-global and cannot provide a per-task baseline while allocation is concurrent.
/// `peak_alloc` samples and resets separately, so concurrent allocation can cause the returned
/// value to understate the cleared peak and leave the reported peak lower than the current total.
/// Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn reset_peak_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        let previous_peak = GLOBAL_ALLOC.peak_usage() as u64;
        GLOBAL_ALLOC.reset_peak_usage();
        previous_peak
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

#[cfg(all(test, not(feature = "alloc-tracking")))]
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

#[cfg(all(test, feature = "alloc-tracking"))]
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
