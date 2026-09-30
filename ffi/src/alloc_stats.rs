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
use std::alloc::{GlobalAlloc, Layout};
#[cfg(feature = "alloc-tracking")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "alloc-tracking")]
use peak_alloc::PeakAlloc;

#[cfg(feature = "alloc-tracking")]
static EXTERNAL_BYTES: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "alloc-tracking")]
static ACCOUNTED_PEAK: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "alloc-tracking")]
struct AccountedAllocator;

#[cfg(feature = "alloc-tracking")]
fn sample_accounted() {
    ACCOUNTED_PEAK.fetch_max(
        PeakAlloc
            .current_usage()
            .saturating_add(EXTERNAL_BYTES.load(Ordering::Relaxed)),
        Ordering::Relaxed,
    );
}

#[cfg(feature = "alloc-tracking")]
unsafe impl GlobalAlloc for AccountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { PeakAlloc.alloc(layout) };
        sample_accounted();
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { PeakAlloc.alloc_zeroed(layout) };
        sample_accounted();
        result
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { PeakAlloc.dealloc(ptr, layout) };
    }

    // GlobalAlloc's default realloc calls this wrapper's alloc and dealloc. Like PeakAlloc,
    // it allocates, copies and frees, while exposing the transient old-plus-new allocation peak.
}

#[cfg(feature = "alloc-tracking")]
#[global_allocator]
static GLOBAL_ALLOC: AccountedAllocator = AccountedAllocator;

/// Report connector-scoped native payload bytes for simultaneous peak accounting.
/// This excludes unreachable allocations awaiting GC, allocator overhead, and unreported buffers.
/// It is a process-wide gauge; the caller must report every scope lifetime change consistently.
#[no_mangle]
pub extern "C" fn set_external_native_bytes(bytes: usize) {
    #[cfg(feature = "alloc-tracking")]
    {
        EXTERNAL_BYTES.store(bytes, Ordering::Relaxed);
        sample_accounted();
    }
    #[cfg(not(feature = "alloc-tracking"))]
    let _ = bytes;
}

/// Maximum simultaneous requested Rust and reported connector-scoped bytes since peak reset.
/// This is an advisory logical-lifetime measurement, not process RSS or a sum of separate peaks.
#[no_mangle]
pub extern "C" fn peak_accounted_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        ACCOUNTED_PEAK.load(Ordering::Relaxed) as u64
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

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
        PeakAlloc.peak_usage() as u64
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
        PeakAlloc.current_usage() as u64
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
        let previous_peak = PeakAlloc.peak_usage() as u64;
        PeakAlloc.reset_peak_usage();
        ACCOUNTED_PEAK.store(
            PeakAlloc
                .current_usage()
                .saturating_add(EXTERNAL_BYTES.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
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
        alloc_tracking_enabled, current_native_bytes, peak_accounted_native_bytes,
        peak_native_bytes, reset_peak_native_bytes, set_external_native_bytes,
    };

    // Far above incidental harness allocation, so the bounds below cannot be met by noise.
    const N: usize = 8 * 1024 * 1024;

    #[test]
    fn joint_peak_includes_both_buffers_during_reallocation() {
        set_external_native_bytes(0);
        let mut buf = Vec::with_capacity(N);
        buf.resize(N, 1u8);
        reset_peak_native_bytes();
        let before = current_native_bytes();
        buf.reserve_exact(N);
        assert!(peak_native_bytes() >= before + 2 * N as u64);
        assert!(peak_accounted_native_bytes() >= peak_native_bytes());
    }

    #[test]
    fn joint_peak_observes_overlapping_external_and_rust_allocations() {
        set_external_native_bytes(N);
        reset_peak_native_bytes();
        let before = current_native_bytes();
        let buf = vec![1u8; N];
        assert!(peak_accounted_native_bytes() >= before + 2 * N as u64);
        drop(buf);
        set_external_native_bytes(0);
        reset_peak_native_bytes();
        assert!(peak_accounted_native_bytes() >= current_native_bytes());
    }

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
