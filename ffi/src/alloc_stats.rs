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
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "alloc-tracking")]
use peak_alloc::PeakAlloc;

#[cfg(feature = "alloc-tracking")]
struct TrackingAlloc<A> {
    inner: A,
    total_allocated: AtomicU64,
    total_freed: AtomicU64,
}

#[cfg(feature = "alloc-tracking")]
impl<A> TrackingAlloc<A> {
    const fn new(inner: A) -> Self {
        Self {
            inner,
            total_allocated: AtomicU64::new(0),
            total_freed: AtomicU64::new(0),
        }
    }

    fn add_bytes(counter: &AtomicU64, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_add(bytes))
        });
    }
}

#[cfg(feature = "alloc-tracking")]
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            Self::add_bytes(&self.total_allocated, layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        Self::add_bytes(&self.total_freed, layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            Self::add_bytes(&self.total_allocated, layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            Self::add_bytes(&self.total_allocated, new_size);
            Self::add_bytes(&self.total_freed, layout.size());
        }
        new_ptr
    }
}

#[cfg(feature = "alloc-tracking")]
#[global_allocator]
static GLOBAL_ALLOC: TrackingAlloc<PeakAlloc> = TrackingAlloc::new(PeakAlloc);

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
        GLOBAL_ALLOC.inner.peak_usage() as u64
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
        GLOBAL_ALLOC.inner.current_usage() as u64
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
        let previous_peak = GLOBAL_ALLOC.inner.peak_usage() as u64;
        GLOBAL_ALLOC.inner.reset_peak_usage();
        previous_peak
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Total native bytes successfully allocated since the library was loaded.
///
/// The value is process-wide, counts requested Rust allocation sizes only, and saturates at
/// `u64::MAX`. This getter and [`total_freed_native_bytes`] are independent atomic samples;
/// callers must quiesce native work before comparing them. A successful reallocation counts the
/// new block size as allocated. Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn total_allocated_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.total_allocated.load(Ordering::Relaxed)
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Total native bytes freed since the library was loaded.
///
/// The value is process-wide, counts requested Rust allocation sizes only, and saturates at
/// `u64::MAX`. This getter and [`total_allocated_native_bytes`] are independent atomic samples;
/// callers must quiesce native work before comparing them. A successful reallocation counts the
/// old block size as freed. Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn total_freed_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.total_freed.load(Ordering::Relaxed)
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
        total_allocated_native_bytes, total_freed_native_bytes,
    };

    #[test]
    fn getters_report_disabled_tracking() {
        assert!(!alloc_tracking_enabled());
        assert_eq!(peak_native_bytes(), 0);
        assert_eq!(current_native_bytes(), 0);
        assert_eq!(reset_peak_native_bytes(), 0);
        assert_eq!(total_allocated_native_bytes(), 0);
        assert_eq!(total_freed_native_bytes(), 0);
    }
}

#[cfg(all(test, feature = "alloc-tracking"))]
mod global_allocator_tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::ptr::null_mut;
    use std::sync::atomic::Ordering;

    use super::{
        alloc_tracking_enabled, current_native_bytes, peak_native_bytes, reset_peak_native_bytes,
        total_allocated_native_bytes, total_freed_native_bytes, TrackingAlloc,
    };

    // Far above incidental harness allocation, so the bounds below cannot be met by noise.
    const N: usize = 8 * 1024 * 1024;

    #[test]
    fn installed_global_allocator_accounts_a_large_allocation() {
        assert!(alloc_tracking_enabled());
        let _ = reset_peak_native_bytes();
        let before = current_native_bytes();
        let allocated_before = total_allocated_native_bytes();
        let freed_before = total_freed_native_bytes();

        let buf = vec![0u8; N];
        let during = current_native_bytes();
        assert!(
            during >= before + N as u64,
            "alloc not tracked: {before} -> {during}"
        );
        assert!(peak_native_bytes() >= during);
        assert!(total_allocated_native_bytes() >= allocated_before.saturating_add(N as u64));

        drop(buf);
        assert!(total_freed_native_bytes() >= freed_before.saturating_add(N as u64));
        let previous_peak = reset_peak_native_bytes();
        assert!(previous_peak >= during);
    }

    #[test]
    fn cumulative_counters_track_all_allocator_operations_and_balance() {
        let allocator = TrackingAlloc::new(System);
        let initial_layout = Layout::from_size_align(64, 8).unwrap();
        let grown_layout = Layout::from_size_align(128, 8).unwrap();
        let zeroed_layout = Layout::from_size_align(32, 8).unwrap();

        unsafe {
            let ptr = allocator.alloc(initial_layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.total_allocated.load(Ordering::Relaxed), 64);
            assert_eq!(allocator.total_freed.load(Ordering::Relaxed), 0);

            let ptr = allocator.realloc(ptr, initial_layout, grown_layout.size());
            assert!(!ptr.is_null());
            assert_eq!(allocator.total_allocated.load(Ordering::Relaxed), 192);
            assert_eq!(allocator.total_freed.load(Ordering::Relaxed), 64);

            allocator.dealloc(ptr, grown_layout);
            assert_eq!(allocator.total_allocated.load(Ordering::Relaxed), 192);
            assert_eq!(allocator.total_freed.load(Ordering::Relaxed), 192);

            let ptr = allocator.alloc_zeroed(zeroed_layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.total_allocated.load(Ordering::Relaxed), 224);
            assert_eq!(allocator.total_freed.load(Ordering::Relaxed), 192);
            for offset in 0..zeroed_layout.size() {
                assert_eq!(*ptr.add(offset), 0);
            }

            allocator.dealloc(ptr, zeroed_layout);
        }

        let allocated = allocator.total_allocated.load(Ordering::Relaxed);
        let freed = allocator.total_freed.load(Ordering::Relaxed);
        assert_eq!(allocated, 224);
        assert_eq!(freed, allocated);
    }

    #[test]
    fn cumulative_counters_saturate() {
        let allocator = TrackingAlloc::new(System);
        let layout = Layout::from_size_align(64, 8).unwrap();
        allocator
            .total_allocated
            .store(u64::MAX - 32, Ordering::Relaxed);

        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.total_allocated.load(Ordering::Relaxed), u64::MAX);

            allocator
                .total_freed
                .store(u64::MAX - 32, Ordering::Relaxed);
            allocator.dealloc(ptr, layout);
        }

        assert_eq!(allocator.total_freed.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn failed_allocator_operations_do_not_change_cumulative_counters() {
        let layout = Layout::from_size_align(64, 8).unwrap();

        let allocator = TrackingAlloc::new(FailingAlloc(FailedOperation::Alloc));
        assert!(unsafe { allocator.alloc(layout) }.is_null());
        assert_eq!(counters(&allocator), (0, 0));

        let allocator = TrackingAlloc::new(FailingAlloc(FailedOperation::AllocZeroed));
        assert!(unsafe { allocator.alloc_zeroed(layout) }.is_null());
        assert_eq!(counters(&allocator), (0, 0));

        let allocator = TrackingAlloc::new(FailingAlloc(FailedOperation::Realloc));
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            assert_eq!(counters(&allocator), (64, 0));

            assert!(allocator.realloc(ptr, layout, 128).is_null());
            assert_eq!(counters(&allocator), (64, 0));

            allocator.dealloc(ptr, layout);
        }
        assert_eq!(counters(&allocator), (64, 64));
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FailedOperation {
        Alloc,
        AllocZeroed,
        Realloc,
    }

    struct FailingAlloc(FailedOperation);

    unsafe impl GlobalAlloc for FailingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if self.0 == FailedOperation::Alloc {
                null_mut()
            } else {
                unsafe { System.alloc(layout) }
            }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if self.0 == FailedOperation::AllocZeroed {
                null_mut()
            } else {
                unsafe { System.alloc_zeroed(layout) }
            }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if self.0 == FailedOperation::Realloc {
                null_mut()
            } else {
                unsafe { System.realloc(ptr, layout, new_size) }
            }
        }
    }

    fn counters<A>(allocator: &TrackingAlloc<A>) -> (u64, u64) {
        (
            allocator.total_allocated.load(Ordering::Relaxed),
            allocator.total_freed.load(Ordering::Relaxed),
        )
    }
}
