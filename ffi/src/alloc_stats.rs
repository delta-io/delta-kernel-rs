//! Process-wide accounting of the kernel's Rust-allocated native heap via
//! [`TrackingAlloc`] (installed as `#[global_allocator]` under `alloc-tracking`).
//!
//! # Scope
//!
//! Counters record requested [`Layout::size`] only, so they are a lower bound on
//! process RSS (C/`mmap` allocations and allocator overhead are outside the path).
//! Process-global: shared across concurrent work; [`peak_native_bytes`] is a
//! process-wide high-water mark since the library was loaded (or the last
//! [`reset_peak_native_bytes`]), not a per-operation figure. Updates use `Relaxed`
//! ordering -- advisory stats: two atomic RMWs per allocation (`fetch_add` +
//! `fetch_max`) and one per deallocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Live-bytes and peak-bytes counters. Split out from [`TrackingAlloc`] so the arithmetic can be
/// unit-tested against a local instance without touching the process-global allocator state.
struct Counters {
    current: AtomicU64,
    peak: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    #[inline]
    #[cfg_attr(not(feature = "alloc-tracking"), allow(dead_code))]
    fn record_alloc(&self, n: usize) {
        let new_current = self.current.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
        self.peak.fetch_max(new_current, Ordering::Relaxed);
    }

    /// Leave `peak` unchanged on dealloc.
    #[inline]
    #[cfg_attr(not(feature = "alloc-tracking"), allow(dead_code))]
    fn record_dealloc(&self, n: usize) {
        self.current.fetch_sub(n as u64, Ordering::Relaxed);
    }

    /// Record a successful resize from `old` to `new` bytes as a single net update.
    #[inline]
    #[cfg_attr(not(feature = "alloc-tracking"), allow(dead_code))]
    fn record_realloc(&self, old: usize, new: usize) {
        match new.checked_sub(old) {
            Some(growth) => self.record_alloc(growth),
            None => self.record_dealloc(old - new),
        }
    }

    fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    fn current_bytes(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Set peak to current live bytes, returning the peak that was cleared.
    ///
    /// After `store`/`swap`, a concurrent alloc can leave `peak < current`; `fetch_max`
    /// repairs that invariant.
    fn take_peak(&self) -> u64 {
        let current = self.current.load(Ordering::Relaxed);
        let previous = self.peak.swap(current, Ordering::Relaxed);
        self.peak
            .fetch_max(self.current.load(Ordering::Relaxed), Ordering::Relaxed);
        previous
    }
}

static COUNTERS: Counters = Counters::new();

/// A [`GlobalAlloc`] that forwards every request to the [`System`] allocator while maintaining the
/// process-global [`COUNTERS`].
///
/// Constructed only via `#[global_allocator]` when `alloc-tracking` is enabled.
#[cfg_attr(not(feature = "alloc-tracking"), allow(dead_code))]
struct TrackingAlloc;

#[cfg(feature = "alloc-tracking")]
#[global_allocator]
static GLOBAL_ALLOC: TrackingAlloc = TrackingAlloc;

// SAFETY: every method forwards to the corresponding `System` method with the exact same
// arguments, so the allocator contract is upheld. The counter updates only read/write atomics and
// never touch the returned memory, so they cannot violate allocation safety. Counters are adjusted
// only when `System` reports success (non-null pointer) to keep `current` in step with live bytes.
unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            COUNTERS.record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            COUNTERS.record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        COUNTERS.record_dealloc(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        // On failure `System.realloc` leaves the original allocation intact, so live bytes are
        // unchanged. On success the block moves from `layout.size()` to `new_size` bytes.
        if !new_ptr.is_null() {
            COUNTERS.record_realloc(layout.size(), new_size);
        }
        new_ptr
    }
}

/// Whether this library was built with allocation tracking. The `*_native_bytes` getters return
/// zeros when false.
#[no_mangle]
pub extern "C" fn alloc_tracking_enabled() -> bool {
    cfg!(feature = "alloc-tracking")
}

/// Peak native bytes ever simultaneously live since the library was loaded or the last
/// [`reset_peak_native_bytes`].
///
/// Process-wide (not per-operation). Counts Rust [`Layout::size`] only (excludes C/`mmap`
/// allocations and allocator overhead). Returns 0 if built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn peak_native_bytes() -> u64 {
    COUNTERS.peak_bytes()
}

/// Native bytes currently live (allocated but not yet freed).
///
/// Process-wide. Counts Rust [`Layout::size`] only. Returns 0 if built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn current_native_bytes() -> u64 {
    COUNTERS.current_bytes()
}

/// Set peak to the current live total and return the peak that was cleared.
///
/// Process-global: not safe as a per-task baseline under concurrency. Callers that need a
/// single-tenant measurement must serialize resets against the work they measure. Returns 0 if
/// built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn reset_peak_native_bytes() -> u64 {
    COUNTERS.take_peak()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::Counters;

    #[derive(Clone, Copy)]
    enum Op {
        Alloc(usize),
        Dealloc(usize),
        Realloc { old: usize, new: usize },
        TakePeak,
    }

    #[rstest]
    #[case::alloc_raises_current_and_peak(&[Op::Alloc(4096)], 4096, 4096)]
    #[case::dealloc_lowers_current_but_not_peak(
        &[Op::Alloc(8192), Op::Dealloc(8192)],
        0,
        8192
    )]
    #[case::peak_tracks_max_not_last(
        &[Op::Alloc(1000), Op::Alloc(500), Op::Dealloc(500), Op::Alloc(200)],
        1200,
        1500
    )]
    #[case::take_peak_returns_cleared_and_drops_to_current(
        &[Op::Alloc(1024), Op::Dealloc(512), Op::TakePeak],
        512,
        512
    )]
    #[case::realloc_growth_raises_peak(
        &[Op::Alloc(100), Op::Realloc { old: 100, new: 300 }],
        300,
        300
    )]
    #[case::realloc_shrink_keeps_peak(
        &[Op::Alloc(300), Op::Realloc { old: 300, new: 100 }],
        100,
        300
    )]
    fn counter_arithmetic(#[case] ops: &[Op], #[case] current: u64, #[case] peak: u64) {
        let c = Counters::new();
        let mut cleared_peak = None;
        for op in ops {
            match *op {
                Op::Alloc(n) => c.record_alloc(n),
                Op::Dealloc(n) => c.record_dealloc(n),
                Op::Realloc { old, new } => c.record_realloc(old, new),
                Op::TakePeak => cleared_peak = Some(c.take_peak()),
            }
        }
        assert_eq!((c.current_bytes(), c.peak_bytes()), (current, peak));
        if let Some(cleared) = cleared_peak {
            // TakePeak after Alloc(1024)+Dealloc(512) cleared peak 1024.
            assert_eq!(cleared, 1024);
        }
    }

    #[test]
    fn take_peak_repairs_peak_ge_current_after_store() {
        // Simulate the concurrent-alloc race: after swap sets peak to a stale current, a
        // later fetch_max must restore peak >= current.
        let c = Counters::new();
        c.record_alloc(100);
        let previous = c.take_peak();
        assert_eq!(previous, 100);
        assert!(c.peak_bytes() >= c.current_bytes());
        c.record_alloc(50);
        assert!(c.peak_bytes() >= c.current_bytes());
        assert_eq!(c.current_bytes(), 150);
        assert_eq!(c.peak_bytes(), 150);
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
        let _cleared = reset_peak_native_bytes();
        let before = current_native_bytes();

        let buf = vec![0u8; N];
        let during = current_native_bytes();
        assert!(
            during >= before + N as u64,
            "alloc not tracked: {before} -> {during}"
        );
        assert!(peak_native_bytes() >= during);

        drop(buf);
        // Real peak now exceeds real current, which also pins each getter to the right
        // counter: swapping them inverts this.
        assert!(peak_native_bytes() > current_native_bytes());
    }
}
