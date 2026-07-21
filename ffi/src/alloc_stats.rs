//! Process-wide off-heap (native) memory accounting for the kernel FFI library.
//!
//! When the `alloc-tracking` feature is enabled, [`crate`] installs [`TrackingAlloc`] as the
//! `#[global_allocator]` for the whole cdylib. Every Rust allocation performed inside the kernel
//! (Arrow scan buffers, log-replay state, object-store client buffers, the Tokio runtime, ...)
//! then flows through it, so the counters below reflect the kernel's entire *Rust-allocated*
//! native heap -- not just scan data.
//!
//! What they do NOT capture: allocations made directly by linked C libraries (e.g. compression
//! codecs, TLS) that call `malloc`/`mmap` without going through Rust's global allocator, and
//! allocator fragmentation/overhead (the counters record the requested `Layout::size`, not the
//! footprint the allocator actually reserves). A JVM caller comparing this against process RSS
//! should therefore expect it to be a lower bound, not an exact match.
//!
//! == Scope ==
//!
//! The counters are process-global: the FFI `.so` is loaded once per JVM, so a single set of
//! counters is shared across every concurrent operation. [`peak_bytes`] is therefore an
//! executor-wide high-water mark, not a per-operation figure. Under concurrency it cannot be
//! attributed to one task, and [`reset_peak`] races other in-flight work. Treat it as a
//! best-effort memory-pressure gauge.
//!
//! All updates use `Relaxed` ordering: the counters are advisory statistics with no happens-before
//! relationship to the data they describe, so the cost is a single atomic add/sub per
//! allocation/deallocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Live-bytes and peak-bytes counters. Split out from [`TrackingAlloc`] so the arithmetic can be
/// unit-tested against a local instance without touching the process-global allocator state.
struct Counters {
    /// Bytes currently allocated (live).
    current: AtomicU64,
    /// High-water mark of `current` since creation (or the last [`Counters::reset_peak`]).
    peak: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    /// Add `n` bytes to `current` and raise `peak` to the new live total if it is now higher.
    #[inline]
    fn record_alloc(&self, n: usize) {
        let new_current = self.current.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
        // Wait-free monotonic max; leaves `peak` unchanged when `new_current` is not a new high.
        self.peak.fetch_max(new_current, Ordering::Relaxed);
    }

    /// Subtract `n` bytes from `current`. `peak` is intentionally left untouched.
    #[inline]
    fn record_dealloc(&self, n: usize) {
        self.current.fetch_sub(n as u64, Ordering::Relaxed);
    }

    fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    fn current_bytes(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Reset the peak high-water mark down to the current live-bytes value.
    fn reset_peak(&self) {
        self.peak
            .store(self.current.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

static COUNTERS: Counters = Counters::new();

/// A [`GlobalAlloc`] that forwards every request to the [`System`] allocator while maintaining the
/// process-global [`COUNTERS`].
pub struct TrackingAlloc;

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
            COUNTERS.record_dealloc(layout.size());
            COUNTERS.record_alloc(new_size);
        }
        new_ptr
    }
}

/// Peak native bytes ever simultaneously live, since process start or the last [`reset_peak`].
pub fn peak_bytes() -> u64 {
    COUNTERS.peak_bytes()
}

/// Native bytes currently live (allocated but not yet freed).
pub fn current_bytes() -> u64 {
    COUNTERS.current_bytes()
}

/// Reset the peak high-water mark down to the current live-bytes value.
///
/// Lets a caller establish a per-operation baseline. Because the counters are process-global, a
/// reset concurrent with other in-flight work will discard those allocations' contribution to the
/// peak; callers must serialize resets against the operations they intend to measure.
pub fn reset_peak() {
    COUNTERS.reset_peak()
}

#[cfg(test)]
mod tests {
    use super::Counters;

    #[test]
    fn alloc_raises_current_and_peak() {
        let c = Counters::new();
        c.record_alloc(4096);
        assert_eq!(c.current_bytes(), 4096);
        assert_eq!(c.peak_bytes(), 4096);
    }

    #[test]
    fn dealloc_lowers_current_but_not_peak() {
        let c = Counters::new();
        c.record_alloc(8192);
        c.record_dealloc(8192);
        assert_eq!(c.current_bytes(), 0);
        // Peak is a high-water mark; freeing must not lower it.
        assert_eq!(c.peak_bytes(), 8192);
    }

    #[test]
    fn peak_tracks_max_not_last() {
        let c = Counters::new();
        c.record_alloc(1000);
        c.record_alloc(500); // current = 1500, peak = 1500
        c.record_dealloc(500); // current = 1000, peak stays 1500
        c.record_alloc(200); // current = 1200, peak stays 1500
        assert_eq!(c.current_bytes(), 1200);
        assert_eq!(c.peak_bytes(), 1500);
    }

    #[test]
    fn reset_peak_drops_to_current() {
        let c = Counters::new();
        c.record_alloc(1024);
        c.record_dealloc(512); // current = 512, peak = 1024
        c.reset_peak();
        assert_eq!(c.peak_bytes(), 512);
        assert_eq!(c.current_bytes(), 512);
    }
}
