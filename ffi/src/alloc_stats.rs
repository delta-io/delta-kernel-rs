//! FFI access to process-wide Rust allocation counters.
//!
//! # Scope
//!
//! Counters record logical requested Rust allocation sizes, not process RSS. C/`mmap` allocations,
//! allocator overhead, and temporary storage inside `System.realloc` are not included. Allocations
//! are counted after success; deallocation stops counting a block before releasing it. Values are
//! process-global and advisory, not per-operation. Counters use relaxed atomics and do not form a
//! coherent snapshot. Combined getters normalize returned bounds to include sampled current usage;
//! separate scalar getters do not guarantee ordered bounds. See [`reset_native_memory_stats`] for
//! the concurrent-reset contract.

#[cfg(all(feature = "alloc-tracking", not(target_has_atomic = "64")))]
compile_error!("alloc-tracking requires native 64-bit atomics");

#[cfg(feature = "alloc-tracking")]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "alloc-tracking")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "alloc-tracking")]
#[global_allocator]
static GLOBAL_ALLOC: TrackingAlloc = TrackingAlloc::new();

/// Whether this library was built with allocation tracking.
///
/// Native-memory getters return zero when this is false.
#[no_mangle]
pub extern "C" fn alloc_tracking_enabled() -> bool {
    cfg!(feature = "alloc-tracking")
}

/// Independently sampled allocation statistics.
///
/// [`get_native_memory_stats`] and [`reset_native_memory_stats`] normalize returned extrema to
/// include sampled current usage. The fields need not describe a single point in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct NativeMemoryStats {
    /// Reported minimum native bytes, no greater than sampled current usage.
    pub min_bytes: u64,
    /// Sampled live native bytes.
    pub current_bytes: u64,
    /// Reported maximum native bytes, no less than sampled current usage.
    pub max_bytes: u64,
}

/// Reported minimum simultaneously live native bytes since the library was loaded or reset.
///
/// The value is process-wide, not per-operation, and counts requested Rust allocation sizes only.
/// The value remains zero until [`reset_native_memory_stats`] starts a new measurement window.
/// Its relationship to the current total is advisory; see the concurrent-reset contract.
/// Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn min_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.min_usage()
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Reported maximum simultaneously live native bytes since the library was loaded or reset.
///
/// The value is process-wide, not per-operation, and counts requested Rust allocation sizes only.
/// Its relationship to the current total is advisory; see [`reset_native_memory_stats`].
/// Returns zero when built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn max_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.max_usage()
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Advisory current requested Rust allocation bytes.
///
/// The value is process-wide and counts requested Rust allocation sizes only. Returns zero when
/// built without `alloc-tracking`.
#[no_mangle]
pub extern "C" fn current_native_bytes() -> u64 {
    #[cfg(feature = "alloc-tracking")]
    {
        GLOBAL_ALLOC.current_usage()
    }
    #[cfg(not(feature = "alloc-tracking"))]
    {
        0
    }
}

/// Samples the current live total, independently resets both extrema to that sample, and writes
/// the previous extrema and current sample to `stats`.
///
/// Returned bounds are normalized to satisfy `min_bytes <= current_bytes <= max_bytes`.
/// Concurrent allocation or reset can lose extrema or mix measurement windows. The returned fields
/// are not a coherent snapshot, and stored bounds can remain on the wrong side of current. Callers
/// requiring reliable windows must quiesce tracked allocation and serialize resets.
///
/// All fields are zero when built without `alloc-tracking`.
///
/// # Safety
///
/// `stats` must be non-null, aligned, and writable for a `NativeMemoryStats`. The storage may be
/// uninitialized and must not be accessed by other code for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn reset_native_memory_stats(stats: *mut NativeMemoryStats) {
    #[cfg(feature = "alloc-tracking")]
    let value = GLOBAL_ALLOC.reset_stats();
    #[cfg(not(feature = "alloc-tracking"))]
    let value = NativeMemoryStats::default();
    // SAFETY: the caller guarantees aligned, exclusively writable storage for the output.
    unsafe { stats.write(value) };
}

/// Samples native-memory counters without resetting the measurement window and writes them to
/// `stats`.
///
/// Returned bounds satisfy `min_bytes <= current_bytes <= max_bytes`. Only the returned extrema
/// are normalized; stored counters are unchanged. Concurrent allocation or reset can still lose
/// extrema or mix windows, so the fields do not form a coherent snapshot.
///
/// All fields are zero when built without `alloc-tracking`.
///
/// # Safety
///
/// `stats` must be non-null, aligned, and writable for a `NativeMemoryStats`. The storage may be
/// uninitialized and must not be accessed by other code for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn get_native_memory_stats(stats: *mut NativeMemoryStats) {
    #[cfg(feature = "alloc-tracking")]
    let value = GLOBAL_ALLOC.stats();
    #[cfg(not(feature = "alloc-tracking"))]
    let value = NativeMemoryStats::default();
    // SAFETY: the caller guarantees aligned, exclusively writable storage for the output.
    unsafe { stats.write(value) };
}

#[cfg(feature = "alloc-tracking")]
struct TrackingAlloc {
    current: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

#[cfg(feature = "alloc-tracking")]
impl TrackingAlloc {
    const fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            min: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    fn min_usage(&self) -> u64 {
        self.min.load(Ordering::Relaxed)
    }

    fn max_usage(&self) -> u64 {
        self.max.load(Ordering::Relaxed)
    }

    fn current_usage(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    fn stats(&self) -> NativeMemoryStats {
        let current_bytes = self.current_usage();
        NativeMemoryStats {
            min_bytes: self.min_usage().min(current_bytes),
            current_bytes,
            max_bytes: self.max_usage().max(current_bytes),
        }
    }

    fn reset_stats(&self) -> NativeMemoryStats {
        let current_bytes = self.current_usage();
        NativeMemoryStats {
            min_bytes: self
                .min
                .swap(current_bytes, Ordering::Relaxed)
                .min(current_bytes),
            current_bytes,
            max_bytes: self
                .max
                .swap(current_bytes, Ordering::Relaxed)
                .max(current_bytes),
        }
    }

    fn grow_usage(&self, size: u64) {
        let previous = self.current.fetch_add(size, Ordering::Relaxed);
        self.max
            .fetch_max(previous.saturating_add(size), Ordering::Relaxed);
    }

    fn shrink_usage(&self, size: u64) {
        let previous = self.current.fetch_sub(size, Ordering::Relaxed);
        self.min
            .fetch_min(previous.saturating_sub(size), Ordering::Relaxed);
    }

    fn record_realloc(&self, succeeded: bool, old_size: u64, new_size: u64) {
        if !succeeded {
            return;
        }
        if new_size > old_size {
            self.grow_usage(new_size - old_size);
        } else if old_size > new_size {
            self.shrink_usage(old_size - new_size);
        }
    }

    #[inline]
    fn dealloc_with(&self, size: u64, release: impl FnOnce()) {
        // Remove logical ownership before another allocation can reuse and count the memory.
        self.shrink_usage(size);
        release();
    }
}

#[cfg(feature = "alloc-tracking")]
// SAFETY: pointers, layouts, and realloc's new_size are forwarded to `System` unchanged.
// Bookkeeping must not allocate or unwind: it only uses atomics and guarded or saturating
// arithmetic, without accessing the allocated memory.
unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            self.grow_usage(layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.dealloc_with(layout.size() as u64, || unsafe {
            System.dealloc(ptr, layout)
        });
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            self.grow_usage(layout.size() as u64);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        self.record_realloc(!new_ptr.is_null(), layout.size() as u64, new_size as u64);
        new_ptr
    }
}

#[cfg(all(test, not(feature = "alloc-tracking")))]
mod disabled_tests {
    use std::mem::MaybeUninit;

    use rstest::rstest;

    use super::{
        alloc_tracking_enabled, current_native_bytes, get_native_memory_stats, max_native_bytes,
        min_native_bytes, reset_native_memory_stats, NativeMemoryStats,
    };

    #[rstest]
    fn getters_report_disabled_tracking(
        #[values(false, true)] initialized: bool,
        #[values(false, true)] reset: bool,
    ) {
        assert!(!alloc_tracking_enabled());
        assert_eq!(min_native_bytes(), 0);
        assert_eq!(max_native_bytes(), 0);
        assert_eq!(current_native_bytes(), 0);
        let mut stats = if initialized {
            MaybeUninit::new(NativeMemoryStats {
                min_bytes: 1,
                current_bytes: 2,
                max_bytes: 3,
            })
        } else {
            MaybeUninit::uninit()
        };
        // SAFETY: stats is aligned, writable storage, and either getter initializes all its fields.
        let stats = unsafe {
            if reset {
                reset_native_memory_stats(stats.as_mut_ptr());
            } else {
                get_native_memory_stats(stats.as_mut_ptr());
            }
            stats.assume_init()
        };
        assert_eq!(stats, NativeMemoryStats::default());
    }
}

#[cfg(all(test, feature = "alloc-tracking"))]
mod global_allocator_tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::mem::MaybeUninit;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use rstest::rstest;

    use super::{
        alloc_tracking_enabled, current_native_bytes, get_native_memory_stats, max_native_bytes,
        min_native_bytes, reset_native_memory_stats, NativeMemoryStats, TrackingAlloc,
    };

    // Far above incidental harness allocation, so the bounds below cannot be met by noise.
    const N: usize = 8 * 1024 * 1024;

    #[test]
    fn installed_global_allocator_accounts_a_large_allocation() {
        // This must be the only test in this binary that resets GLOBAL_ALLOC: the initial minimum
        // assertion relies on no other test rebasing it when cargo test runs tests concurrently.
        assert!(alloc_tracking_enabled());
        assert_eq!(min_native_bytes(), 0);

        let buf = vec![0u8; N];
        assert!(current_native_bytes() >= N as u64);
        assert!(max_native_bytes() >= N as u64);

        for reset in [false, true] {
            for mut stats in [
                MaybeUninit::uninit(),
                MaybeUninit::new(NativeMemoryStats {
                    min_bytes: u64::MAX,
                    current_bytes: u64::MAX,
                    max_bytes: u64::MAX,
                }),
            ] {
                // SAFETY: stats is aligned and writable; either getter initializes all its fields.
                let stats = unsafe {
                    if reset {
                        reset_native_memory_stats(stats.as_mut_ptr());
                    } else {
                        get_native_memory_stats(stats.as_mut_ptr());
                    }
                    stats.assume_init()
                };
                assert!(stats.current_bytes >= N as u64);
                assert!(stats.current_bytes < u64::MAX);
                assert!(stats.max_bytes >= N as u64);
                assert!(stats.max_bytes < u64::MAX);
                assert!(stats.min_bytes < u64::MAX);
                assert!(stats.min_bytes <= stats.current_bytes);
                assert!(stats.current_bytes <= stats.max_bytes);
            }
        }

        drop(buf);
    }

    #[test]
    fn allocator_tracks_current_minimum_and_maximum_usage() {
        let allocator = TrackingAlloc::new();
        let initial_layout = Layout::from_size_align(64, 8).unwrap();
        let grown_layout = Layout::from_size_align(128, 8).unwrap();
        let shrunk_layout = Layout::from_size_align(32, 8).unwrap();
        let zeroed_layout = Layout::from_size_align(16, 8).unwrap();

        unsafe {
            let ptr = allocator.alloc(initial_layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.current_usage(), 64);
            assert_eq!(allocator.min_usage(), 0);
            assert_eq!(allocator.max_usage(), 64);
            assert_eq!(
                allocator.reset_stats(),
                NativeMemoryStats {
                    min_bytes: 0,
                    current_bytes: 64,
                    max_bytes: 64,
                }
            );

            let ptr = allocator.realloc(ptr, initial_layout, grown_layout.size());
            assert!(!ptr.is_null());
            assert_eq!(allocator.current_usage(), 128);
            assert_eq!(allocator.min_usage(), 64);
            assert_eq!(allocator.max_usage(), 128);

            let ptr = allocator.realloc(ptr, grown_layout, shrunk_layout.size());
            assert!(!ptr.is_null());
            assert_eq!(allocator.current_usage(), 32);
            assert_eq!(allocator.min_usage(), 32);
            assert_eq!(allocator.max_usage(), 128);

            allocator.dealloc(ptr, shrunk_layout);
            assert_eq!(allocator.current_usage(), 0);
            assert_eq!(allocator.min_usage(), 0);
            assert_eq!(allocator.max_usage(), 128);

            let ptr = allocator.alloc_zeroed(zeroed_layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.current_usage(), 16);
            assert_eq!(allocator.max_usage(), 128);
            for offset in 0..zeroed_layout.size() {
                assert_eq!(*ptr.add(offset), 0);
            }
            allocator.dealloc(ptr, zeroed_layout);
        }
    }

    #[test]
    fn reset_returns_distinct_extrema_and_rebases_both_to_current() {
        let allocator = TrackingAlloc::new();
        allocator.grow_usage(64);
        allocator.reset_stats();
        allocator.shrink_usage(32);
        allocator.grow_usage(96);
        allocator.shrink_usage(64);

        assert_eq!(
            allocator.reset_stats(),
            NativeMemoryStats {
                min_bytes: 32,
                current_bytes: 64,
                max_bytes: 128,
            }
        );
        assert_eq!(allocator.min_usage(), 64);
        assert_eq!(allocator.current_usage(), 64);
        assert_eq!(allocator.max_usage(), 64);
        assert_eq!(
            allocator.reset_stats(),
            NativeMemoryStats {
                min_bytes: 64,
                current_bytes: 64,
                max_bytes: 64,
            }
        );
    }

    #[test]
    fn failed_realloc_does_not_change_usage() {
        let allocator = TrackingAlloc::new();
        allocator.grow_usage(64);
        let before = allocator.reset_stats();

        allocator.record_realloc(false, 64, 128);

        assert_eq!(allocator.current_usage(), before.current_bytes);
        assert_eq!(allocator.min_usage(), before.current_bytes);
        assert_eq!(allocator.max_usage(), before.current_bytes);
    }

    #[test]
    fn deallocation_replacement_after_free_does_not_inflate_maximum() {
        let allocator = TrackingAlloc::new();
        let layout = Layout::from_size_align(64, 8).unwrap();
        // SAFETY: checked System-backed allocations are each freed once with the original layout.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            allocator.reset_stats();
            allocator.dealloc_with(layout.size() as u64, || {
                assert_eq!(allocator.current_usage(), 0);
                assert_eq!(allocator.min_usage(), 0);
                System.dealloc(ptr, layout);
                let replacement = allocator.alloc(layout);
                assert!(!replacement.is_null());
                assert_eq!(allocator.current_usage(), 64);
                assert_eq!(allocator.max_usage(), 64);
                allocator.dealloc(replacement, layout);
            });
        }
        assert_eq!(allocator.current_usage(), 0);
        assert_eq!(allocator.max_usage(), 64);
    }

    #[rstest]
    #[case::ordered_bounds(16, 32, 64, 16, 64)]
    #[case::minimum_above_current(64, 32, 64, 32, 64)]
    #[case::maximum_below_current(16, 32, 16, 16, 32)]
    #[case::inverted_bounds(64, 32, 16, 32, 32)]
    #[case::zero_current(u64::MAX, 0, 0, 0, 0)]
    #[case::maximum_current(0, u64::MAX, 0, 0, u64::MAX)]
    fn sampled_bounds_include_current(
        #[case] min: u64,
        #[case] current: u64,
        #[case] max: u64,
        #[case] expected_min: u64,
        #[case] expected_max: u64,
        #[values(false, true)] reset: bool,
    ) {
        let allocator = TrackingAlloc::new();
        allocator.min.store(min, Ordering::Relaxed);
        allocator.current.store(current, Ordering::Relaxed);
        allocator.max.store(max, Ordering::Relaxed);

        let stats = if reset {
            allocator.reset_stats()
        } else {
            allocator.stats()
        };
        assert_eq!(
            stats,
            NativeMemoryStats {
                min_bytes: expected_min,
                current_bytes: current,
                max_bytes: expected_max,
            }
        );
        assert_eq!(allocator.current_usage(), current);
        assert_eq!(allocator.min_usage(), if reset { current } else { min });
        assert_eq!(allocator.max_usage(), if reset { current } else { max });
    }

    #[rstest]
    fn sampled_bounds_remain_ordered_during_concurrent_accounting_and_resets(
        #[values(false, true)] reset: bool,
    ) {
        let allocator = TrackingAlloc::new();
        let barrier = Barrier::new(5);
        thread::scope(|scope| {
            for _ in 0..3 {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..128 {
                        allocator.grow_usage(64);
                        thread::yield_now();
                        allocator.shrink_usage(64);
                    }
                });
            }
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..128 {
                    let stats = allocator.reset_stats();
                    assert!(stats.min_bytes <= stats.current_bytes);
                    assert!(stats.current_bytes <= stats.max_bytes);
                    thread::yield_now();
                }
            });

            barrier.wait();
            for _ in 0..128 {
                let stats = if reset {
                    allocator.reset_stats()
                } else {
                    allocator.stats()
                };
                assert!(stats.min_bytes <= stats.current_bytes);
                assert!(stats.current_bytes <= stats.max_bytes);
                thread::yield_now();
            }
        });
        assert_eq!(allocator.current_usage(), 0);
    }

    #[test]
    fn concurrent_frees_lower_minimum_below_reset_baseline_and_balance_current_usage() {
        let allocator = Arc::new(TrackingAlloc::new());
        let layout = Layout::from_size_align(64, 8).unwrap();
        let barrier = Arc::new(Barrier::new(5));
        let threads = (0..4)
            .map(|_| {
                let allocator = Arc::clone(&allocator);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let ptr = unsafe { allocator.alloc(layout) };
                    assert!(!ptr.is_null());
                    barrier.wait();
                    barrier.wait();
                    unsafe { allocator.dealloc(ptr, layout) };
                    barrier.wait();
                    for _ in 0..32 {
                        unsafe {
                            let ptr = allocator.alloc(layout);
                            assert!(!ptr.is_null());
                            allocator.dealloc(ptr, layout);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        // The barriers isolate reset and make the minimum reach zero before the loops.
        barrier.wait();
        assert_eq!(
            allocator.reset_stats(),
            NativeMemoryStats {
                min_bytes: 0,
                current_bytes: 256,
                max_bytes: 256,
            }
        );
        assert_eq!(allocator.min_usage(), 256);
        barrier.wait();
        barrier.wait();

        for thread in threads {
            thread.join().unwrap();
        }

        assert_eq!(allocator.current_usage(), 0);
        assert_eq!(allocator.min_usage(), 0);
        assert_eq!(allocator.max_usage(), 256);
    }
}
