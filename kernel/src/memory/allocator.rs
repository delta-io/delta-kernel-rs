use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{MemoryError, MemoryResult};

/// A global Rust allocator with shared requested-byte tracking and an optional limit.
///
/// Install one instance with `#[global_allocator]`. Standard containers then participate without
/// carrying an allocator. Every instance uses the same counters and limit across threads and FFI.
/// Fallible calls such as `Vec::try_reserve` return errors when an allocation is rejected;
/// infallible allocation calls retain Rust's allocation-error-handler behavior.
#[derive(Clone, Copy, Debug)]
pub struct KernelAllocator<A = System> {
    backing: A,
}

/// Access to the shared global allocator's counters and configuration.
///
/// Rust applications install a [`KernelAllocator`] as their global allocator. The FFI crate does
/// this with its `global-allocator` feature. Enforcement is disabled until a limit is configured.
pub static KERNEL_ALLOCATOR: KernelAllocator = KernelAllocator::new();

/// A coherent observation of the shared allocator's requested-byte counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryUsage {
    /// Configured cap, or `None` when enforcement is disabled.
    pub limit: Option<usize>,
    /// Outstanding requested bytes, including allocations currently in flight.
    pub reserved: usize,
    /// Largest reserved count since initialization or peak reset, including old-plus-new growth.
    pub peak: usize,
    /// Requests rejected before calling the backing allocator.
    pub rejected: usize,
}

static STATE: State = State::new();

impl KernelAllocator {
    /// Create a global allocator backed by the system allocator.
    pub const fn new() -> Self {
        Self { backing: System }
    }
}

impl Default for KernelAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> KernelAllocator<A> {
    /// Wrap a backing allocator without changing the shared limit or counters.
    ///
    /// This supports instrumentation around the system allocator. The backing implementation must
    /// not delegate directly back to this allocator through `std::alloc::alloc`.
    pub const fn with_backing(backing: A) -> Self {
        Self { backing }
    }

    /// Set or clear the shared limit without resetting usage or historical counters.
    ///
    /// The first call initializes and tracks shared canonical schemas before applying the limit.
    /// A limit below current usage is rejected without changing the previous limit. Configuration
    /// excludes allocation reservations, so successful updates also cover allocations
    /// already in flight. `None` disables enforcement while retaining tracking.
    pub fn set_limit(&self, limit: Option<usize>) -> MemoryResult<()> {
        super::initialize_schemas();
        let _configuration = STATE.gate.exclusive();
        if let Some(limit) = limit
            && limit < STATE.reserved.load(Ordering::Relaxed)
        {
            return Err(MemoryError::LimitBelowUsage);
        }
        STATE
            .limit
            .store(limit.unwrap_or(usize::MAX), Ordering::Relaxed);
        STATE.enforced.store(limit.is_some(), Ordering::Relaxed);
        Ok(())
    }

    /// Read the shared counters, including usage allocated before a limit was configured.
    pub fn usage(&self) -> MemoryUsage {
        let _snapshot = STATE.gate.exclusive();
        MemoryUsage {
            limit: STATE
                .enforced
                .load(Ordering::Relaxed)
                .then(|| STATE.limit.load(Ordering::Relaxed)),
            reserved: STATE.reserved.load(Ordering::Relaxed),
            peak: STATE.peak.load(Ordering::Relaxed),
            rejected: STATE.rejected.load(Ordering::Relaxed),
        }
    }

    /// Reset the historical peak to current usage, returning the previous peak.
    ///
    /// The reset excludes new reservations; concurrent frees can only decrease live usage.
    pub fn reset_peak(&self) -> usize {
        let _reset = STATE.gate.exclusive();
        STATE
            .peak
            .swap(STATE.reserved.load(Ordering::Relaxed), Ordering::Relaxed)
    }

    pub(crate) fn allocation_error(&self) -> MemoryError {
        MemoryError::AllocationFailed {
            limit: self.usage().limit.unwrap_or(usize::MAX),
        }
    }

    fn reserve(&self, bytes: usize) -> Option<Reservation> {
        let _reservation = STATE.gate.shared();
        let limit = STATE.limit.load(Ordering::Relaxed);
        match STATE
            .reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                reserved.checked_add(bytes).filter(|next| *next <= limit)
            }) {
            Ok(previous) => {
                STATE.peak.fetch_max(previous + bytes, Ordering::Relaxed);
                Some(Reservation { bytes })
            }
            Err(_) => {
                let _ =
                    STATE
                        .rejected
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rejected| {
                            Some(rejected.saturating_add(1))
                        });
                None
            }
        }
    }

    fn release(&self, bytes: usize) {
        STATE.reserved.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(super) fn allocate_with(
        &self,
        layout: Layout,
        backing: impl FnOnce(Layout) -> *mut u8,
    ) -> *mut u8 {
        let Some(reservation) = self.reserve(layout.size()) else {
            return std::ptr::null_mut();
        };
        let allocation = backing(layout);
        if !allocation.is_null() {
            std::mem::forget(reservation);
        }
        allocation
    }
}

// SAFETY: Storage and its original layouts are forwarded to the retained backing allocator.
// Reservations finish before backing calls and are rolled back on failure. Reallocation reserves
// a complete replacement before invoking the backing allocator, including possible in-place growth.
unsafe impl<A: GlobalAlloc> GlobalAlloc for KernelAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocate_with(layout, |layout| unsafe { self.backing.alloc(layout) })
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocate_with(layout, |layout| unsafe {
            self.backing.alloc_zeroed(layout)
        })
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let Some(reservation) = self.reserve(new_size) else {
            return std::ptr::null_mut();
        };
        // SAFETY: The caller provides a live block, its original layout, and a valid new size.
        let resized = unsafe { self.backing.realloc(ptr, layout, new_size) };
        if !resized.is_null() {
            self.release(layout.size());
            std::mem::forget(reservation);
        }
        resized
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The caller supplies a live allocation and the layout used to allocate it.
        unsafe { self.backing.dealloc(ptr, layout) };
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

struct State {
    gate: Gate,
    reserved: AtomicUsize,
    peak: AtomicUsize,
    rejected: AtomicUsize,
    limit: AtomicUsize,
    enforced: AtomicBool,
}

impl State {
    const fn new() -> Self {
        Self {
            gate: Gate(AtomicUsize::new(0)),
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            limit: AtomicUsize::new(usize::MAX),
            enforced: AtomicBool::new(false),
        }
    }
}

// Reservations run concurrently. Configuration and coherent snapshots exclude reservations, so a
// lowered limit cannot race a successful CAS. Frees only reduce usage and need no gate. No backing
// allocator call, schema initialization, or user callback holds a gate. System mutexes can allocate
// during initialization, so the gate uses only atomics and never parks a thread.
struct Gate(AtomicUsize);
const EXCLUSIVE: usize = 1usize << (usize::BITS - 1);

impl Gate {
    fn shared(&self) -> Shared<'_> {
        loop {
            let state = self.0.load(Ordering::Relaxed);
            if state < EXCLUSIVE - 1
                && self
                    .0
                    .compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                return Shared(self);
            }
            std::hint::spin_loop();
        }
    }

    fn exclusive(&self) -> Exclusive<'_> {
        loop {
            let state = self.0.load(Ordering::Relaxed);
            if state < EXCLUSIVE
                && self
                    .0
                    .compare_exchange_weak(
                        state,
                        state | EXCLUSIVE,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                // Setting the high bit prevents new reservations while existing ones finish.
                while self.0.load(Ordering::Acquire) != EXCLUSIVE {
                    std::hint::spin_loop();
                }
                return Exclusive(self);
            }
            std::hint::spin_loop();
        }
    }
}

struct Shared<'a>(&'a Gate);
impl Drop for Shared<'_> {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::Release);
    }
}

struct Exclusive<'a>(&'a Gate);
impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        self.0.0.store(0, Ordering::Release);
    }
}
