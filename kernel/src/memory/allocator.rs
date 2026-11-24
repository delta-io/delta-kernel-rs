use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{MemoryError, MemoryResult};

/// An explicit allocator with one shared requested-byte budget across threads and FFI calls.
///
/// Available with `memory-budget` on stable Rust 1.100 or newer. Participating containers select
/// this allocator and use fallible growth; ordinary process allocations are never rejected by it.
/// Infallible constructors on allocator-backed containers retain allocation-error-handler behavior.
#[derive(Clone, Copy, Debug, Default)]
pub struct KernelAllocator {
    _private: (),
}

/// Shared allocator instance for participating Kernel metadata storage.
///
/// Enforcement is disabled until [`KernelAllocator::set_limit`] configures a limit.
pub static KERNEL_ALLOCATOR: KernelAllocator = KernelAllocator::new();

/// A coherent observation of the shared allocator's requested-byte counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryUsage {
    /// Configured cap, or `None` when enforcement is disabled.
    pub limit: Option<usize>,
    /// Outstanding requested bytes, including allocations currently in flight.
    pub reserved: usize,
    /// Largest requested-byte reservation since initialization or peak reset.
    pub peak: usize,
    /// Requests rejected before calling the backing allocator.
    pub rejected: usize,
}

static STATE: State = State::new();

impl KernelAllocator {
    /// Create a handle to the shared allocator without changing its counters or limit.
    pub const fn new() -> Self {
        Self { _private: () }
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
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
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
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |rejected| {
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
        backing: impl FnOnce(Layout) -> Result<NonNull<[u8]>, AllocError>,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let reservation = self.reserve(layout.size()).ok_or(AllocError)?;
        let allocation = backing(layout)?;
        std::mem::forget(reservation);
        Ok(NonNull::slice_from_raw_parts(
            allocation.cast(),
            layout.size(),
        ))
    }

    fn resize_with(
        &self,
        old_layout: Layout,
        new_layout: Layout,
        backing: impl FnOnce() -> Result<NonNull<[u8]>, AllocError>,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let growth = new_layout.size().saturating_sub(old_layout.size());
        let reservation = self.reserve(growth).ok_or(AllocError)?;
        let allocation = backing()?;
        self.release(old_layout.size().saturating_sub(new_layout.size()));
        std::mem::forget(reservation);
        Ok(NonNull::slice_from_raw_parts(
            allocation.cast(),
            new_layout.size(),
        ))
    }
}

// SAFETY: Global supplies aligned storage. Exposing exactly the charged length ensures fitting
// layouts retain the same charge. Failed backing calls roll back reservations and preserve old
// storage. Growth reserves only the size delta; allocator-internal copies are outside this budget.
unsafe impl Allocator for KernelAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, |layout| Global.allocate(layout))
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocate_with(layout, |layout| Global.allocate_zeroed(layout))
    }

    unsafe fn grow(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.resize_with(old_layout, new_layout, || unsafe {
            Global.grow(pointer, old_layout, new_layout)
        })
    }

    unsafe fn grow_zeroed(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.resize_with(old_layout, new_layout, || unsafe {
            Global.grow_zeroed(pointer, old_layout, new_layout)
        })
    }

    unsafe fn shrink(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.resize_with(old_layout, new_layout, || unsafe {
            Global.shrink(pointer, old_layout, new_layout)
        })
    }

    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        // SAFETY: The caller provides a live block and a fitting layout from this allocator.
        unsafe { Global.deallocate(pointer, layout) };
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
