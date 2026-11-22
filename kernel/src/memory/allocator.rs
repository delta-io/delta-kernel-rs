use std::alloc::{AllocError, Allocator, Global, Layout};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// A shared cap on simultaneously reserved requested allocation bytes.
///
/// Construct this before the operation and pass clones to every participating container. The
/// fixed control block uses the global allocator outside the cap. This does not limit RSS,
/// allocator fragmentation, or allocations through other allocators.
#[derive(Clone, Debug)]
pub struct MemoryBudget(Arc<State>);

/// A point-in-time observation of a budget's counters.
///
/// Counters are individually atomic; concurrent operations may run between their observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryUsage {
    /// Maximum permitted simultaneous requested bytes.
    pub limit: usize,
    /// Outstanding requested bytes, including in-flight allocation reservations.
    pub reserved: usize,
    /// Largest observed reserved count, including transient old-plus-new growth capacity.
    pub peak: usize,
    /// Allocation requests rejected before calling the backing allocator.
    pub rejected: usize,
}

#[derive(Debug)]
struct State {
    limit: usize,
    reserved: AtomicUsize,
    peak: AtomicUsize,
    rejected: AtomicUsize,
}

// The first policy lookup seals configuration, preventing older owners from escaping a later cap.
static GLOBAL_POLICY: OnceLock<Option<MemoryBudget>> = OnceLock::new();

thread_local! {
    static CURRENT: RefCell<Option<MemoryBudget>> = const { RefCell::new(None) };
}

impl MemoryBudget {
    /// Construct participating storage with this budget on the calling thread.
    ///
    /// Scopes nest and restore their predecessor on return or unwind. Storage that escapes the
    /// closure retains its charge. Standalone worker calls must enter the budget explicitly;
    /// lifecycle objects configured with `with_allocator` retain and activate their own owner.
    pub fn with<T>(&self, operation: impl FnOnce() -> T) -> T {
        let _scope = AllocationScope::new(Some(&self.allocator()));
        operation()
    }

    pub(crate) fn current() -> Option<Self> {
        CURRENT
            .with(|current| current.borrow().clone())
            .or_else(|| GLOBAL_POLICY.get_or_init(|| None).as_ref().cloned())
    }

    /// Install the shared process-local limit before any Kernel work that consults memory policy.
    ///
    /// The policy cannot be replaced while allocations may retain it. All limited allocators
    /// also charge this budget, including allocators with a smaller explicit local cap.
    /// A policy lookup seals configuration even when it finds no installed budget.
    pub fn install_global(limit: usize) -> Result<Self, super::MemoryError> {
        let budget = Self::new(limit);
        GLOBAL_POLICY
            .set(Some(budget.clone()))
            .map_err(|_| super::MemoryError::AlreadyConfigured)?;
        Ok(budget)
    }

    /// Return the configured shared owner, if installed before the first memory-policy lookup.
    pub fn global() -> Option<Self> {
        GLOBAL_POLICY.get().and_then(Option::as_ref).cloned()
    }

    /// Observe the configured shared budget without cloning or creating an allocator.
    pub fn global_usage() -> Option<MemoryUsage> {
        GLOBAL_POLICY
            .get()
            .and_then(Option::as_ref)
            .map(Self::usage)
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Create a budget with an immutable requested-byte cap.
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(State {
            limit,
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
        }))
    }

    /// Obtain an allocator sharing this budget and retaining its control block.
    pub fn allocator(&self) -> LimitedAllocator {
        self.allocator_over(Global)
    }

    /// Wrap a backing allocator, charging requested bytes before delegating to it.
    ///
    /// Backing-allocation failure releases the reservation before returning `AllocError`.
    pub fn allocator_over<A: Allocator>(&self, backing: A) -> LimitedAllocator<A> {
        let global = GLOBAL_POLICY
            .get_or_init(|| None)
            .as_ref()
            .filter(|global| !global.ptr_eq(self))
            .cloned();
        LimitedAllocator {
            budget: self.clone(),
            global,
            backing,
        }
    }

    /// Observe requested-byte usage, peak, and rejection counters.
    pub fn usage(&self) -> MemoryUsage {
        MemoryUsage {
            limit: self.0.limit,
            reserved: self.0.reserved.load(Ordering::Relaxed),
            peak: self.0.peak.load(Ordering::Relaxed),
            rejected: self.0.rejected.load(Ordering::Relaxed),
        }
    }

    fn reserve(&self, bytes: usize) -> Result<Reservation<'_>, AllocError> {
        let mut used = self.0.reserved.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(bytes).filter(|next| *next <= self.0.limit) else {
                self.0.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(AllocError);
            };
            match self.0.reserved.compare_exchange_weak(
                used,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.0.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(Reservation {
                        budget: self,
                        bytes,
                    });
                }
                Err(current) => used = current,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.0.reserved.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// An explicit [`Allocator`] that returns [`AllocError`] instead of exceeding its budget.
///
/// Use fallible container growth: an infallible `Vec::push` on a full allocator-aware vector can
/// still invoke Rust's allocation-error handler. The safe containers in this crate hide those
/// growth methods. No process-global allocator or thread-local ownership interception is used.
#[derive(Clone, Debug)]
pub struct LimitedAllocator<A = Global> {
    budget: MemoryBudget,
    global: Option<MemoryBudget>,
    backing: A,
}

impl<A> LimitedAllocator<A> {
    /// Return the budget retained by allocations through this allocator.
    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    /// Smallest configured cap applying to this allocator's requests.
    pub fn limit(&self) -> usize {
        self.global.as_ref().map_or(self.budget.0.limit, |global| {
            self.budget.0.limit.min(global.0.limit)
        })
    }
}

// SAFETY: The backing Allocator supplies correctly aligned storage. Returning the requested length
// makes every fitting deallocation layout have the charged size. Clones share the same live
// accounting state. Default grow/shrink allocate before copying/freeing, charging both blocks.
unsafe impl<A: Allocator> Allocator for LimitedAllocator<A> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let global = self
            .global
            .as_ref()
            .map(|budget| budget.reserve(layout.size()))
            .transpose()?;
        let reservation = self.budget.reserve(layout.size())?;
        let allocation = self.backing.allocate(layout)?;
        let allocation = NonNull::slice_from_raw_parts(allocation.cast(), layout.size());
        std::mem::forget(reservation);
        if let Some(global) = global {
            std::mem::forget(global);
        }
        Ok(allocation)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: Allocator's caller supplies a live allocation and a fitting layout.
        unsafe { self.backing.deallocate(ptr, layout) };
        self.budget.release(layout.size());
        if let Some(global) = &self.global {
            global.release(layout.size());
        }
    }
}

struct Reservation<'a> {
    budget: &'a MemoryBudget,
    bytes: usize,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

// Serde and handler traits cannot accept an allocator argument. Public lifecycle objects retain
// the allocator explicitly and activate this construction adapter for each call, including next().
pub(crate) struct AllocationScope {
    previous: Option<MemoryBudget>,
    thread_bound: PhantomData<*mut ()>,
}

impl AllocationScope {
    pub(crate) fn new(allocator: Option<&LimitedAllocator>) -> Self {
        Self {
            previous: CURRENT.with(|current| {
                current.replace(allocator.map(|allocator| allocator.budget().clone()))
            }),
            thread_bound: PhantomData,
        }
    }
}

impl Drop for AllocationScope {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = self.previous.take());
    }
}

pub(crate) struct AllocatedIterator<I> {
    inner: I,
    allocator: Option<LimitedAllocator>,
}

impl<I> AllocatedIterator<I> {
    pub(crate) fn new(inner: I, allocator: Option<LimitedAllocator>) -> Self {
        Self { inner, allocator }
    }
}

impl<I: Iterator> Iterator for AllocatedIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let _scope = AllocationScope::new(self.allocator.as_ref());
        self.inner.next()
    }
}
