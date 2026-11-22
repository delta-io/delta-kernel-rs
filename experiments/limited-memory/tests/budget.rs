use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use delta_kernel_limited_memory_poc::{LimitedBytes, LimitedString, LimitedVec, MemoryBudget};

#[derive(Clone, Copy)]
struct FailNext<'a>(&'a AtomicBool);

// SAFETY: Successful allocation/deallocation delegate to Global with the caller's layouts.
unsafe impl Allocator for FailNext<'_> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if self.0.swap(false, Ordering::Relaxed) {
            Err(AllocError)
        } else {
            Global.allocate(layout)
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: The caller supplies a live Global allocation and its fitting layout.
        unsafe { Global.deallocate(ptr, layout) };
    }
}

#[test]
fn backing_allocator_failure_rolls_back_reservations_and_preserves_old_growth_storage() {
    let fail = AtomicBool::new(true);
    let budget = MemoryBudget::new(100);
    let allocator = budget.allocator_over(FailNext(&fail));
    let old = Layout::from_size_align(8, 8).unwrap();
    let new = Layout::from_size_align(16, 8).unwrap();
    assert!(allocator.allocate(old).is_err());
    assert_eq!(budget.usage().reserved, 0);
    assert_eq!(budget.usage().rejected, 0);
    let block = allocator.allocate(old).unwrap();
    fail.store(true, Ordering::Relaxed);
    // SAFETY: Failed grow preserves the original block, which is subsequently deallocated once.
    unsafe {
        block.cast::<u8>().as_ptr().write(42);
        assert!(allocator.grow(block.cast(), old, new).is_err());
        assert_eq!(block.cast::<u8>().as_ptr().read(), 42);
        assert_eq!(budget.usage().reserved, 8);
        allocator.deallocate(block.cast(), old);
    }
    assert_eq!(budget.usage().reserved, 0);
    assert_eq!(budget.usage().peak, 24);
}

#[test]
fn exact_limit_zero_size_and_alignment_are_respected() {
    let budget = MemoryBudget::new(64);
    let allocator = budget.allocator();
    let layout = Layout::from_size_align(64, 64).unwrap();
    let block = allocator.allocate(layout).unwrap();
    assert_eq!(block.len(), 64);
    assert_eq!(block.cast::<u8>().as_ptr() as usize % 64, 0);
    assert!(allocator.allocate(Layout::new::<u8>()).is_err());
    let zero = Layout::from_size_align(0, 256).unwrap();
    let empty = allocator.allocate(zero).unwrap();
    assert_eq!(empty.cast::<u8>().as_ptr() as usize % 256, 0);
    assert_eq!(budget.usage().reserved, 64);
    // SAFETY: The blocks are live, uniquely owned, and use their original layouts.
    unsafe {
        allocator.deallocate(empty.cast(), zero);
        allocator.deallocate(block.cast(), layout);
    }
    assert_eq!(budget.usage().reserved, 0);
    assert_eq!(budget.usage().peak, 64);
    assert_eq!(budget.usage().rejected, 1);
}

#[test]
fn failed_growth_preserves_bytes_and_counts_old_plus_new_capacity() {
    let budget = MemoryBudget::new(12);
    let allocator = budget.allocator();
    let old = Layout::from_size_align(8, 8).unwrap();
    let new = Layout::from_size_align(12, 8).unwrap();
    let block = allocator.allocate(old).unwrap();
    // SAFETY: The allocation contains eight writable bytes and remains live on growth failure.
    unsafe {
        block.cast::<u8>().as_ptr().write_bytes(42, 8);
        assert!(allocator.grow(block.cast(), old, new).is_err());
        assert_eq!(
            std::slice::from_raw_parts(block.cast::<u8>().as_ptr(), 8),
            &[42; 8]
        );
        allocator.deallocate(block.cast(), old);
    }
    assert_eq!(budget.usage().reserved, 0);
    assert_eq!(budget.usage().peak, 8);

    let budget = MemoryBudget::new(20);
    let allocator = budget.allocator();
    let block = allocator.allocate(old).unwrap();
    // SAFETY: The original allocation is live; successful grow transfers ownership to replacement.
    unsafe {
        block.cast::<u8>().as_ptr().write_bytes(42, 8);
        let grown = allocator.grow_zeroed(block.cast(), old, new).unwrap();
        assert_eq!(budget.usage().reserved, 12);
        assert_eq!(budget.usage().peak, 20);
        assert_eq!(
            std::slice::from_raw_parts(grown.cast::<u8>().as_ptr(), 12),
            &[42, 42, 42, 42, 42, 42, 42, 42, 0, 0, 0, 0]
        );
        allocator.deallocate(grown.cast(), new);
    }
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn vector_capacity_and_nested_strings_remain_charged_until_free() {
    let budget = MemoryBudget::new(4096);
    let mut values = LimitedVec::new(&budget);
    values
        .try_push(LimitedString::try_copy(&budget, "nested-name").unwrap())
        .unwrap();
    let full = budget.usage().reserved;
    values.clear();
    assert_eq!(budget.usage().reserved, full - "nested-name".len());
    assert!(budget.usage().reserved > 0);
    drop(values);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn failed_vector_growth_and_capacity_overflow_preserve_contents() {
    let budget = MemoryBudget::new(8);
    let mut bytes = LimitedBytes::try_copy(&budget, b"12345678").unwrap();
    assert!(bytes.try_push(9).is_err());
    assert_eq!(&bytes[..], b"12345678");
    assert!(bytes.try_reserve(usize::MAX).is_err());
    assert_eq!(budget.usage().reserved, 8);
    drop(bytes);
    assert_eq!(budget.usage().reserved, 0);
    let zero = MemoryBudget::new(0);
    let mut values = LimitedVec::new(&zero);
    values.try_push(()).unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(zero.usage().reserved, 0);
}

#[test]
fn retained_containers_keep_the_budget_alive_after_original_handle_is_dropped() {
    let budget = MemoryBudget::new(8);
    let mut bytes = LimitedBytes::try_copy(&budget, b"12345678").unwrap();
    drop(budget);
    assert!(bytes.try_push(9).is_err());
    assert_eq!(bytes.budget().usage().reserved, 8);
}

#[test]
fn overlapping_threads_cannot_each_spend_the_same_capacity() {
    let budget = MemoryBudget::new(4 * 64);
    let barrier = Barrier::new(8);
    let successes = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                let block = LimitedBytes::try_copy(&budget, &[0; 64]);
                if block.is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait();
                assert!(budget.usage().peak <= budget.usage().limit);
                drop(block);
            });
        }
    });
    assert_eq!(successes.load(Ordering::Relaxed), 4);
    assert_eq!(budget.usage().peak, 256);
    assert_eq!(budget.usage().rejected, 4);
    assert_eq!(budget.usage().reserved, 0);
}
