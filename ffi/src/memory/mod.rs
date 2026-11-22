//! Caller-controlled allocation budgets for participating Kernel metadata storage.
//!
//! Requires Rust 1.100 and the `limited-memory` feature. The C ABI transports opaque owners;
//! Rust's `std::alloc::Allocator` trait and allocator-aware collections stay inside Kernel.
//! Handles can be shared across calling threads. These counters exclude engine buffers,
//! unconverted Kernel allocations, allocator overhead, and host-side copies; they do not cap RSS.

use std::sync::Arc;

use delta_kernel::memory::MemoryBudget;
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::scan::ExclusiveScanBuilder;
use crate::transaction::{
    ExclusiveCreateTableTransactionBuilder, ExclusiveUpdateTableTransactionBuilder,
};

#[cfg(test)]
mod tests;

/// An owned reference to a thread-safe budget. Release with [`free_memory_budget`].
#[handle_descriptor(target=MemoryBudget, mutable=false, sized=true)]
pub struct SharedMemoryBudget;

/// Observations of requested allocation bytes; fields are read independently under concurrency.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FfiMemoryUsage {
    /// Immutable maximum simultaneously reserved requested bytes.
    pub limit: usize,
    /// Outstanding requested bytes, including allocations in progress.
    pub reserved: usize,
    /// Largest observed reservation, including overlapping allocations during growth.
    pub peak: usize,
    /// Number of allocation requests rejected by the cap.
    pub rejected: usize,
}

/// Install one process-local budget before doing any other Kernel work.
///
/// Ordinary snapshot, scan, and transaction calls then share this cap across threads. Explicit
/// object budgets also charge the global budget. Returns false if configuration or construction
/// has already sealed the policy, including a lookup that found no installed budget. The limit
/// cannot be changed or removed. A zero limit rejects
/// nonempty participating allocations. This limits requested bytes in converted storage, not RSS.
#[unsafe(no_mangle)]
pub extern "C" fn kernel_memory_budget_install_global(limit: usize) -> bool {
    MemoryBudget::install_global(limit).is_ok()
}

/// Copy the shared global counters, returning false if no limit is installed or `out` is null.
///
/// # Safety
///
/// A non-null `out` must be aligned, writable storage for one `FfiMemoryUsage`, exclusively
/// borrowed for this call. No caller memory is retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn memory_budget_global_usage(out: *mut FfiMemoryUsage) -> bool {
    if out.is_null() {
        return false;
    }
    let Some(usage) = MemoryBudget::global_usage() else {
        return false;
    };
    unsafe { out.write(usage.into()) };
    true
}

impl From<delta_kernel::memory::MemoryUsage> for FfiMemoryUsage {
    fn from(usage: delta_kernel::memory::MemoryUsage) -> Self {
        Self {
            limit: usage.limit,
            reserved: usage.reserved,
            peak: usage.peak,
            rejected: usage.rejected,
        }
    }
}

/// Create an owned budget handle. A zero limit rejects nonempty participating allocations.
///
/// The small handle and accounting control block use the global allocator outside the cap.
/// Attach this budget to a snapshot or scan builder before building. Derived objects retain
/// their own ownership, so the caller may release this handle independently.
#[unsafe(no_mangle)]
pub extern "C" fn kernel_memory_budget_new(limit: usize) -> Handle<SharedMemoryBudget> {
    Arc::new(MemoryBudget::new(limit)).into()
}

/// Return another owned handle to the same budget, without resetting its counters.
///
/// # Safety
///
/// `budget` must be valid and is borrowed. Release each owned handle exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn clone_memory_budget(
    budget: Handle<SharedMemoryBudget>,
) -> Handle<SharedMemoryBudget> {
    unsafe { budget.clone_handle() }
}

/// Copy usage counters into caller-owned storage, returning false when `out` is null.
///
/// # Safety
///
/// `budget` must be valid and is borrowed. A non-null `out` must be aligned, writable storage for
/// one `FfiMemoryUsage`, exclusively borrowed for this call. No caller memory is retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn memory_budget_usage(
    budget: Handle<SharedMemoryBudget>,
    out: *mut FfiMemoryUsage,
) -> bool {
    if out.is_null() {
        return false;
    }
    let usage = unsafe { budget.as_ref() }.usage();
    unsafe { out.write(usage.into()) };
    true
}

/// Release one budget handle. Allocations and derived objects retain their own owners.
///
/// # Safety
///
/// `budget` must be valid and is consumed. Do not reuse or free this handle again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_memory_budget(budget: Handle<SharedMemoryBudget>) {
    unsafe { budget.drop_handle() }
}

/// Select a budget for new participating scan allocations, including deferred iterator work.
///
/// By default scans inherit their snapshot's allocator. Shared snapshot storage retains its
/// existing owner when overridden here. The budget handle may be freed after this call.
///
/// # Safety
///
/// Both handles must be valid. `builder` is consumed and must not be reused or freed. `budget`
/// is borrowed. The returned builder must be built or released with `free_scan_builder`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn scan_builder_with_memory_budget(
    builder: Handle<ExclusiveScanBuilder>,
    budget: Handle<SharedMemoryBudget>,
) -> Handle<ExclusiveScanBuilder> {
    let builder = unsafe { builder.into_inner() };
    Box::new(builder.with_allocator(unsafe { budget.as_ref() }.allocator())).into()
}

/// Select a budget for new participating update-transaction allocations.
///
/// Update transactions inherit the snapshot's allocator unless overridden. Write state and
/// post-commit snapshots retain their owners across calls and threads.
///
/// # Safety
///
/// Both handles must be valid. `builder` is consumed and must not be reused or freed. `budget`
/// is borrowed. The returned builder must be built or released with its builder release function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_with_memory_budget(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    budget: Handle<SharedMemoryBudget>,
) -> Handle<ExclusiveUpdateTableTransactionBuilder> {
    let builder = unsafe { builder.into_inner() };
    Box::new(builder.with_allocator(unsafe { budget.as_ref() }.allocator())).into()
}

/// Select a budget for new participating create-transaction allocations.
///
/// # Safety
///
/// Both handles must be valid. `builder` is consumed and must not be reused or freed. `budget`
/// is borrowed. The returned builder must be built or released with its builder release function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn create_table_txn_builder_with_memory_budget(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    budget: Handle<SharedMemoryBudget>,
) -> Handle<ExclusiveCreateTableTransactionBuilder> {
    let builder = unsafe { builder.into_inner() };
    Box::new(builder.with_allocator(unsafe { budget.as_ref() }.allocator())).into()
}
