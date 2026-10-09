use std::sync::Arc;

use delta_kernel::transaction::CommitResult;

use super::ExclusiveCommittedTransaction;
use crate::handle::Handle;
use crate::{OptionalValue, Result, SharedSnapshot};

/// Converts a [`CommitResult`] to an owned FFI handle, or returns an error when the commit was not
/// successful.
///
/// The returned handle owns the committed transaction and must be released with
/// [`free_committed_transaction`].
// TODO(#3504): Preserve retryable and conflicted transaction state across the FFI boundary.
pub(super) fn commit_result_to_committed_handle<S>(
    result: Result<CommitResult<S>>,
) -> Result<Handle<ExclusiveCommittedTransaction>> {
    match result? {
        CommitResult::Committed(committed) => Ok(Box::new(committed).into()),
        CommitResult::Retryable(_) => Err(delta_kernel::KernelError::unsupported(
            "commit failed: retryable transaction not supported in FFI (yet)",
        )),
        CommitResult::Conflicted(conflicted) => Err(delta_kernel::KernelError::Generic(format!(
            "commit conflict at version {}",
            conflicted.conflict_version()
        ))),
    }
}

/// Free a committed-transaction handle.
///
/// # Safety
/// `txn` must be a valid handle and is consumed. Do not use or free it again.
#[no_mangle]
pub unsafe extern "C" fn free_committed_transaction(txn: Handle<ExclusiveCommittedTransaction>) {
    txn.drop_handle();
}

/// Read the committed version without consuming the handle.
///
/// # Safety
/// `txn` must be a valid handle and is borrowed for this call.
#[no_mangle]
pub unsafe extern "C" fn committed_transaction_version(
    txn: &Handle<ExclusiveCommittedTransaction>,
) -> u64 {
    unsafe { txn.as_ref() }.commit_version()
}

/// Return a fresh handle for the post-commit snapshot when one is available.
///
/// The returned snapshot handle is independently owned. This does not consume `txn`.
///
/// # Safety
/// `txn` must be a valid handle and is borrowed for this call.
#[no_mangle]
pub unsafe extern "C" fn committed_transaction_post_commit_snapshot(
    txn: &Handle<ExclusiveCommittedTransaction>,
) -> OptionalValue<Handle<SharedSnapshot>> {
    unsafe { txn.as_ref() }
        .post_commit_snapshot()
        .map(|snapshot| Arc::clone(snapshot).into())
        .into()
}
