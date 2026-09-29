use std::sync::Arc;

use super::ExclusiveCommittedTransaction;
use crate::handle::Handle;
use crate::{OptionalValue, SharedSnapshot};

/// Free a committed-transaction handle.
///
/// # Safety
/// The handle must be valid and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn free_committed_transaction(txn: Handle<ExclusiveCommittedTransaction>) {
    txn.drop_handle();
}

/// Read the committed version without consuming the handle.
///
/// # Safety
/// The handle must be valid.
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
/// The handle must be valid.
#[no_mangle]
pub unsafe extern "C" fn committed_transaction_post_commit_snapshot(
    txn: &Handle<ExclusiveCommittedTransaction>,
) -> OptionalValue<Handle<SharedSnapshot>> {
    unsafe { txn.as_ref() }
        .post_commit_snapshot()
        .map(|snapshot| Arc::clone(snapshot).into())
        .into()
}
