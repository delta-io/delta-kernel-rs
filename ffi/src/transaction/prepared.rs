//! Declarative transactions and owning commit outcomes.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use delta_kernel::KernelError;
use delta_kernel::plans::ScopedPlanExecutor;
use delta_kernel::transaction::{
    CreateTable, Declarative, PreparedTransaction, PreparedTransactionCommitResult, Transaction,
};
use delta_kernel_ffi_macros::handle_descriptor;

use super::committer::with_commit_relations;
use super::{
    ExclusiveCommittedTransaction, ExclusiveCreateTableTransactionBuilder,
    ExclusiveUpdateTableTransactionBuilder, FfiCommitRelation, MutableCommitter,
};
use crate::commit_range::SharedCommitRange;
use crate::error::{ExternResult, FFIKernelError, IntoExternResult};
use crate::handle::Handle;
use crate::plans::scoped::{SharedRelationRef, SharedScopedPlanExecutor};
use crate::{
    FfiSlice, KernelOwnedBytes, NullableCvoid, OptionalValue, SharedExternEngine, SharedSnapshot,
};

/// Exclusive handle to a declarative existing-table transaction.
#[handle_descriptor(target=Transaction<delta_kernel::transaction::UpdateTable, Declarative>, mutable=true, sized=true)]
pub struct ExclusiveDeclarativeUpdateTableTransaction;

/// Exclusive handle to a declarative create-table transaction.
#[handle_descriptor(target=Transaction<CreateTable, Declarative>, mutable=true, sized=true)]
pub struct ExclusiveDeclarativeCreateTableTransaction;

enum PreparedState {
    Update(PreparedTransaction),
    Create(PreparedTransaction<CreateTable>),
}

/// Prepared actions with ownership of the executor scope that retains their rows.
pub struct FfiPreparedTransaction {
    state: PreparedState,
    executor: Arc<dyn ScopedPlanExecutor>,
}

/// Exclusive prepared-transaction handle; commit it or call `free_prepared_transaction`.
#[handle_descriptor(target=FfiPreparedTransaction, mutable=true, sized=true)]
pub struct ExclusivePreparedTransaction;

enum ResultState {
    Update(PreparedTransactionCommitResult),
    Create(PreparedTransactionCommitResult<CreateTable>),
}

/// Owning result, including pending work, history cache, and the retained executor scope.
pub struct FfiPreparedTransactionCommitResult {
    state: ResultState,
    executor: Arc<dyn ScopedPlanExecutor>,
}

/// Exclusive result handle; release it with `free_prepared_transaction_commit_result`.
#[handle_descriptor(target=FfiPreparedTransactionCommitResult, mutable=true, sized=true)]
pub struct ExclusivePreparedTransactionCommitResult;

/// Physical outcome of a prepared commit attempt.
/// cbindgen:prefix-with-name=true
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedTransactionOutcome {
    /// Accepted commit; no pending transaction or winning history.
    Committed,
    /// Confirmed version conflict; pending work and history discovery remain available.
    Conflicted,
    /// Confirmed temporary rejection; pending work can retry the same version.
    Retryable,
    /// Acceptance is uncertain; pending work cannot be blindly submitted again.
    Unknown,
}

/// Independent handles for inspecting prepared state without transferring the transaction.
///
/// Free `actions` with `free_relation_ref` and `read_snapshot`, if present, with `free_snapshot`.
#[repr(C)]
pub struct PreparedTransactionView {
    /// Proposed commit version.
    pub target_version: u64,
    /// Original read snapshot; no snapshot is constructed by inspection.
    pub read_snapshot: OptionalValue<Handle<SharedSnapshot>>,
    /// Prepared action rows, excluding the attempt's separately finalized CommitInfo.
    pub actions: Handle<SharedRelationRef>,
    /// Whether the state retains an unknown submitted attempt that blocks resubmission.
    pub has_unknown_attempt: bool,
}

/// Error code and an owned message; release it with `free_prepared_transaction_error`.
#[repr(C)]
pub struct PreparedTransactionError {
    /// Kernel error classification.
    pub code: FFIKernelError,
    /// Full diagnostic message as UTF-8 bytes.
    pub message: KernelOwnedBytes,
}

/// Borrowed record of the exact submitted attempt retained after an unknown outcome.
#[repr(C)]
pub struct SubmittedAttemptView {
    /// Submitted version.
    pub version: u64,
    /// CommitInfo timestamp.
    pub timestamp: i64,
    /// Ordered timestamp when enabled.
    pub in_commit_timestamp: OptionalValue<i64>,
    /// Ordered action relations, with CommitInfo first. Slices are borrowed for the visitor call.
    pub actions: FfiSlice<FfiCommitRelation>,
}

/// Receives a submitted attempt for the duration of one call. Copy any values retained afterward.
pub type SubmittedAttemptVisitor = extern "C" fn(NullableCvoid, SubmittedAttemptView);

// These variants differ only in their table-type parameter; keep all ownership transitions shared.
macro_rules! on_prepared {
    ($state:expr, $p:ident => $body:expr) => {
        match $state {
            PreparedState::Update($p) => $body,
            PreparedState::Create($p) => $body,
        }
    };
}
macro_rules! on_result {
    ($state:expr, $r:ident => $body:expr) => {
        match $state {
            ResultState::Update($r) => $body,
            ResultState::Create($r) => $body,
        }
    };
}

/// Builds an existing-table declarative transaction using an optional custom committer.
///
/// `None` selects the filesystem committer. Returns builder-validation errors through `engine`.
///
/// # Safety
/// All handles must be valid. `builder` and an optional `committer` are consumed on success or
/// error. `engine` is borrowed. Configure intent through the existing builder setters first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_build_declarative(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    committer: OptionalValue<Handle<MutableCommitter>>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeUpdateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let committer = unsafe { take_committer(committer) };
    let engine = unsafe { engine.as_ref() };
    let result = builder
        .with_execution_mode::<Declarative>()
        .build(engine.engine().as_ref(), committer)
        .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Builds a create-table declarative transaction, defaulting to a filesystem committer.
///
/// Returns builder-validation errors through `engine`.
///
/// # Safety
/// All handles must be valid. `builder` and optional `committer` are consumed, including on error;
/// `engine` is borrowed. Configure the create intent using the existing builder setters first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn create_table_txn_builder_build_declarative(
    builder: Handle<ExclusiveCreateTableTransactionBuilder>,
    committer: OptionalValue<Handle<MutableCommitter>>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeCreateTableTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let committer = unsafe { take_committer(committer) };
    let engine = unsafe { engine.as_ref() };
    let result = builder
        .with_execution_mode::<Declarative>()
        .build(engine.engine().as_ref(), committer)
        .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Appends an engine-held file-description relation without executing it.
///
/// # Safety
/// `txn` must point to a valid handle borrowed exclusively for this call. `adds` is borrowed and
/// its rows must remain available in the scope later supplied to prepare.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_update_table_txn_add_files(
    txn: *mut Handle<ExclusiveDeclarativeUpdateTableTransaction>,
    adds: Handle<SharedRelationRef>,
) {
    unsafe { (&mut *txn).as_mut() }.add_files_ref(unsafe { adds.as_ref() }.clone());
}

/// Adds file descriptions to a create-table transaction without executing the relation.
///
/// # Safety
/// `txn` must point to a valid handle borrowed exclusively for this call. `adds` is borrowed; its
/// rows must survive in the scope supplied to prepare.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_create_table_txn_add_files(
    txn: *mut Handle<ExclusiveDeclarativeCreateTableTransaction>,
    adds: Handle<SharedRelationRef>,
) {
    unsafe { (&mut *txn).as_mut() }.add_files_ref(unsafe { adds.as_ref() }.clone());
}

/// Adds a selected scan-file relation for removal without executing it.
///
/// # Safety
/// `txn` must point to a valid handle borrowed exclusively. `removes` is borrowed and must contain
/// only selected rows with the scan-row schema, retained in the scope supplied to prepare.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_update_table_txn_remove_files(
    txn: *mut Handle<ExclusiveDeclarativeUpdateTableTransaction>,
    removes: Handle<SharedRelationRef>,
) {
    unsafe { (&mut *txn).as_mut() }.remove_files_ref(unsafe { removes.as_ref() }.clone());
}

/// Consumes the transaction and prepares actions, retaining a reference to `executor` on success.
///
/// Returns preparation/unsupported-feature errors through `engine`; no commit is submitted.
///
/// # Safety
/// All handles must be valid. `txn` is consumed, including on error; `engine` and `executor` are
/// borrowed. All input relations must belong to this scope.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_update_table_txn_prepare(
    txn: Handle<ExclusiveDeclarativeUpdateTableTransaction>,
    executor: Handle<SharedScopedPlanExecutor>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusivePreparedTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let executor = unsafe { executor.clone_as_arc() };
    let engine = unsafe { engine.as_ref() };
    let result = txn
        .prepare(engine.engine().as_ref(), executor.as_ref())
        .map(|p| {
            Box::new(FfiPreparedTransaction {
                state: PreparedState::Update(p),
                executor,
            })
            .into()
        });
    unsafe { result.into_extern_result(&engine) }
}

/// Consumes a create-table transaction and prepares actions, retaining `executor` on success.
///
/// Returns validation errors through `engine`; no commit is submitted.
///
/// # Safety
/// Handles must be valid. `txn` is consumed even on error; `executor` and `engine` are borrowed.
/// All relations must belong to this scope.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_create_table_txn_prepare(
    txn: Handle<ExclusiveDeclarativeCreateTableTransaction>,
    executor: Handle<SharedScopedPlanExecutor>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusivePreparedTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let executor = unsafe { executor.clone_as_arc() };
    let engine = unsafe { engine.as_ref() };
    let result = txn
        .prepare(engine.engine().as_ref(), executor.as_ref())
        .map(|p| {
            Box::new(FfiPreparedTransaction {
                state: PreparedState::Create(p),
                executor,
            })
            .into()
        });
    unsafe { result.into_extern_result(&engine) }
}

/// Submits one commit attempt, consuming `prepared` and returning an owning outcome handle.
///
/// Validation and post-commit Snapshot construction errors use `ExternResult`. A post-commit error
/// does not undo the accepted write. Conflict, retryable, and unknown outcomes retain pending work.
/// Confirmed conflicts attempt to save history before returning; load failure preserves the
/// conflict. Other outcomes do not discover history.
///
/// # Safety
/// Handles must be valid. `prepared` is consumed including on validation error; `engine` is
/// borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit(
    prepared: Handle<ExclusivePreparedTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusivePreparedTransactionCommitResult>> {
    let FfiPreparedTransaction { state, executor } = unsafe { *prepared.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = match state {
        PreparedState::Update(p) => p
            .commit(engine.engine().as_ref(), executor.as_ref())
            .map(ResultState::Update),
        PreparedState::Create(p) => p
            .commit(engine.engine().as_ref(), executor.as_ref())
            .map(ResultState::Create),
    }
    .map(|state| Box::new(FfiPreparedTransactionCommitResult { state, executor }).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Inspects prepared state without I/O or transferring the transaction.
///
/// # Safety
/// `prepared` must point to a valid, borrowed handle. Release the returned view's owned handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_inspect(
    prepared: *const Handle<ExclusivePreparedTransaction>,
) -> PreparedTransactionView {
    on_prepared!(&unsafe { (&*prepared).as_ref() }.state, p => prepared_view(p))
}

/// Visits the exact unknown submitted attempt without reading its rows.
///
/// Returns false when no unknown attempt is retained, otherwise invokes `visitor` once and returns
/// true. The prepared transaction remains owned by the caller.
///
/// # Safety
/// `prepared` must point to a valid borrowed handle. The visitor/context must remain valid for the
/// call and must not unwind, mutate or free `prepared`, or retain borrowed slices after returning.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_visit_submitted_attempt(
    prepared: *const Handle<ExclusivePreparedTransaction>,
    context: NullableCvoid,
    visitor: SubmittedAttemptVisitor,
) -> bool {
    on_prepared!(&unsafe { (&*prepared).as_ref() }.state, p => visit_attempt(p, context, visitor))
}

/// Returns the rejected version for a conflict, including after pending work was extracted.
///
/// # Safety
/// `result` must point to a valid borrowed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_conflict_version(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
) -> OptionalValue<u64> {
    on_result!(&unsafe { (&*result).as_ref() }.state, r => match r {
        PreparedTransactionCommitResult::Conflicted { conflict_version, .. } => Some(*conflict_version).into(),
        _ => OptionalValue::None,
    })
}

/// Returns the physical outcome without I/O.
///
/// # Safety
/// `result` must point to a valid handle and is borrowed for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_outcome(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
) -> PreparedTransactionOutcome {
    on_result!(&unsafe { (&*result).as_ref() }.state, r => match r {
        PreparedTransactionCommitResult::Committed(_) => PreparedTransactionOutcome::Committed,
        PreparedTransactionCommitResult::Conflicted { .. } => PreparedTransactionOutcome::Conflicted,
        PreparedTransactionCommitResult::Retryable { .. } => PreparedTransactionOutcome::Retryable,
        PreparedTransactionCommitResult::Unknown { .. } => PreparedTransactionOutcome::Unknown,
    })
}

/// Inspects retained pending state without taking it. Returns `None` after success or extraction.
///
/// # Safety
/// `result` must point to a valid borrowed handle. Release the returned view's owned handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_pending(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
) -> OptionalValue<PreparedTransactionView> {
    on_result!(&unsafe { (&*result).as_ref() }.state, r => r.get_prepared_transaction().map(prepared_view).into())
}

/// Transfers pending work while leaving saved history or its discovery error in `result`.
///
/// Returns `None` after success or if already taken. The returned handle owns its executor scope.
///
/// # Safety
/// `result` must point to a valid handle borrowed exclusively. Free or commit the returned handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_take_pending(
    result: *mut Handle<ExclusivePreparedTransactionCommitResult>,
) -> OptionalValue<Handle<ExclusivePreparedTransaction>> {
    let result = unsafe { (&mut *result).as_mut() };
    let state = match &mut result.state {
        ResultState::Update(r) => r.take_prepared_transaction().map(PreparedState::Update),
        ResultState::Create(r) => r.take_prepared_transaction().map(PreparedState::Create),
    };
    state
        .map(|state| {
            Box::new(FfiPreparedTransaction {
                state,
                executor: result.executor.clone(),
            })
            .into()
        })
        .into()
}

/// Returns saved winning history without performing I/O.
///
/// Returns `None` for non-conflicts and the saved discovery error when history is unavailable.
/// Repeated calls do not retry discovery. The returned range is independently owned; release it
/// with `free_commit_range`. `engine` supplies error allocation only.
///
/// # Safety
/// `result` must point to a valid borrowed handle; `engine` is valid and borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_get_winning_commits(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<OptionalValue<Handle<SharedCommitRange>>> {
    let result = unsafe { (&*result).as_ref() };
    let engine = unsafe { engine.as_ref() };
    let history = on_result!(&result.state, r => r.get_winning_commits()
        .map(|h| h.map(|range| range.clone().into()).into()));
    unsafe { history.into_extern_result(&engine) }
}

/// Returns a retryable/unknown outcome error, without consuming pending work or performing I/O.
///
/// # Safety
/// `result` must point to a valid borrowed handle. Release the returned error with
/// `free_prepared_transaction_error`, if present.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_error(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
) -> OptionalValue<PreparedTransactionError> {
    on_result!(&unsafe { (&*result).as_ref() }.state, r => match r {
        PreparedTransactionCommitResult::Retryable { error, .. } | PreparedTransactionCommitResult::Unknown { error, .. } => Some(error_view(error)).into(),
        _ => OptionalValue::None,
    })
}

/// Returns the last history-load error without performing I/O, including an eager load failure.
///
/// # Safety
/// `result` must point to a valid borrowed handle. Release the returned error with
/// `free_prepared_transaction_error`, if present.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_history_error(
    result: *const Handle<ExclusivePreparedTransactionCommitResult>,
) -> OptionalValue<PreparedTransactionError> {
    on_result!(&unsafe { (&*result).as_ref() }.state, r => r.winning_commits_error().map(error_view).into())
}

/// Consumes a successful result and returns the existing committed-transaction handle.
///
/// Returns an invalid-state error for other outcomes. Inspect the outcome before calling.
///
/// # Safety
/// `result` and `engine` must be valid. `result` is consumed even on error; `engine` is borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_commit_result_into_committed(
    result: Handle<ExclusivePreparedTransactionCommitResult>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let result = unsafe { *result.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = on_result!(result.state, r => match r {
        PreparedTransactionCommitResult::Committed(c) => Ok(Box::new(c).into()),
        _ => Err(KernelError::invalid_transaction_state("Expected a committed outcome")),
    });
    unsafe { result.into_extern_result(&engine) }
}

/// Rebase boundary. Returns Unsupported without changing prepared state or its baseline.
///
/// # Safety
/// All handles must be valid. `prepared` is borrowed exclusively; `winners` and `engine` are
/// borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn prepared_transaction_rebase(
    prepared: *mut Handle<ExclusivePreparedTransaction>,
    winners: Handle<SharedCommitRange>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let prepared = unsafe { (&mut *prepared).as_mut() };
    let winners = unsafe { winners.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = on_prepared!(&mut prepared.state, p => p.rebase(winners)).map(|()| true);
    unsafe { result.into_extern_result(&engine) }
}

/// Drops a prepared transaction without submitting it.
///
/// # Safety
/// `prepared` must be valid and is consumed. Do not use it afterward.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_prepared_transaction(prepared: Handle<ExclusivePreparedTransaction>) {
    unsafe { prepared.drop_handle() }
}

/// Drops a result and any pending work it still owns.
///
/// # Safety
/// `result` must be valid and is consumed. Extracted transactions/ranges remain independently
/// owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_prepared_transaction_commit_result(
    result: Handle<ExclusivePreparedTransactionCommitResult>,
) {
    unsafe { result.drop_handle() }
}

/// Drops a declarative update-table transaction before preparation.
///
/// # Safety
/// `txn` must be valid and is consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_declarative_update_table_txn(
    txn: Handle<ExclusiveDeclarativeUpdateTableTransaction>,
) {
    unsafe { txn.drop_handle() }
}

/// Drops a declarative create-table transaction before preparation.
///
/// # Safety
/// `txn` must be valid and is consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_declarative_create_table_txn(
    txn: Handle<ExclusiveDeclarativeCreateTableTransaction>,
) {
    unsafe { txn.drop_handle() }
}

/// Releases an error returned by the outcome or history-error accessors.
///
/// # Safety
/// `error.message` must be a valid owned buffer; this call consumes it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_prepared_transaction_error(error: PreparedTransactionError) {
    unsafe { crate::free_kernel_bytes(error.message) }
}

fn prepared_view<S>(p: &PreparedTransaction<S>) -> PreparedTransactionView {
    PreparedTransactionView {
        target_version: p.target_version(),
        read_snapshot: p.read_snapshot().cloned().map(Into::into).into(),
        actions: Arc::new(p.get_delta_actions().clone()).into(),
        has_unknown_attempt: p.submitted_attempt().is_some(),
    }
}

fn error_view(error: &KernelError) -> PreparedTransactionError {
    PreparedTransactionError {
        code: error.into(),
        message: error.to_string().into_bytes().into(),
    }
}

unsafe fn take_committer(
    committer: OptionalValue<Handle<MutableCommitter>>,
) -> Box<dyn delta_kernel::committer::Committer> {
    match committer {
        OptionalValue::Some(handle) => unsafe { handle.into_inner() },
        OptionalValue::None => Box::new(delta_kernel::committer::FileSystemCommitter::new()),
    }
}

fn visit_attempt<S>(
    prepared: &PreparedTransaction<S>,
    context: NullableCvoid,
    visitor: SubmittedAttemptVisitor,
) -> bool {
    let Some(attempt) = prepared.submitted_attempt() else {
        return false;
    };
    with_commit_relations(attempt.actions(), |actions| {
        visitor(
            context,
            SubmittedAttemptView {
                version: attempt.version(),
                timestamp: attempt.timestamp(),
                in_commit_timestamp: attempt.in_commit_timestamp().into(),
                actions,
            },
        );
    });
    true
}
