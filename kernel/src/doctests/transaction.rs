//! Compile-time boundaries for transaction states and execution modes.

/// Create-table transactions cannot remove files.
///
/// ```compile_fail,E0599
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{CreateTable, Transaction};
///
/// fn remove(txn: &mut Transaction<CreateTable>, data: FilteredEngineData) {
///     txn.remove_files(data);
/// }
/// ```
pub struct CreateRemove;

/// Alter-table transactions cannot add files.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{AlterTable, Transaction};
/// use delta_kernel::EngineData;
///
/// fn add(txn: &mut Transaction<AlterTable>, data: Box<dyn EngineData>) {
///     txn.add_files(data);
/// }
/// ```
pub struct AlterAdd;

/// Shared configuration and write-state APIs are available for any execution mode.
///
/// ```no_run
/// use delta_kernel::transaction::{ExistingTable, Transaction, ExecutionMode};
/// use delta_kernel::Result;
///
/// fn configure<E: ExecutionMode>(txn: Transaction<ExistingTable, E>) -> Result<()> {
///     let txn = txn.with_blind_append().with_engine_info("connector");
///     let _state = txn.write_state()?;
///     Ok(())
/// }
/// ```
pub struct SharedConfiguration;

/// The default transaction mode is imperative, and its commit API remains available.
///
/// ```no_run
/// use delta_kernel::transaction::{CommitResult, ExistingTable, Imperative, Transaction};
/// use delta_kernel::{Result, Engine};
///
/// fn commit(txn: Transaction, engine: &dyn Engine) -> Result<CommitResult> {
///     let txn: Transaction<ExistingTable, Imperative> = txn;
///     txn.commit(engine)
/// }
/// ```
pub struct ImperativeCommit;
