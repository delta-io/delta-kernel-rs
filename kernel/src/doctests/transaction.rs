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

/// An execution-mode bound does not provide the imperative commit API.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{ExecutionMode, ExistingTable, Transaction};
/// use delta_kernel::Engine;
///
/// fn commit<MODE: ExecutionMode>(txn: Transaction<ExistingTable, MODE>, engine: &dyn Engine) {
///     txn.commit(engine);
/// }
/// ```
pub struct GenericModeCommit;

/// An execution-mode bound does not provide EngineData addition staging.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{ExecutionMode, ExistingTable, Transaction};
/// use delta_kernel::EngineData;
///
/// fn add<MODE: ExecutionMode>(
///     txn: &mut Transaction<ExistingTable, MODE>,
///     data: Box<dyn EngineData>,
/// ) {
///     txn.add_files(data);
/// }
/// ```
pub struct GenericModeAdd;

/// An execution-mode bound does not provide EngineData removal staging.
///
/// ```compile_fail,E0599
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{ExecutionMode, ExistingTable, Transaction};
///
/// fn remove<MODE: ExecutionMode>(
///     txn: &mut Transaction<ExistingTable, MODE>,
///     data: FilteredEngineData,
/// ) {
///     txn.remove_files(data);
/// }
/// ```
pub struct GenericModeRemove;

/// Downstream types cannot implement execution modes.
///
/// ```compile_fail,E0277
/// use delta_kernel::transaction::ExecutionMode;
///
/// #[derive(Debug, Default)]
/// struct CustomMode;
///
/// impl ExecutionMode for CustomMode {}
/// ```
pub struct SealedExecutionMode;

/// Shared configuration and write-state APIs are available for any execution mode.
///
/// ```no_run
/// use delta_kernel::transaction::{ExistingTable, Transaction, ExecutionMode};
/// use delta_kernel::Result;
///
/// fn configure<MODE: ExecutionMode>(txn: Transaction<ExistingTable, MODE>) -> Result<()> {
///     let txn = txn.with_blind_append().with_engine_info("connector");
///     let _state = txn.write_state()?;
///     Ok(())
/// }
/// ```
pub struct SharedConfiguration;

/// The default transaction mode is imperative, with file staging and commit APIs.
///
/// ```no_run
/// use std::collections::HashMap;
///
/// use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{CommitResult, ExistingTable, Imperative, Transaction};
/// use delta_kernel::{Engine, EngineData, Result};
///
/// fn stage_and_commit(
///     txn: Transaction,
///     engine: &dyn Engine,
///     add_metadata: Box<dyn EngineData>,
///     remove_metadata: FilteredEngineData,
///     new_dv_descriptors: HashMap<String, DeletionVectorDescriptor>,
///     existing_data_files: impl Iterator<Item = Result<FilteredEngineData>>,
/// ) -> Result<CommitResult> {
///     let mut txn: Transaction<ExistingTable, Imperative> = txn;
///     txn.add_files(add_metadata);
///     txn.remove_files(remove_metadata);
///     #[cfg(feature = "internal-api")]
///     txn.update_deletion_vectors(new_dv_descriptors, existing_data_files)?;
///     txn.commit(engine)
/// }
/// ```
pub struct ImperativeCommit;
