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

/// An execution-mode bound does not provide the imperative commit API.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{ExecutionMode, UpdateTable, Transaction};
/// use delta_kernel::Engine;
///
/// fn commit<MODE: ExecutionMode>(txn: Transaction<UpdateTable, MODE>, engine: &dyn Engine) {
///     txn.commit(engine);
/// }
/// ```
pub struct GenericModeCommit;

/// An execution-mode bound does not provide EngineData addition staging.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{ExecutionMode, UpdateTable, Transaction};
/// use delta_kernel::EngineData;
///
/// fn add<MODE: ExecutionMode>(
///     txn: &mut Transaction<UpdateTable, MODE>,
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
/// use delta_kernel::transaction::{ExecutionMode, UpdateTable, Transaction};
///
/// fn remove<MODE: ExecutionMode>(
///     txn: &mut Transaction<UpdateTable, MODE>,
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
/// use delta_kernel::transaction::{UpdateTable, Transaction, ExecutionMode};
/// use delta_kernel::KernelResult;
///
/// fn configure<MODE: ExecutionMode>(txn: Transaction<UpdateTable, MODE>) -> KernelResult<()> {
///     let txn = txn.with_operation_metrics([("numFiles", Some("1"))]);
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
/// use delta_kernel::transaction::{CommitResult, UpdateTable, Imperative, Transaction};
/// use delta_kernel::{Engine, EngineData, KernelResult, KernelResultIterator};
///
/// fn stage_and_commit(
///     txn: Transaction,
///     engine: &dyn Engine,
///     add_metadata: Box<dyn EngineData>,
///     remove_metadata: FilteredEngineData,
///     new_dv_descriptors: HashMap<String, DeletionVectorDescriptor>,
///     existing_data_files: impl KernelResultIterator<FilteredEngineData>,
/// ) -> KernelResult<CommitResult> {
///     let mut txn: Transaction<UpdateTable, Imperative> = txn;
///     txn.add_files(add_metadata);
///     txn.remove_files(remove_metadata);
///     #[cfg(feature = "internal-api")]
///     txn.update_deletion_vectors(new_dv_descriptors, existing_data_files)?;
///     txn.commit(engine)
/// }
/// ```
pub struct ImperativeCommit;

/// Builder entry points default to imperative transactions without an explicit mode selection.
///
/// ```no_run
/// use delta_kernel::schema::SchemaRef;
/// use delta_kernel::snapshot::SnapshotRef;
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::transaction::{
///     CreateTable, Imperative, Transaction, UpdateTable, UpdateTableOperation,
/// };
/// use delta_kernel::{Engine, KernelResult};
///
/// fn write_states(
///     snapshot: SnapshotRef,
///     engine: &dyn Engine,
///     schema: SchemaRef,
/// ) -> KernelResult<()> {
///     let update = snapshot
///         .update_table(UpdateTableOperation::Write, "connector/1.0", true)
///         .build_with_filesystem_committer(engine)?;
///     let _update_state = update.write_state()?;
///     let _: Transaction<UpdateTable, Imperative> = update;
///
///     let create = create_table("/path/to/new-table", schema, "connector/1.0", true)
///         .build_with_filesystem_committer(engine)?;
///     let _create_state = create.write_state()?;
///     let _: Transaction<CreateTable, Imperative> = create;
///     Ok(())
/// }
/// ```
#[cfg(feature = "internal-api")]
pub struct BuilderDefaultMode;

/// Selecting an execution mode requires the declarative-plans feature.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{Imperative, TransactionBuilder};
///
/// fn select_mode(builder: TransactionBuilder) {
///     builder.with_execution_mode::<Imperative>();
/// }
/// ```
#[cfg(not(feature = "declarative-plans"))]
pub struct ModeSelectionRequiresDeclarativePlans;

/// Declarative transactions stage relation handles and expose a separate commit entry point.
///
/// ```no_run
/// use delta_kernel::plans::{RelationRef, ScopedPlanExecutor};
/// use delta_kernel::snapshot::SnapshotRef;
/// use delta_kernel::transaction::{CommitResult, Declarative, UpdateTable, UpdateTableOperation};
/// use delta_kernel::{KernelResult, Engine};
///
/// fn commit(
///     snapshot: SnapshotRef,
///     engine: &dyn Engine,
///     executor: &dyn ScopedPlanExecutor,
///     adds: RelationRef,
///     removes: RelationRef,
/// ) -> KernelResult<CommitResult<UpdateTable, Declarative>> {
///     let mut txn = snapshot
///         .update_table(UpdateTableOperation::Write, "connector/1.0", true)
///         .with_execution_mode::<Declarative>()
///         .with_transaction_id("app", 1)
///         .build_with_filesystem_committer(engine)?;
///     txn.add_files_ref(adds);
///     txn.remove_files_ref(removes);
///     txn.commit_declarative(executor)
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCommit;

/// Create-table builders support declarative mode and retain their table-specific setters.
///
/// ```no_run
/// use delta_kernel::plans::{RelationRef, ScopedPlanExecutor};
/// use delta_kernel::schema::SchemaRef;
/// use delta_kernel::transaction::create_table::create_table;
/// use delta_kernel::transaction::{CommitResult, CreateTable, Declarative};
/// use delta_kernel::{Engine, KernelResult};
///
/// fn create(
///     engine: &dyn Engine,
///     executor: &dyn ScopedPlanExecutor,
///     schema: SchemaRef,
///     adds: RelationRef,
/// ) -> KernelResult<CommitResult<CreateTable, Declarative>> {
///     let mut txn = create_table("/path/to/new-table", schema, "connector/1.0", true)
///         .with_execution_mode::<Declarative>()
///         .with_table_properties([("app.version", "1")])
///         .build_with_filesystem_committer(engine)?;
///     txn.add_files_ref(adds);
///     txn.commit_declarative(executor)
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCreateTableCommit;

/// Declarative transactions cannot invoke the imperative commit path.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{Declarative, UpdateTable, Transaction};
/// use delta_kernel::Engine;
///
/// fn commit(txn: Transaction<UpdateTable, Declarative>, engine: &dyn Engine) {
///     txn.commit(engine);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeTxnWithImperativeCommit;

/// Declarative transactions accept relation handles rather than EngineData additions.
///
/// ```compile_fail,E0308
/// use delta_kernel::transaction::{Declarative, UpdateTable, Transaction};
/// use delta_kernel::EngineData;
///
/// fn add(txn: &mut Transaction<UpdateTable, Declarative>, data: Box<dyn EngineData>) {
///     txn.add_files_ref(data);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeWithEngineData;

/// Declarative create-table transactions cannot remove files.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::RelationRef;
/// use delta_kernel::transaction::{CreateTable, Declarative, Transaction};
///
/// fn remove(txn: &mut Transaction<CreateTable, Declarative>, files: RelationRef) {
///     txn.remove_files_ref(files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCreateTableCannotRemove;

/// Declarative create-table transactions cannot update deletion vectors.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::RelationRef;
/// use delta_kernel::transaction::{CreateTable, Declarative, Transaction};
///
/// fn update_dvs(
///     txn: &mut Transaction<CreateTable, Declarative>,
///     descriptors: RelationRef,
///     files: RelationRef,
/// ) {
///     txn.update_deletion_vectors_ref(descriptors, files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCreateTableCannotUpdateDVs;

/// Declarative transactions cannot stage additions through the imperative API.
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{Declarative, Transaction, UpdateTable};
/// use delta_kernel::EngineData;
///
/// fn add(txn: &mut Transaction<UpdateTable, Declarative>, data: Box<dyn EngineData>) {
///     txn.add_files(data);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCannotAddFiles;

/// Declarative transactions cannot stage removals through the imperative API.
///
/// ```compile_fail,E0599
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{Declarative, Transaction, UpdateTable};
///
/// fn remove(txn: &mut Transaction<UpdateTable, Declarative>, data: FilteredEngineData) {
///     txn.remove_files(data);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCannotRemoveFiles;

/// Declarative transactions cannot stage deletion vector updates through the imperative API.
///
/// ```compile_fail,E0599
/// use std::collections::HashMap;
///
/// use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
/// use delta_kernel::engine_data::FilteredEngineData;
/// use delta_kernel::transaction::{Declarative, Transaction, UpdateTable};
/// use delta_kernel::KernelResult;
///
/// fn update_dvs(
///     txn: &mut Transaction<UpdateTable, Declarative>,
///     descriptors: HashMap<String, DeletionVectorDescriptor>,
///     files: impl Iterator<Item = KernelResult<FilteredEngineData>>,
/// ) {
///     txn.update_deletion_vectors(descriptors, files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct DeclarativeCannotUpdateDeletionVectors;

/// Imperative transactions cannot stage addition relations.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::RelationRef;
/// use delta_kernel::transaction::Transaction;
///
/// fn add(txn: &mut Transaction, files: RelationRef) {
///     txn.add_files_ref(files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct ImperativeCannotAddFilesRef;

/// Imperative transactions cannot stage removal relations.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::RelationRef;
/// use delta_kernel::transaction::Transaction;
///
/// fn remove(txn: &mut Transaction, files: RelationRef) {
///     txn.remove_files_ref(files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct ImperativeCannotRemoveFilesRef;

/// Imperative transactions cannot stage deletion vector update relations.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::RelationRef;
/// use delta_kernel::transaction::Transaction;
///
/// fn update_dvs(txn: &mut Transaction, descriptors: RelationRef, files: RelationRef) {
///     txn.update_deletion_vectors_ref(descriptors, files);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct ImperativeCannotUpdateDeletionVectorsRef;

/// Imperative transactions cannot invoke the declarative commit path.
///
/// ```compile_fail,E0599
/// use delta_kernel::plans::ScopedPlanExecutor;
/// use delta_kernel::transaction::Transaction;
///
/// fn commit(txn: Transaction, executor: &dyn ScopedPlanExecutor) {
///     txn.commit_declarative(executor);
/// }
/// ```
#[cfg(feature = "declarative-plans")]
pub struct ImperativeTxnWithDeclarativeCommit;
