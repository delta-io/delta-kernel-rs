//! Update table transaction methods.
//!
//! This module contains the constructor, public API, and deletion vector update logic for
//! update-table transactions. Each transaction type lives in its own file;
//! see [`mod.rs`](super) for shared commit logic.
//!
//! Includes:
//! - [`try_new_existing_table`](Transaction::try_new_existing_table) constructor
//! - Deletion vector updates
//! - Blind append, operation setting, domain metadata removal, and file removal

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};

use tracing::instrument;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::get_log_add_schema;
use crate::committer::Committer;
use crate::engine_data::{
    FilteredEngineData, FilteredRowVisitor, GetData, RowEvent, RowIndexIterator, TypedGetData,
};
use crate::error::Error;
use crate::expressions::{column_name, ArrayData, ColumnName, Scalar, StructData, Transform};
use crate::scan::data_skipping::stats_schema::NullableStatsTransform;
use crate::scan::log_replay::get_scan_metadata_transform_expr;
use crate::scan::{restored_add_schema, scan_row_schema};
use crate::schema::{ArrayType, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::SnapshotRef;
use crate::table_features::{Operation, TableFeature};
use crate::utils::current_time_ms;
use crate::{DataType, DeltaResult, Engine, Expression, SchemaTransform};
use delta_kernel_derive::internal_api;

use super::Transaction;

// =============================================================================
// Update table transactions only
// =============================================================================
impl Transaction {
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /// Create a new transaction from a snapshot for an existing table. The snapshot will be used
    /// to read the current state of the table (e.g. to read the current version).
    ///
    /// Instead of using this API, the more typical (user-facing) API is
    /// [Snapshot::transaction](crate::snapshot::Snapshot::transaction) to create a transaction from
    /// a snapshot.
    pub(crate) fn try_new_existing_table(
        snapshot: impl Into<SnapshotRef>,
        committer: Box<dyn Committer>,
        engine: &dyn Engine,
    ) -> DeltaResult<Self> {
        let read_snapshot = snapshot.into();

        // important! before writing to the table we must check it is supported
        read_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Write)?;

        // Read clustering columns from snapshot (returns None if clustering not enabled)
        let clustering_columns = read_snapshot.get_clustering_columns(engine)?;

        let commit_timestamp = current_time_ms()?;

        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
        );

        Ok(Transaction {
            span,
            read_snapshot,
            committer,
            operation: None,
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata: vec![],
            set_transactions: vec![],
            commit_timestamp,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            user_domain_removals: vec![],
            data_change: true,
            is_blind_append: false,
            dv_matched_files: vec![],
            clustering_columns,
            _state: PhantomData,
        })
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// Mark this transaction as a blind append.
    ///
    /// Blind append transactions should only add new files and avoid write operations that
    /// depend on existing table state.
    pub fn with_blind_append(mut self) -> Self {
        self.is_blind_append = true;
        self
    }

    /// Set the operation that this transaction is performing. This string will be persisted in the
    /// commit and visible to anyone who describes the table history.
    pub fn with_operation(mut self, operation: String) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Remove domain metadata from the Delta log.
    /// If the domain exists in the Delta log, this creates a tombstone to logically delete
    /// the domain. The tombstone preserves the previous configuration value.
    /// If the domain does not exist in the Delta log, this is a no-op.
    /// Note that each domain can only appear once per transaction. That is, multiple operations
    /// on the same domain are disallowed in a single transaction, as well as setting and removing
    /// the same domain in a single transaction. If a duplicate domain is included, the `commit` will
    /// fail (that is, we don't eagerly check domain validity here).
    /// Removing metadata for multiple distinct domains is allowed.
    pub fn with_domain_metadata_removed(mut self, domain: String) -> Self {
        self.user_domain_removals.push(domain);
        self
    }

    /// Remove files from the table in this transaction. This API generally enables the engine to
    /// delete data (at file-level granularity) from the table. Note that this API can be called
    /// multiple times to remove multiple batches.
    ///
    /// The expected schema for `remove_metadata` is given by [`scan_row_schema`]. It is expected
    /// this will be the result of passing [`FilteredEngineData`] returned from a scan
    /// with the selection vector modified to select rows for removal (selected rows in the selection vector are the ones to be removed).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use delta_kernel::Engine;
    /// # use delta_kernel::snapshot::Snapshot;
    /// # #[cfg(feature = "catalog-managed")]
    /// # use delta_kernel::committer::FileSystemCommitter;
    /// # fn example(engine: Arc<dyn Engine>, table_url: url::Url) -> delta_kernel::DeltaResult<()> {
    /// # #[cfg(feature = "catalog-managed")]
    /// # {
    /// // Create a snapshot and transaction
    /// let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    /// let mut txn = snapshot.clone().transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    ///
    /// // Get file metadata from a scan
    /// let scan = snapshot.scan_builder().build()?;
    /// let scan_metadata = scan.scan_metadata(engine.as_ref())?;
    ///
    /// // Remove specific files based on scan metadata
    /// for metadata in scan_metadata {
    ///     let metadata = metadata?;
    ///     // In practice, you would modify the selection vector to choose which files to remove
    ///     let files_to_remove = metadata.scan_files;
    ///     txn.remove_files(files_to_remove);
    /// }
    ///
    /// // Commit the transaction
    /// txn.commit(engine.as_ref())?;
    /// # }
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_files(&mut self, remove_metadata: FilteredEngineData) {
        self.remove_files_metadata.push(remove_metadata);
    }

    // -------------------------------------------------------------------------
    // Deletion vector updates
    // -------------------------------------------------------------------------

    /// Helper function to convert scan metadata iterator to filtered engine data iterator.
    ///
    /// This adapter extracts the `scan_files` field from each [`crate::scan::ScanMetadata`] item,
    /// making it easy to pass scan results directly to [`Self::update_deletion_vectors`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let scan = snapshot.scan_builder().build()?;
    /// let metadata = scan.scan_metadata(engine)?;
    /// let mut dv_map = HashMap::new();
    /// // ... populate dv_map ...
    /// let files_iter = Transaction::scan_metadata_to_engine_data(metadata);
    /// txn.update_deletion_vectors(dv_map, files_iter)?;
    /// ```
    pub fn scan_metadata_to_engine_data(
        scan_metadata: impl Iterator<Item = DeltaResult<crate::scan::ScanMetadata>>,
    ) -> impl Iterator<Item = DeltaResult<FilteredEngineData>> {
        scan_metadata.map(|result| result.map(|metadata| metadata.scan_files))
    }

    /// Update deletion vectors for files in the table.
    ///
    /// This method can be called multiple times to update deletion vectors for different sets of files.
    ///
    /// This method takes a map of file paths to new deletion vector descriptors and an iterator
    /// of scan file data. It joins the two together internally and will generate appropriate
    /// remove/add actions on commit to update the deletion vectors.
    ///
    /// # Arguments
    ///
    /// * `new_dv_descriptors` - A map from data file path (as provided in scan operations) to
    ///   the new deletion vector descriptor for that file.
    /// * `existing_data_files` - An iterator over FilteredEngineData from scan metadata. The
    ///   selected elements of each FilteredEngineData must be a superset of the paths that key
    ///   `new_dv_descriptors`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A file path in `new_dv_descriptors` is not found in `existing_data_files`
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut txn = snapshot.clone().transaction(Box::new(FileSystemCommitter::new()))?
    ///     .with_operation("UPDATE".to_string());
    ///
    /// let scan = snapshot.scan_builder().build()?;
    /// let files: Vec<FilteredEngineData> = scan.scan_metadata(engine)?
    ///     .collect::<Result<Vec<_>, _>>()?
    ///     .into_iter()
    ///     .map(|sm| sm.scan_files)
    ///     .collect();
    ///
    /// // Create map of file paths to new deletion vector descriptors
    /// let mut dv_map = HashMap::new();
    /// // ... populate dv_map with file paths and their new DV descriptors ...
    ///
    /// txn.update_deletion_vectors(dv_map, files.into_iter())?;
    /// txn.commit(engine)?;
    /// ```
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[instrument(
        name = "txn.update_dvs",
        skip_all,
        fields(num_dv_updates = new_dv_descriptors.len()),
        err
    )]
    pub(crate) fn update_deletion_vectors(
        &mut self,
        new_dv_descriptors: HashMap<String, DeletionVectorDescriptor>,
        existing_data_files: impl Iterator<Item = DeltaResult<FilteredEngineData>>,
    ) -> DeltaResult<()> {
        if self.is_create_table() {
            return Err(Error::generic(
                "Deletion vector operations require an existing table",
            ));
        }
        if !self
            .read_snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::DeletionVectors)
        {
            return Err(Error::unsupported(
                "Deletion vector operations require reader version 3, writer version 7, \
                 and the 'deletionVectors' feature in both reader and writer features",
            ));
        }

        let mut matched_dv_files = 0;
        let mut visitor = DvMatchVisitor::new(&new_dv_descriptors);

        // Process each batch of scan file metadata to prepare for DV updates:
        // 1. Visit rows to match file paths against the DV descriptor map
        // 2. Append new DV descriptors as a temporary column to matched files
        // 3. Update selection vector to only keep files that need DV updates
        // 4. Cache the result in dv_matched_files for generating remove/add actions during commit
        for scan_file_result in existing_data_files {
            let scan_file = scan_file_result?;
            visitor.new_dv_entries.clear();
            visitor.matched_file_indexes.clear();
            visitor.visit_rows_of(&scan_file)?;
            let (data, mut selection_vector) = scan_file.into_parts();

            // Update selection vector to keep only files that matched DV descriptors.
            // This ensures we only generate remove/add actions for files being updated.
            let mut current_matched_index = 0;
            for (i, selected) in selection_vector.iter_mut().enumerate() {
                if current_matched_index < visitor.matched_file_indexes.len() {
                    if visitor.matched_file_indexes[current_matched_index] != i {
                        *selected = false;
                    } else {
                        current_matched_index += 1;
                        matched_dv_files += if *selected { 1 } else { 0 };
                    }
                } else {
                    // Deselect any files after the last matched file
                    *selected = false;
                }
            }

            let new_columns = vec![ArrayData::try_new(
                struct_deletion_vector_schema().clone(),
                visitor.new_dv_entries.clone(),
            )?];
            self.dv_matched_files.push(FilteredEngineData::try_new(
                data.append_columns(new_dv_column_schema().clone(), new_columns)?,
                selection_vector,
            )?);
        }

        if matched_dv_files != new_dv_descriptors.len() {
            return Err(Error::generic(format!(
                "Number of matched DV files does not match number of new DV descriptors: {} != {}",
                matched_dv_files,
                new_dv_descriptors.len()
            )));
        }

        Ok(())
    }
}

// =============================================================================
// Deletion vector schemas and commit helpers
// =============================================================================

/// Column name for temporary column used during deletion vector updates.
/// This column holds new DV descriptors appended to scan file metadata before transforming to final add actions.
static NEW_DELETION_VECTOR_NAME: &str = "newDeletionVector";

/// Schema for scan row data with an additional column for new deletion vector descriptors.
/// This is an intermediate schema used during deletion vector updates before transforming to final add actions.
static INTERMEDIATE_DV_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(
        scan_row_schema()
            .fields()
            .cloned()
            .chain([StructField::nullable(
                NEW_DELETION_VECTOR_NAME.to_string(),
                DeletionVectorDescriptor::to_schema(),
            )]),
    ))
});

/// Returns the intermediate schema with deletion vector column appended to scan row schema.
fn intermediate_dv_schema() -> &'static SchemaRef {
    &INTERMEDIATE_DV_SCHEMA
}

/// Schema for scan row data with nullable statistics fields.
/// Used when generating remove actions to ensure statistics can be null if missing.
// Safety: The panic here is acceptable because scan_row_schema() is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_SCAN_ROWS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(scan_row_schema().as_ref())
        .unwrap_or_else(|| panic!("Failed to transform scan_row_schema"))
        .into_owned()
        .into()
});

/// Returns the nullable scan row schema.
fn nullable_scan_rows_schema() -> &'static SchemaRef {
    &NULLABLE_SCAN_ROWS_SCHEMA
}

/// Schema for restored add actions with nullable statistics fields.
/// Used when transforming scan data back to add actions with potentially missing statistics.
// Safety: The panic here is acceptable because restored_add_schema() is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_RESTORED_ADD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(restored_add_schema())
        .unwrap_or_else(|| panic!("Failed to transform restored_add_schema"))
        .into_owned()
        .into()
});

/// Returns the nullable restored add action schema.
fn nullable_restored_add_schema() -> &'static SchemaRef {
    &NULLABLE_RESTORED_ADD_SCHEMA
}

/// Schema for add actions that is nullable for use in transforms as as a workaround to avoid issues with null values in required fields
/// that aren't selected.
// Safety: The panic here is acceptable because add_log_schema is a known valid schema.
// If transformation fails, it indicates a programmer error in schema construction that should be caught during development.
#[allow(clippy::panic)]
static NULLABLE_ADD_LOG_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    NullableStatsTransform
        .transform_struct(get_log_add_schema())
        .unwrap_or_else(|| panic!("Failed to transform nullable_restored_add_schema"))
        .into_owned()
        .into()
});

/// Returns the schema for nullable restored add actions with dataChange field.
/// This schema extends the nullable restored add schema with a dataChange boolean field
/// that indicates whether the add action represents a logical data change.
fn nullable_add_log_schema() -> &'static SchemaRef {
    &NULLABLE_ADD_LOG_SCHEMA
}

/// Schema for an array of deletion vector descriptors.
/// Used when appending DV columns to scan file data.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
static STRUCT_DELETION_VECTOR_SCHEMA: LazyLock<ArrayType> =
    LazyLock::new(|| ArrayType::new(DeletionVectorDescriptor::to_schema().into(), true));

/// Returns the schema for an array of deletion vector descriptors.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
fn struct_deletion_vector_schema() -> &'static ArrayType {
    &STRUCT_DELETION_VECTOR_SCHEMA
}

/// Schema for the intermediate column holding new DV descriptors.
/// This temporary column is dropped during transformation to final add actions.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
static NEW_DV_COLUMN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked(vec![StructField::nullable(
        NEW_DELETION_VECTOR_NAME,
        DeletionVectorDescriptor::to_schema(),
    )]))
});

/// Returns the schema for the intermediate column holding new DV descriptors.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
fn new_dv_column_schema() -> &'static SchemaRef {
    &NEW_DV_COLUMN_SCHEMA
}

// These methods are generic over the transaction state `S` because they are called from the
// shared `commit()` path in `mod.rs` (`impl<S> Transaction<S>`). DV updates can only be
// populated on `ExistingTableTransaction`, so the `is_create_table()` guard below is
// defence-in-depth against future misuse.
impl<S> Transaction<S> {
    /// Generate remove/add action pairs for files with DV updates.
    ///
    /// This method processes the cached matched files, generating the necessary Remove and Add actions.
    /// For each file:
    /// 1. A Remove action is generated for the old file
    /// 2. An Add action is generated with the new DV descriptor
    pub(super) fn generate_dv_update_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        // Create-table transactions should not have any DV update actions
        if self.is_create_table() && !self.dv_matched_files.is_empty() {
            return Err(crate::error::Error::internal_error(
                "CREATE TABLE transaction cannot have DV update actions",
            ));
        }

        static COLUMNS_TO_DROP: &[&str] = &[NEW_DELETION_VECTOR_NAME];
        let remove_actions =
            self.generate_remove_actions(engine, self.dv_matched_files.iter(), COLUMNS_TO_DROP)?;
        let add_actions = self.generate_adds_for_dv_update(engine, self.dv_matched_files.iter())?;
        Ok(remove_actions.chain(add_actions))
    }

    /// Generates Add actions for files with updated deletion vectors.
    ///
    /// This transforms scan file metadata with new DV descriptors (appended as a temporary column)
    /// into Add actions for the Delta log.
    fn generate_adds_for_dv_update<'a>(
        &'a self,
        engine: &'a dyn Engine,
        file_metadata_batch: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'a> {
        let evaluation_handler = engine.evaluation_handler();
        // Transform to replace the deletionVector field with the new DV from NEW_DELETION_VECTOR_NAME,
        // then drop the NEW_DELETION_VECTOR_NAME column. The engine data has this temporary column
        // appended by update_deletion_vectors(), but it is not expected by the transforms used in
        // generate_remove_actions() which expect only the scan row schema fields.
        let with_new_dv_transform = Expression::transform(
            Transform::new_top_level()
                .with_replaced_field(
                    "deletionVector",
                    Expression::column([NEW_DELETION_VECTOR_NAME]).into(),
                )
                .with_dropped_field(NEW_DELETION_VECTOR_NAME),
        );
        let with_new_dv_eval = evaluation_handler.new_expression_evaluator(
            intermediate_dv_schema().clone(),
            Arc::new(with_new_dv_transform),
            nullable_scan_rows_schema().clone().into(),
        )?;
        let restored_add_eval = evaluation_handler.new_expression_evaluator(
            nullable_scan_rows_schema().clone(),
            get_scan_metadata_transform_expr(),
            nullable_restored_add_schema().clone().into(),
        )?;
        let with_data_change_transform =
            Arc::new(Expression::struct_from([Expression::transform(
                Transform::new_nested(["add"]).with_inserted_field(
                    Some("modificationTime"),
                    Expression::literal(self.data_change).into(),
                ),
            )]));
        let with_data_change_eval = evaluation_handler.new_expression_evaluator(
            nullable_restored_add_schema().clone(),
            with_data_change_transform,
            nullable_add_log_schema().clone().into(),
        )?;
        Ok(file_metadata_batch.map(
            move |file_metadata_batch| -> DeltaResult<FilteredEngineData> {
                let with_new_dv_data = with_new_dv_eval.evaluate(file_metadata_batch.data())?;

                let as_partial_add_data = restored_add_eval.evaluate(with_new_dv_data.as_ref())?;

                let with_data_change_data =
                    with_data_change_eval.evaluate(as_partial_add_data.as_ref())?;

                FilteredEngineData::try_new(
                    with_data_change_data,
                    file_metadata_batch.selection_vector().to_vec(),
                )
            },
        ))
    }
}

// =============================================================================
// DvMatchVisitor: matches file paths from scan data against new DV descriptors
// =============================================================================

/// Visitor that matches file paths from scan data against new deletion vector descriptors.
/// Used by update_deletion_vectors() to attach new DV descriptors to scan file metadata.
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
struct DvMatchVisitor<'a> {
    /// Map from file path to the new deletion vector descriptor for that file
    dv_updates: &'a HashMap<String, DeletionVectorDescriptor>,
    /// Accumulated DV descriptors (or nulls) for each visited row, in visit order
    new_dv_entries: Vec<Scalar>,
    /// Indexes of rows that matched a file path in dv_update. These must be in
    /// ascending order
    matched_file_indexes: Vec<usize>,
}

impl<'a> DvMatchVisitor<'a> {
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    const PATH_INDEX: usize = 0;

    /// Creates a new DvMatchVisitor that will match file paths against the provided DV updates map.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    fn new(dv_updates: &'a HashMap<String, DeletionVectorDescriptor>) -> Self {
        Self {
            dv_updates,
            new_dv_entries: Vec::new(),
            matched_file_indexes: Vec::new(),
        }
    }
}

/// A `FilteredRowVisitor` that matches file paths against the provided DV updates map.
impl FilteredRowVisitor for DvMatchVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<(Vec<ColumnName>, Vec<DataType>)> = LazyLock::new(|| {
            let names = vec![column_name!("path")];
            let types = vec![DataType::STRING];
            (names, types)
        });
        (&NAMES_AND_TYPES.0, &NAMES_AND_TYPES.1)
    }

    /// For each selected row checks if the path is in the hash-map and if so, extracts DV
    /// details that can be appended back to the EngineData. Also tracks matched row indexes
    /// so the selection vector can be updated to contain only DV-matched files.
    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        let null_dv = || Scalar::Null(DataType::from(DeletionVectorDescriptor::to_schema()));
        for event in rows {
            match event {
                RowEvent::Skipped(n) => {
                    // These rows were deselected — emit nulls to keep new_dv_entries aligned.
                    for _ in 0..n {
                        self.new_dv_entries.push(null_dv());
                    }
                }
                RowEvent::Row(range) => {
                    for row_index in range {
                        let path_opt: Option<String> =
                            getters[Self::PATH_INDEX].get_opt(row_index, "path")?;
                        let Some(path) = path_opt else {
                            // Null path means a non-add action row (remove, metadata, etc.)
                            self.new_dv_entries.push(null_dv());
                            continue;
                        };
                        if let Some(dv_result) = self.dv_updates.get(&path) {
                            self.new_dv_entries.push(Scalar::Struct(StructData::try_new(
                                DeletionVectorDescriptor::to_schema()
                                    .into_fields()
                                    .collect(),
                                vec![
                                    Scalar::from(dv_result.storage_type.to_string()),
                                    Scalar::from(dv_result.path_or_inline_dv.clone()),
                                    Scalar::from(dv_result.offset),
                                    Scalar::from(dv_result.size_in_bytes),
                                    Scalar::from(dv_result.cardinality),
                                ],
                            )?));
                            self.matched_file_indexes.push(row_index);
                        } else {
                            self.new_dv_entries.push(null_dv());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
