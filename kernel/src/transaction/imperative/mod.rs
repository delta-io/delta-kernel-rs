//! Imperative transaction staging, action generation, and commit execution.

use std::iter;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use tracing::instrument;
use update_table::{intermediate_dv_schema, new_dv_column_schema};

use super::stats_verifier::StatsColumnVerifier;
use super::{
    augmented_write_metadata_schema, stats_verifier, with_row_tracking_cols, CommitResult,
    EngineDataResultIterator, Imperative, NonfileCommitActions, PostCommitInputs, Transaction,
    UpdateTable,
};
use crate::actions::{
    as_log_add_schema, CommitInfo, DomainMetadata, Metadata, Protocol, SetTransaction,
    LOG_DOMAIN_METADATA_SCHEMA, LOG_METADATA_SCHEMA, LOG_PROTOCOL_SCHEMA, LOG_REMOVE_SCHEMA,
    LOG_TXN_SCHEMA,
};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{BackReference, CheckpointAction};
use crate::crc::FileStatsDelta;
use crate::engine_data::FilteredEngineData;
use crate::error::KernelError;
use crate::expressions::UnaryExpressionOp::ToJson;
use crate::expressions::{
    col, column_name, lit, null_lit, ArrayData, ColumnName, ExpressionStructPatch,
    ExpressionStructPatchBuilder,
};
use crate::log_replay::HasSelectionVector;
use crate::metrics::events::TRANSACTION_COMMIT_SPAN;
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, FILE_CONSTANT_VALUES_NAME,
    PARTITION_VALUES_NAME, PARTITION_VALUES_PARSED_NAME, SIZE_NAME, STATS_PARSED_NAME, TAGS_NAME,
};
use crate::scan::scan_row_schema;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::schema::ToSchema;
use crate::schema::{schema_ref, ArrayType, SchemaRef, StructField, StructType};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{
    create_row, version_as_i64, DataType, Engine, EngineData, Expression, KernelResult,
    KernelResultIterator, Predicate, RowVisitor, Version,
};

mod update_table;
mod write_validation;

/// The encoded commit actions, plus the state the post-commit CRC and snapshot are built from.
struct CommitBatches<'a> {
    actions: Box<dyn KernelResultIterator<FilteredEngineData> + 'a>,
    dm_changes: Vec<DomainMetadata>,
    /// The `checkpoint` action this commit writes, if any (adaptiveMetadata).
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint_action: Option<CheckpointAction>,
}

// =============================================================================
// Imperative commit APIs
// =============================================================================
impl<STATE> Transaction<STATE, Imperative> {
    /// Consume the transaction and commit it to the table. The result is a result of
    /// [CommitResult] with the following semantics:
    /// - Ok(CommitResult) for either success or a recoverable error (includes the failed
    ///   transaction in case of a conflict so the user can retry, etc.)
    /// - Err(KernelError) indicates a non-retryable error (e.g. logic/validation error).
    ///
    /// `ALTER TABLE` returns a validation error if any data-file batches or manifest writes are
    /// staged. Empty or fully unselected data-file batches are also rejected.
    #[instrument(
        parent = &self.span,
        name = TRANSACTION_COMMIT_SPAN,
        skip_all,
        fields(
            report,
            operation_id = %self.operation_id,
            is_catalog_managed = self.effective_table_config.is_catalog_managed(),
            correlation_id = self.correlation_id.as_deref().unwrap_or(""),
            commit_version = self.get_commit_version(),
            num_add_files,
            num_remove_files,
            num_dv_updates,
            add_files_bytes,
            remove_files_bytes,
            is_blind_append,
            data_change,
            operation,
            prepare_duration_ns,
            committer_duration_ns,
            failure_reason,
        ),
        err
    )]
    pub fn commit(self, engine: &dyn Engine) -> KernelResult<CommitResult<STATE>> {
        let commit_start = Instant::now();

        // === Pre-commit validation ===
        self.validate_operation_compatibility()?;
        self.validate_commit()?;
        self.validate_file_actions()?;

        // === Action generation ===
        let NonfileCommitActions {
            commit_version,
            kernel_commit_info,
            protocol,
            metadata,
            set_transactions,
        } = self.prepare_nonfile_actions(engine)?;
        let CommitBatches {
            actions,
            dm_changes,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action,
        } = self.assemble_commit_batches(
            engine,
            commit_version,
            kernel_commit_info.clone(),
            protocol.as_ref(),
            metadata.as_ref(),
            set_transactions,
        )?;

        // === Commit ===
        let commit_metadata = self.create_commit_metadata(
            commit_version,
            kernel_commit_info.in_commit_timestamp,
            protocol,
            metadata,
            dm_changes.clone(),
        )?;
        let prepare_duration = commit_start.elapsed();
        let (commit_response, committer_duration) =
            self.perform_commit(engine, actions, commit_metadata);

        // === Result translation ===
        let post_commit_inputs = PostCommitInputs {
            commit_info: kernel_commit_info,
            dm_changes,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action,
        };
        self.translate_commit_result(
            commit_response,
            post_commit_inputs,
            prepare_duration,
            committer_duration,
            Self::compute_file_stats,
        )
    }

    /// Set the content of the commitInfo action for this transaction. Note that kernel will
    /// _always_ write a commitInfo, this function simply allows engines to add their own data
    /// into that action if they wish. Kernel overrides the following fields if they are set in
    /// `engine_commit_info`, so connectors should not set them:
    ///
    /// - `timestamp`
    /// - `inCommitTimestamp`
    /// - `operation`
    /// - `operationParameters`
    /// - `operationMetrics`
    /// - `kernelVersion`
    /// - `isBlindAppend`
    /// - `engineInfo`
    /// - `txnId`
    /// - `dataChange` and `lastManifestCommit` (adaptiveMetadata)
    ///
    /// Kernel merges the following field if it is set:
    ///
    /// - `tags`: When a connector tag has the same key as a Kernel-provided tag, Kernel's value
    ///   takes precedence. Otherwise, the connector-provided tag is preserved.
    pub fn with_commit_info(
        mut self,
        engine_commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.engine_commit_info = Some((engine_commit_info, commit_info_schema));
        self
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple batches.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    ///
    /// TODO(#3553): Support writing clusteringProvider to AddFile actions.
    pub fn add_files(&mut self, add_metadata: Box<dyn EngineData>) {
        self.staged_data_changes
            .add_files_metadata
            .push(add_metadata);
    }
}

// =============================================================================
// Imperative action processing
// =============================================================================
impl<STATE> Transaction<STATE, Imperative> {
    // Reject data-file removals / DV updates on appendOnly tables when `data_change` is true.
    pub(super) fn validate_append_only_semantics(&self) -> KernelResult<()> {
        if !self.data_change
            || !self
                .effective_table_config
                .is_feature_enabled(&TableFeature::AppendOnly)
        {
            return Ok(());
        }

        let removes_data = self
            .staged_data_changes
            .remove_files_metadata
            .iter()
            .chain(&self.staged_data_changes.dv_matched_files)
            .any(HasSelectionVector::has_selected_rows);
        require!(
            !removes_data,
            KernelError::invalid_transaction_state(
                "Append-only tables cannot remove files or update deletion vectors when data_change is true",
            )
        );
        Ok(())
    }

    /// Validates the rows and required fields in staged addFile, removeFile, and dv updates.
    fn validate_file_actions(&self) -> KernelResult<()> {
        self.validate_append_only_semantics()?;

        // Validate protocol-required add-file statistics.
        // Note: Stats validation cannot use `StagedDataValidator` because its columns and types
        // are determined at runtime, whereas `RowVisitor::selected_column_names_and_types` must
        // return a static projection. It therefore uses dynamic per-column visitors.
        self.validate_add_files_stats(&self.staged_data_changes.add_files_metadata)?;

        // Validate required fields for addFile.
        write_validation::StagedDataValidator::for_add_files(
            self.effective_table_config.physical_partition_columns(),
        )
        .validate_batches(&self.staged_data_changes.add_files_metadata)?;

        write_validation::StagedDataValidator::for_dv_updates(
            self.effective_table_config.physical_partition_columns(),
        )?
        .validate_selected_rows(&self.staged_data_changes.dv_matched_files)?;

        // Validate required fields for RemoveFile.
        write_validation::StagedDataValidator::for_remove_files()
            .validate_selected_rows(&self.staged_data_changes.remove_files_metadata)?;

        Ok(())
    }

    /// Encodes actions (both non-file and file) into a committable iterator of EngineData,
    /// retaining per-action errors in the commit iterator. Returns the action iterator with the
    /// domain changes and checkpoint action used by the post-commit CRC and snapshot logic.
    fn assemble_commit_batches<'a>(
        &'a self,
        engine: &'a dyn Engine,
        commit_version: Version,
        kernel_commit_info: CommitInfo,
        protocol: Option<&Protocol>,
        metadata: Option<&Metadata>,
        set_transactions: Vec<SetTransaction>,
    ) -> KernelResult<CommitBatches<'a>> {
        let set_transaction_actions = set_transactions
            .into_iter()
            .map(|txn| create_row(engine, LOG_TXN_SCHEMA.clone(), txn));
        let commit_info_action = self.generate_commit_info(engine, kernel_commit_info);
        let protocol_action = protocol
            .map(|protocol| create_row(engine, LOG_PROTOCOL_SCHEMA.clone(), protocol.clone()))
            .transpose()?;
        let metadata_action = metadata
            .map(|metadata| create_row(engine, LOG_METADATA_SCHEMA.clone(), metadata.clone()))
            .transpose()?;

        let (add_actions, row_tracking_domain_metadata) =
            self.generate_adds(engine, commit_version)?;

        let dm_changes =
            self.generate_domain_metadata_actions(engine, row_tracking_domain_metadata)?;
        let domain_metadata_actions: Vec<_> = dm_changes
            .iter()
            .cloned()
            .map(|dm| create_row(engine, LOG_DOMAIN_METADATA_SCHEMA.clone(), dm))
            .collect();

        let dv_update_actions = self.generate_dv_update_actions(engine)?;

        let remove_actions = self.generate_remove_actions(
            engine,
            self.staged_data_changes.remove_files_metadata.iter(),
            false, /* has_dv_update_columns */
        )?;

        #[cfg(feature = "adaptive-metadata-in-dev")]
        let checkpoint_action =
            self.generate_checkpoint_action(engine, commit_version, &dm_changes)?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let checkpoint_action_data = checkpoint_action
            .clone()
            .map(|action| action.into_engine_data(engine))
            .transpose()?;
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let checkpoint_action_data: Option<Box<dyn EngineData>> = None;

        // CommitInfo must precede all other actions.
        let actions = iter::once(commit_info_action)
            .chain(protocol_action.map(Ok))
            .chain(metadata_action.map(Ok))
            .chain(checkpoint_action_data.map(Ok))
            .chain(add_actions)
            .chain(set_transaction_actions)
            .chain(domain_metadata_actions);

        let filtered_actions = actions
            .map(|action_result| action_result.map(FilteredEngineData::with_all_rows_selected))
            .chain(remove_actions)
            .chain(dv_update_actions);

        Ok(CommitBatches {
            actions: Box::new(filtered_actions),
            dm_changes,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action,
        })
    }

    /// Computes file statistics for a successful imperative commit.
    fn compute_file_stats(&self) -> KernelResult<(FileStatsDelta, u64)> {
        let bin_boundaries = self
            .read_snapshot_opt
            .as_ref()
            .and_then(|snap| snap.get_file_stats_if_present())
            .and_then(|s| s.file_size_histogram)
            .map(|h| h.sorted_bin_boundaries);
        let file_stats = FileStatsDelta::try_compute_for_txn(
            &self.staged_data_changes.add_files_metadata,
            &self.staged_data_changes.remove_files_metadata,
            bin_boundaries.as_deref(),
        )?;
        Ok((file_stats, self.staged_data_changes.num_dv_updates as u64))
    }

    /// Validate that add files carry the per-file statistics required by the table's protocol.
    ///
    /// Currently checks two protocol requirements:
    /// - `stats.numRecords` must be present when [`requires_stats_num_records`] returns true.
    /// - Per-file min/max/nullCount must be present for clustering columns when the
    ///   `ClusteredTable` feature is enabled.
    ///
    /// Other stat columns (e.g. the conventional "first 32 columns") are not validated here
    /// because they are not protocol-required.
    ///
    /// Only add files are validated(remove files do not carry statistics).
    ///
    /// [`requires_stats_num_records`]: crate::table_configuration::TableConfiguration::requires_stats_num_records
    pub(super) fn validate_add_files_stats(
        &self,
        add_files: &[Box<dyn EngineData>],
    ) -> KernelResult<()> {
        if add_files.is_empty() {
            return Ok(());
        }
        if self.effective_table_config.requires_stats_num_records() {
            // TODO: Likely it's better to merge this with the clustering column validation below,
            // benchmark it and see if it's faster. If so, refactor this to do both validations in
            // one pass.
            stats_verifier::verify_num_records_present(add_files)?;
        }
        if let Some(ref clustering_cols) = self.physical_clustering_columns {
            if !clustering_cols.is_empty() {
                let physical_schema = self.effective_table_config.physical_schema();
                let columns_with_types: Vec<(ColumnName, DataType)> = clustering_cols
                    .iter()
                    .map(|col| {
                        let data_type = physical_schema
                            .fields_of_path(col)?
                            .last()
                            .map(|field| field.data_type().clone())
                            .ok_or_else(|| {
                                KernelError::internal_error(format!(
                                    "Required column '{col}' not found in table schema"
                                ))
                            })?;
                        Ok((col.clone(), data_type))
                    })
                    .collect::<KernelResult<_>>()?;
                let verifier = StatsColumnVerifier::new(columns_with_types);
                verifier.verify(add_files)?;
            }
        }
        Ok(())
    }

    /// Generates add actions and row tracking domain metadata for a commit.
    #[instrument(name = "txn.gen_adds", skip_all, err)]
    pub(super) fn generate_adds<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: u64,
    ) -> KernelResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
        // Note: this does not require delta.enableRowTracking=true. "supported" is sufficient
        // for writers to assign row IDs.
        let assign_fresh_row_tracking_metadata = self
            .effective_table_config
            .should_assign_fresh_row_tracking_metadata();

        if self.staged_data_changes.add_files_metadata.is_empty() {
            // No files to add. For an empty CREATE TABLE with row tracking, emit the initial
            // high water mark domain metadata (rowIdHighWaterMark = -1) so subsequent writes
            // have a valid starting point. For all other empty commits (metadata-only, etc.),
            // nothing row-tracking-related needs to be written.
            let row_tracking_dm = (assign_fresh_row_tracking_metadata && self.is_create_table())
                .then(RowTrackingDomainMetadata::initial);
            return Ok((Box::new(iter::empty()), row_tracking_dm));
        }

        let commit_version = version_as_i64(commit_version)?;

        if assign_fresh_row_tracking_metadata {
            self.generate_adds_with_row_tracking(engine, commit_version)
        } else {
            let add_actions = build_add_actions(
                engine,
                self.staged_data_changes
                    .add_files_metadata
                    .iter()
                    .map(|a| Ok(a.deref())),
                self.add_files_schema().clone(),
                self.data_change,
            )?;
            Ok((Box::new(add_actions), None))
        }
    }

    /// Generates add actions with row tracking columns and the row ID high water mark
    /// domain metadata.
    ///
    /// Reads `numRecords` in one pass to compute the final high water mark, then assigns unique
    /// non-overlapping `baseRowId` ranges in a second pass. The initial high water mark is read
    /// from the snapshot for existing tables, or defaults to -1 for create-table (no prior log
    /// to read from).
    fn generate_adds_with_row_tracking<'a>(
        &'a self,
        engine: &dyn Engine,
        commit_version: i64,
    ) -> KernelResult<(
        EngineDataResultIterator<'a>,
        Option<RowTrackingDomainMetadata>,
    )> {
        let row_id_high_water_mark = if self.is_create_table() {
            None
        } else {
            self.read_snapshot()?
                .get_row_tracking_high_water_mark(engine)?
        };

        // Assign base row IDs starting from the current high water mark.
        let mut row_tracking_visitor = RowTrackingVisitor::new(row_id_high_water_mark);
        let extended_add_files =
            self.staged_data_changes
                .add_files_metadata
                .iter()
                .map(move |add_files_batch| {
                    row_tracking_visitor.visit_rows_of(add_files_batch.deref())?;
                    let base_row_ids = row_tracking_visitor.take_base_row_ids();
                    let commit_versions = vec![commit_version; base_row_ids.len()];
                    let base_row_ids_array =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), base_row_ids)?;
                    let commit_versions_array =
                        ArrayData::try_new(ArrayType::new(DataType::LONG, true), commit_versions)?;

                    let row_tracking_schema = with_row_tracking_cols(&schema_ref! {})?;
                    add_files_batch.append_columns(
                        row_tracking_schema,
                        vec![base_row_ids_array, commit_versions_array],
                    )
                });

        // Generate add actions including row tracking metadata
        let add_actions = build_add_actions(
            engine,
            extended_add_files,
            augmented_write_metadata_schema()?,
            self.data_change,
        )?;

        // Do a dry run to eagerly compute the new high water mark so we can store it. The row
        // tracking calculation above happens only lazily as our caller drives the iterator.
        let mut dry_run = RowTrackingVisitor::dry_run(row_id_high_water_mark);
        for add_files_batch in &self.staged_data_changes.add_files_metadata {
            dry_run.visit_rows_of(add_files_batch.deref())?;
        }
        let row_tracking_domain_metadata =
            RowTrackingDomainMetadata::new(dry_run.row_id_high_water_mark);

        Ok((Box::new(add_actions), Some(row_tracking_domain_metadata)))
    }

    /// Generates Remove actions from scan file metadata.
    ///
    /// This internal method transforms scan row metadata into Remove actions for the Delta log.
    /// It's called during commit to process files staged via [`remove_files`] or files being
    /// updated with new deletion vectors via [`update_deletion_vectors`].
    ///
    /// # Parameters
    ///
    /// - `engine`: The engine used for expression evaluation
    /// - `remove_files_metadata`: Iterator over scan file metadata to transform into Remove actions
    /// - `has_dv_update_columns`: Whether `remove_files_metadata` contains the temporary columns
    ///   added for a deletion vector update
    ///
    /// # Returns
    ///
    /// An iterator of FilteredEngineData containing Remove actions in the log schema format.
    ///
    /// [`remove_files`]: Transaction::remove_files
    /// [`update_deletion_vectors`]: Transaction::update_deletion_vectors
    #[instrument(name = "txn.gen_removes", skip_all, err)]
    fn generate_remove_actions<'a>(
        &'a self,
        engine: &dyn Engine,
        remove_files_metadata: impl Iterator<Item = &'a FilteredEngineData> + Send + 'a,
        has_dv_update_columns: bool,
    ) -> KernelResult<impl KernelResultIterator<FilteredEngineData> + 'a> {
        // Create-table transactions should not have any remove actions.
        // Only error if there are actually files queued for removal.
        if self.is_create_table() && !self.staged_data_changes.remove_files_metadata.is_empty() {
            return Err(KernelError::internal_error(
                "CREATE TABLE transaction cannot have remove actions",
            ));
        }

        let target_schema = schema_with_all_fields_nullable(&LOG_REMOVE_SCHEMA);
        let evaluation_handler = engine.evaluation_handler();
        // TODO(#3263): `remove_files_metadata` may contain `stats_parsed` and
        // `partitionValues_parsed`; provide its full schema to both evaluators.
        let input_schema = if has_dv_update_columns {
            intermediate_dv_schema().clone()
        } else {
            scan_row_schema()
        };
        let columns_to_drop: Vec<_> = has_dv_update_columns
            .then(new_dv_column_schema)
            .into_iter()
            .flat_map(|schema| schema.fields().map(|field| field.name().to_owned()))
            .collect();

        // adaptiveMetadata removes must carry a null deletionTimestamp and extendedFileMetadata =
        // true (see `Remove` and `build_remove_struct_patch`).
        let adaptive_metadata_enabled = self.is_adaptive_metadata_enabled();

        let make_eval = |coalesce_stats_with_parsed: bool| {
            let columns_to_drop: Vec<_> = columns_to_drop.iter().map(String::as_str).collect();
            let patch = build_remove_struct_patch(
                self.commit_timestamp,
                self.data_change,
                &columns_to_drop,
                coalesce_stats_with_parsed,
                adaptive_metadata_enabled,
            )?;
            let expr = Arc::new(Expression::struct_from([Expression::struct_patch(patch)?]));
            evaluation_handler.new_expression_evaluator(
                input_schema.clone(),
                expr,
                target_schema.clone().into(),
            )
        };

        // Build two evaluators: one for the common case where scan files do not include a
        // stats_parsed column, and one for predicate-based scans that include stats_parsed.
        // The stats_parsed evaluator coalesces stats with ToJson(stats_parsed) to handle the
        // case where stats is null (e.g., on V2 checkpoints with writeStatsAsJson=false) and
        // then drops the stats_parsed column.
        let base_eval = make_eval(false /* coalesce_stats_with_parsed */)?;
        let stats_parsed_eval = make_eval(true /* coalesce_stats_with_parsed */)?;
        let stats_parsed_col = column_name!(STATS_PARSED_NAME);

        Ok(remove_files_metadata.map(move |file_metadata_batch| {
            let data = file_metadata_batch.data();
            let evaluator = if data.has_field(&stats_parsed_col) {
                &stats_parsed_eval
            } else {
                &base_eval
            };
            let updated_engine_data = evaluator.evaluate(data)?;
            FilteredEngineData::try_new(
                updated_engine_data,
                file_metadata_batch.selection_vector().to_vec(),
            )
        }))
    }
}

static DATA_CHANGE_COLUMN: LazyLock<StructField> =
    LazyLock::new(|| StructField::not_null("dataChange", DataType::BOOLEAN));

/// Builds the projection for converting add file metadata into commit-ready Add actions.
pub(super) fn build_add_action_projection(
    input_schema: &StructType,
    data_change: bool,
) -> KernelResult<(SchemaRef, Expression)> {
    let (output_schema, patch) = ProjectionStructPatchBuilder::new(input_schema)
        .insert_after(
            "modificationTime",
            DATA_CHANGE_COLUMN.clone(),
            lit(data_change),
        )
        .replace(
            "stats",
            StructField::nullable("stats", DataType::STRING),
            Expression::unary(ToJson, col!("stats")),
        )
        .build()?;
    let patch = Expression::struct_from([patch]);
    Ok((output_schema, patch))
}

/// Transforms add file metadata into commit-ready add actions by converting stats to JSON and
/// setting the `dataChange` field.
fn build_add_actions<'a, I, T>(
    engine: &dyn Engine,
    add_files_metadata: I,
    input_schema: SchemaRef,
    data_change: bool,
) -> KernelResult<impl KernelResultIterator<Box<dyn EngineData>> + 'a>
where
    I: KernelResultIterator<T> + 'a,
    T: Deref<Target = dyn EngineData> + Send + 'a,
{
    let evaluation_handler = engine.evaluation_handler();
    let (output_schema, adds_expr) = build_add_action_projection(&input_schema, data_change)?;
    let adds_expr = Arc::new(adds_expr);
    Ok(add_files_metadata.map(move |add_files_batch| {
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            input_schema.clone(),
            adds_expr.clone(),
            as_log_add_schema(output_schema.clone()).into(),
        )?;
        adds_evaluator.evaluate(add_files_batch?.deref())
    }))
}

/// Builds the struct patch for converting scan row metadata into a Remove action.
///
/// Handles two "parsed" columns that predicate-based scans add to scan metadata:
///
/// - `stats_parsed`: when `coalesce_stats_with_parsed` is true, the `stats` field is replaced with
///   `COALESCE(stats, TO_JSON(stats_parsed))` and `stats_parsed` is dropped. The coalesce handles
///   cases where `stats` is null (e.g., V2 checkpoints with `writeStatsAsJson=false`) by
///   reconstructing the JSON from the parsed representation.
/// - `partitionValues_parsed`: dropped if present. Unlike stats, no reconstruction is needed: the
///   Remove action's `partitionValues` is sourced from `fileConstantValues.partitionValues`, which
///   scans always populate from `add.partitionValues`.
///
/// When `adaptive_metadata_enabled` is set, the RFC requires `deletionTimestamp` to be null
/// (cleanup uses tree reachability, not timestamp expiry), so it is emitted as a fixed literal
/// rather than derived from the input.
pub(super) fn build_remove_struct_patch(
    commit_timestamp: i64,
    data_change: bool,
    columns_to_drop: &[&str],
    coalesce_stats_with_parsed: bool,
    adaptive_metadata_enabled: bool,
) -> KernelResult<ExpressionStructPatch> {
    let deletion_timestamp = if adaptive_metadata_enabled {
        null_lit(DataType::LONG)
    } else {
        lit(commit_timestamp)
    };
    // Note: The Delta protocol requires `partitionValues`, `size`, and `tags` when
    // `extendedFileMetadata` is true. We require only `partitionValues` and `size` to match Spark.
    // Under adaptiveMetadata both are guaranteed present
    let extended_file_metadata = Predicate::and_from([
        col!(SIZE_NAME).is_not_null(),
        col!(FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME).is_not_null(),
    ]);
    let mut patch = ExpressionStructPatchBuilder::new()
        // deletionTimestamp
        .insert_after("path", deletion_timestamp)
        // dataChange
        .insert_after("path", lit(data_change))
        // extended_file_metadata
        .insert_after("path", Expression::from(extended_file_metadata))
        .insert_after(
            "path",
            col!(FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME),
        );

    if coalesce_stats_with_parsed {
        // Replace stats with COALESCE(stats, TO_JSON(stats_parsed)) and drop stats_parsed.
        let coalesce_stats = Expression::coalesce([
            col!("stats"),
            Expression::unary(ToJson, col!(STATS_PARSED_NAME)),
        ]);
        patch = patch
            .replace("stats", coalesce_stats)
            .drop(STATS_PARSED_NAME);
    }

    patch = patch
        .insert_after("stats", col!(FILE_CONSTANT_VALUES_NAME, TAGS_NAME))
        .insert_after(
            "deletionVector",
            col!(FILE_CONSTANT_VALUES_NAME, BASE_ROW_ID_NAME),
        )
        .insert_after(
            "deletionVector",
            col!(FILE_CONSTANT_VALUES_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME),
        )
        .drop(FILE_CONSTANT_VALUES_NAME)
        .drop("modificationTime")
        // Added to scan output when the predicate touches a partition column.
        .drop_if_exists(PARTITION_VALUES_PARSED_NAME);

    // Kernel does not populate adaptive-metadata-tree back references on writes, so emit a null to
    // keep the produced struct aligned with the `backReference` field of LOG_REMOVE_SCHEMA.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        patch = patch.append(null_lit(BackReference::to_schema()));
    }

    for column_to_drop in columns_to_drop {
        patch = patch.drop(*column_to_drop);
    }

    patch.build()
}
