//! Imperative transaction staging, action generation, and commit execution.

use std::iter;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use tracing::instrument;
use update_table::{intermediate_dv_schema, new_dv_column_schema};

use super::stats_verifier::StatsColumnVerifier;
use super::{
    add_files_schema_with, stats_verifier, with_row_tracking_cols, CommitResult,
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
use crate::committer::{CommitMetadata, CommitResponse};
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
use crate::metrics::CommitFailureReason;
use crate::row_tracking::{RowTrackingDomainMetadata, RowTrackingVisitor};
use crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable;
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, CLUSTERING_PROVIDER_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME,
    FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME, PARTITION_VALUES_PARSED_NAME, SIZE_NAME,
    STATS_PARSED_NAME, TAGS_NAME,
};
use crate::scan::scan_row_schema;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::schema::ToSchema;
use crate::schema::{schema_ref, ArrayType, SchemaRef, StructField, StructType};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{
    create_row, version_as_i64, DataType, Engine, EngineData, Expression, ExpressionEvaluator,
    KernelResult, KernelResultIterator, Predicate, RowVisitor, Version,
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
    /// The base schema and supported optional columns for `add_metadata` are described by
    /// [`Transaction::add_files_schema`].
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
    /// Submits actions and returns the committer's response with its elapsed time.
    fn perform_commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        commit_metadata: CommitMetadata,
    ) -> (KernelResult<CommitResponse>, Duration) {
        let committer_start = Instant::now();
        let commit_response = self.committer.commit(engine, actions, commit_metadata);
        (commit_response, committer_start.elapsed())
    }

    /// Translates a commit response, retaining the transaction on recoverable failures.
    /// File statistics are computed only when the committer reports success.
    fn translate_commit_result(
        self,
        commit_response: KernelResult<CommitResponse>,
        post_commit_inputs: PostCommitInputs,
        prepare_duration: Duration,
        committer_duration: Duration,
    ) -> KernelResult<CommitResult<STATE>> {
        match commit_response {
            Ok(CommitResponse::Committed { file_meta }) => {
                let PostCommitInputs {
                    commit_info,
                    dm_changes,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_action,
                } = post_commit_inputs;
                // TODO(#2717): the commit already succeeded atomically; the post-commit `?`
                //              below must not fail the txn (and must not mislabel the metric).
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
                self.record_commit_success_metrics(
                    &file_stats,
                    self.staged_data_changes.num_dv_updates as u64,
                    prepare_duration,
                    committer_duration,
                );
                let crc_delta = self.build_crc_delta(file_stats, commit_info, dm_changes)?;
                Ok(CommitResult::Committed(self.into_committed(
                    file_meta,
                    crc_delta,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_action,
                )?))
            }
            Ok(CommitResponse::Conflict { version }) => {
                tracing::Span::current()
                    .record("failure_reason", CommitFailureReason::Conflict.as_ref());
                Ok(CommitResult::Conflicted(self.into_conflicted(version)))
            }
            // TODO: we may want to be more or less selective about what is retryable (this is tied
            // to the idea of "what kind of Errors should write_json_file return?")
            Err(e @ KernelError::IOError(_)) => {
                tracing::Span::current()
                    .record("failure_reason", CommitFailureReason::RetryableIo.as_ref());
                Ok(CommitResult::Retryable(self.into_retryable(e)))
            }
            Err(e) => Err(e),
        }
    }

    // Reject data-file removals / DV updates on appendOnly tables when `data_change` is true.
    fn validate_append_only_semantics(&self) -> KernelResult<()> {
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

        if !self
            .effective_table_config
            .is_feature_supported(&TableFeature::ClusteredTable)
        {
            write_validation::validate_no_clustering_providers_set(
                &self.staged_data_changes.add_files_metadata,
            )?;
        }

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
    fn validate_add_files_stats(&self, add_files: &[Box<dyn EngineData>]) -> KernelResult<()> {
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
    fn generate_adds<'a>(
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
                false, // no row tracking
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

        let add_actions = build_add_actions(
            engine,
            extended_add_files,
            true, // row tracking
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
fn build_add_action_projection(
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
    row_tracking: bool,
    data_change: bool,
) -> KernelResult<impl KernelResultIterator<Box<dyn EngineData>> + 'a>
where
    I: KernelResultIterator<T> + 'a,
    T: Deref<Target = dyn EngineData> + Send + 'a,
{
    let evaluation_handler = engine.evaluation_handler();
    let mut last_evaluator: Option<((bool, bool), Arc<dyn ExpressionEvaluator>)> = None;
    Ok(add_files_metadata.map(move |add_files_batch| {
        let add_files_batch = add_files_batch?;
        let optional_columns = (
            add_files_batch.has_field(&column_name!(TAGS_NAME)),
            add_files_batch.has_field(&column_name!(CLUSTERING_PROVIDER_NAME)),
        );
        if let Some((cached_columns, evaluator)) = &last_evaluator {
            if *cached_columns == optional_columns {
                return evaluator.evaluate(add_files_batch.deref());
            }
        }
        let input_schema = add_files_schema_with(optional_columns.0, optional_columns.1);
        let input_schema = if row_tracking {
            with_row_tracking_cols(input_schema)?
        } else {
            input_schema.clone()
        };
        let (output_schema, adds_expr) = build_add_action_projection(&input_schema, data_change)?;
        let adds_evaluator = evaluation_handler.new_expression_evaluator(
            input_schema,
            Arc::new(adds_expr),
            as_log_add_schema(output_schema).into(),
        )?;
        let result = adds_evaluator.evaluate(add_files_batch.deref());
        last_evaluator = Some((optional_columns, adds_evaluator));
        result
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
fn build_remove_struct_patch(
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use ::test_utils::get_column;
    use rstest::rstest;

    use super::*;
    use crate::actions::{ADD_SCHEMA, MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS};
    use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
    use crate::arrow::array::{
        new_null_array, ArrayRef, BooleanArray, Int64Array, StringArray, StructArray,
    };
    use crate::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_conversion::TryIntoArrow;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::expressions::{ExpressionRef, MapData, PredicateRef, Scalar, StructData};
    use crate::scan::log_replay::PATH_NAME;
    use crate::schema::{column_name, schema, MapType};
    use crate::table_configuration::TableConfiguration;
    use crate::table_properties::APPEND_ONLY;
    use crate::transaction::create_table::create_table;
    use crate::transaction::operation::UpdateTableOperation;
    use crate::unit_test_utils::{copy_test_table, create_valid_add_file_batch, test_schema_flat};
    use crate::{EvaluationHandler, ExpressionEvaluator, PredicateEvaluator, Snapshot};

    impl Transaction {
        /// Set clustering columns for testing purposes without needing a table
        /// with the ClusteredTable feature enabled.
        fn with_clustering_columns_for_test(mut self, columns: Vec<ColumnName>) -> Self {
            self.physical_clustering_columns = Some(columns);
            self
        }
    }

    #[rstest]
    fn test_add_action_projection_schema(
        #[values(false, true)] row_tracking: bool,
        #[values(false, true)] tags: bool,
        #[values(false, true)] clustering_provider: bool,
    ) -> KernelResult<()> {
        let add_files_schema = add_files_schema_with(tags, clustering_provider);
        let input_schema = if row_tracking {
            with_row_tracking_cols(add_files_schema)?
        } else {
            add_files_schema.clone()
        };
        let (schema, _) = build_add_action_projection(input_schema.as_ref(), true)?;
        let field_names: Vec<_> = schema.fields().map(|f| f.name().as_str()).collect();
        let mut expected_field_names = vec![
            "path",
            "partitionValues",
            "size",
            "modificationTime",
            "dataChange",
            "stats",
        ];
        if tags {
            expected_field_names.push("tags");
        }
        if clustering_provider {
            expected_field_names.push("clusteringProvider");
        }
        if row_tracking {
            expected_field_names.extend(["baseRowId", "defaultRowCommitVersion"]);
        }
        assert_eq!(field_names, expected_field_names);
        assert_eq!(schema.field("dataChange"), Some(&*DATA_CHANGE_COLUMN));
        assert_eq!(
            schema.field("stats").unwrap().data_type(),
            &DataType::STRING
        );
        for name in [TAGS_NAME, CLUSTERING_PROVIDER_NAME] {
            assert_eq!(schema.field(name), input_schema.field(name));
            if let Some(field) = schema.field(name) {
                assert_eq!(Some(field), ADD_SCHEMA.field(name));
            }
        }
        Ok(())
    }

    #[derive(Default)]
    struct CountingEvaluationHandler {
        evaluators_created: AtomicUsize,
    }

    impl EvaluationHandler for CountingEvaluationHandler {
        fn new_expression_evaluator(
            &self,
            schema: SchemaRef,
            expression: ExpressionRef,
            output_type: DataType,
        ) -> KernelResult<Arc<dyn ExpressionEvaluator>> {
            self.evaluators_created.fetch_add(1, Ordering::Relaxed);
            ArrowEvaluationHandler.new_expression_evaluator(schema, expression, output_type)
        }

        fn new_predicate_evaluator(
            &self,
            schema: SchemaRef,
            predicate: PredicateRef,
        ) -> KernelResult<Arc<dyn PredicateEvaluator>> {
            ArrowEvaluationHandler.new_predicate_evaluator(schema, predicate)
        }

        fn create_many(
            &self,
            schema: SchemaRef,
            rows: Vec<Vec<Scalar>>,
        ) -> KernelResult<Box<dyn EngineData>> {
            ArrowEvaluationHandler.create_many(schema, rows)
        }
    }

    #[rstest]
    #[case::base(&[(false, false), (false, false)], 1)]
    #[case::tags(&[(true, false), (true, false)], 1)]
    #[case::provider(&[(false, true), (false, true)], 1)]
    #[case::both(&[(true, true), (true, true)], 1)]
    #[case::changing_columns(
        &[(false, false), (false, false), (true, true), (true, true), (false, false), (false, false)],
        3
    )]
    fn test_add_action_evaluators_reused_for_repeated_schema(
        #[values(false, true)] row_tracking: bool,
        #[case] optional_columns: &[(bool, bool)],
        #[case] max_evaluators: usize,
    ) -> KernelResult<()> {
        let handler = Arc::new(CountingEvaluationHandler::default());
        let engine = DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_evaluation_handler(handler.clone());
        let batches = optional_columns
            .iter()
            .map(|&(tags, provider)| {
                let schema = add_files_schema_with(tags, provider);
                let schema = if row_tracking {
                    with_row_tracking_cols(schema)?
                } else {
                    schema.clone()
                };
                let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
                let mut columns = create_valid_add_file_batch(false).columns().to_vec();
                columns.extend(
                    arrow_schema.fields()[columns.len()..]
                        .iter()
                        .map(|field| new_null_array(field.data_type(), 1)),
                );
                let batch = RecordBatch::try_new(Arc::new(arrow_schema), columns)?;
                Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let actions = build_add_actions(&engine, batches.into_iter().map(Ok), row_tracking, true)?;
        assert_eq!(handler.evaluators_created.load(Ordering::Relaxed), 0);
        let actions = actions.collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(actions.len(), optional_columns.len());
        for (action, &(tags, provider)) in actions.iter().zip(optional_columns) {
            assert_eq!(action.has_field(&column_name!("add", TAGS_NAME)), tags);
            assert_eq!(
                action.has_field(&column_name!("add", CLUSTERING_PROVIDER_NAME)),
                provider
            );
            assert_eq!(
                action.has_field(&column_name!("add.baseRowId")),
                row_tracking
            );
        }
        let created = handler.evaluators_created.load(Ordering::Relaxed);
        assert!(
            created <= max_evaluators,
            "created {created} evaluators for {} batches; expected at most {max_evaluators}",
            optional_columns.len()
        );
        Ok(())
    }

    /// Verifies the Remove projection's `deletionTimestamp` and `extendedFileMetadata` fields.
    /// `extendedFileMetadata` is always a presence predicate; `deletionTimestamp` derives from the
    /// input outside adaptiveMetadata and is fixed to null under it per the RFC.
    #[rstest]
    #[case::standard(false)]
    #[case::adaptive_metadata(true)]
    fn test_remove_action_projection_deletion_timestamp_and_extended_metadata(
        #[case] adaptive_metadata_enabled: bool,
    ) -> KernelResult<()> {
        let commit_timestamp = 123;
        let patch = build_remove_struct_patch(
            commit_timestamp,
            true,  /* data_change */
            &[],   /* columns_to_drop */
            false, /* coalesce_stats_with_parsed */
            adaptive_metadata_enabled,
        )?;
        let path_patch = patch
            .field_patches
            .get("path")
            .expect("path should have inserted fields");
        // Insertions preserve `insert_after` call order: deletionTimestamp, dataChange,
        // extendedFileMetadata, partitionValues.
        let deletion_timestamp = path_patch
            .insertions
            .first()
            .expect("deletionTimestamp should be the first inserted field");
        let extended_file_metadata = path_patch
            .insertions
            .get(2)
            .expect("extendedFileMetadata should follow deletionTimestamp and dataChange");

        let expected_deletion_timestamp = if adaptive_metadata_enabled {
            null_lit(DataType::LONG)
        } else {
            lit(commit_timestamp)
        };
        let expected_extended_file_metadata = Expression::from_pred(Predicate::and_from([
            col!(SIZE_NAME).is_not_null(),
            col!(FILE_CONSTANT_VALUES_NAME, PARTITION_VALUES_NAME).is_not_null(),
        ]));
        assert_eq!(deletion_timestamp.as_ref(), &expected_deletion_timestamp);
        assert_eq!(
            extended_file_metadata.as_ref(),
            &expected_extended_file_metadata
        );
        Ok(())
    }

    fn existing_table_txn(
        data_change: bool,
    ) -> KernelResult<(SyncEngine, Transaction, tempfile::TempDir)> {
        let (url, tempdir) = copy_test_table("table-without-dv-small")?;
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine)?;
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", data_change)
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        Ok((engine, txn, tempdir))
    }

    fn non_dv_table() -> (SyncEngine, Arc<Snapshot>) {
        let engine = SyncEngine::new();
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        (engine, snapshot)
    }

    #[derive(Clone, Copy, Debug)]
    enum DataRemoval {
        RemoveFile,
        DeletionVectorUpdate,
    }

    fn set_append_only(txn: &mut Transaction, enabled: bool) -> KernelResult<()> {
        let metadata = txn
            .effective_table_config
            .metadata()
            .clone()
            .with_configuration_entry(APPEND_ONLY, enabled.to_string());
        let table_config = TableConfiguration::try_new_from(
            &txn.effective_table_config,
            Some(metadata),
            None,
            txn.effective_table_config.version(),
        )?;
        txn.replace_effective_table_config(table_config);
        Ok(())
    }

    fn make_scan_files(selection_vector: &[bool]) -> FilteredEngineData {
        let schema: ArrowSchema = scan_row_schema().as_ref().try_into_arrow().unwrap();
        let row_count = selection_vector.len();
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == PATH_NAME {
                    Arc::new(StringArray::from_iter_values(
                        (0..row_count).map(|index| format!("file-{index}.parquet")),
                    )) as ArrayRef
                } else if field.name() == SIZE_NAME {
                    Arc::new(Int64Array::from(vec![1; row_count]))
                } else if field.name() == FILE_CONSTANT_VALUES_NAME {
                    let ArrowDataType::Struct(fields) = field.data_type() else {
                        panic!("fileConstantValues should be a struct");
                    };
                    let child_arrays = fields
                        .iter()
                        .map(|field| {
                            if field.name() == PARTITION_VALUES_NAME {
                                let names = MapFieldNames {
                                    entry: "key_value".to_string(),
                                    key: "key".to_string(),
                                    value: "value".to_string(),
                                };
                                let mut builder = MapBuilder::new(
                                    Some(names),
                                    StringBuilder::new(),
                                    StringBuilder::new(),
                                );
                                for _ in 0..row_count {
                                    builder.append(true).unwrap();
                                }
                                Arc::new(builder.finish()) as ArrayRef
                            } else {
                                new_null_array(field.data_type(), row_count)
                            }
                        })
                        .collect();
                    Arc::new(StructArray::new(fields.clone(), child_arrays, None))
                } else {
                    new_null_array(field.data_type(), row_count)
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        FilteredEngineData::try_new(
            Box::new(ArrowEngineData::new(batch)),
            selection_vector.to_vec(),
        )
        .unwrap()
    }

    fn stage_data_removal(txn: &mut Transaction, removal: DataRemoval, selection_vector: &[bool]) {
        let data = make_scan_files(selection_vector);
        match removal {
            DataRemoval::RemoveFile => txn.remove_files(data),
            DataRemoval::DeletionVectorUpdate => {
                txn.staged_data_changes.dv_matched_files.push(data)
            }
        }
    }

    #[rstest]
    fn create_table_add_actions_preserve_data_change(
        #[values(false, true)] data_change: bool,
    ) -> KernelResult<()> {
        let tempdir = tempfile::tempdir()?;
        let engine = SyncEngine::new();
        let mut transaction = create_table(
            tempdir.path().to_string_lossy(),
            test_schema_flat(),
            "test-engine",
            data_change,
        )
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
        transaction.add_files(Box::new(ArrowEngineData::new(create_valid_add_file_batch(
            false, /* all_nullable */
        ))));

        let (mut adds, _) = transaction.generate_adds(&engine, 0)?;
        let add = ArrowEngineData::try_from_engine_data(adds.next().expect("Add action")?)?;
        let add = get_column!(add.record_batch(), "add", StructArray);
        assert_eq!(
            get_column!(add, "dataChange", BooleanArray).value(0),
            data_change
        );
        assert!(adds.next().is_none());
        Ok(())
    }

    #[rstest]
    #[case::append_only_disabled(
        false, /* append_only */
        true, /* data_change */
        &[true, true, true], /* selection_vector */
        false, /* expected_error */
    )]
    #[case::no_data_change(
        true, /* append_only */
        false, /* data_change */
        &[true, true, true], /* selection_vector */
        false, /* expected_error */
    )]
    #[case::all_unselected(
        true, /* append_only */
        true, /* data_change */
        &[false, false, false], /* selection_vector */
        false, /* expected_error */
    )]
    #[case::partial_selected(
        true, /* append_only */
        true, /* data_change */
        &[false, true, false], /* selection_vector */
        true, /* expected_error */
    )]
    #[case::all_selected(
        true, /* append_only */
        true, /* data_change */
        &[true, true, true], /* selection_vector */
        true, /* expected_error */
    )]
    fn append_only_rejects_data_removal_when_data_change(
        #[values(DataRemoval::RemoveFile, DataRemoval::DeletionVectorUpdate)] removal: DataRemoval,
        #[values(0, 1)] batch_index: usize,
        #[case] append_only: bool,
        #[case] data_change: bool,
        #[case] selection_vector: &[bool],
        #[case] expected_error: bool,
    ) -> KernelResult<()> {
        let (_engine, mut txn, _tempdir) = existing_table_txn(data_change)?;
        set_append_only(&mut txn, append_only)?;
        for index in 0..2 {
            let selection_vector = if index == batch_index {
                selection_vector
            } else {
                &[false, false, false]
            };
            stage_data_removal(&mut txn, removal, selection_vector);
        }

        let result = txn.validate_append_only_semantics();
        if expected_error {
            assert!(matches!(
                result,
                Err(KernelError::InvalidTransactionState(_))
            ));
        } else {
            result?;
        }
        Ok(())
    }

    // =========================================================================
    // Stats validation tests for clustering columns
    // =========================================================================

    /// Per-file stats configuration for test add file helpers.
    enum TestFileStats {
        /// No stats (null stats struct)
        None,
        /// Normal stats with non-null min/max
        Present,
        /// All-null column: nullCount == numRecords, null min/max
        AllNull,
    }

    /// Creates test add file metadata with configurable stats for the "value" column.
    fn create_test_add_files(paths: Vec<&str>, stats: Vec<TestFileStats>) -> Box<dyn EngineData> {
        let value_schema = schema! { nullable "value": LONG };
        let value_fields = value_schema.fields().cloned().collect::<Vec<_>>();
        let value_struct_type = DataType::from(value_schema);
        let stats_schema = schema! {
            nullable NUM_RECORDS: LONG,
            nullable NULL_COUNT: (value_struct_type.clone()),
            nullable MIN_VALUES: (value_struct_type.clone()),
            nullable MAX_VALUES: (value_struct_type.clone()),
        };
        let stats_fields = stats_schema.fields().cloned().collect::<Vec<_>>();
        let stats_type = DataType::from(stats_schema);
        let schema = schema_ref! {
            not_null "path": STRING,
            not_null "partitionValues": { STRING => nullable STRING },
            not_null "size": LONG,
            not_null "modificationTime": LONG,
            nullable "stats": (stats_type.clone()),
        };

        let empty_map = Scalar::Map(
            MapData::try_new(
                MapType::new(DataType::STRING, DataType::STRING, true),
                Vec::<(&str, &str)>::new(),
            )
            .unwrap(),
        );

        let rows: Vec<Vec<Scalar>> = paths
            .iter()
            .zip(stats.iter())
            .map(|(path, stat)| {
                let stats_scalar = match stat {
                    TestFileStats::None => Scalar::Null(stats_type.clone()),
                    TestFileStats::Present | TestFileStats::AllNull => {
                        let value_struct = |v: Option<i64>| {
                            let scalar = v.map_or(Scalar::Null(DataType::LONG), |n| n.into());
                            Scalar::Struct(
                                StructData::try_new(value_fields.clone(), vec![scalar]).unwrap(),
                            )
                        };
                        let (null_count, min, max) = match stat {
                            TestFileStats::Present => (
                                value_struct(Some(0)),
                                value_struct(Some(1)),
                                value_struct(Some(100)),
                            ),
                            _ => (
                                value_struct(Some(100)),
                                value_struct(None),
                                value_struct(None),
                            ),
                        };
                        Scalar::Struct(
                            StructData::try_new(
                                stats_fields.clone(),
                                vec![100i64.into(), null_count, min, max],
                            )
                            .unwrap(),
                        )
                    }
                };
                vec![
                    (*path).into(),
                    empty_map.clone(),
                    1024i64.into(),
                    1000000i64.into(),
                    stats_scalar,
                ]
            })
            .collect();
        ArrowEvaluationHandler.create_many(schema, rows).unwrap()
    }

    #[test]
    fn test_stats_validation_allows_all_null_clustering_column() {
        let (engine, snapshot) = non_dv_table();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap()
            .with_clustering_columns_for_test(vec![column_name!("value")]);

        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::AllNull]);

        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should pass for all-null clustering columns, got: {result:?}",
        );
    }

    #[test]
    fn test_stats_validation_when_clustering_cols_missing_stats() {
        let (engine, snapshot) = non_dv_table();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap()
            // Enable clustering columns for this test
            .with_clustering_columns_for_test(vec![column_name!("value")]);

        // Add files WITHOUT stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

        // Directly test the validation method instead of committing
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_err(),
            "Expected validation to fail when stats are missing for clustering columns"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Stats validation error") || err_msg.contains("no stats"),
            "Expected stats validation error, got: {err_msg}"
        );
    }

    #[test]
    fn test_stats_validation_when_clustering_stats_present() {
        let (engine, snapshot) = non_dv_table();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap()
            // Enable clustering columns for this test
            .with_clustering_columns_for_test(vec![column_name!("value")]);

        // Add files WITH stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::Present]);

        // Directly test the validation method
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should pass when stats are present, got: {result:?}"
        );
    }

    #[test]
    fn test_stats_validation_skipped_without_clustering() {
        let (engine, snapshot) = non_dv_table();
        let txn = snapshot
            .update_table(UpdateTableOperation::Write, "test-engine", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap();
        // No clustering columns set (default)

        // Add files WITHOUT stats
        let add_files = create_test_add_files(vec!["file1.parquet"], vec![TestFileStats::None]);

        // Directly test the validation method - should pass because no clustering
        let result = txn.validate_add_files_stats(&[add_files]);

        assert!(
            result.is_ok(),
            "Stats validation should be skipped without clustering, got: {result:?}"
        );
    }
}
