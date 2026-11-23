//! Transaction context and helpers independent of staged data changes.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "adaptive-metadata-in-dev")]
use super::ManifestWrite;
use super::builder::TransactionBuilderState;
use super::{
    CommitOperation, CommittedTransaction, NonfileCommitActions, PostCommitStats, TransactionState,
};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, LastManifestCommit};
use crate::actions::{CommitInfo, DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::committer::{CommitMetadata, CommitProtocolMetadata, CommitType, Committer};
use crate::crc::{CrcDelta, FileStatsDelta};
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::path::{LogRoot, ParsedLogPath};
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::utils::require;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::version_as_i64;
use crate::{Engine, FileMeta, KernelError, KernelResult, Version};

impl TransactionState {
    /// Initializes transaction context from builder intent and the supplied table configuration,
    /// committer, operation, span, and timestamp.
    pub(super) fn new(
        common_state: TransactionBuilderState,
        effective_table_config: TableConfiguration,
        committer: Box<dyn Committer>,
        operation: CommitOperation,
        span: tracing::Span,
        commit_timestamp: i64,
    ) -> Self {
        let set_transactions = common_state
            .transaction_ids
            .into_iter()
            .map(|(app_id, version)| SetTransaction::new(app_id, version, Some(commit_timestamp)))
            .collect();

        Self {
            span,
            operation_id: MetricId::new(),
            correlation_id: common_state.correlation_id,
            read_snapshot_opt: None,
            effective_table_config,
            should_emit_protocol: false,
            should_emit_metadata: false,
            committer,
            operation,
            operation_parameters: common_state.operation_parameters,
            operation_metrics: common_state.operation_metrics,
            engine_info: common_state.engine_info,
            engine_commit_info: common_state.engine_commit_info,
            skip_dedup_validation: common_state.skip_dedup_validation,
            set_transactions,
            commit_timestamp,
            user_domain_metadata_additions: common_state.domain_metadata_additions,
            system_domain_metadata_additions: vec![],
            provided_row_tracking_high_water_mark: None,
            user_domain_removals: vec![],
            data_change: common_state.data_change,
            column_defaults_acknowledged: false,
            #[cfg(feature = "check-constraints-in-dev")]
            check_constraints_acknowledged: false,
            generated_columns_acknowledged: false,
            row_tracking_preservation_acknowledged: false,
            is_blind_append: false,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            manifest_write: None,
            physical_clustering_columns: None,
        }
    }

    /// Determines the commit type based on whether this is a create-table operation and whether
    /// the table is catalog-managed.
    pub(super) fn determine_commit_type(
        is_create: bool,
        table_config: &crate::table_configuration::TableConfiguration,
    ) -> CommitType {
        let is_catalog_managed = table_config.is_catalog_managed();

        // TODO: Handle UpgradeToCatalogManaged and DowngradeToPathBased when ALTER TABLE
        // SET TBLPROPERTIES is supported.
        match (is_create, is_catalog_managed) {
            (true, true) => CommitType::CatalogManagedCreate,
            (true, false) => CommitType::PathBasedCreate,
            (false, true) => CommitType::CatalogManagedWrite,
            // TODO(#3852): Publish retained catalog commits before executing a path-based write.
            (false, false) => CommitType::PathBasedWrite,
        }
    }

    /// Validates that the committer type matches the commit type. A catalog committer must be
    /// used for catalog-managed operations, and a non-catalog committer for path-based operations.
    pub(super) fn validate_commit_type(
        is_catalog_committer: bool,
        commit_type: &CommitType,
    ) -> KernelResult<()> {
        match (
            is_catalog_committer,
            commit_type.requires_catalog_committer(),
        ) {
            (true, true) | (false, false) => Ok(()),
            (false, true) => Err(KernelError::generic(
                "This table is catalog-managed and requires a catalog committer. \
                 Please provide a catalog committer via UpdateTableTransactionBuilder::build().",
            )),
            (true, false) => Err(KernelError::generic(
                "This table is path-based and cannot be committed to with a catalog committer.",
            )),
        }
    }

    /// Builds the [`CommitMetadata`] for this transaction. Determines the commit type,
    /// validates the committer, and assembles the protocol/metadata state.
    pub(super) fn create_commit_metadata(
        &self,
        commit_version: Version,
        in_commit_timestamp: Option<i64>,
        new_protocol: Option<Protocol>,
        new_metadata: Option<Metadata>,
        domain_metadata_changes: Vec<crate::actions::DomainMetadata>,
    ) -> KernelResult<CommitMetadata> {
        let log_root = LogRoot::new(self.effective_table_config.table_root().clone())?;
        let is_create = self.is_create_table();
        let commit_type = Self::determine_commit_type(is_create, &self.effective_table_config);
        Self::validate_commit_type(self.committer.is_catalog_committer(), &commit_type)?;
        // For create-table: previous P&M is None (no prior table), new P&M is set.
        // For existing table with metadata change: previous P&M is from snapshot, new P&M
        // is from effective config.
        // For existing table without metadata change: previous P&M is from snapshot, new is None.
        let (read_protocol, read_metadata, max_published_version) = if is_create {
            (None, None, None)
        } else {
            let snap = self.read_snapshot()?;
            let read_config = snap.table_configuration();
            (
                Some(read_config.protocol().clone()),
                Some(read_config.metadata().clone()),
                snap.log_segment().listed.max_published_version,
            )
        };
        let protocol_metadata = CommitProtocolMetadata::try_new(
            read_protocol,
            read_metadata,
            new_protocol,
            new_metadata,
        )?;
        Ok(CommitMetadata::new(
            log_root,
            commit_version,
            commit_type,
            in_commit_timestamp.unwrap_or(self.commit_timestamp),
            max_published_version,
            protocol_metadata,
            domain_metadata_changes,
        ))
    }

    /// Builds the `checkpoint` action committing the configured root manifest file, or `None` if
    /// this transaction has none.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(super) fn generate_checkpoint_action(
        &self,
        engine: &dyn Engine,
        commit_version: Version,
        dm_changes: &[DomainMetadata],
    ) -> KernelResult<Option<CheckpointAction>> {
        let Some(ManifestWrite::RootFile(root_manifest_file)) = &self.manifest_write else {
            return Ok(None);
        };
        let action = root_manifest_file.compute_checkpoint_action(
            engine,
            commit_version,
            &self.effective_table_config,
            dm_changes,
            &self.set_transactions,
        )?;
        Ok(Some(action))
    }

    pub(super) fn is_adaptive_metadata_enabled(&self) -> bool {
        self.effective_table_config
            .is_feature_enabled(&TableFeature::AdaptiveMetadataPreview)
    }

    /// Sets the adaptiveMetadata `dataChange` and `lastManifestCommit` fields on `commit_info` when
    /// the feature is enabled.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(super) fn set_adaptive_metadata_commit_info(
        &self,
        engine: &dyn Engine,
        commit_info: &mut CommitInfo,
    ) -> KernelResult<()> {
        if self.is_adaptive_metadata_enabled() {
            commit_info.set_data_change(self.data_change);
            commit_info.set_last_manifest_commit(self.last_manifest_commit(engine)?);
        }
        Ok(())
    }

    /// The `lastManifestCommit` this commit records: its own when it writes a root manifest file
    /// (whose content root is at the commit version), otherwise the read snapshot's carried
    /// forward. `None` before the table's first manifest commit.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(super) fn last_manifest_commit(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Option<LastManifestCommit>> {
        match &self.manifest_write {
            Some(ManifestWrite::RootFile(_)) => {
                let version = version_as_i64(self.get_commit_version())?;
                LastManifestCommit::new(version, version).map(Some)
            }
            // TODO(#3352): a manifest commit must record (its commit version, the contentRoot
            // version of its emitted checkpoint action).
            Some(ManifestWrite::Commit(_)) => Err(KernelError::unsupported(
                "lastManifestCommit for a manifest commit is not yet supported",
            )),
            None => match &self.read_snapshot_opt {
                Some(snapshot) => snapshot.last_manifest_commit(engine),
                None => Ok(None),
            },
        }
    }

    /// Reject write-state creation on empty-schema tables, so engines fail before staging any
    /// parquet. CREATE TABLE is exempt.
    pub(super) fn ensure_schema_non_empty_for_write_state(&self) -> KernelResult<()> {
        if self.is_create_table() {
            return Ok(());
        }
        if self.effective_table_config.logical_schema().num_fields() == 0 {
            return Err(KernelError::generic(
                "Cannot write data files to a Delta table with empty schema; \
                 use `snapshot.update_table(UpdateTableOperation::AlterTable, engine_info, false)` and \
                 `add_column(...)` to add at least one column before writing data",
            ));
        }
        Ok(())
    }

    /// Rejects write-state creation when a table declares column defaults and the connector has
    /// not acknowledged handling them.
    pub(super) fn ensure_column_defaults_acknowledged(&self) -> KernelResult<()> {
        require!(
            self.column_defaults_acknowledged
                || !self
                    .effective_table_config
                    .is_feature_enabled(&TableFeature::AllowColumnDefaults)
                || !self.effective_table_config.has_column_with_default(),
            KernelError::invalid_transaction_state(
                "Writing data to a table with column defaults requires calling \
                 Transaction::ack_column_defaults() first",
            )
        );
        Ok(())
    }

    /// Rejects writing or committing when this transaction has effective CHECK constraints that
    /// the connector has not acknowledged.
    #[cfg(feature = "check-constraints-in-dev")]
    pub(super) fn ensure_check_constraints_acknowledged(&self) -> KernelResult<()> {
        require!(
            !self.effective_table_config.has_check_constraints()
                || self.check_constraints_acknowledged,
            KernelError::invalid_transaction_state(
                "Writing to a table with CHECK constraints requires calling \
                 Transaction::ack_check_constraints() first",
            )
        );
        Ok(())
    }

    /// Fails `write_state` and `commit` when this transaction has effective generated columns that
    /// the connector hasn't acknowledged.
    pub(super) fn ensure_generated_columns_acknowledged(&self) -> KernelResult<()> {
        require!(
            self.generated_columns_acknowledged
                || self.effective_table_config.generated_columns()?.is_empty(),
            KernelError::invalid_transaction_state(
                "Writing to a table with generated columns requires calling \
                 Transaction::ack_generated_columns() first",
            )
        );
        Ok(())
    }

    pub(super) fn ensure_row_tracking_preservation_acknowledged(&self) -> KernelResult<()> {
        if !self
            .effective_table_config
            .is_feature_enabled(&TableFeature::RowTracking)
        {
            return Ok(());
        }
        require!(
            self.row_tracking_preservation_acknowledged,
            KernelError::invalid_transaction_state(
                "Data manipulation on a table with Row Tracking enabled requires preserving stable \
                 Row IDs for copied or updated rows and stable Row Commit Versions for copied \
                 rows. See Transaction::ack_row_tracking_preservation() for more details",
            )
        );
        Ok(())
    }

    /// Returns true if this is a create-table transaction.
    /// A create-table transaction has no read snapshot (no pre-existing table).
    pub(super) fn is_create_table(&self) -> bool {
        self.read_snapshot_opt.is_none()
    }

    // Returns the read snapshot. Returns an error if this is a create-table transaction.
    // To get the `Option<SnapshotRef>` directly, use the `read_snapshot_opt` field.
    pub(super) fn read_snapshot(&self) -> KernelResult<&Snapshot> {
        self.read_snapshot_opt.as_deref().ok_or_else(|| {
            KernelError::internal_error("read_snapshot() called on create-table transaction")
        })
    }

    /// Computes the in-commit timestamp for this transaction if ICT is enabled.
    /// Returns `None` if ICT is not enabled on the table. A feature being in the protocol
    /// (`is_feature_supported`) is not sufficient -- the `delta.enableInCommitTimestamps`
    /// property must also be `true` (`is_feature_enabled`).
    pub(super) fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> KernelResult<Option<i64>> {
        let has_ict = self
            .effective_table_config
            .is_feature_enabled(&TableFeature::InCommitTimestamp);

        if !has_ict {
            return Ok(None);
        }

        if self.is_create_table() {
            // For CREATE TABLE there are no prior commits -- use the wall-clock time directly.
            return Ok(Some(self.commit_timestamp));
        }

        // Existing table: enforce monotonicity per the Delta protocol. The timestamp
        // must be the larger of:
        // - The time at which the writer attempted the commit
        // - One millisecond later than the previous commit's inCommitTimestamp
        Ok(self
            .read_snapshot()?
            .reader()
            .get_in_commit_timestamp(engine)?
            .map(|prev_ict| self.commit_timestamp.max(prev_ict + 1)))
    }

    /// Returns the commit version for this transaction.
    /// For existing table transactions, this is snapshot.version() + 1.
    /// For create-table transactions, this is 0.
    pub(super) fn get_commit_version(&self) -> Version {
        match &self.read_snapshot_opt {
            Some(snap) => snap.version() + 1,
            None => 0,
        }
    }

    /// Generate non-file commit actions as in-memory structs, that can then be encoded into the
    /// committable format (e.g EngineData)
    pub(super) fn prepare_nonfile_actions(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<NonfileCommitActions> {
        let set_transactions = self.set_transactions.clone();
        let in_commit_timestamp = self.get_in_commit_timestamp(engine)?;
        let mut kernel_commit_info = CommitInfo::new(
            self.commit_timestamp,
            in_commit_timestamp,
            Some(self.operation.to_string()),
            Some(self.engine_info.clone()),
            self.is_blind_append,
        );
        if let Some(operation_parameters) = self.operation_parameters.clone() {
            kernel_commit_info.set_operation_parameters(operation_parameters);
        }
        if let Some(operation_metrics) = self.operation_metrics.clone() {
            kernel_commit_info.set_operation_metrics(operation_metrics);
        }

        // Kernel requires every commit on an existing Row Tracking-enabled table to preserve
        // Stable Row IDs and Stable Row Commit Versions, so it always emits true. CREATE TABLE has
        // no existing Row Tracking state to preserve and therefore omits the flag.
        if !self.is_create_table()
            && self
                .effective_table_config
                .is_feature_enabled(&TableFeature::RowTracking)
        {
            kernel_commit_info.set_row_tracking_preserved();
        }

        #[cfg(feature = "adaptive-metadata-in-dev")]
        self.set_adaptive_metadata_commit_info(engine, &mut kernel_commit_info)?;
        Ok(NonfileCommitActions {
            commit_version: self.get_commit_version(),
            kernel_commit_info,
            protocol: self
                .should_emit_protocol
                .then(|| self.effective_table_config.protocol().clone()),
            metadata: self
                .should_emit_metadata
                .then(|| self.effective_table_config.metadata().clone()),
            set_transactions,
        })
    }

    pub(super) fn record_commit_success_metrics(
        &self,
        file_stats: &FileStatsDelta,
        num_dv_updates: u64,
        prepare_duration: Duration,
        committer_duration: Duration,
    ) {
        let span = tracing::Span::current();
        span.record("num_add_files", file_stats.gross_add_files);
        span.record("num_remove_files", file_stats.gross_remove_files);
        span.record("num_dv_updates", num_dv_updates);
        span.record("add_files_bytes", file_stats.gross_add_bytes);
        span.record("remove_files_bytes", file_stats.gross_remove_bytes);
        span.record("is_blind_append", self.is_blind_append);
        span.record("data_change", self.data_change);
        span.record("operation", self.operation.as_str());
        span.record("prepare_duration_ns", prepare_duration.as_nanos() as u64);
        span.record(
            "committer_duration_ns",
            committer_duration.as_nanos() as u64,
        );
    }

    pub(super) fn into_committed(
        self,
        file_meta: FileMeta,
        crc_delta: CrcDelta,
        #[cfg(feature = "adaptive-metadata-in-dev")] checkpoint_action: Option<CheckpointAction>,
    ) -> KernelResult<CommittedTransaction> {
        let parsed_commit = ParsedLogPath::parse_commit(file_meta)?;

        let commit_version = parsed_commit.version;

        let (post_commit_stats, post_commit_snapshot) = match &self.read_snapshot_opt {
            Some(snap) => {
                // Existing table path: use the read snapshot to compute post-commit state.
                let stats = PostCommitStats {
                    commits_since_checkpoint: snap.log_segment().commits_since_checkpoint() + 1,
                    commits_since_log_compaction: snap
                        .log_segment()
                        .commits_since_log_compaction_or_checkpoint()
                        + 1,
                };
                let snapshot = snap.new_post_commit(
                    parsed_commit,
                    crc_delta,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_action,
                )?;
                (stats, Arc::new(snapshot))
            }
            None => {
                // CREATE TABLE path: build a fresh Snapshot at version 0.
                let log_root = self
                    .effective_table_config
                    .table_root()
                    .join("_delta_log/")?;
                let log_segment = LogSegment::new_for_version_zero(log_root, parsed_commit)?;
                let crc = crc_delta.into_complete_crc(0).ok_or_else(|| {
                    KernelError::internal_error(
                        "CREATE TABLE CRC delta is missing protocol or metadata",
                    )
                })?;
                let stats = PostCommitStats {
                    commits_since_checkpoint: 1,
                    commits_since_log_compaction: 1,
                };
                let snapshot = Snapshot::new_with_crc(
                    log_segment,
                    self.effective_table_config,
                    Some(Arc::new(crc)),
                    true,  /* built_as_latest */
                    false, /* skipped_new_checkpoints */
                )?;
                (stats, Arc::new(snapshot))
            }
        };

        Ok(CommittedTransaction {
            commit_version,
            post_commit_stats,
            post_commit_snapshot: Some(post_commit_snapshot),
        })
    }

    /// Build a [`CrcDelta`] from the transaction's commit state, the commitInfo it wrote, and a
    /// precomputed [`FileStatsDelta`].
    pub(super) fn build_crc_delta(
        &self,
        file_stats: FileStatsDelta,
        commit_info: CommitInfo,
        dm_changes: Vec<DomainMetadata>,
    ) -> KernelResult<CrcDelta> {
        // TODO: drop these conversions by migrating the upstream chain
        //       (`CommitMetadata.domain_metadata_changes`, `TransactionState.set_transactions`)
        //       to `HashMap<String, _>`, lifting protocol-mandated uniqueness from runtime
        //       checks into the type system.
        let domain_metadata = dm_changes
            .into_iter()
            .map(|dm| (dm.domain().to_string(), dm))
            .collect();
        let set_transactions = self
            .set_transactions
            .iter()
            .map(|txn| (txn.app_id.clone(), txn.clone()))
            .collect();
        // Although `remove.size` is optional per the Delta protocol, the kernel write path
        // enforces presence: `try_compute_for_txn` in the imperative commit path errors if any
        // add or remove row lacks `size` (see `FileStatsVisitor::visit` in
        // `kernel/src/crc/file_stats.rs`). So at this point every size is known to be
        // present, and only operation classification can flip `is_incremental_safe`.
        // TODO(#3714): Preserve incremental file stats for commits without add/remove actions,
        // matching replay's safety rule.
        let is_incremental_safe = self.operation.is_incremental_safe();
        Ok(CrcDelta {
            file_stats,
            protocol: self
                .should_emit_protocol
                .then(|| self.effective_table_config.protocol().clone()),
            metadata: self
                .should_emit_metadata
                .then(|| self.effective_table_config.metadata().clone()),
            domain_metadata,
            set_transactions,
            in_commit_timestamp: commit_info.in_commit_timestamp,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: commit_info.last_manifest_commit,
            is_incremental_safe,
            unsupported_operation: (!is_incremental_safe)
                .then(|| self.operation.as_str().to_string()),
        })
    }
}
