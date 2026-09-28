use crate::committer::Committer;
use crate::engine_data::FilteredEngineData;
use crate::metrics::MetricId;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::transaction::{OverwriteTable, Transaction};
use crate::utils::{current_time_ms, PhantomType};
use crate::DeltaResult;

/// A transaction replacing all data files, the schema, and partitioning in one commit.
///
/// Construct through [`Snapshot::overwrite`](crate::snapshot::Snapshot::overwrite). Write only
/// new rows using this transaction's [`write_state`](Self::write_state), then stage their file
/// metadata with [`add_files`](Self::add_files). Committing without additions produces an empty
/// table with the replacement schema. Old files are logically removed, not physically deleted.
///
/// All rows are new, including their row-tracking identities. Do not supply materialized row IDs
/// or row commit versions from the old data. Kernel retains the existing high-water mark and
/// assigns fresh row-tracking metadata to added files when required by the protocol.
///
/// The transaction requires `dataChange = true` and never rebases after a version conflict.
/// Committers for catalog-managed tables must support metadata updates.
pub type OverwriteTableTransaction = Transaction<OverwriteTable>;

impl OverwriteTableTransaction {
    pub(crate) fn try_new_overwrite(
        read_snapshot: SnapshotRef,
        effective_table_config: TableConfiguration,
        remove_files_metadata: Vec<FilteredEngineData>,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<Self> {
        let span = tracing::info_span!(
            "txn",
            path = %read_snapshot.table_root(),
            read_version = read_snapshot.version(),
            operation = "WRITE",
        );
        Ok(Transaction {
            span,
            operation_id: MetricId::new(),
            correlation_id: None,
            read_snapshot_opt: Some(read_snapshot),
            effective_table_config,
            should_emit_protocol: false,
            should_emit_metadata: true,
            committer,
            operation: Some("WRITE".to_string()),
            engine_info: None,
            add_files_metadata: vec![],
            remove_files_metadata,
            set_transactions: vec![],
            commit_timestamp: current_time_ms()?,
            user_domain_metadata_additions: vec![],
            system_domain_metadata_additions: vec![],
            provided_row_tracking_high_water_mark: None,
            user_domain_removals: vec![],
            data_change: true,
            full_overwrite: true,
            column_defaults_acknowledged: false,
            // No old rows are carried into the replacement data.
            row_tracking_preservation_acknowledged: true,
            engine_commit_info: None,
            is_blind_append: false,
            dv_matched_files: vec![],
            num_dv_updates: 0,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            root_manifest_file: None,
            physical_clustering_columns: None,
            _state: PhantomType::default(),
        })
    }
}
