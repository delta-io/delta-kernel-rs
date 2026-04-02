//! CRC state computation via reverse log replay.
//!
//! Builds a [`CrcUpdate`] by reading commits in reverse order (newest to oldest) from a
//! [`LogSegment`]. The update captures all CRC-relevant fields in a single pass: Protocol,
//! Metadata, DomainMetadata, SetTransactions, file stats (add.size/remove.size), ICT, and
//! operation safety.
//!
//! The entry point is [`LogSegment::build_crc_from_stale`], which handles the stale CRC case:
//! replay commits after the CRC version, then merge (apply) the result onto the base CRC.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::LazyLock;

use tracing::instrument;

use crate::actions::{
    get_commit_schema, DomainMetadata, Metadata, Protocol, SetTransaction, ADD_NAME,
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use crate::crc::{Crc, CrcUpdate, FileStatsDelta};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, SchemaRef};
use crate::{DeltaResult, Engine, RowVisitor};

use super::LogSegment;

/// Schema for CRC incremental replay. Reads add.size, remove.path, remove.size, full P&M,
/// DM, txns, and commitInfo from commit files. remove.path is included to detect remove
/// actions with missing size (remove.path present but remove.size null). Does NOT read
/// add.path or DV columns (no dedup needed in incremental mode).
fn crc_incremental_schema() -> DeltaResult<SchemaRef> {
    get_commit_schema().project(&[
        ADD_NAME,
        REMOVE_NAME,
        PROTOCOL_NAME,
        METADATA_NAME,
        SET_TRANSACTION_NAME,
        DOMAIN_METADATA_NAME,
        COMMIT_INFO_NAME,
    ])
}

impl LogSegment {
    /// Build a complete [`Crc`] by replaying commits after a stale CRC and applying the
    /// accumulated changes onto the base.
    ///
    /// The segment should already be pruned via [`segment_after_crc`](Self::segment_after_crc).
    #[instrument(name = "log_seg.build_crc_from_stale", skip_all, err)]
    pub(crate) fn build_crc_from_stale(
        &self,
        engine: &dyn Engine,
        base: &mut Crc,
    ) -> DeltaResult<()> {
        let update = self.replay_for_crc_update(engine)?;
        base.apply(update);
        Ok(())
    }

    /// Build a fresh [`Crc`] from scratch by replaying the entire log segment (checkpoint +
    /// commits). Used when no CRC file exists on disk.
    ///
    /// The resulting CRC has:
    /// - P&M from the log (first-seen-wins in reverse order)
    /// - DM/txns: Complete (the full log was read)
    /// - File stats: Valid (checkpoint adds + commit adds - commit removes)
    ///
    /// Returns `Err` if P&M cannot be found in the log.
    #[instrument(name = "log_seg.build_crc_from_scratch", skip_all, err)]
    pub(crate) fn build_crc_from_scratch(&self, engine: &dyn Engine) -> DeltaResult<Crc> {
        self.replay_for_crc_update(engine)?.into_fresh_crc()
    }

    /// Replay this log segment's commits in reverse order and accumulate a [`CrcUpdate`].
    ///
    /// Reads all CRC-relevant fields from each commit batch via [`CrcReplayVisitor`].
    /// Protocol and Metadata use first-seen-wins (newest takes priority). DomainMetadata
    /// and SetTransactions use first-seen-wins per domain/app_id. File stats are summed.
    fn replay_for_crc_update(&self, engine: &dyn Engine) -> DeltaResult<CrcUpdate> {
        let schema = crc_incremental_schema()?;
        let mut accumulator = CrcReplayAccumulator::new();

        for batch_result in self.read_actions(engine, schema)? {
            let batch = batch_result?;
            let data = batch.actions.as_ref();

            // Extract P&M (first-seen-wins: skip if already found)
            if accumulator.protocol.is_none() {
                accumulator.protocol = Protocol::try_new_from_data(data)?;
            }
            if accumulator.metadata.is_none() {
                accumulator.metadata = Metadata::try_new_from_data(data)?;
            }

            // Extract DM, txns, file stats, and commitInfo via visitor
            let mut visitor = CrcReplayVisitor {
                is_log_batch: batch.is_log_batch,
                acc: &mut accumulator,
            };
            visitor.visit_rows_of(data)?;
        }

        Ok(accumulator.into_crc_update())
    }
}

// ============================================================================
// Accumulator and visitor
// ============================================================================

/// Accumulates CRC-relevant state from a reverse log replay pass.
struct CrcReplayAccumulator {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    domain_metadata: HashMap<String, DomainMetadata>,
    set_transactions: HashMap<String, SetTransaction>,
    add_count: i64,
    add_bytes: i64,
    remove_count: i64,
    remove_bytes: i64,
    has_missing_remove_size: bool,
    operation_safe: bool,
    in_commit_timestamp: Option<i64>,
    ict_seen: bool,
    /// Whether any commitInfo action was seen across all commit batches. When no commitInfo
    /// is found in any commit, the operation safety cannot be determined, so file stats are
    /// treated as indeterminate.
    has_commit_info: bool,
}

impl CrcReplayAccumulator {
    fn new() -> Self {
        Self {
            protocol: None,
            metadata: None,
            domain_metadata: HashMap::new(),
            set_transactions: HashMap::new(),
            add_count: 0,
            add_bytes: 0,
            remove_count: 0,
            remove_bytes: 0,
            has_missing_remove_size: false,
            operation_safe: true,
            in_commit_timestamp: None,
            ict_seen: false,
            has_commit_info: false,
        }
    }

    fn into_crc_update(self) -> CrcUpdate {
        // If no commitInfo was seen in any commit batch, we cannot determine operation
        // safety, so treat it as unsafe.
        let operation_safe = self.operation_safe && self.has_commit_info;
        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: self.add_count - self.remove_count,
                net_bytes: self.add_bytes - self.remove_bytes,
                // TODO: build added/removed histograms during replay for histogram support
                added_histogram: None,
                removed_histogram: None,
            },
            protocol: self.protocol,
            metadata: self.metadata,
            domain_metadata: self.domain_metadata,
            set_transactions: self.set_transactions,
            in_commit_timestamp: self.in_commit_timestamp,
            operation_safe,
            has_missing_file_size: self.has_missing_remove_size,
        }
    }
}

/// Visitor that extracts CRC-relevant fields from a single batch of actions during
/// reverse log replay. Protocol and Metadata are handled separately (via
/// `try_new_from_data` before the visitor runs). This visitor handles:
/// - DomainMetadata (first-seen-wins per domain)
/// - SetTransaction (first-seen-wins per app_id)
/// - add.size / remove.size (summed for file stats)
/// - commitInfo.operation (incremental safety check)
/// - commitInfo.inCommitTimestamp (ICT, from newest commit only)
struct CrcReplayVisitor<'a> {
    is_log_batch: bool,
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CrcReplayVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    // File stats columns
                    ColumnName::new(["add", "size"]),    // 0
                    ColumnName::new(["remove", "path"]), // 1: detect remove presence
                    ColumnName::new(["remove", "size"]), // 2
                    // DomainMetadata columns
                    ColumnName::new(["domainMetadata", "domain"]), // 3
                    ColumnName::new(["domainMetadata", "configuration"]), // 4
                    ColumnName::new(["domainMetadata", "removed"]), // 5
                    // SetTransaction columns
                    ColumnName::new(["txn", "appId"]),       // 6
                    ColumnName::new(["txn", "version"]),     // 7
                    ColumnName::new(["txn", "lastUpdated"]), // 8
                    // CommitInfo columns
                    ColumnName::new(["commitInfo", "operation"]), // 9
                    ColumnName::new(["commitInfo", "inCommitTimestamp"]), // 10
                ],
                vec![
                    DataType::LONG,    // add.size
                    DataType::STRING,  // remove.path
                    DataType::LONG,    // remove.size
                    DataType::STRING,  // domainMetadata.domain
                    DataType::STRING,  // domainMetadata.configuration
                    DataType::BOOLEAN, // domainMetadata.removed
                    DataType::STRING,  // txn.appId
                    DataType::LONG,    // txn.version
                    DataType::LONG,    // txn.lastUpdated
                    DataType::STRING,  // commitInfo.operation
                    DataType::LONG,    // commitInfo.inCommitTimestamp
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // File stats: add.size (index 0)
            let add_size: Option<i64> = getters[0].get_opt(i, "add.size")?;
            if let Some(size) = add_size {
                self.acc.add_count += 1;
                self.acc.add_bytes += size;
            }

            // File stats: remove (only from commit batches, not checkpoint tombstones).
            // Use remove.path (index 1) to detect remove presence, remove.size (index 2)
            // for the actual byte count. If remove.path is present but remove.size is null,
            // that is a remove with missing size which makes file stats untrackable.
            if self.is_log_batch {
                let remove_path: Option<String> = getters[1].get_opt(i, "remove.path")?;
                if remove_path.is_some() {
                    let remove_size: Option<i64> = getters[2].get_opt(i, "remove.size")?;
                    if let Some(size) = remove_size {
                        self.acc.remove_count += 1;
                        self.acc.remove_bytes += size;
                    } else {
                        self.acc.has_missing_remove_size = true;
                    }
                }
            }

            // DomainMetadata: first-seen-wins per domain (index 3, 4, 5)
            let dm_domain: Option<String> = getters[3].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = dm_domain {
                if let Entry::Vacant(e) = self.acc.domain_metadata.entry(domain.clone()) {
                    let configuration: String = getters[4]
                        .get_opt(i, "domainMetadata.configuration")?
                        .unwrap_or_default();
                    let removed: bool = getters[5]
                        .get_opt(i, "domainMetadata.removed")?
                        .unwrap_or(false);
                    let dm = if removed {
                        DomainMetadata::remove(domain, configuration)
                    } else {
                        DomainMetadata::new(domain, configuration)
                    };
                    e.insert(dm);
                }
            }

            // SetTransaction: first-seen-wins per app_id (index 6, 7, 8)
            let txn_app_id: Option<String> = getters[6].get_opt(i, "txn.appId")?;
            if let Some(app_id) = txn_app_id {
                if let Entry::Vacant(e) = self.acc.set_transactions.entry(app_id.clone()) {
                    let version: i64 = getters[7].get(i, "txn.version")?;
                    let last_updated: Option<i64> = getters[8].get_opt(i, "txn.lastUpdated")?;
                    let txn = SetTransaction::new(app_id, version, last_updated);
                    e.insert(txn);
                }
            }

            // CommitInfo: operation safety and ICT (only from commit batches, index 9, 10).
            // Only process rows that actually have a commitInfo action (operation is Some).
            // Other rows (protocol, metadata, add, remove, etc.) have null commitInfo
            // fields.
            if self.is_log_batch {
                let operation: Option<String> = getters[9].get_opt(i, "commitInfo.operation")?;
                if let Some(op) = operation {
                    self.acc.has_commit_info = true;
                    if !FileStatsDelta::is_incremental_safe(&op) {
                        self.acc.operation_safe = false;
                    }

                    // ICT: newest commit only (first commitInfo seen going backwards)
                    if !self.acc.ict_seen {
                        self.acc.in_commit_timestamp =
                            getters[10].get_opt(i, "commitInfo.inCommitTimestamp")?;
                        self.acc.ict_seen = true;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::FileStatsValidity;

    #[test]
    fn test_accumulator_empty_state_has_unsafe_operation_due_to_missing_commit_info() {
        let acc = CrcReplayAccumulator::new();
        let update = acc.into_crc_update();

        assert!(update.protocol.is_none());
        assert!(update.metadata.is_none());
        assert!(update.domain_metadata.is_empty());
        assert!(update.set_transactions.is_empty());
        assert!(update.in_commit_timestamp.is_none());
        // No commitInfo was seen, so operations are treated as unsafe.
        assert!(!update.operation_safe);
        assert!(!update.has_missing_file_size);
        assert_eq!(update.file_stats.net_files, 0);
        assert_eq!(update.file_stats.net_bytes, 0);
    }

    #[test]
    fn test_accumulator_tracks_file_stats() {
        let mut acc = CrcReplayAccumulator::new();
        acc.add_count = 3;
        acc.add_bytes = 600;
        acc.remove_count = 1;
        acc.remove_bytes = 100;

        let update = acc.into_crc_update();
        assert_eq!(update.file_stats.net_files, 2);
        assert_eq!(update.file_stats.net_bytes, 500);
    }

    #[test]
    fn test_accumulator_tracks_missing_remove_size() {
        let mut acc = CrcReplayAccumulator::new();
        acc.has_missing_remove_size = true;

        let update = acc.into_crc_update();
        assert!(update.has_missing_file_size);
    }

    #[test]
    fn test_accumulator_unsafe_operation_with_commit_info_produces_unsafe_update() {
        let mut acc = CrcReplayAccumulator::new();
        acc.has_commit_info = true;
        acc.operation_safe = false;

        let update = acc.into_crc_update();
        assert!(!update.operation_safe);
    }

    #[test]
    fn test_accumulator_safe_operation_with_commit_info_produces_safe_update() {
        let mut acc = CrcReplayAccumulator::new();
        acc.has_commit_info = true;
        acc.operation_safe = true;

        let update = acc.into_crc_update();
        assert!(update.operation_safe);
    }

    #[test]
    fn test_crc_update_applied_to_base_updates_file_stats() {
        // Deserialize a base CRC from JSON (the way it would be loaded from disk)
        let base_json = r#"{
            "tableSizeBytes": 300,
            "numFiles": 2,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":0},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
        }"#;
        let mut base: Crc = serde_json::from_str(base_json).unwrap();

        let update = CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 400,
                ..Default::default()
            },
            operation_safe: true,
            ..Default::default()
        };

        base.apply(update);

        let stats = base.file_stats().expect("file stats should be Valid");
        assert_eq!(stats.num_files, 3);
        assert_eq!(stats.table_size_bytes, 700);
        assert_eq!(base.file_stats_validity, FileStatsValidity::Valid);
    }
}
