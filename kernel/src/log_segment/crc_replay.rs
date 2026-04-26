//! Reverse log replay accumulator for CRC builds.
//!
//! Builds a [`CrcUpdate`] by reading commits and (optionally) a checkpoint in reverse via
//! [`LogSegment::read_actions`]. The accumulator does first-seen-wins for per-key fields
//! (Protocol, Metadata, DomainMetadata, SetTransaction, ICT) so that the resulting
//! `CrcUpdate` is *equivalent* to a forward-produced delta — the apply step
//! ([`Crc::apply`](crate::crc::Crc::apply)) is direction-agnostic and reads "old + delta =
//! new" regardless of how the delta was produced.
//!
//! Two entry points on [`LogSegment`]:
//!
//! - [`build_crc_from_stale`](LogSegment::build_crc_from_stale): segment is `(X, M]` (commits after
//!   stale CRC); applies the accumulated update onto the loaded base CRC, producing `Crc[M]`.
//! - [`build_crc_from_scratch`](LogSegment::build_crc_from_scratch): segment is `[0, M]` or
//!   `(checkpoint_C, M] + checkpoint_C`; calls [`CrcUpdate::into_fresh_crc`] to produce `Crc[M]`
//!   from the captured complete state.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::LazyLock;

use tracing::instrument;

use super::LogSegment;
use crate::actions::{
    get_commit_schema, DomainMetadata, Metadata, Protocol, SetTransaction, ADD_NAME,
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use crate::crc::{Crc, CrcUpdate, FileSizeHistogram, FileStatsDelta};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, SchemaRef};
use crate::{DeltaResult, Engine, RowVisitor};

/// Schema for CRC reverse replay. Reads file stats columns (add.size, remove.path,
/// remove.size), full Protocol/Metadata, DomainMetadata, SetTransaction, and commitInfo
/// (operation + inCommitTimestamp) from each batch.
///
/// `remove.path` is included alongside `remove.size` to detect removes whose `size` is
/// null: if `path` is set but `size` is null, that's a "remove with missing size" which
/// transitions file stats to [`Untrackable`](crate::crc::FileStatsState::Untrackable).
///
/// Does NOT include `add.path` or DV columns -- the accumulator does no deduplication
/// (Incremental strategy). Action reconciliation (priority 5) will need a wider schema.
fn crc_replay_schema() -> DeltaResult<SchemaRef> {
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
    /// accumulated update onto the loaded base.
    ///
    /// The segment should already be pruned via [`segment_after_crc`](Self::segment_after_crc)
    /// to contain only commits in `(crc_version, end_version]`. The base CRC is the loaded
    /// value of `{crc_version}.crc` from disk.
    ///
    /// Implements the universal invariant `Crc[X] + CrcUpdate(X→M) = Crc[M]`. The update
    /// is produced via reverse iteration with first-seen-wins semantics; the apply is
    /// forward (last-write-wins, but the update has already pre-merged via first-seen).
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
    /// commits). Used when no CRC file is on disk.
    ///
    /// The resulting CRC has:
    /// - P&M from the log (first-seen in reverse = newest = correct for end_version)
    /// - DM/txns: [`Complete`](crate::crc::DomainMetadataState::Complete) (full state was captured)
    /// - File stats: [`Valid`](crate::crc::FileStatsState::Valid) when no unsafe ops were seen and
    ///   no remove had a missing size
    ///
    /// Returns `Err` if Protocol or Metadata cannot be found in the log (a malformed log).
    #[instrument(name = "log_seg.build_crc_from_scratch", skip_all, err)]
    pub(crate) fn build_crc_from_scratch(&self, engine: &dyn Engine) -> DeltaResult<Crc> {
        self.replay_for_crc_update(engine)?.into_fresh_crc()
    }

    /// Reverse-replay this segment and accumulate a [`CrcUpdate`].
    ///
    /// For each [`ActionsBatch`](crate::log_replay::ActionsBatch) returned by
    /// [`read_actions`](Self::read_actions):
    /// - Extract Protocol and Metadata via [`Protocol::try_new_from_data`] /
    ///   [`Metadata::try_new_from_data`] (first-seen-wins, since the accumulator skips if already
    ///   populated).
    /// - Run [`CrcReplayVisitor`] for DM, txn, file stats, and commitInfo extraction.
    fn replay_for_crc_update(&self, engine: &dyn Engine) -> DeltaResult<CrcUpdate> {
        let schema = crc_replay_schema()?;
        let mut accumulator = CrcReplayAccumulator::new();

        for batch_result in self.read_actions(engine, schema)? {
            let batch = batch_result?;
            let data = batch.actions.as_ref();

            // Protocol / Metadata: first-seen-wins (skip if already found).
            if accumulator.protocol.is_none() {
                accumulator.protocol = Protocol::try_new_from_data(data)?;
            }
            if accumulator.metadata.is_none() {
                accumulator.metadata = Metadata::try_new_from_data(data)?;
            }

            // DM, txn, file stats, commitInfo via visitor.
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
// Accumulator
// ============================================================================

/// In-progress state for a single CRC reverse replay. Once all batches are absorbed, call
/// [`into_crc_update`](Self::into_crc_update) to finalize.
///
/// First-seen-wins semantics for per-key fields means: in reverse iteration, the FIRST
/// time we see a Protocol/Metadata/DM/txn/ICT, that value wins. This is equivalent to
/// "newest wins at end_version" because we read newest-first.
struct CrcReplayAccumulator {
    // First-seen-wins (per key)
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    domain_metadata: HashMap<String, DomainMetadata>,
    set_transactions: HashMap<String, SetTransaction>,

    // Sums
    add_count: i64,
    add_bytes: i64,
    add_histogram: FileSizeHistogram,
    remove_count: i64,
    remove_bytes: i64,
    remove_histogram: FileSizeHistogram,

    // Flags
    has_missing_remove_size: bool,
    /// Cumulative AND of operation safety across all observed commitInfo rows.
    /// `true` initially; flips to `false` on any unrecognized/unsafe op.
    operation_safe: bool,
    /// Whether any commitInfo action was seen across all commit batches. If no commit had
    /// a commitInfo (malformed log or no commits at all), operation safety cannot be
    /// determined and we must treat file stats as Indeterminate.
    has_commit_info: bool,

    // ICT: first-seen (newest) wins
    in_commit_timestamp: Option<i64>,
    ict_seen: bool,
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
            add_histogram: FileSizeHistogram::create_default(),
            remove_count: 0,
            remove_bytes: 0,
            remove_histogram: FileSizeHistogram::create_default(),
            has_missing_remove_size: false,
            operation_safe: true,
            has_commit_info: false,
            in_commit_timestamp: None,
            ict_seen: false,
        }
    }

    fn into_crc_update(self) -> CrcUpdate {
        // If no commitInfo was seen across the entire replay, we cannot determine
        // operation safety. Conservative: mark as unsafe, transitioning file stats to
        // Indeterminate.
        let operation_safe = self.operation_safe && self.has_commit_info;

        // Build the net histogram by subtracting removes from adds. If subtraction fails
        // (e.g. negative bins from a corrupted log) we drop the histogram. The resulting
        // CRC is still valid; just the histogram is missing.
        let net_histogram = negate_histogram(&self.remove_histogram)
            .and_then(|negated| self.add_histogram.try_apply_delta(&negated))
            .ok();

        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: self.add_count - self.remove_count,
                net_bytes: self.add_bytes - self.remove_bytes,
                net_histogram,
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

/// Negate every bin in a histogram (for subtraction via try_apply_delta). Returns an error
/// if the input histogram fails its structural invariants -- in practice this can't happen
/// because the input was already validated, but we propagate rather than panic.
fn negate_histogram(hist: &FileSizeHistogram) -> DeltaResult<FileSizeHistogram> {
    let counts: Vec<i64> = hist.file_counts().iter().map(|&c| -c).collect();
    let bytes: Vec<i64> = hist.total_bytes().iter().map(|&b| -b).collect();
    FileSizeHistogram::try_new(hist.sorted_bin_boundaries().to_vec(), counts, bytes)
}

// ============================================================================
// Visitor
// ============================================================================

/// Single-batch visitor that extracts CRC-relevant fields from one
/// [`ActionsBatch`](crate::log_replay::ActionsBatch). Protocol and Metadata are handled
/// separately (via `try_new_from_data` before this visitor runs); this visitor handles
/// DomainMetadata, SetTransaction, file stats, and commitInfo.
///
/// The `is_log_batch` flag distinguishes commit-file batches (where remove actions and
/// commitInfo are meaningful) from checkpoint batches (where remove actions are tombstones
/// for vacuum and commitInfo is absent).
struct CrcReplayVisitor<'a> {
    is_log_batch: bool,
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CrcReplayVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    // File stats
                    ColumnName::new(["add", "size"]),    // 0
                    ColumnName::new(["remove", "path"]), // 1: detect remove presence
                    ColumnName::new(["remove", "size"]), // 2
                    // DomainMetadata
                    ColumnName::new(["domainMetadata", "domain"]), // 3
                    ColumnName::new(["domainMetadata", "configuration"]), // 4
                    ColumnName::new(["domainMetadata", "removed"]), // 5
                    // SetTransaction
                    ColumnName::new(["txn", "appId"]),       // 6
                    ColumnName::new(["txn", "version"]),     // 7
                    ColumnName::new(["txn", "lastUpdated"]), // 8
                    // commitInfo
                    ColumnName::new(["commitInfo", "operation"]), // 9
                    ColumnName::new(["commitInfo", "inCommitTimestamp"]), // 10
                ],
                vec![
                    DataType::LONG,    // add.size
                    DataType::STRING,  // remove.path
                    DataType::LONG,    // remove.size
                    DataType::STRING,  // dm.domain
                    DataType::STRING,  // dm.configuration
                    DataType::BOOLEAN, // dm.removed
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
            // Add: contributes to file stats sums (commit and checkpoint Adds both).
            if let Some(size) = getters[0].get_opt(i, "add.size")? {
                let size: i64 = size;
                self.acc.add_count += 1;
                self.acc.add_bytes += size;
                self.acc.add_histogram.insert(size)?;
            }

            // Remove: contributes only on commit batches (checkpoint Removes are
            // tombstones for vacuum, not active state).
            //
            // remove.path detects remove presence; remove.size is the bytes. If path is
            // set but size is null, that's a "remove with missing size" which transitions
            // file stats to Untrackable.
            if self.is_log_batch {
                let remove_path: Option<String> = getters[1].get_opt(i, "remove.path")?;
                if remove_path.is_some() {
                    let remove_size: Option<i64> = getters[2].get_opt(i, "remove.size")?;
                    if let Some(size) = remove_size {
                        self.acc.remove_count += 1;
                        self.acc.remove_bytes += size;
                        self.acc.remove_histogram.insert(size)?;
                    } else {
                        self.acc.has_missing_remove_size = true;
                    }
                }
            }

            // DomainMetadata: first-seen-wins per domain.
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

            // SetTransaction: first-seen-wins per app_id.
            let txn_app_id: Option<String> = getters[6].get_opt(i, "txn.appId")?;
            if let Some(app_id) = txn_app_id {
                if let Entry::Vacant(e) = self.acc.set_transactions.entry(app_id.clone()) {
                    let version: i64 = getters[7].get(i, "txn.version")?;
                    let last_updated: Option<i64> = getters[8].get_opt(i, "txn.lastUpdated")?;
                    e.insert(SetTransaction::new(app_id, version, last_updated));
                }
            }

            // commitInfo: only on commit batches. Operation safety + ICT.
            // We use the operation column to detect commitInfo presence (every commitInfo
            // action has an operation per protocol spec).
            if self.is_log_batch {
                let operation: Option<String> = getters[9].get_opt(i, "commitInfo.operation")?;
                if let Some(op) = operation {
                    self.acc.has_commit_info = true;
                    if !FileStatsDelta::is_incremental_safe(&op) {
                        self.acc.operation_safe = false;
                    }

                    // ICT: first commitInfo seen in reverse = newest commit's ICT.
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::FileStatsValidity;

    fn empty_acc() -> CrcReplayAccumulator {
        CrcReplayAccumulator::new()
    }

    #[test]
    fn empty_accumulator_with_no_commit_info_produces_indeterminate_update() {
        // No commitInfo was ever seen → operation_safe AND has_commit_info → false → !ok.
        let acc = empty_acc();
        let update = acc.into_crc_update();
        assert!(update.protocol.is_none());
        assert!(update.metadata.is_none());
        assert!(update.domain_metadata.is_empty());
        assert!(update.set_transactions.is_empty());
        assert!(!update.operation_safe);
        assert!(!update.has_missing_file_size);
        assert_eq!(update.file_stats.net_files, 0);
        assert_eq!(update.file_stats.net_bytes, 0);
    }

    #[test]
    fn accumulator_with_commit_info_and_safe_op_produces_safe_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.operation_safe = true;
        let update = acc.into_crc_update();
        assert!(update.operation_safe);
    }

    #[test]
    fn accumulator_unsafe_op_propagates_to_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.operation_safe = false;
        let update = acc.into_crc_update();
        assert!(!update.operation_safe);
    }

    #[test]
    fn accumulator_missing_remove_size_propagates_to_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_missing_remove_size = true;
        let update = acc.into_crc_update();
        assert!(update.has_missing_file_size);
    }

    #[test]
    fn accumulator_sums_file_counts_and_bytes() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.add_count = 5;
        acc.add_bytes = 1500;
        acc.remove_count = 2;
        acc.remove_bytes = 400;
        let update = acc.into_crc_update();
        assert_eq!(update.file_stats.net_files, 3);
        assert_eq!(update.file_stats.net_bytes, 1100);
    }

    #[test]
    fn replay_update_applied_to_base_produces_combined_crc() {
        // Simulates: load Crc[X] from disk, replay X+1..M, apply.
        // Asserts the universal equation: Crc[X] + Update = Crc[M].
        let base_json = r#"{
            "tableSizeBytes": 300,
            "numFiles": 2,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":0},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
        }"#;
        let mut base: Crc = serde_json::from_str(base_json).unwrap();
        assert_eq!(base.file_stats_validity(), FileStatsValidity::Valid);

        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.add_count = 1;
        acc.add_bytes = 400;
        let update = acc.into_crc_update();

        base.apply(update);

        let stats = base.file_stats().unwrap();
        assert_eq!(stats.num_files(), 3);
        assert_eq!(stats.table_size_bytes(), 700);
    }

    #[test]
    fn negate_histogram_inverts_signs() {
        let mut h = FileSizeHistogram::create_default();
        h.insert(100).unwrap();
        h.insert(200).unwrap();
        let neg = negate_histogram(&h).unwrap();
        assert_eq!(neg.file_counts()[0], -2);
        assert_eq!(neg.total_bytes()[0], -300);
    }
}
