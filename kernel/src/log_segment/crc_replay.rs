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

            // DM, txn, file stats, commitInfo via visitor. The visitor tracks per-batch
            // "saw file actions" and "saw commitInfo" flags so we can detect commit
            // batches that had file actions but no commitInfo (provenance unknown).
            let mut visitor = CrcReplayVisitor {
                is_log_batch: batch.is_log_batch,
                batch_saw_file_action: false,
                batch_saw_commit_info: false,
                acc: &mut accumulator,
            };
            visitor.visit_rows_of(data)?;
            // After this batch: if it was a commit batch, had file actions, but no
            // commitInfo, the operation is unknown for that commit.
            if batch.is_log_batch && visitor.batch_saw_file_action && !visitor.batch_saw_commit_info
            {
                accumulator.operation_safe = false;
            }
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

    // Net file stats: a single histogram that adds increment and removes decrement (may
    // contain negative bins for stale-replay ranges where some files were added before
    // the range and removed within it). The non-negativity check happens in
    // [`Crc::apply`](crate::crc::Crc::apply) when this delta is merged with the absolute
    // base histogram, not here.
    net_count: i64,
    net_bytes: i64,
    net_histogram: FileSizeHistogram,

    // Flags
    has_missing_remove_size: bool,
    /// Cumulative AND of operation safety across all observed commitInfo rows.
    /// `true` initially; flips to `false` on any unrecognized/unsafe op.
    operation_safe: bool,
    /// Whether any commitInfo action was seen across all commit batches. Used together
    /// with `has_log_action_inputs` to decide whether an empty / checkpoint-only segment
    /// should be classified as safe (no work) or indeterminate (no provenance).
    has_commit_info: bool,
    /// Whether the segment had any log-batch input that could plausibly carry an
    /// operation: a commit's add, remove, or commitInfo row. If false, the segment is
    /// either empty or checkpoint-only, in which case there were no operations to
    /// evaluate -- a degenerate "trivially safe" case.
    has_log_action_inputs: bool,

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
            net_count: 0,
            net_bytes: 0,
            net_histogram: FileSizeHistogram::create_default(),
            has_missing_remove_size: false,
            operation_safe: true,
            has_commit_info: false,
            has_log_action_inputs: false,
            in_commit_timestamp: None,
            ict_seen: false,
        }
    }

    fn into_crc_update(self) -> CrcUpdate {
        // Operation-safety classification:
        // - If we observed log inputs and saw commitInfo: trust `operation_safe`.
        // - If we observed log inputs but no commitInfo: provenance unknown → unsafe.
        // - If we observed no log inputs (empty range or checkpoint-only): trivially safe -- there
        //   were no operations to evaluate.
        let operation_safe = if self.has_log_action_inputs {
            self.operation_safe && self.has_commit_info
        } else {
            true
        };

        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: self.net_count,
                net_bytes: self.net_bytes,
                net_histogram: Some(self.net_histogram),
            },
            protocol: self.protocol,
            metadata: self.metadata,
            domain_metadata: self.domain_metadata,
            set_transactions: self.set_transactions,
            // Outer Some only if we actually saw a commitInfo with ICT data; otherwise
            // None tells `Crc::apply` "I didn't observe ICT, leave the base alone."
            in_commit_timestamp: self.ict_seen.then_some(self.in_commit_timestamp),
            operation_safe,
            has_missing_file_size: self.has_missing_remove_size,
        }
    }
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
    /// Set to true if this batch contained any add or remove row. Used together with
    /// `batch_saw_commit_info` to detect "commit batch with file actions but no
    /// commitInfo" -- a configuration with unknown operation provenance, treated as
    /// unsafe.
    batch_saw_file_action: bool,
    /// Set to true if this batch contained a commitInfo row. (Per protocol every commit
    /// has at most one commitInfo, but a batch might span any subset of a commit's rows.)
    batch_saw_commit_info: bool,
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
            // Add: contributes positively to file stats sums (commit AND checkpoint Adds).
            if let Some(size) = getters[0].get_opt(i, "add.size")? {
                let size: i64 = size;
                self.acc.net_count += 1;
                self.acc.net_bytes += size;
                self.acc.net_histogram.insert(size)?;
                if self.is_log_batch {
                    self.acc.has_log_action_inputs = true;
                    self.batch_saw_file_action = true;
                }
            }

            // Remove: contributes negatively, but only on commit batches (checkpoint
            // Removes are vacuum tombstones, not active state).
            //
            // remove.path detects remove presence; remove.size is the bytes. If path is
            // set but size is null, that's a "remove with missing size" which transitions
            // file stats to Untrackable.
            if self.is_log_batch {
                let remove_path: Option<String> = getters[1].get_opt(i, "remove.path")?;
                if remove_path.is_some() {
                    self.acc.has_log_action_inputs = true;
                    self.batch_saw_file_action = true;
                    let remove_size: Option<i64> = getters[2].get_opt(i, "remove.size")?;
                    if let Some(size) = remove_size {
                        self.acc.net_count -= 1;
                        self.acc.net_bytes -= size;
                        // FileSizeHistogram::remove correctly produces negative bin
                        // counts when removes outweigh adds in a bin -- the net
                        // histogram is a delta, not an absolute. The non-negativity
                        // check happens later when Crc::apply merges with the base.
                        self.acc.net_histogram.remove(size)?;
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
            // We use the operation column to detect commitInfo presence (in practice
            // every commitInfo action has an operation; missing operation is treated as
            // 'no provenance' and flips operation_safe via the per-batch heuristic below).
            if self.is_log_batch {
                let operation: Option<String> = getters[9].get_opt(i, "commitInfo.operation")?;
                if let Some(op) = operation {
                    self.acc.has_commit_info = true;
                    self.acc.has_log_action_inputs = true;
                    self.batch_saw_commit_info = true;
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
    fn empty_accumulator_with_no_log_inputs_is_trivially_safe() {
        // No log inputs at all = trivially safe (no operations to evaluate). This is the
        // empty-stale-segment case (CRC at target version + segment_after_crc returns
        // empty) and the checkpoint-only case (no commits in the segment).
        let acc = empty_acc();
        let update = acc.into_crc_update();
        assert!(update.protocol.is_none());
        assert!(update.metadata.is_none());
        assert!(update.domain_metadata.is_empty());
        assert!(update.set_transactions.is_empty());
        assert!(
            update.operation_safe,
            "empty/checkpoint-only is trivially safe"
        );
        assert!(!update.has_missing_file_size);
        assert_eq!(update.file_stats.net_files, 0);
        assert_eq!(update.file_stats.net_bytes, 0);
    }

    #[test]
    fn accumulator_with_log_inputs_but_no_commit_info_is_indeterminate() {
        // We saw add/remove rows in commit batches but no commitInfo: provenance unknown.
        // Conservative: mark unsafe → file stats become Indeterminate via Crc::apply.
        let mut acc = empty_acc();
        acc.has_log_action_inputs = true;
        // has_commit_info stays false.
        let update = acc.into_crc_update();
        assert!(!update.operation_safe);
    }

    #[test]
    fn accumulator_with_commit_info_and_safe_op_produces_safe_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_log_action_inputs = true;
        acc.operation_safe = true;
        let update = acc.into_crc_update();
        assert!(update.operation_safe);
    }

    #[test]
    fn accumulator_unsafe_op_propagates_to_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_log_action_inputs = true;
        acc.operation_safe = false;
        let update = acc.into_crc_update();
        assert!(!update.operation_safe);
    }

    #[test]
    fn accumulator_missing_remove_size_propagates_to_update() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_log_action_inputs = true;
        acc.has_missing_remove_size = true;
        let update = acc.into_crc_update();
        assert!(update.has_missing_file_size);
    }

    #[test]
    fn accumulator_net_file_counts_and_bytes() {
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_log_action_inputs = true;
        acc.net_count = 3;
        acc.net_bytes = 1100;
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
        acc.has_log_action_inputs = true;
        acc.net_count = 1;
        acc.net_bytes = 400;
        let update = acc.into_crc_update();

        base.apply(update);

        let stats = base.file_stats().unwrap();
        assert_eq!(stats.num_files(), 3);
        assert_eq!(stats.table_size_bytes(), 700);
    }

    #[test]
    fn accumulator_negative_histogram_bin_when_removes_outweigh_adds_in_range() {
        // Stale-replay scenario: the segment removes more bytes in some bin than it
        // adds (e.g. removing files that were added before the CRC base). The accumulator
        // produces a delta histogram with negative bins; the merge with the base must
        // result in a non-negative absolute histogram, but the delta itself can be
        // negative. This test asserts the visitor correctly produces such a delta and
        // that Crc::apply merges it correctly when the base has matching counts.
        let mut acc = empty_acc();
        acc.has_commit_info = true;
        acc.has_log_action_inputs = true;
        // Two adds (100, 200) and three removes (100, 100, 200) all fall in bin 0.
        // Net: -1 file, -100 bytes in bin 0.
        for sz in [100i64, 200] {
            acc.net_count += 1;
            acc.net_bytes += sz;
            acc.net_histogram.insert(sz).unwrap();
        }
        for sz in [100i64, 100, 200] {
            acc.net_count -= 1;
            acc.net_bytes -= sz;
            acc.net_histogram.remove(sz).unwrap();
        }
        let update = acc.into_crc_update();
        assert_eq!(update.file_stats.net_files, -1);
        assert_eq!(update.file_stats.net_bytes, -100);
        let net_hist = update.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(net_hist.file_counts()[0], -1);
        assert_eq!(net_hist.total_bytes()[0], -100);

        // Apply onto a base that has enough to absorb the removes.
        let mut base = Crc {
            file_stats: crate::crc::FileStatsState::Valid {
                num_files: 5,
                table_size_bytes: 1000,
                histogram: Some({
                    let mut h = FileSizeHistogram::create_default();
                    for _ in 0..5 {
                        h.insert(200).unwrap();
                    }
                    h
                }),
            },
            ..Default::default()
        };
        base.apply(update);
        let stats = base.file_stats().unwrap();
        assert_eq!(stats.num_files(), 4); // 5 - 1
        assert_eq!(stats.table_size_bytes(), 900); // 1000 - 100
        let merged = stats.file_size_histogram().unwrap();
        assert_eq!(merged.file_counts()[0], 4);
        assert_eq!(merged.total_bytes()[0], 900);
    }
}
