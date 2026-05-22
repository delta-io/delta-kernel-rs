#![allow(dead_code)]

//! Reverse log replay for incremental CRC construction.
//!
//! Reverse-replays a log segment's commit files to produce a [`CrcDelta`] covering
//! commits `(X, Y]`. Per the incremental equation `Crc[X] + CrcDelta = Crc[Y]`, that
//! delta is applied to a stale base via [`Crc::apply`].
//
// TODO: see if we can support log compaction files.

use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock};

use tracing::{instrument, warn};

use super::LogSegment;
use crate::actions::{
    get_commit_schema, DomainMetadata, Metadata, Protocol, SetTransaction, ADD_NAME,
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use crate::crc::{is_incremental_safe_operation, Crc, CrcDelta, FileSizeHistogram};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{
    column_name, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef,
};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, RowVisitor, Version};

#[allow(clippy::expect_used)]
static REPLAY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let projected = get_commit_schema()
        .project_as_struct(&[
            ADD_NAME,
            REMOVE_NAME,
            PROTOCOL_NAME,
            METADATA_NAME,
            SET_TRANSACTION_NAME,
            DOMAIN_METADATA_NAME,
            COMMIT_INFO_NAME,
        ])
        .expect("project_as_struct on commit schema");
    let with_file = projected
        .add_metadata_column("_file", MetadataColumnSpec::FilePath)
        .expect("add _file metadata column");
    Arc::new(with_file)
});

impl LogSegment {
    /// Produce a fresh `Crc` at `self.end_version` by reverse-replaying the commits in
    /// `(base_crc.version, self.end_version]` and applying the resulting delta to
    /// `base_crc` via [`Crc::apply`].
    #[instrument(name = "log_seg.build_incremental_crc_from_stale", skip_all, err)]
    pub(crate) fn build_incremental_crc_from_stale(
        &self,
        engine: &dyn Engine,
        base_crc: &Crc,
    ) -> DeltaResult<Crc> {
        let seed_histogram = base_crc
            .file_stats()
            .and_then(|s| s.file_size_histogram())
            .map(|h| {
                FileSizeHistogram::create_empty_with_boundaries(h.sorted_bin_boundaries().to_vec())
            })
            .transpose()?;
        let delta = self.build_incremental_crc_delta(engine, base_crc.version, seed_histogram)?;
        Ok(base_crc.clone().apply(delta, self.end_version))
    }

    /// Build a `CrcDelta` covering commits `(base_version, self.end_version]` via reverse
    /// log replay. `seed_histogram` is an empty histogram with the same bin boundaries as
    /// the downstream base CRC, or `None` to skip histogram tracking on the delta.
    ///
    /// Errors if `base_version >= self.end_version` or if the segment is missing the
    /// commit at `base_version + 1` (i.e. has a gap above `base_version`).
    pub(crate) fn build_incremental_crc_delta(
        &self,
        engine: &dyn Engine,
        base_version: Version,
        seed_histogram: Option<FileSizeHistogram>,
    ) -> DeltaResult<CrcDelta> {
        require!(
            base_version < self.end_version,
            Error::internal_error(format!(
                "build_incremental_crc_delta: base_version ({}) must be strictly less than \
                 end_version ({})",
                base_version, self.end_version,
            ))
        );

        let deltas: Vec<_> = self
            .listed
            .ascending_commit_files
            .iter()
            .filter(|c| c.version > base_version)
            .collect();

        let first_above = deltas.first().map(|c| c.version);
        require!(
            first_above == Some(base_version + 1),
            Error::internal_error(format!(
                "build_incremental_crc_delta: segment is missing commit {} \
                 (lowest commit above base_version is {:?})",
                base_version + 1,
                first_above,
            ))
        );

        let mut acc = CrcReplayAccumulator::new(seed_histogram);
        let files: Vec<_> = deltas.iter().rev().map(|c| c.location.clone()).collect();
        let batches = engine
            .json_handler()
            .read_json_files(&files, REPLAY_SCHEMA.clone(), None)?;

        for batch_result in batches {
            let data = batch_result?;
            let data = data.as_ref();

            if acc.delta.protocol.is_none() {
                acc.delta.protocol = Protocol::try_new_from_data(data)?;
            }
            if acc.delta.metadata.is_none() {
                acc.delta.metadata = Metadata::try_new_from_data(data)?;
            }

            // Transient visitor borrows the shared accumulator for the duration of the
            // batch; same pattern as `ActionReconciliationVisitor`.
            let mut visitor = CrcReplayVisitor { acc: &mut acc };
            visitor.visit_rows_of(data)?;
        }

        // Run the per-commit invariant on the final (oldest) commit; no successor batch
        // will trigger it.
        acc.process_commit_file_end();

        Ok(acc.into_crc_delta())
    }
}

// ============================================================================
// Accumulator
// ============================================================================

/// In-progress [`CrcDelta`] plus the scaffolding needed to build it correctly during reverse
/// replay. The visitor calls `process_batch_start` on each batch and the `on_*` methods on
/// each row. After all batches have been folded in and `process_commit_file_end` has run for
/// the final commit, [`Self::into_crc_delta`] returns the result.
struct CrcReplayAccumulator {
    delta: CrcDelta,

    /// True while the visitor is still on the newest commit. Used to gate ICT capture
    /// (only the newest commit contributes to [`CrcDelta::in_commit_timestamp`]). Cannot
    /// be derived from `current_file_url` alone, since after the first batch the URL is
    /// `Some` but we're still on the newest commit until a transition.
    is_first_commit: bool,

    /// URL of the commit file currently being processed. Drives commit-boundary detection:
    /// a different URL on a later batch means we've moved to an older commit.
    current_file_url: Option<String>,

    /// True if the current commit had at least one add/remove row. Combined with
    /// `current_commit_saw_safe_op` for the per-commit invariant check.
    current_commit_saw_file_action: bool,

    /// True if the current commit had a commitInfo row whose `operation` was in the
    /// [`is_incremental_safe_operation`] safelist. If false at commit end and the commit
    /// had file actions, we can't trust the file-stats delta and mark it unsafe.
    current_commit_saw_safe_op: bool,
}

impl CrcReplayAccumulator {
    fn new(seed_histogram: Option<FileSizeHistogram>) -> Self {
        Self {
            delta: CrcDelta {
                is_incremental_safe: true,
                file_stats: crate::crc::FileStatsDelta {
                    net_histogram: seed_histogram,
                    ..Default::default()
                },
                ..Default::default()
            },
            is_first_commit: true,
            current_file_url: None,
            current_commit_saw_file_action: false,
            current_commit_saw_safe_op: false,
        }
    }

    fn process_batch_start(&mut self, batch_file_url: &str) {
        if self.current_file_url.as_deref() == Some(batch_file_url) {
            return; // same file, still inside the current commit
        }
        if self.current_file_url.is_some() {
            // commit boundary: finalize the previous one, drop "newest" status
            self.process_commit_file_end();
            self.is_first_commit = false;
        }
        self.current_file_url = Some(batch_file_url.to_owned());
    }

    fn process_commit_file_end(&mut self) {
        // `is_incremental_safe` is one-way; once false, this check has nothing to set.
        if !self.delta.is_incremental_safe {
            return;
        }
        // File actions without a safe-classified operation: we can't trust file stats.
        // Covers both "no commitInfo at all" and "commitInfo with no `operation` field".
        if self.current_commit_saw_file_action && !self.current_commit_saw_safe_op {
            warn!(
                "CRC reverse-replay: commit at {} carried file actions but no safe-classified \
                 operation; defaulting to non-incremental-safe",
                self.current_file_url.as_deref().unwrap_or("?")
            );
            self.delta.is_incremental_safe = false;
        }
        self.current_commit_saw_file_action = false;
        self.current_commit_saw_safe_op = false;
    }

    // ===== Row-level updates (also the seams used by `on_*` unit tests) =====

    /// Called by the visitor once per commitInfo row (gated on operation or ict being
    /// present). Handles both pieces: `operation` drives per-commit safety classification,
    /// `ict` is captured into the delta from the newest commit only.
    fn on_commit_info(&mut self, operation: Option<&str>, ict: Option<i64>) {
        if let Some(op) = operation {
            if is_incremental_safe_operation(op) {
                self.current_commit_saw_safe_op = true;
            } else {
                warn!("CRC reverse-replay: non-incremental op {op}");
                self.delta.is_incremental_safe = false;
            }
        }
        if self.is_first_commit {
            self.delta.in_commit_timestamp = ict;
        }
    }

    fn on_add(&mut self, size: i64) -> DeltaResult<()> {
        self.current_commit_saw_file_action = true;
        // Once the delta is no longer incremental-safe, [`Crc::apply`] will transition the
        // file-stats state to `Indeterminate` and discard `net_files`/`net_bytes`/histogram.
        // Stop accumulating; further math is wasted work.
        if !self.delta.is_incremental_safe {
            return Ok(());
        }
        let fs = &mut self.delta.file_stats;
        fs.net_files += 1;
        fs.net_bytes += size;
        if let Some(hist) = fs.net_histogram.as_mut() {
            hist.insert(size)?;
        }
        Ok(())
    }

    /// `size = None` means the remove row had a path but no size, which makes incremental
    /// tracking impossible.
    fn on_remove(&mut self, path: &str, size: Option<i64>) -> DeltaResult<()> {
        self.current_commit_saw_file_action = true;
        // Once the delta is no longer incremental-safe, [`Crc::apply`] will transition the
        // file-stats state to `Indeterminate` and discard `net_files`/`net_bytes`/histogram.
        // Stop accumulating; further math is wasted work.
        if !self.delta.is_incremental_safe {
            return Ok(());
        }
        match size {
            Some(s) => {
                let fs = &mut self.delta.file_stats;
                fs.net_files -= 1;
                fs.net_bytes -= s;
                if let Some(hist) = fs.net_histogram.as_mut() {
                    hist.remove(s)?;
                }
            }
            None => {
                warn!("CRC reverse-replay: remove action at {path} has missing size");
                self.delta.is_incremental_safe = false;
            }
        }
        Ok(())
    }

    /// Tombstones (`removed=true`) are kept in the delta; [`Crc::apply`] consumes them as
    /// removals from the base map.
    fn on_domain_metadata(&mut self, domain: String, configuration: String, removed: bool) {
        if let Entry::Vacant(e) = self.delta.domain_metadata.entry(domain.clone()) {
            let dm = if removed {
                DomainMetadata::remove(domain, configuration)
            } else {
                DomainMetadata::new(domain, configuration)
            };
            e.insert(dm);
        }
    }

    fn on_set_transaction(&mut self, app_id: String, version: i64, last_updated: Option<i64>) {
        if let Entry::Vacant(e) = self.delta.set_transactions.entry(app_id.clone()) {
            e.insert(SetTransaction::new(app_id, version, last_updated));
        }
    }

    fn into_crc_delta(self) -> CrcDelta {
        self.delta
    }
}

// ============================================================================
// Visitor
// ============================================================================

// ===== Visitor column indices =====
// Indices into the column list returned by [`CrcReplayVisitor::selected_column_names_and_types`].
const COL_FILE: usize = 0;
const COL_OP: usize = 1;
const COL_ICT: usize = 2;
const COL_ADD_SIZE: usize = 3;
const COL_REMOVE_PATH: usize = 4;
const COL_REMOVE_SIZE: usize = 5;
const COL_DM_DOMAIN: usize = 6;
const COL_DM_CONFIG: usize = 7;
const COL_DM_REMOVED: usize = 8;
const COL_TXN_APP_ID: usize = 9;
const COL_TXN_VERSION: usize = 10;
const COL_TXN_LAST_UPDATED: usize = 11;

/// Thin shim that pulls leaf values from `getters` and forwards them to the accumulator's
/// `on_*` methods. All behavior lives in [`CrcReplayAccumulator`].
struct CrcReplayVisitor<'a> {
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CrcReplayVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const LONG: DataType = DataType::LONG;
            const BOOLEAN: DataType = DataType::BOOLEAN;
            let types_and_names = vec![
                (STRING, column_name!("_file")),
                (STRING, column_name!("commitInfo.operation")),
                (LONG, column_name!("commitInfo.inCommitTimestamp")),
                (LONG, column_name!("add.size")),
                (STRING, column_name!("remove.path")),
                (LONG, column_name!("remove.size")),
                (STRING, column_name!("domainMetadata.domain")),
                (STRING, column_name!("domainMetadata.configuration")),
                (BOOLEAN, column_name!("domainMetadata.removed")),
                (STRING, column_name!("txn.appId")),
                (LONG, column_name!("txn.version")),
                (LONG, column_name!("txn.lastUpdated")),
            ];
            let (types, names): (Vec<_>, Vec<_>) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == COL_TXN_LAST_UPDATED + 1,
            Error::internal_error(format!(
                "Wrong number of CrcReplayVisitor getters: {}",
                getters.len()
            ))
        );
        if row_count == 0 {
            return Ok(());
        }
        // `_file` is constant across all rows of a batch per the JsonHandler contract. Read
        // once from row 0 and signal a potential file (commit) transition.
        let file_url: String = getters[COL_FILE].get(0, "_file")?;
        self.acc.process_batch_start(&file_url);

        for i in 0..row_count {
            let operation: Option<String> = getters[COL_OP].get_opt(i, "commitInfo.operation")?;
            let ict: Option<i64> = getters[COL_ICT].get_opt(i, "commitInfo.inCommitTimestamp")?;
            if operation.is_some() || ict.is_some() {
                self.acc.on_commit_info(operation.as_deref(), ict);
            }

            if let Some(size) = getters[COL_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.on_add(size)?;
            }

            let remove_path: Option<String> = getters[COL_REMOVE_PATH].get_opt(i, "remove.path")?;
            if let Some(path) = remove_path {
                let remove_size: Option<i64> =
                    getters[COL_REMOVE_SIZE].get_opt(i, "remove.size")?;
                self.acc.on_remove(&path, remove_size)?;
            }

            let dm_domain: Option<String> =
                getters[COL_DM_DOMAIN].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = dm_domain {
                let configuration: String =
                    getters[COL_DM_CONFIG].get(i, "domainMetadata.configuration")?;
                let removed: bool = getters[COL_DM_REMOVED].get(i, "domainMetadata.removed")?;
                self.acc.on_domain_metadata(domain, configuration, removed);
            }

            let txn_app_id: Option<String> = getters[COL_TXN_APP_ID].get_opt(i, "txn.appId")?;
            if let Some(app_id) = txn_app_id {
                let version: i64 = getters[COL_TXN_VERSION].get(i, "txn.version")?;
                let last_updated: Option<i64> =
                    getters[COL_TXN_LAST_UPDATED].get_opt(i, "txn.lastUpdated")?;
                self.acc.on_set_transaction(app_id, version, last_updated);
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
    use crate::crc::FileSizeHistogram;

    // ===== Unit tests on `on_*` methods =====

    // ===== commitInfo =====

    #[test]
    fn on_commit_info_safe_op_keeps_is_incremental_safe_true() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some("WRITE"), None);
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn on_commit_info_unsafe_op_trips_is_incremental_safe() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some("ANALYZE STATS"), None);
        assert!(!acc.delta.is_incremental_safe);
        assert!(!acc.current_commit_saw_safe_op);
    }

    #[test]
    fn on_commit_info_safe_op_marks_saw_safe_op() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some("WRITE"), None);
        assert!(acc.current_commit_saw_safe_op);
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn on_commit_info_ict_only_no_operation_does_not_mark_saw_safe_op() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(None, Some(1234));
        assert!(!acc.current_commit_saw_safe_op);
        assert!(acc.delta.is_incremental_safe);
        assert_eq!(acc.delta.in_commit_timestamp, Some(1234));
    }

    #[test]
    fn on_commit_info_captures_ict_only_on_first_commit() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.on_commit_info(Some("WRITE"), Some(2000));
        assert_eq!(acc.delta.in_commit_timestamp, Some(2000));
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), Some(1000));
        assert_eq!(acc.delta.in_commit_timestamp, Some(2000));
    }

    #[test]
    fn on_commit_info_first_commit_none_ict_does_not_get_overwritten_by_older() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.on_commit_info(Some("WRITE"), None);
        assert_eq!(acc.delta.in_commit_timestamp, None);
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), Some(1000));
        assert_eq!(acc.delta.in_commit_timestamp, None);
    }

    // ===== add =====

    #[test]
    fn on_add_increments_files_and_bytes() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(100).unwrap();
        acc.on_add(200).unwrap();
        assert_eq!(acc.delta.file_stats.net_files, 2);
        assert_eq!(acc.delta.file_stats.net_bytes, 300);
        assert!(acc.delta.is_incremental_safe);
    }

    // ===== remove =====

    #[test]
    fn on_remove_with_size_decrements_files_and_bytes() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_remove("p", Some(50)).unwrap();
        assert_eq!(acc.delta.file_stats.net_files, -1);
        assert_eq!(acc.delta.file_stats.net_bytes, -50);
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn on_remove_missing_size_trips_is_incremental_safe() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_remove("p", None).unwrap();
        assert!(!acc.delta.is_incremental_safe);
        assert!(acc.current_commit_saw_file_action);
    }

    // ===== domainMetadata =====

    #[test]
    fn on_domain_metadata_first_seen_in_reverse_wins() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_domain_metadata("d".into(), "new".into(), false);
        acc.on_domain_metadata("d".into(), "old".into(), false);
        assert_eq!(acc.delta.domain_metadata["d"].configuration(), "new");
        assert!(!acc.delta.domain_metadata["d"].is_removed());
    }

    #[test]
    fn on_domain_metadata_tombstone_is_kept() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_domain_metadata("d".into(), "v".into(), true);
        assert!(acc.delta.domain_metadata["d"].is_removed());
    }

    // ===== txn =====

    #[test]
    fn on_set_transaction_first_seen_in_reverse_wins() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_set_transaction("a".into(), 99, Some(123));
        acc.on_set_transaction("a".into(), 1, Some(0));
        assert_eq!(acc.delta.set_transactions["a"].version, 99);
    }

    // ===== auxiliary =====

    #[test]
    fn accumulator_with_seed_histogram_inserts_into_seeded_bin() {
        let seed = FileSizeHistogram::create_empty_with_boundaries(vec![0, 200, 1000]).unwrap();
        let mut acc = CrcReplayAccumulator::new(Some(seed));
        acc.on_add(150).unwrap();
        let hist = acc.delta.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries(), &[0, 200, 1000]);
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 150);
    }

    #[test]
    fn accumulator_with_no_seed_histogram_keeps_delta_histogram_none() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(150).unwrap();
        assert!(acc.delta.file_stats.net_histogram.is_none());
    }

    #[test]
    fn into_crc_delta_transfers_accumulated_state() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(42).unwrap();
        let delta = acc.into_crc_delta();
        assert_eq!(delta.file_stats.net_files, 1);
        assert_eq!(delta.file_stats.net_bytes, 42);
    }

    #[test]
    fn visitor_schema_length_matches_column_indices() {
        let mut acc = CrcReplayAccumulator::new(None);
        let visitor = CrcReplayVisitor { acc: &mut acc };
        let (names, types) = visitor.selected_column_names_and_types();
        assert_eq!(names.len(), COL_TXN_LAST_UPDATED + 1);
        assert_eq!(types.len(), COL_TXN_LAST_UPDATED + 1);
    }

    // ===== Commit-boundary state machine: direct accumulator tests =====
    //
    // `SyncEngine` emits one batch per file, so multi-batch-per-file scenarios are only
    // reachable by driving the accumulator directly.

    #[test]
    fn per_commit_invariant_holds_when_file_action_and_commit_info_split_across_batches_of_one_file(
    ) {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(0).unwrap();
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end();
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_no_safe_op_across_batches() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(0).unwrap();
        acc.process_batch_start("v1.json");
        acc.process_commit_file_end();
        assert!(!acc.delta.is_incremental_safe);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_commit_info_but_no_operation() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(100).unwrap();
        acc.on_commit_info(None, Some(42));
        acc.process_commit_file_end();
        assert!(!acc.delta.is_incremental_safe);
    }

    #[test]
    fn is_first_commit_stays_true_across_batches_of_same_file() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v2.json");
        assert!(acc.is_first_commit);
    }

    #[test]
    fn is_first_commit_becomes_false_after_file_transition() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v1.json");
        assert!(!acc.is_first_commit);
    }

    // ===== End-to-end smoke =====
    //
    // Tests all aspects of the full pipeline via the visitor.

    #[tokio::test]
    async fn end_to_end_smoke_full_coverage() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use test_utils::add_commit;

        use crate::crc::{DomainMetadataState, FileStats, FileStatsState, SetTransactionState};
        use crate::engine::sync::SyncEngine;
        use crate::object_store::memory::InMemory;
        use crate::table_features::TableFeature;

        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let root = "memory:///t/";

        // v0: bootstrap. Protocol already supports `domainMetadata` and `inCommitTimestamp`
        // so the v1 DM action and v2 commitInfo with ICT are well-formed. v0 is outside
        // the replay range; the segment covers (0, 2].
        add_commit(root, store.as_ref(), 0, r#"
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["domainMetadata","inCommitTimestamp"]}}
{"metaData":{"id":"id-0","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":0}}
"#.to_string()).await.unwrap();

        // v1: DM add, txn add, four file adds spanning histogram bins 0 (sizes 100, 200),
        // 1 (size 10000), and 2 (size 20000).
        add_commit(
            root,
            store.as_ref(),
            1,
            r#"
{"add":{"path":"a","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true}}
{"add":{"path":"b","partitionValues":{},"size":200,"modificationTime":1,"dataChange":true}}
{"add":{"path":"c","partitionValues":{},"size":10000,"modificationTime":1,"dataChange":true}}
{"add":{"path":"d","partitionValues":{},"size":20000,"modificationTime":1,"dataChange":true}}
{"domainMetadata":{"domain":"keep","configuration":"cfg","removed":false}}
{"txn":{"appId":"app1","version":1,"lastUpdated":1}}
{"commitInfo":{"timestamp":1,"operation":"WRITE"}}
"#
            .to_string(),
        )
        .await
        .unwrap();

        // v2 (newest): protocol upgrade that adds `rowTracking` on top of v0's features,
        // new metadata, DM tombstone, second txn, one remove (bin 0), commitInfo with ICT.
        add_commit(root, store.as_ref(), 2, r#"
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["domainMetadata","inCommitTimestamp","rowTracking"]}}
{"metaData":{"id":"id-2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.enableRowTracking":"true"},"createdTime":2}}
{"remove":{"path":"a","deletionTimestamp":2,"dataChange":true,"size":100}}
{"domainMetadata":{"domain":"drop","configuration":"","removed":true}}
{"txn":{"appId":"app2","version":5,"lastUpdated":2}}
{"commitInfo":{"timestamp":2,"operation":"WRITE","inCommitTimestamp":9999}}
"#.to_string()).await.unwrap();

        let log_root = url::Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let segment = LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root,
            vec![],
            None,
            Some(2),
        )
        .unwrap();

        // A seed histogram is required for the replay to track histogram bin updates.
        let base = Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 0,
                table_size_bytes: 0,
                file_size_histogram: Some(FileSizeHistogram::create_default()),
            }),
            domain_metadata_state: DomainMetadataState::Complete(HashMap::new()),
            set_transaction_state: SetTransactionState::Complete(HashMap::new()),
            ..Default::default()
        };
        let crc = segment
            .build_incremental_crc_from_stale(&engine, &base)
            .unwrap();

        // Newest-wins: v2's upgraded protocol (now carries `rowTracking` on top of v0's
        // features), v2's metadata, v2's ICT.
        assert_eq!(crc.version, 2);
        assert_eq!(crc.protocol.min_writer_version(), 7);
        assert!(crc.protocol.has_table_feature(&TableFeature::RowTracking));
        assert!(crc
            .protocol
            .has_table_feature(&TableFeature::InCommitTimestamp));
        assert!(crc
            .protocol
            .has_table_feature(&TableFeature::DomainMetadata));
        assert_eq!(crc.metadata.id(), "id-2");
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));

        // DM: "keep" inserted from v1; "drop" tombstone applied (no-op on empty base).
        let dm = crc.domain_metadata_state.expect_complete();
        assert_eq!(dm.get("keep").unwrap().configuration(), "cfg");
        assert!(!dm.contains_key("drop"));

        // Both txns upserted across the two commits.
        let txn = crc.set_transaction_state.expect_complete();
        assert_eq!(txn.get("app1").unwrap().version, 1);
        assert_eq!(txn.get("app2").unwrap().version, 5);

        // 4 adds spanning bins 0/1/2 minus 1 remove in bin 0:
        //   bin 0: +2 files (100 + 200 = 300 bytes) - 1 file (100 bytes) = 1 file, 200 bytes
        //   bin 1: +1 file (10000 bytes)
        //   bin 2: +1 file (20000 bytes)
        // Totals: 3 files, 30200 bytes.
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 3);
        assert_eq!(stats.table_size_bytes(), 30_200);
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 200);
        assert_eq!(hist.file_counts()[1], 1);
        assert_eq!(hist.total_bytes()[1], 10_000);
        assert_eq!(hist.file_counts()[2], 1);
        assert_eq!(hist.total_bytes()[2], 20_000);
    }

    #[tokio::test]
    async fn build_incremental_crc_delta_errors_when_base_version_geq_end_version() {
        use std::sync::Arc;

        use test_utils::add_commit;

        use crate::engine::sync::SyncEngine;
        use crate::object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let root = "memory:///t/";
        add_commit(
            root,
            store.as_ref(),
            0,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        )
        .await
        .unwrap();
        let log_root = url::Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let segment = LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root,
            vec![],
            None,
            Some(0),
        )
        .unwrap();
        for base in [0, 5] {
            let err = segment
                .build_incremental_crc_delta(&engine, base, None)
                .unwrap_err();
            assert!(
                matches!(err, Error::InternalError(_)),
                "base={base}: {err:?}"
            );
        }
    }
}
