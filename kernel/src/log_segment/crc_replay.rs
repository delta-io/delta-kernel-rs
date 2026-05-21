// `build_crc_from_stale` is exercised by the in-file test module but has no production
// caller yet. Snapshot wiring lands in a follow-up PR.
#![cfg_attr(not(test), allow(dead_code))]

//! Reverse log replay for stale-CRC catch-up.
//!
//! Given a stale `Crc` at version `X`, this module builds the `Crc` at version `N` by
//! reverse-replaying a log segment's commit files. Internally it accumulates a [`CrcDelta`]
//! over commits `(X, N]`, then applies it via [`Crc::apply`].
//!
//! Entry point: [`LogSegment::build_crc_from_stale`].
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
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, RowVisitor};

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
    /// Reverse-replay this log segment, producing a fresh `Crc` at `self.end_version` by
    /// applying the changes in commits `(base.version, self.end_version]` to `base`.
    ///
    /// The strategy: hand all commit files in `(base.version, self.end_version]` to
    /// [`Engine`] for streaming reads (newest first), then build up a [`CrcDelta`] in one
    /// shared [`CrcReplayAccumulator`] across all batches. Per batch, a transient
    /// [`CrcReplayVisitor`] pulls leaf values from the row data and forwards them to the
    /// accumulator's `on_*` methods. For per-key fields (Protocol, Metadata, DomainMetadata,
    /// SetTransaction) the first observation wins; older commits cannot override it. ICT is
    /// special: it is captured only from the newest commit. Once the delta is built, it is
    /// applied to a clone of `base` via [`Crc::apply`] and the result's `version` is
    /// stamped to `self.end_version`.
    ///
    /// # Preconditions
    ///
    /// 1. The caller must skip calling this when `base.version == self.end_version` (the resulting
    ///    `Crc` would just be a clone of `base`).
    /// 2. `self.listed.ascending_commit_files` must cover every commit in `(base.version,
    ///    self.end_version]`. The typical caller arranges this with
    ///    [`LogSegment::segment_after_crc`] when a checkpoint sits above `base.version`.
    ///
    /// Returns `Error::InternalError` when either precondition fails.
    #[instrument(name = "log_seg.build_crc_from_stale", skip_all, err)]
    pub(crate) fn build_crc_from_stale(&self, engine: &dyn Engine, base: &Crc) -> DeltaResult<Crc> {
        require!(
            base.version < self.end_version,
            Error::internal_error(format!(
                "build_crc_from_stale: base.version ({}) must be strictly less than \
                 end_version ({})",
                base.version, self.end_version,
            ))
        );
        let filtered: Vec<_> = self
            .listed
            .ascending_commit_files
            .iter()
            .filter(|c| c.version > base.version)
            .collect();
        // The first commit above base.version must be base.version + 1; otherwise the
        // segment is missing intermediate commits and we'd produce an incorrect result.
        let first_above = filtered.first().map(|c| c.version);
        require!(
            first_above == Some(base.version + 1),
            Error::internal_error(format!(
                "build_crc_from_stale: segment is missing commit {} (lowest commit \
                 above base.version is {:?})",
                base.version + 1,
                first_above,
            ))
        );

        // Empty histogram seeded with the base's bin boundaries so it merges cleanly in
        // `Crc::apply`. If the base has no histogram, we use None and skip histogram
        // tracking on the delta.
        let seed_histogram = base
            .file_stats()
            .and_then(|s| s.file_size_histogram())
            .map(|h| {
                FileSizeHistogram::create_empty_with_boundaries(h.sorted_bin_boundaries().to_vec())
            })
            .transpose()?;
        let mut acc = CrcReplayAccumulator::new(seed_histogram);

        let files: Vec<_> = filtered.iter().rev().map(|c| c.location.clone()).collect();
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

        let mut crc = base.clone();
        crc.apply(acc.into_crc_delta());
        crc.version = self.end_version;
        Ok(crc)
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
    /// `current_commit_saw_commit_info` for the per-commit invariant check.
    current_commit_saw_file_action: bool,
    /// True if the current commit had a commitInfo row. Without one we can't classify the
    /// operation as incremental-safe, so the per-commit invariant defaults the delta to
    /// non-incremental-safe.
    current_commit_saw_commit_info: bool,
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
            current_commit_saw_commit_info: false,
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
        // File actions without a commitInfo: we can't classify the operation, so the
        // delta is no longer incremental-safe. `is_incremental_safe` is one-way (once
        // false, never restored).
        if self.current_commit_saw_file_action && !self.current_commit_saw_commit_info {
            warn!(
                "CRC reverse-replay: commit at {} carried file actions but no commitInfo; \
                 defaulting to non-incremental-safe",
                self.current_file_url.as_deref().unwrap_or("?")
            );
            self.delta.is_incremental_safe = false;
        }
        self.current_commit_saw_file_action = false;
        self.current_commit_saw_commit_info = false;
    }

    // === Row-level updates -- These accessors also make testing easier ===

    fn on_commit_info(&mut self, operation: Option<&str>, ict: Option<i64>) {
        self.current_commit_saw_commit_info = true;
        if let Some(op) = operation {
            if !is_incremental_safe_operation(op) {
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

// === Visitor column indices ===
// Must match the order in `selected_column_names_and_types` below.
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
            (
                vec![
                    ColumnName::new(["_file"]),
                    ColumnName::new(["commitInfo", "operation"]),
                    ColumnName::new(["commitInfo", "inCommitTimestamp"]),
                    ColumnName::new(["add", "size"]),
                    ColumnName::new(["remove", "path"]),
                    ColumnName::new(["remove", "size"]),
                    ColumnName::new(["domainMetadata", "domain"]),
                    ColumnName::new(["domainMetadata", "configuration"]),
                    ColumnName::new(["domainMetadata", "removed"]),
                    ColumnName::new(["txn", "appId"]),
                    ColumnName::new(["txn", "version"]),
                    ColumnName::new(["txn", "lastUpdated"]),
                ],
                vec![
                    DataType::STRING,
                    DataType::STRING,
                    DataType::LONG,
                    DataType::LONG,
                    DataType::STRING,
                    DataType::LONG,
                    DataType::STRING,
                    DataType::STRING,
                    DataType::BOOLEAN,
                    DataType::STRING,
                    DataType::LONG,
                    DataType::LONG,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
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

    // === Unit tests on `on_*` methods, in visitor column order ===

    // commitInfo

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
        assert!(acc.current_commit_saw_commit_info);
    }

    #[test]
    fn on_commit_info_ict_only_no_operation_still_marks_seen() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(None, Some(1234));
        assert!(acc.current_commit_saw_commit_info);
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

    // add

    #[test]
    fn on_add_increments_files_and_bytes() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(100).unwrap();
        acc.on_add(200).unwrap();
        assert_eq!(acc.delta.file_stats.net_files, 2);
        assert_eq!(acc.delta.file_stats.net_bytes, 300);
        assert!(acc.delta.is_incremental_safe);
    }

    // remove

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

    // domainMetadata

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

    // txn

    #[test]
    fn on_set_transaction_first_seen_in_reverse_wins() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_set_transaction("a".into(), 99, Some(123));
        acc.on_set_transaction("a".into(), 1, Some(0));
        assert_eq!(acc.delta.set_transactions["a"].version, 99);
    }

    // auxiliary

    #[test]
    fn histogram_inherits_seed_boundaries() {
        let seed = FileSizeHistogram::create_empty_with_boundaries(vec![0, 200, 1000]).unwrap();
        let mut acc = CrcReplayAccumulator::new(Some(seed));
        acc.on_add(150).unwrap();
        let hist = acc.delta.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries(), &[0, 200, 1000]);
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 150);
    }

    #[test]
    fn no_seed_histogram_means_no_delta_histogram() {
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

    // === Commit-boundary state machine — direct accumulator tests ===
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
    fn per_commit_invariant_trips_when_file_action_has_no_commit_info_across_batches() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(0).unwrap();
        acc.process_batch_start("v1.json");
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
}
