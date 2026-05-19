// `build_crc_delta_from_stale` and its supporting types are exercised by the in-file
// test module but have no production caller yet. Snapshot wiring lands in PR5.
#![cfg_attr(not(test), allow(dead_code))]

//! Reverse log replay accumulator for stale-CRC catch-up.
//!
//! Given a stale CRC at version `X` and a snapshot at version `N`, this module produces
//! a `CrcDelta` covering commits `(X, N]` such that `Crc::apply(delta)` advances the
//! base CRC to the end version of the delta. For per-key fields (Protocol, Metadata,
//! DomainMetadata, SetTransaction, ICT), commits are iterated newest-first and the
//! first observation is the final answer — later (older) commits cannot override it.
//! For ICT specifically, only the first commit contributes: if it has no commitInfo
//! at all, the delta's ICT is `None`, regardless of what older commits carried.
//!
//! Entry point: [`LogSegment::build_crc_delta_from_stale`].
//!
//! # Preconditions
//!
//! The caller is responsible for:
//! 1. Pruning the segment to commits in `(X, N]`, typically via [`LogSegment::segment_after_crc`].
//! 2. Short-circuiting `X == N` before invoking this method (just clone the base).
//!
//! # Per-commit boundaries
//!
//! All commit files are passed to `JsonHandler::read_json_files` in a single call so the
//! engine can parallelize reads. The schema projects a `_file` metadata column; the
//! visitor watches for URL transitions to detect commit boundaries (one commit per
//! plain commit file). Compaction files are skipped (TODO: support by counting
//! commitInfos within each compacted file).

use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock};

use tracing::{instrument, warn};

use super::LogSegment;
use crate::actions::{
    get_commit_schema, DomainMetadata, Metadata, Protocol, SetTransaction, ADD_NAME,
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SET_TRANSACTION_NAME,
};
use crate::crc::{Crc, CrcDelta, FileSizeHistogram, FileStatsDelta};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef};
use crate::{DeltaResult, Engine, Error, RowVisitor, Version};

fn crc_replay_schema() -> DeltaResult<SchemaRef> {
    let projected = get_commit_schema().project_as_struct(&[
        ADD_NAME,
        REMOVE_NAME,
        PROTOCOL_NAME,
        METADATA_NAME,
        SET_TRANSACTION_NAME,
        DOMAIN_METADATA_NAME,
        COMMIT_INFO_NAME,
    ])?;
    // `_file` lets the visitor detect commit-file boundaries within the batch stream.
    let with_file = projected.add_metadata_column("_file", MetadataColumnSpec::FilePath)?;
    Ok(Arc::new(with_file))
}

impl LogSegment {
    /// Reverse-replay this log segment, producing a [`CrcDelta`] that advances `base` (at
    /// `base_version`) to `self.end_version`. See the [module docs](self) for preconditions.
    ///
    /// Errors if `base_version >= self.end_version` (caller should short-circuit `X == N`
    /// before calling; `>` is malformed input).
    // TODO: once `Crc` carries its version (loaded from the `.crc` filename), drop the
    //       `base_version` parameter and use `base.version` directly. Avoids the footgun
    //       of mismatching `base` and `base_version`.
    #[instrument(name = "log_seg.build_crc_delta_from_stale", skip_all, err)]
    pub(crate) fn build_crc_delta_from_stale(
        &self,
        engine: &dyn Engine,
        base: &Crc,
        base_version: Version,
    ) -> DeltaResult<CrcDelta> {
        if base_version >= self.end_version {
            return Err(Error::internal_error(format!(
                "build_crc_delta_from_stale: base_version ({base_version}) must be strictly \
                 less than end_version ({})",
                self.end_version,
            )));
        }
        // Empty histogram with the base's bucket boundaries so `Crc::apply`'s merge
        // lines up. `None` if the base has no histogram (skip histogram entirely).
        let seed_histogram = base
            .file_stats()
            .and_then(|s| s.file_size_histogram())
            .map(|h| {
                FileSizeHistogram::create_empty_with_boundaries(h.sorted_bin_boundaries().to_vec())
            })
            .transpose()?;
        let mut acc = CrcReplayAccumulator::with_histogram(seed_histogram);

        // Read only commits strictly above `base_version`, newest first. Compactions are
        // skipped (see module docs).
        let files: Vec<_> = self
            .listed
            .ascending_commit_files
            .iter()
            .filter(|c| c.version > base_version)
            .rev()
            .map(|c| c.location.clone())
            .collect();
        let batches = engine
            .json_handler()
            .read_json_files(&files, crc_replay_schema()?, None)?;

        for batch_result in batches {
            let data = batch_result?;
            let data = data.as_ref();

            // Re-use existing extractors for Protocol and Metadata rather than custom
            // parsing in our visitor.
            if acc.delta.protocol.is_none() {
                acc.delta.protocol = Protocol::try_new_from_data(data)?;
            }
            if acc.delta.metadata.is_none() {
                acc.delta.metadata = Metadata::try_new_from_data(data)?;
            }

            let mut visitor = CrcReplayVisitor { acc: &mut acc };
            visitor.visit_rows_of(data)?;
        }

        // Run the per-commit invariant on the final commit (no successor to trigger it).
        acc.process_commit_file_end();

        Ok(acc.into_crc_delta())
    }
}

// ============================================================================
// Accumulator
// ============================================================================

/// In-progress [`CrcDelta`] plus the scaffolding needed to build it correctly during
/// reverse replay. After all batches are folded in and `process_commit_file_end` runs
/// for the final commit, the embedded `delta` IS the result.
struct CrcReplayAccumulator {
    delta: CrcDelta,

    /// Are we still on the first (in reverse iteration order = newest) commit? Gates
    /// ICT capture so only the latest commit contributes to
    /// [`CrcDelta::in_commit_timestamp`].
    is_first_commit: bool,

    /// URL of the commit file whose batches are currently being processed. A change in
    /// URL across batches signals the start of a new commit.
    current_file_url: Option<String>,
    /// Did the current commit (across all its batches) carry any add or remove row?
    current_commit_saw_file_action: bool,
    /// Did the current commit (across all its batches) carry a commitInfo row?
    current_commit_saw_commit_info: bool,
}

impl CrcReplayAccumulator {
    /// `seed_histogram` is `Some` with the base CRC's bin boundaries so the resulting
    /// delta histogram merges cleanly in [`Crc::apply`]; `None` when the base lacks one
    /// (histogram tracking deferred to a future full-rebuild path).
    fn with_histogram(seed_histogram: Option<FileSizeHistogram>) -> Self {
        Self {
            delta: CrcDelta {
                file_stats: FileStatsDelta {
                    net_files: 0,
                    net_bytes: 0,
                    net_histogram: seed_histogram,
                },
                is_incremental_safe: true,
                ..Default::default()
            },
            is_first_commit: true,
            current_file_url: None,
            current_commit_saw_file_action: false,
            current_commit_saw_commit_info: false,
        }
    }

    /// Visitor calls this with each batch's `_file` URL. On a file-to-file transition,
    /// runs the prior commit's per-commit invariant via [`process_commit_file_end`] and
    /// sets `is_first_commit = false` so subsequent (older) commits skip the ICT write.
    fn process_batch_start(&mut self, batch_file_url: String) {
        let is_new_file = self.current_file_url.as_deref() != Some(batch_file_url.as_str());
        if !is_new_file {
            return;
        }
        // Skipped on the very first call (no prior commit; `is_first_commit` stays true).
        if self.current_file_url.is_some() {
            self.process_commit_file_end();
            self.is_first_commit = false;
        }
        self.current_file_url = Some(batch_file_url);
    }

    /// If the commit had file actions but no commitInfo, we cannot classify its operation
    /// (`is_incremental_safe(op)`), so conservatively default to unsafe. Warns and resets
    /// per-commit flags for the next commit.
    fn process_commit_file_end(&mut self) {
        if self.current_commit_saw_file_action && !self.current_commit_saw_commit_info {
            warn!(
                "CRC reverse-replay: commit at {:?} carried file actions but no commitInfo; \
                 defaulting to non-incremental-safe",
                self.current_file_url
            );
            self.delta.is_incremental_safe = false;
        }
        self.current_commit_saw_file_action = false;
        self.current_commit_saw_commit_info = false;
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

/// Single-batch visitor that folds row-level data into the accumulator.
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
        // `_file` is constant across all rows of a batch per the JsonHandler contract.
        // Read once from row 0 and signal a potential file (commit) transition.
        let file_url: String = getters[COL_FILE].get(0, "_file")?;
        self.acc.process_batch_start(file_url);

        let file_stats = &mut self.acc.delta.file_stats;
        for i in 0..row_count {
            // commitInfo: operation safety + ICT for the first (newest) commit only.
            let operation: Option<String> = getters[COL_OP].get_opt(i, "commitInfo.operation")?;
            if let Some(op) = operation {
                self.acc.current_commit_saw_commit_info = true;
                if !FileStatsDelta::is_incremental_safe(&op) {
                    warn!("CRC reverse-replay: non-incremental op {op:?}");
                    self.acc.delta.is_incremental_safe = false;
                }
                // ICT capture is unchecked here on purpose: we do not flip
                // `is_incremental_safe` (which is for file-stats tracking, not ICT) and
                // we do not cross-reference the table's protocol. A newest commit that
                // omits `inCommitTimestamp` on an ICT-enabled table will produce a delta
                // whose `in_commit_timestamp = None`, and the apply step will propagate
                // that `None` into the CRC. The catch-net is `try_write_crc_file` in
                // `crc/writer.rs`, which rejects writing a CRC whose ICT field
                // disagrees with the protocol's `inCommitTimestamp` feature enablement.
                if self.acc.is_first_commit {
                    self.acc.delta.in_commit_timestamp =
                        getters[COL_ICT].get_opt(i, "commitInfo.inCommitTimestamp")?;
                }
            }

            // Add: net file count and bytes contribution. The histogram insert uses bin
            // boundaries seeded from the base CRC; skipped when base has no histogram.
            if let Some(size) = getters[COL_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.current_commit_saw_file_action = true;
                let size: i64 = size;
                file_stats.net_files += 1;
                file_stats.net_bytes += size;
                if let Some(hist) = file_stats.net_histogram.as_mut() {
                    hist.insert(size)?;
                }
            }

            // Remove: `remove.path` detects presence; null `remove.size` alongside non-null
            // path means "remove with missing size", which makes incremental tracking
            // impossible.
            let remove_path: Option<String> = getters[COL_REMOVE_PATH].get_opt(i, "remove.path")?;
            if let Some(path) = remove_path {
                self.acc.current_commit_saw_file_action = true;
                let remove_size: Option<i64> =
                    getters[COL_REMOVE_SIZE].get_opt(i, "remove.size")?;
                if let Some(size) = remove_size {
                    file_stats.net_files -= 1;
                    file_stats.net_bytes -= size;
                    if let Some(hist) = file_stats.net_histogram.as_mut() {
                        hist.remove(size)?;
                    }
                } else {
                    warn!("CRC reverse-replay: remove action at {path:?} has missing size");
                    self.acc.delta.is_incremental_safe = false;
                }
            }

            // DomainMetadata: first-seen-wins per domain. Tombstones (`removed=true`) are
            // kept; [`Crc::apply`] consumes them as removals from the base map.
            let dm_domain: Option<String> =
                getters[COL_DM_DOMAIN].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = dm_domain {
                if let Entry::Vacant(e) = self.acc.delta.domain_metadata.entry(domain.clone()) {
                    let configuration: String = getters[COL_DM_CONFIG]
                        .get_opt(i, "domainMetadata.configuration")?
                        .unwrap_or_default();
                    let removed: bool = getters[COL_DM_REMOVED]
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
            let txn_app_id: Option<String> = getters[COL_TXN_APP_ID].get_opt(i, "txn.appId")?;
            if let Some(app_id) = txn_app_id {
                if let Entry::Vacant(e) = self.acc.delta.set_transactions.entry(app_id.clone()) {
                    let version: i64 = getters[COL_TXN_VERSION].get(i, "txn.version")?;
                    let last_updated: Option<i64> =
                        getters[COL_TXN_LAST_UPDATED].get_opt(i, "txn.lastUpdated")?;
                    e.insert(SetTransaction::new(app_id, version, last_updated));
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
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::{json, Value};
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;
    use crate::crc::{DomainMetadataState, FileStats, FileStatsState, SetTransactionState};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStoreExt as _;
    use crate::Snapshot;

    // ===== JSON action builders =====

    /// Permissive protocol: writer version 7 with `domainMetadata` and `inCommitTimestamp`
    /// writer features enabled. Reader stays at v1 since neither is a reader feature.
    fn protocol_action() -> Value {
        json!({"protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 7,
            "writerFeatures": ["domainMetadata", "inCommitTimestamp"]
        }})
    }

    fn metadata_action_with_id(id: &str) -> Value {
        json!({"metaData": {
            "id": id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": r#"{"type":"struct","fields":[]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }})
    }

    fn commit_info(op: &str) -> Value {
        json!({"commitInfo": {"timestamp": 0, "operation": op}})
    }

    fn commit_info_with_ict(op: &str, ict: i64) -> Value {
        json!({"commitInfo": {"timestamp": 0, "operation": op, "inCommitTimestamp": ict}})
    }

    fn add(path: &str, size: i64) -> Value {
        json!({"add": {
            "path": path,
            "size": size,
            "partitionValues": {},
            "modificationTime": 0,
            "dataChange": true
        }})
    }

    /// Remove with a present `size`.
    fn remove_with_size(path: &str, size: i64) -> Value {
        json!({"remove": {
            "path": path,
            "size": size,
            "dataChange": true,
            "deletionTimestamp": 0
        }})
    }

    /// Remove with `path` set but `size` missing. The visitor flips
    /// `delta.is_incremental_safe = false` when it encounters this row.
    fn remove_without_size(path: &str) -> Value {
        json!({"remove": {
            "path": path,
            "dataChange": true,
            "deletionTimestamp": 0
        }})
    }

    fn domain_metadata(domain: &str, config: &str, removed: bool) -> Value {
        json!({"domainMetadata": {
            "domain": domain,
            "configuration": config,
            "removed": removed
        }})
    }

    fn set_transaction(app_id: &str, version: i64) -> Value {
        json!({"txn": {"appId": app_id, "version": version, "lastUpdated": 0}})
    }

    /// CREATE TABLE commit 0: protocol + metadata + commitInfo.
    fn create_table_v0() -> Vec<Value> {
        vec![
            protocol_action(),
            metadata_action_with_id("test-table"),
            commit_info("CREATE TABLE"),
        ]
    }

    // ===== Store helpers =====

    async fn write_commit(store: &InMemory, version: u64, actions: Vec<Value>) {
        let body = actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        store
            .put(
                &delta_path_for_version(version, "json"),
                body.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
    }

    /// Build an in-memory table with `[create_table_v0(), commits[0], commits[1], ...]`,
    /// returning an engine and the snapshot's log segment. Tests pass `base_version = 0`
    /// to [`LogSegment::build_crc_delta_from_stale`] so commit 0 (CREATE TABLE) is
    /// filtered out and `commits` form the replay range `(0, latest]`.
    async fn setup_table(commits: Vec<Vec<Value>>) -> (SyncEngine, LogSegment) {
        let store = Arc::new(InMemory::new());
        write_commit(&store, 0, create_table_v0()).await;
        for (i, actions) in commits.into_iter().enumerate() {
            write_commit(&store, (i + 1) as u64, actions).await;
        }
        let engine = SyncEngine::new_with_store(store);
        let url = Url::parse("memory:///").unwrap();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        (engine, snapshot.log_segment().clone())
    }

    // ===== Base CRC builders =====

    /// `Complete` base CRC with given counts; no histogram.
    fn complete_base(num_files: i64, table_size_bytes: i64) -> Crc {
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files,
                table_size_bytes,
                file_size_histogram: None,
            }),
            ..Default::default()
        }
    }

    /// `Complete` base CRC with a histogram populated from `file_sizes`.
    fn complete_base_with_histogram(file_sizes: &[i64], boundaries: Option<Vec<i64>>) -> Crc {
        let mut hist = match boundaries {
            Some(b) => FileSizeHistogram::create_empty_with_boundaries(b).unwrap(),
            None => FileSizeHistogram::create_default(),
        };
        for &s in file_sizes {
            hist.insert(s).unwrap();
        }
        let total: i64 = file_sizes.iter().sum();
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: file_sizes.len() as i64,
                table_size_bytes: total,
                file_size_histogram: Some(hist),
            }),
            ..Default::default()
        }
    }

    // ===== Tests =====

    /// File-stats accumulation from a single safe-op commit. Histogram-bin coverage lives
    /// in `histogram_boundaries_inherited_from_base`; here we focus on net counts plus the
    /// apply round-trip onto the base.
    #[rstest]
    #[case::adds_only(
        vec![add("a", 100), add("b", 200), commit_info("WRITE")],
        complete_base(0, 0),
        (2, 300),  // delta net (files, bytes)
        (2, 300),  // applied total (files, bytes)
    )]
    #[case::adds_and_removes(
        vec![
            add("a", 100),
            add("b", 200),
            remove_with_size("old", 50),
            commit_info("WRITE"),
        ],
        complete_base(1, 50),
        (1, 250),  // +2 -1, +300 -50
        (2, 300),  // base + delta
    )]
    #[tokio::test]
    async fn single_safe_commit_accumulates_file_stats(
        #[case] actions: Vec<Value>,
        #[case] base: Crc,
        #[case] expected_delta: (i64, i64),
        #[case] expected_applied: (i64, i64),
    ) {
        let (engine, segment) = setup_table(vec![actions]).await;
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert_eq!(
            (delta.file_stats.net_files, delta.file_stats.net_bytes),
            expected_delta
        );
        assert!(delta.is_incremental_safe);

        let mut applied = base.clone();
        applied.apply(delta);
        let stats = applied.file_stats().unwrap();
        assert_eq!(
            (stats.num_files(), stats.table_size_bytes()),
            expected_applied
        );
    }

    #[tokio::test]
    async fn two_safe_commits_accumulate_file_stats_correctly() {
        let (engine, segment) = setup_table(vec![
            vec![add("a", 100), commit_info("WRITE")],
            vec![add("b", 200), add("c", 300), commit_info("WRITE")],
        ])
        .await;

        let base = complete_base(0, 0);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();

        assert_eq!(delta.file_stats.net_files, 3);
        assert_eq!(delta.file_stats.net_bytes, 600);
        assert!(delta.is_incremental_safe);
    }

    /// Apply transitions to `Indeterminate` whenever the delta carries any signal that
    /// breaks incremental tracking. The three signal sources all fold into the single
    /// `is_incremental_safe = false` output.
    #[rstest]
    #[case::unsafe_op(vec![add("a", 100), commit_info("ANALYZE STATS")])]
    #[case::missing_commit_info(vec![add("a", 100), remove_with_size("old", 50)])]
    #[case::remove_with_null_size(vec![remove_without_size("orphan"), commit_info("WRITE")])]
    #[tokio::test]
    async fn commit_signal_transitions_apply_to_indeterminate(#[case] actions: Vec<Value>) {
        let (engine, segment) = setup_table(vec![actions]).await;
        let mut base = complete_base(5, 500);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert!(!delta.is_incremental_safe);

        base.apply(delta);
        assert!(base.file_stats_state().is_indeterminate());
    }

    #[tokio::test]
    async fn dm_upsert_first_seen_in_reverse_wins() {
        // Commit 1: dm("d", "v1"). Commit 2: dm("d", "v2"). Reverse iteration sees commit 2
        // first, so the delta carries "v2".
        let (engine, segment) = setup_table(vec![
            vec![domain_metadata("d", "v1", false), commit_info("WRITE")],
            vec![domain_metadata("d", "v2", false), commit_info("WRITE")],
        ])
        .await;

        let mut base = complete_base(0, 0);
        base.domain_metadata_state = DomainMetadataState::Complete(Default::default());
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();

        assert_eq!(delta.domain_metadata.len(), 1);
        let dm = &delta.domain_metadata["d"];
        assert_eq!(dm.configuration(), "v2");
        assert!(!dm.is_removed());

        base.apply(delta);
        let map = base.domain_metadata_state.expect_complete();
        assert_eq!(map["d"].configuration(), "v2");
    }

    #[tokio::test]
    async fn dm_tombstone_in_delta_removes_from_base() {
        // Base has dm("d"); commit 1 tombstones it.
        let (engine, segment) = setup_table(vec![vec![
            domain_metadata("d", "{}", true), // tombstone
            commit_info("WRITE"),
        ]])
        .await;

        let mut base = complete_base(0, 0);
        base.domain_metadata_state = DomainMetadataState::Complete(
            [(
                "d".to_string(),
                DomainMetadata::new("d".to_string(), "stale".to_string()),
            )]
            .into(),
        );
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert_eq!(delta.domain_metadata.len(), 1);
        assert!(delta.domain_metadata["d"].is_removed());

        base.apply(delta);
        assert!(base.domain_metadata_state.expect_complete().is_empty());
    }

    #[tokio::test]
    async fn txn_first_seen_in_reverse_wins() {
        let (engine, segment) = setup_table(vec![
            vec![set_transaction("a", 1), commit_info("WRITE")],
            vec![set_transaction("a", 99), commit_info("WRITE")],
        ])
        .await;

        let mut base = complete_base(0, 0);
        base.set_transaction_state = SetTransactionState::Complete(Default::default());
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();

        assert_eq!(delta.set_transactions.len(), 1);
        assert_eq!(delta.set_transactions["a"].version, 99);

        base.apply(delta);
        let map = base.set_transaction_state.expect_complete();
        assert_eq!(map["a"].version, 99);
    }

    /// ICT capture from commitInfo and apply behavior. `with_ict` exercises captured
    /// `Some(ts)`. `without_ict` exercises captured `None` (commitInfo present, no
    /// `inCommitTimestamp` field). `two_commits_newest_wins` exercises first-seen-in-reverse
    /// iteration. `no_commit_info_at_all` exercises the degenerate case where no commitInfo
    /// is observed across the segment; the captured value stays `None`. In all cases apply
    /// replaces the base ICT unconditionally with the captured value.
    #[rstest]
    #[case::with_ict(
        vec![vec![commit_info_with_ict("WRITE", 1000)]],
        Some(7777),
        Some(1000),
        Some(1000),
    )]
    #[case::without_ict(
        vec![vec![commit_info("WRITE")]],
        Some(7777),
        None,
        None,
    )]
    #[case::two_commits_newest_wins(
        vec![
            vec![commit_info_with_ict("WRITE", 1000)],
            vec![commit_info_with_ict("WRITE", 2000)],
        ],
        Some(9999),
        Some(2000),
        Some(2000),
    )]
    #[case::newest_without_ict_clears_older_ict(
        // Older commit had ICT, newest does not. Newest wins via first-seen-in-reverse:
        // the captured value is `None` and the older commit's ICT is ignored.
        vec![
            vec![commit_info_with_ict("WRITE", 1000)],
            vec![commit_info("WRITE")],
        ],
        Some(7777),
        None,
        None,
    )]
    #[case::newest_commit_has_no_commit_info_older_has_ict(
        // The newest commit (txn-only, no commitInfo at all) determines ICT; the older
        // commit's ICT is irrelevant. Final ICT must be None even though the older commit
        // carries ICT-A=1000.
        vec![
            vec![commit_info_with_ict("WRITE", 1000)],
            vec![set_transaction("a", 1)],
        ],
        Some(7777),
        None,
        None,
    )]
    #[case::no_commit_info_at_all(
        vec![vec![set_transaction("a", 1)]],
        Some(7777),
        None,
        None,
    )]
    #[tokio::test]
    async fn delta_in_commit_timestamp_observation(
        #[case] commits: Vec<Vec<Value>>,
        #[case] base_ict: Option<i64>,
        #[case] expected_delta_ict: Option<i64>,
        #[case] expected_applied_ict: Option<i64>,
    ) {
        let (engine, segment) = setup_table(commits).await;
        let mut base = complete_base(0, 0);
        base.in_commit_timestamp_opt = base_ict;
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert_eq!(delta.in_commit_timestamp, expected_delta_ict);

        base.apply(delta);
        assert_eq!(base.in_commit_timestamp_opt, expected_applied_ict);
    }

    #[tokio::test]
    async fn metadata_in_segment_replaces_base() {
        // Commit 1 carries a metadata action with a new id; the delta surfaces it and apply
        // replaces the base.
        let (engine, segment) = setup_table(vec![vec![
            metadata_action_with_id("updated-table"),
            commit_info("WRITE"),
        ]])
        .await;

        let mut base = complete_base(0, 0);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert_eq!(
            delta.metadata.as_ref().map(|m| m.id()),
            Some("updated-table")
        );

        base.apply(delta);
        assert_eq!(base.metadata.id(), "updated-table");
    }

    /// Two commits each carry a protocol action; the newer one (commit 2 in reverse
    /// iteration) wins. Pins the `is_none()` guard at the protocol-extraction call site.
    #[tokio::test]
    async fn protocol_in_segment_newest_in_reverse_wins() {
        let p_v1 = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 1}});
        let p_v2 = json!({"protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }});
        let (engine, segment) = setup_table(vec![
            vec![p_v1, commit_info("WRITE")],
            vec![p_v2, commit_info("WRITE")],
        ])
        .await;

        let base = complete_base(0, 0);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        let p = delta.protocol.expect("newest protocol must win");
        assert_eq!(p.min_reader_version(), 3);
        assert_eq!(p.min_writer_version(), 7);
    }

    /// Pins the `c.version > base_version` filter (strict-greater). Without this, an
    /// off-by-one to `>=` or `> base_version + 1` would silently regress.
    #[tokio::test]
    async fn base_version_filter_is_strictly_greater() {
        // Two appended commits at v1 and v2.
        let (engine, segment) = setup_table(vec![
            vec![add("a", 100), commit_info("WRITE")], // v1
            vec![add("b", 200), commit_info("WRITE")], // v2
        ])
        .await;

        // base_version = 1 → only v2 in the replay range → +1 file, +200 bytes.
        let delta = segment
            .build_crc_delta_from_stale(&engine, &complete_base(0, 0), 1)
            .unwrap();
        assert_eq!(delta.file_stats.net_files, 1);
        assert_eq!(delta.file_stats.net_bytes, 200);
    }

    /// `base_version >= end_version` is a caller precondition violation; the function
    /// returns an internal error rather than silently producing a no-op delta.
    #[tokio::test]
    async fn base_version_at_or_above_end_version_errors() {
        let (engine, segment) = setup_table(vec![vec![add("a", 100), commit_info("WRITE")]]).await;
        // end_version is 1 (v0 CREATE + v1 commit). Pass base_version=1 → equal → error.
        let err = segment
            .build_crc_delta_from_stale(&engine, &complete_base(0, 0), 1)
            .unwrap_err();
        assert!(
            err.to_string().contains("base_version"),
            "expected base_version precondition error, got: {err}"
        );
    }

    #[rstest]
    #[case::default_boundaries(None)]
    #[case::custom_boundaries(Some(vec![0i64, 200, 1000]))]
    #[tokio::test]
    async fn histogram_boundaries_inherited_from_base(#[case] boundaries: Option<Vec<i64>>) {
        // Base has 1 file of size 100 (in bin 0 for both boundary configurations).
        let base = complete_base_with_histogram(&[100], boundaries.clone());
        let expected_boundaries = base
            .file_stats()
            .unwrap()
            .file_size_histogram()
            .unwrap()
            .sorted_bin_boundaries()
            .to_vec();

        // Commit 1: add(150) -> bin 0 in both configurations.
        let (engine, segment) =
            setup_table(vec![vec![add("new", 150), commit_info("WRITE")]]).await;

        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        let hist = delta.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(
            hist.sorted_bin_boundaries(),
            expected_boundaries.as_slice(),
            "delta histogram must inherit base's bin boundaries"
        );

        // Apply: the merge must succeed (boundaries match) and produce the right counts.
        let mut applied = base.clone();
        applied.apply(delta);
        let merged = applied.file_stats().unwrap().file_size_histogram().unwrap();
        assert_eq!(merged.file_counts()[0], 2);
        assert_eq!(merged.total_bytes()[0], 250);
    }

    /// When the base has no histogram, the reverse-replay produces no delta histogram
    /// either. Histogram tracking is disabled until a future full-rebuild path lands.
    #[tokio::test]
    async fn no_base_histogram_means_no_delta_histogram() {
        let (engine, segment) = setup_table(vec![vec![add("a", 100), commit_info("WRITE")]]).await;
        let base = complete_base(0, 0);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert!(delta.file_stats.net_histogram.is_none());
    }

    /// A commit with only a `SetTransaction` action and no file actions or commitInfo
    /// preserves the base's `Complete` state. The per-commit invariant doesn't fire (no
    /// file actions), and no other signal trips Indeterminate.
    #[tokio::test]
    async fn degenerate_txn_only_commit_stays_complete() {
        let (engine, segment) = setup_table(vec![vec![set_transaction("a", 1)]]).await;

        let mut base = complete_base(10, 1000);
        let delta = segment
            .build_crc_delta_from_stale(&engine, &base, 0)
            .unwrap();
        assert!(delta.is_incremental_safe);
        assert_eq!(delta.file_stats.net_files, 0);
        assert_eq!(delta.file_stats.net_bytes, 0);

        base.apply(delta);
        let stats = base.file_stats().unwrap();
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 1000);
        assert!(base.file_stats_state().is_complete());
    }

    // ===== Direct accumulator tests (bypass the engine) =====
    //
    // The engine emits at most one batch per file in current tests, so the per-commit
    // flag accumulation across batches of the SAME file is not reachable from the
    // engine-driven tests above. These tests drive `process_batch_start` and the
    // per-commit flags directly, simulating arbitrary N files × M batches sequences.

    /// One step in a simulated visitor run: `(file_url, "F" for file action / "C" for
    /// commitInfo / "FC" for both / "" for neither)`.
    type AccStep<'a> = (&'a str, &'a str);

    fn drive_accumulator(steps: &[AccStep<'_>]) -> CrcReplayAccumulator {
        let mut acc = CrcReplayAccumulator::with_histogram(None);
        for (url, actions) in steps {
            acc.process_batch_start((*url).to_string());
            if actions.contains('F') {
                acc.current_commit_saw_file_action = true;
            }
            if actions.contains('C') {
                acc.current_commit_saw_commit_info = true;
            }
        }
        // Mimic `build_crc_delta_from_stale`'s end-of-replay finalize.
        acc.process_commit_file_end();
        acc
    }

    /// N files × M batches per file. The per-commit invariant (file action ⇒ commitInfo)
    /// must hold per-file, with flags accumulating across same-URL batches.
    #[rstest]
    // 1 file × 1 batch
    #[case::one_file_complete(&[("a", "FC")], true)]
    #[case::one_file_no_commit_info(&[("a", "F")], false)]
    #[case::one_file_no_file_action(&[("a", "C")], true)]
    // 1 file × M batches: per-commit flags persist across batches of the same file.
    #[case::one_file_actions_split_across_batches(
        &[("a", "F"), ("a", ""), ("a", "C")], true
    )]
    #[case::one_file_no_commit_info_across_batches(
        &[("a", "F"), ("a", ""), ("a", "")], false
    )]
    // N files × 1 batch each
    #[case::two_files_both_complete(&[("b", "FC"), ("a", "FC")], true)]
    #[case::two_files_second_missing_commit_info(&[("b", "FC"), ("a", "F")], false)]
    #[case::two_files_first_missing_commit_info(&[("b", "F"), ("a", "FC")], false)]
    // N files × M batches each
    #[case::two_files_each_split_into_two_batches(
        &[("b", "F"), ("b", "C"), ("a", "F"), ("a", "C")], true
    )]
    #[case::three_files_mixed_layouts(
        &[("c", "FC"), ("b", "F"), ("b", "C"), ("a", "FC")], true
    )]
    fn accumulator_per_commit_invariant_across_files_and_batches(
        #[case] steps: &[AccStep<'_>],
        #[case] expected_is_incremental_safe: bool,
    ) {
        let acc = drive_accumulator(steps);
        assert_eq!(acc.delta.is_incremental_safe, expected_is_incremental_safe);
    }

    /// `is_first_commit` flips on the first file transition (and only then). Same-URL
    /// batches don't flip; subsequent (older) file transitions don't flip again.
    #[rstest]
    #[case::single_file_one_batch(&[("a", "")], true)]
    #[case::single_file_multi_batches(&[("a", ""), ("a", ""), ("a", "")], true)]
    #[case::two_files(&[("b", ""), ("a", "")], false)]
    #[case::two_files_multi_batches_each(
        &[("b", ""), ("b", ""), ("a", ""), ("a", "")], false
    )]
    #[case::many_files(&[("c", ""), ("b", ""), ("a", "")], false)]
    fn is_first_commit_flips_exactly_on_first_file_boundary(
        #[case] steps: &[AccStep<'_>],
        #[case] expected_is_first_commit_at_end: bool,
    ) {
        let acc = drive_accumulator(steps);
        assert_eq!(acc.is_first_commit, expected_is_first_commit_at_end);
    }
}
