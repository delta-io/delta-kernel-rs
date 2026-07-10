//! Incremental [`Snapshot`] update.
//!
//! Public API: `Snapshot::builder_from(existing).build(engine)`. The case-by-case
//! behavior lives in [`Snapshot::try_new_from`].

use std::sync::Arc;

use tracing::instrument;

use super::{IncrementalReplay, Snapshot};
use crate::log_segment::LogSegment;
use crate::log_segment_files::LogSegmentFiles;
use crate::metrics::SnapshotLoadMetricContext;
use crate::path::ParsedLogPath;
use crate::table_configuration::TableConfiguration;
use crate::{DeltaResult, Engine, Error, Version};

impl Snapshot {
    // ============================================================================
    // Public entry
    // ============================================================================

    /// Incrementally update an existing [`Snapshot`] to a more recent version. Variables used
    /// below:
    ///
    /// - `T` = effective target version: the time-travel version from
    ///   [`SnapshotBuilder::at_version`] when set, otherwise the `max_catalog_version` from
    ///   [`SnapshotBuilder::with_max_catalog_version`] for catalog-managed tables, otherwise unset
    ///   (defaults to latest).
    /// - `S1` = existing snapshot version.
    /// - `S2` = new snapshot version (= end version of the new listing). When `T` is set, the
    ///   listing is upper-bounded by `T`, so `S2 == T`.
    /// - `C1` = existing checkpoint version (if any).
    /// - `C2` = new checkpoint version (if found in the listing). Always `<= S2`, since `C2` is by
    ///   definition a checkpoint observed within the listing. By transitivity, `C2 <= T` when `T`
    ///   is set.
    ///
    /// The log listing is catalog-log-tail aware: any `log_tail` provided to the builder is
    /// merged with filesystem listings.
    ///
    /// Position layout per case (`....` is the version axis; `====` marks the range read for
    /// P+M replay; `listed` is the listing range; `read` is what's read for P+M):
    ///
    /// ```text
    ///             positions                       listed       read       outcome
    /// Case A:     C1 ........ S1 (= T)            -            -          return existing
    /// Case B:     C1 .. T .. S1                   -            -          error (T < S1)
    /// Case C.1:   C1 .. S1 .. T                   empty        -          error (T unreachable)
    /// Case C.2:   C1 .. S1                        empty        -          return existing
    /// Case D.1:   C1 .. S1 .. C2 ===== S2         [C1+1, S2]   [C2, S2]   rebuild from C2
    /// Case D.2:   C1 . C2 .. S1 ====== S2         [C1+1, S2]   (S1, S2]   advance base, -> F
    /// Case E:     C1 .. S1 (= S2)                 [C1+1, S1]   -          return existing
    /// Case F:     C1 .. S1 ====== S2              [C1+1, S2]   (S1, S2]   incremental update
    /// ```
    ///
    /// In the incremental cases (D.2, F), the existing snapshot's P+M at `S1` is the baseline, and
    /// only commits in `(S1, S2]` are replayed for newer P+M on top of it. A base CRC newer than
    /// `S1`, when present, serves as the baseline instead.
    ///
    /// - **A.** `T == S1`: return the existing snapshot unchanged.
    /// - **B.** `T < S1`: error. The incremental path only moves forward.
    /// - Otherwise (`T` unset or `T > S1`), list the log from `C1+1` (or from version 1 if there is
    ///   no existing checkpoint), and one of the following applies:
    ///   - **C.** Listing is empty:
    ///     - **C.1.** `T` is set: error (target is newer than anything in the log).
    ///     - **C.2.** `T` is unset: return the existing snapshot.
    ///   - **D.** Listing contains a checkpoint:
    ///     - **D.1.** `C2 > S1`: the new checkpoint at `C2` already captures the table state
    ///       through version `C2`, including changes in `(S1, C2]`, so we can use it as the new
    ///       base instead of replaying those commits. Build a fresh snapshot from `C2`.
    ///     - **D.2.** `C2 <= S1`: the existing snapshot's P+M (at `S1`) already reflects everything
    ///       `C2` would tell us, so we skip reading `C2` for P+M. Fall through to case F to replay
    ///       only commits `> S1` for P+M; the combined segment uses `C2` as its checkpoint base,
    ///       producing a better log segment with fewer deltas above the checkpoint (e.g. faster
    ///       distributed log replay).
    ///   - **E.** Listing contains commits but no new checkpoint, and `S2 == S1`: return the
    ///     existing snapshot.
    ///   - **F.** Listing contains new commits (no new checkpoint, or fall through from D.2): run
    ///     lightweight P+M replay on commits `> S1` and merge them into the existing log segment.
    ///
    /// Each case is marked with `// Case X` in the function body.
    ///
    /// [`SnapshotBuilder::at_version`]: crate::snapshot::SnapshotBuilder::at_version
    /// [`SnapshotBuilder::with_max_catalog_version`]: crate::snapshot::SnapshotBuilder::with_max_catalog_version
    #[instrument(err, fields(version, operation_id = %metric_context.operation_id, correlation_id = metric_context.correlation_id.as_deref().unwrap_or("")), skip(engine, target_version))]
    pub(super) fn try_new_from(
        existing_snapshot: Arc<Snapshot>,
        log_tail: Vec<ParsedLogPath>,
        engine: &dyn Engine,
        target_version: impl Into<Option<Version>>,
        metric_context: SnapshotLoadMetricContext,
        incremental_replay: IncrementalReplay,
        built_as_latest: bool,
    ) -> DeltaResult<Arc<Self>> {
        let existing_log_segment = &existing_snapshot.log_segment;
        let existing_snapshot_version = existing_snapshot.version();
        let requested_version = target_version.into();
        if let Some(requested_version) = requested_version {
            tracing::Span::current().record("version", requested_version);
            // Case A: re-requesting the same version.
            if requested_version == existing_snapshot_version {
                return Self::reuse_with_intent(&existing_snapshot, built_as_latest);
            }
            // Case B: incremental path only moves forward.
            if requested_version < existing_snapshot_version {
                return Err(Error::Generic(format!(
                    "Requested snapshot version {requested_version} is older than snapshot \
                    hint version {existing_snapshot_version}"
                )));
            }
        } else {
            tracing::Span::current().record("version", existing_snapshot_version);
        }

        let log_root = existing_log_segment.log_root.clone();
        let storage = engine.storage_handler();

        // Start listing just after the previous segment's checkpoint, if any.
        let listing_start = existing_log_segment.checkpoint_version.unwrap_or(0) + 1;

        // Check for new commits (and CRC)
        let new_listed_files = LogSegmentFiles::list(
            storage.as_ref(),
            &log_root,
            log_tail,
            Some(listing_start),
            requested_version,
        )?;

        // NB: we need to check both checkpoints and commits since we filter commits at and below
        // the checkpoint version. Example: if we have a checkpoint + commit at version 1, the log
        // listing above will only return the checkpoint and not the commit.
        if new_listed_files.ascending_commit_files().is_empty()
            && new_listed_files.checkpoint_parts().is_empty()
        {
            match requested_version {
                // Case C.1: caller requested a specific version (necessarily >
                // existing_snapshot_version since cases A and B were handled above), but
                // no such commit exists in the log.
                Some(requested_version) => {
                    return Err(Error::Generic(format!(
                        "Requested snapshot version {requested_version} is not available: \
                         no new commits were found after existing snapshot version \
                         {existing_snapshot_version}"
                    )));
                }
                // Case C.2: no new commits and no explicit target; latest is existing.
                None => {
                    return Self::reuse_with_intent(&existing_snapshot, built_as_latest);
                }
            }
        }

        // create a log segment just from existing_checkpoint.version -> new_version
        // OR could be from 1 -> new_version
        // Save the latest_commit before moving new_listed_files
        let new_latest_commit_file = new_listed_files.latest_commit_file().clone();
        // Note: new_log_segment won't have last_checkpoint_metadata since we're listing without a
        // hint. If the new segment has a checkpoint, we will return it as is. Otherwise, we
        // will preserve last_checkpoint_metadata when merging the new log segment with the
        // old one.
        let mut new_log_segment =
            LogSegment::try_new(new_listed_files, log_root.clone(), requested_version, None)?;

        let new_end_version = new_log_segment.end_version;
        if new_end_version < existing_snapshot_version {
            // we should never see a new log segment with a version < the existing snapshot
            // version, that would mean a commit was incorrectly deleted from the log
            return Err(Error::Generic(format!(
                "Unexpected state: the newest version in the log {new_end_version} is \
                 older than the existing snapshot version {existing_snapshot_version}"
            )));
        }
        if let Some(new_checkpoint_version) = new_log_segment.checkpoint_version {
            if new_checkpoint_version > existing_snapshot_version {
                // Case D.1: checkpoint ahead of existing snapshot. Commits between
                // existing_snapshot_version+1 and new_checkpoint_version were filtered out
                // of the listing, so a full rebuild is required. The existing snapshot's CRC
                // is older than the new checkpoint and cannot apply, but a fresh on-disk CRC
                // at or above the new checkpoint still advances per `incremental_replay`.
                let snapshot = Self::try_new_from_log_segment(
                    existing_snapshot.table_root().clone(),
                    new_log_segment,
                    engine,
                    metric_context,
                    incremental_replay,
                    built_as_latest,
                );
                return Ok(Arc::new(snapshot?));
            }
            // Case D.2: checkpoint at or below existing snapshot; fall through to case F to
            // replay only the new commits and advance the checkpoint base.
        }

        // Case E: no new checkpoint, version did not advance; return existing.
        if new_end_version == existing_snapshot_version
            && new_log_segment.checkpoint_version.is_none()
        {
            // We must check checkpoint_version here: if a checkpoint at or below the existing
            // snapshot version was discovered, we still need to fall through to advance the
            // checkpoint base even though the version did not change.
            return Self::reuse_with_intent(&existing_snapshot, built_as_latest);
        }

        // Case F: lightweight P+M replay on new commits, merge with existing segment.
        // (Also reached from Case D.2 when a checkpoint at or below the existing snapshot
        // version was discovered.)
        //
        // The example below illustrates Case D.2 -> F.
        #[rustfmt::skip]
        // Example: existing segment = checkpoint@v0 + commit@v1, v2, v3
        //          existing_snapshot_version = v3, listing_start = v1
        //          target=v4, new checkpoint@v2 written since last build (Case D.2 -> F):
        //
        //    listing:          commit@v1, then checkpoint@v2 + commit@v3, v4
        //                      (checkpoint flush drops v1)
        //    new segment:      checkpoint@v2 + commit@v3, v4
        //    after dedup:      drop commit@v3 (already in existing snapshot)
        //                      -> ascending_commit_files = [commit@v4]
        //    commit@v3 is not lost: the existing segment contributes it below
        //    (existing commits above checkpoint@v2 = [commit@v3])
        //    combined segment: checkpoint@v2 + commit@v3 (from existing) + commit@v4 (from new)
        //    checkpoint@v2 is kept in checkpoint_parts to advance the listing base.
        new_log_segment
            .listed
            .ascending_commit_files
            .retain(|log_path| existing_snapshot_version < log_path.version);
        // Deduplicate compaction files the same way: the new listing re-lists from
        // checkpoint_version, so it includes compaction files already in the existing segment.
        // Note: This removes all _new_ compaction files that start at or before
        // `existing_snapshot_version`, which may drop useful compaction files that span
        // across the old/new boundary (e.g. a new compaction(1, 3) when
        // existing_snapshot_version=2). This is conservative but safe.
        new_log_segment
            .listed
            .ascending_compaction_files
            .retain(|log_path| existing_snapshot_version < log_path.version);

        // The CRC file the combined segment carries on disk.
        let crc_file = Self::resolve_crc_file(&new_log_segment, existing_log_segment);

        // Combine existing and new log segments. When the new listing found a checkpoint at or
        // below the existing snapshot version, trim existing commits and compaction files
        // already covered by it; otherwise keep all existing files.
        let new_checkpoint_version = new_log_segment.checkpoint_version;
        let mut ascending_commit_files = Self::files_after_checkpoint(
            &existing_log_segment.listed.ascending_commit_files,
            new_checkpoint_version,
        );
        ascending_commit_files.extend(new_log_segment.listed.ascending_commit_files);
        let mut ascending_compaction_files = Self::files_after_checkpoint(
            &existing_log_segment.listed.ascending_compaction_files,
            new_checkpoint_version,
        );
        ascending_compaction_files.extend(new_log_segment.listed.ascending_compaction_files);

        // When the new listing found a checkpoint at or below the existing snapshot version,
        // advance the base so future listings start from new_checkpoint_version+1; otherwise
        // preserve the existing base.
        // Note: the incremental listing never uses a _last_checkpoint hint, so
        // new_log_segment.last_checkpoint_metadata is always None when a new checkpoint
        // is found here. The combined segment will read the new checkpoint's parquet footer at
        // scan time. When no new checkpoint was found, preserve the existing hint if available.
        let (new_checkpoint_parts, new_checkpoint_hint) = if new_checkpoint_version.is_some() {
            (
                new_log_segment.listed.checkpoint_parts.clone(),
                new_log_segment.last_checkpoint_metadata.clone(),
            )
        } else {
            (
                existing_log_segment.listed.checkpoint_parts.clone(),
                existing_log_segment.last_checkpoint_metadata.clone(),
            )
        };
        let combined_log_segment = LogSegment::try_new(
            LogSegmentFiles {
                ascending_commit_files,
                ascending_compaction_files,
                checkpoint_parts: new_checkpoint_parts,
                latest_crc_file: crc_file,
                // In Case F there are new commits (new_end_version > existing_snapshot_version
                // with no new checkpoint), so the new listing's `latest_commit_file` is always
                // `Some(commit @ new_end_version)` and matches the combined snapshot version.
                latest_commit_file: new_latest_commit_file,
                max_published_version: new_log_segment
                    .listed
                    .max_published_version
                    .max(existing_log_segment.listed.max_published_version),
            },
            log_root,
            requested_version,
            new_checkpoint_hint,
        )?;

        // Advance the latest available base (the existing snapshot's in-memory CRC, or a newer
        // on-disk CRC the combined segment carries) to the new end version, subject to
        // `incremental_replay`.
        let base_crc = combined_log_segment.pick_latest_base_crc(engine, existing_snapshot.crc());
        let crc_at_version = combined_log_segment.try_build_crc_within_budget(
            engine,
            base_crc.as_ref(),
            incremental_replay,
        )?;

        let existing_table_config = existing_snapshot.table_configuration();
        let (new_metadata, new_protocol) = match &crc_at_version {
            Some(crc) => {
                // If we were able to build a new CRC, then re-use it for TableConfiguration
                // creation.
                let new_metadata = (crc.metadata != *existing_table_config.metadata())
                    .then(|| crc.metadata.clone());
                let new_protocol = (crc.protocol != *existing_table_config.protocol())
                    .then(|| crc.protocol.clone());
                (new_metadata, new_protocol)
            }
            None => {
                // Incremental CRC replay wasn't applicable or failed (note: we have *not* yet
                // scanned any log files). Replay the new commits `(existing_snapshot_version, end]`
                // for P&M, rooted at the base CRC when it is newer than the existing snapshot
                // (else the existing snapshot is the baseline).
                let newer_base = base_crc
                    .as_ref()
                    .filter(|c| c.version > existing_snapshot_version);
                combined_log_segment
                    .segment_after_version(existing_snapshot_version)
                    .read_protocol_metadata_opt(engine, newer_base)?
            }
        };
        let table_configuration = TableConfiguration::try_new_from(
            existing_table_config,
            new_metadata,
            new_protocol,
            new_end_version,
        )?;

        tracing::Span::current().record("version", table_configuration.version());
        Ok(Arc::new(Snapshot::new_with_crc(
            combined_log_segment,
            table_configuration,
            crc_at_version,
            built_as_latest,
        )?))
    }

    // ============================================================================
    // Helpers
    // ============================================================================

    /// Return `existing` when its intent flag already matches `built_as_latest`,
    /// otherwise a copy carrying the new flag.
    fn reuse_with_intent(
        existing: &Arc<Snapshot>,
        built_as_latest: bool,
    ) -> DeltaResult<Arc<Snapshot>> {
        if existing.built_as_latest == built_as_latest {
            return Ok(existing.clone());
        }
        Ok(Arc::new(Snapshot::new_with_crc(
            existing.log_segment.clone(),
            existing.table_configuration.clone(),
            existing.crc.clone(),
            built_as_latest,
        )?))
    }

    /// Determine the on-disk CRC file the combined segment should carry.
    ///
    /// Prefers the new segment's CRC; falls back to the existing segment's CRC if and only if
    /// its version is >= the new segment's checkpoint version, so the fallback never violates the
    /// [`LogSegmentFiles`] invariant `crc.version >= checkpoint.version` (enforced in
    /// [`LogSegment::try_new`]).
    fn resolve_crc_file(
        new_log_segment: &LogSegment,
        existing_log_segment: &LogSegment,
    ) -> Option<ParsedLogPath> {
        let crc_satisfies_new_ckpt = |crc: &ParsedLogPath| {
            new_log_segment
                .checkpoint_version
                .is_none_or(|ckpt_v| crc.version >= ckpt_v)
        };
        let existing_crc_file = existing_log_segment
            .listed
            .latest_crc_file
            .clone()
            .filter(crc_satisfies_new_ckpt);
        new_log_segment
            .listed
            .latest_crc_file
            .clone()
            .or(existing_crc_file)
    }

    /// Filters `files` to only those with version above `checkpoint_version`, or clones all if
    /// `checkpoint_version` is `None`.
    fn files_after_checkpoint(
        files: &[ParsedLogPath],
        checkpoint_version: Option<Version>,
    ) -> Vec<ParsedLogPath> {
        match checkpoint_version {
            Some(ckpt_v) => files
                .iter()
                .filter(|f| f.version > ckpt_v)
                .cloned()
                .collect(),
            None => files.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::rstest;
    use serde_json::json;
    use test_utils::delta_path_for_version;
    use test_utils::table_builder::TestTableBuilder;
    use url::Url;

    use super::*;
    use crate::arrow::array::StringArray;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::ArrowWriter;
    use crate::path::LogPathFileType;
    use crate::snapshot::commit;
    use crate::utils::test_utils::string_array_to_engine_data;

    // ============================================================================
    // Helpers
    // ============================================================================

    // Action builders for incremental-snapshot tests. Centralized so commit setup stays
    // consistent across tests (e.g. the schema matches `make_test_crc_json`).
    fn protocol_action(min_reader: u32, min_writer: u32) -> serde_json::Value {
        json!({"protocol": {"minReaderVersion": min_reader, "minWriterVersion": min_writer}})
    }

    fn metadata_action(configuration: serde_json::Value) -> serde_json::Value {
        json!({
            "metaData": {
                "id": "test-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": configuration,
                "createdTime": 1587968585495i64,
            }
        })
    }

    fn add_action(path: &str) -> serde_json::Value {
        json!({
            "add": {
                "path": path,
                "partitionValues": {},
                "size": 100,
                "modificationTime": 1000,
                "dataChange": true,
            }
        })
    }

    // Helper: create a minimal test table with commits 0..num_commits.
    async fn setup_test_table_with_commits(
        table_root: impl AsRef<str>,
        store: &InMemory,
        num_commits: u64,
    ) -> DeltaResult<()> {
        // Commit 0: protocol + metadata + first file.
        commit(
            table_root.as_ref(),
            store,
            0,
            vec![
                protocol_action(1, 2),
                metadata_action(json!({})),
                add_action("file1.parquet"),
            ],
        )
        .await;

        // Additional commits with just add actions.
        for i in 1..num_commits {
            let path = format!("file{}.parquet", i + 1);
            commit(table_root.as_ref(), store, i, vec![add_action(&path)]).await;
        }
        Ok(())
    }

    // Helper: write a compaction file
    async fn write_compaction_file(store: &InMemory, start: u64, end: u64) -> DeltaResult<()> {
        let content = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
        store
            .put(
                &test_utils::compacted_log_path_for_versions(start, end, "json"),
                content.into(),
            )
            .await?;
        Ok(())
    }

    struct IncrementalSnapshotTestContext {
        store: Arc<InMemory>,
        url: Url,
        engine: Arc<SyncEngine>,
    }

    fn setup_incremental_snapshot_test() -> DeltaResult<IncrementalSnapshotTestContext> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = Arc::new(SyncEngine::new_with_store(store.clone()));

        Ok(IncrementalSnapshotTestContext { store, url, engine })
    }

    /// Compares two Snapshots field-by-field. LogSegment fields are compared individually,
    /// intentionally skipping `last_checkpoint_metadata` which is only populated on the
    /// from-scratch path (via `_last_checkpoint` hint) and not on the incremental update path.
    fn compare_snapshots(left: &Snapshot, right: &Snapshot) {
        assert_eq!(left.table_configuration, right.table_configuration);
        assert_eq!(left.log_segment.end_version, right.log_segment.end_version);
        assert_eq!(
            left.log_segment.checkpoint_version,
            right.log_segment.checkpoint_version
        );
        assert_eq!(left.log_segment.log_root, right.log_segment.log_root);
        assert_eq!(left.log_segment.listed, right.log_segment.listed);
    }

    // CRC JSON for the standard test table (see `setup_test_table_with_commits`).
    fn make_test_crc_json(table_size_bytes: i64, num_files: i64) -> serde_json::Value {
        json!({
            "tableSizeBytes": table_size_bytes,
            "numFiles": num_files,
            "numMetadata": 1,
            "numProtocol": 1,
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "metadata": {
                "id": "test-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        })
    }

    // ============================================================================
    // Tests: try_new_from direct API
    // ============================================================================

    #[test]
    fn test_try_new_from_empty_log_tail() -> DeltaResult<()> {
        let table = TestTableBuilder::new().build().unwrap();
        let engine = SyncEngine::new_with_store(table.store().clone());

        let base_snapshot = Snapshot::builder_for(table.table_root())
            .at_version(0)
            .build(&engine)?;

        let result = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            None,
            SnapshotLoadMetricContext::default(),
            IncrementalReplay::Disabled,
            true, /* built_as_latest */
        )?;
        assert_eq!(result, base_snapshot);
        // `PartialEq` ignores `built_as_latest`, so assert it explicitly.
        assert!(result.built_as_latest());

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_latest_commit_preservation() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits 0-2
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(url.as_str(), store.as_ref(), 0, base_commit.clone()).await;
        commit(
            url.as_str(),
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;
        commit(
            url.as_str(),
            store.as_ref(),
            2,
            vec![json!({"commitInfo": {"timestamp": 5678}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(url.as_str())
            .at_version(1)
            .build(&engine)?;

        // Verify base snapshot has latest_commit_file at version 1
        assert_eq!(
            base_snapshot
                .log_segment
                .listed
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(1)
        );

        // Create log_tail with FileMeta for version 2
        let commit_2_url = url.join("_delta_log/")?.join("00000000000000000002.json")?;
        let file_meta = crate::FileMeta {
            location: commit_2_url,
            last_modified: 1234567890,
            size: 100,
        };
        let parsed_path = ParsedLogPath::try_from(file_meta)?
            .ok_or_else(|| Error::Generic("Failed to parse log path".to_string()))?;
        let log_tail = vec![parsed_path];

        // Create new snapshot from base to version 2 using try_new_from directly
        let new_snapshot = Snapshot::try_new_from(
            base_snapshot.clone(),
            log_tail,
            &engine,
            Some(2),
            SnapshotLoadMetricContext::default(),
            IncrementalReplay::Disabled,
            false, /* built_as_latest */
        )?;

        // Latest commit should now be version 2
        assert_eq!(
            new_snapshot
                .log_segment
                .listed
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_version_boundary_cases() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(table_root, store.as_ref(), 0, base_commit).await;
        commit(
            table_root,
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;

        // Test requesting same version - should return same snapshot
        let same_version = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            Some(1),
            SnapshotLoadMetricContext::default(),
            IncrementalReplay::Disabled,
            false, /* built_as_latest */
        )?;
        assert!(Arc::ptr_eq(&same_version, &base_snapshot));

        // Test requesting older version - should error
        let older_version = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            Some(0),
            SnapshotLoadMetricContext::default(),
            IncrementalReplay::Disabled,
            false, /* built_as_latest */
        );
        assert!(matches!(
            older_version,
            Err(Error::Generic(msg)) if msg.contains("older than snapshot hint version")
        ));

        Ok(())
    }

    // ============================================================================
    // Tests: incremental builder paths (Cases A/B/C/D.1/D.2/E/F)
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_picks_up_checkpoint_written_at_current_version(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 2).await?;

        let snapshot_v1 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v1.log_segment.checkpoint_version, None);

        snapshot_v1.clone().checkpoint(ctx.engine.as_ref(), None)?;

        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(fresh.version(), 1);
        assert_eq!(fresh.log_segment.checkpoint_version, Some(1));

        let updated = Snapshot::builder_from(snapshot_v1).build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 1);
        assert_eq!(updated.log_segment.checkpoint_version, Some(1));
        compare_snapshots(&updated, &fresh);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_picks_up_newer_checkpoint_below_current_version(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 4).await?;

        Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        let snapshot_v3 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(3)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v3.log_segment.checkpoint_version, Some(1));

        Snapshot::builder_for(ctx.url.as_str())
            .at_version(2)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(fresh.version(), 3);
        assert_eq!(fresh.log_segment.checkpoint_version, Some(2));

        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 3);
        assert_eq!(updated.log_segment.checkpoint_version, Some(2));
        compare_snapshots(&updated, &fresh);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_explicit_same_version_request_keeps_existing_snapshot_after_checkpoint_write(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 2).await?;

        let snapshot_v1 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v1.log_segment.checkpoint_version, None);

        snapshot_v1.clone().checkpoint(ctx.engine.as_ref(), None)?;

        let refreshed = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(refreshed.log_segment.checkpoint_version, Some(1));

        let pinned = Snapshot::builder_from(snapshot_v1.clone())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert!(Arc::ptr_eq(&pinned, &snapshot_v1));
        assert_eq!(pinned.log_segment.checkpoint_version, None);

        Ok(())
    }

    /// When a checkpoint at or below the existing snapshot version is discovered and commits
    /// between that checkpoint and the existing snapshot version contain P&M changes, the
    /// incremental update must preserve those P&M changes rather than regressing to the
    /// checkpoint's older P&M.
    ///
    /// Setup: initial checkpoint@v0 so listing_start=1. A checkpoint at v1 is written (discoverable
    /// since v1 >= listing_start). The P&M change at commit 2 lies between the new checkpoint (v1)
    /// and existing_snapshot_version (v3); the incremental update must preserve it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_checkpoint_at_or_below_snapshot_version_preserves_pm_from_commits_in_between(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        let table_root = ctx.url.as_str();

        // Commit 0: initial protocol (1, 2) + metadata + first file.
        commit(
            table_root,
            &ctx.store,
            0,
            vec![
                protocol_action(1, 2),
                metadata_action(json!({})),
                add_action("file1.parquet"),
            ],
        )
        .await;
        // Commit 1: data-only add.
        commit(table_root, &ctx.store, 1, vec![add_action("file2.parquet")]).await;
        // Commit 2: protocol upgrade to (2, 5). This is the change the incremental update
        // must preserve rather than regressing to checkpoint@v1's older protocol.
        commit(table_root, &ctx.store, 2, vec![protocol_action(2, 5)]).await;
        // Commit 3: data-only add.
        commit(table_root, &ctx.store, 3, vec![add_action("file3.parquet")]).await;

        // Write initial checkpoint at v0 so that listing_start = v0+1 = 1.
        Snapshot::builder_for(table_root)
            .at_version(0)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        // Build snapshot at v3: has checkpoint@v0, protocol (2, 5) from commit 2.
        let snapshot_v3 = Snapshot::builder_for(table_root)
            .at_version(3)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v3.log_segment.checkpoint_version, Some(0));
        assert_eq!(
            snapshot_v3
                .table_configuration()
                .protocol()
                .min_reader_version(),
            2
        );
        assert_eq!(
            snapshot_v3
                .table_configuration()
                .protocol()
                .min_writer_version(),
            5
        );

        // Write a checkpoint at v1 (v1 >= listing_start=1, so it is discoverable).
        // This checkpoint captures only protocol (1, 2) from commit 0.
        Snapshot::builder_for(table_root)
            .at_version(1)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        // Commit 4: add file (new state beyond existing_snapshot_version=3).
        commit(table_root, &ctx.store, 4, vec![add_action("file4.parquet")]).await;

        // Incremental update from v3: listing starts from v1, discovers checkpoint@v1.
        // Must preserve protocol (2, 5) from commit 2, not regress to (1, 2) from checkpoint@v1.
        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 4);
        assert_eq!(updated.log_segment.checkpoint_version, Some(1));

        // Critical: protocol must still be (2, 5) from commit 2, not (1, 2) from ckpt@v1.
        assert_eq!(
            updated
                .table_configuration()
                .protocol()
                .min_reader_version(),
            2
        );
        assert_eq!(
            updated
                .table_configuration()
                .protocol()
                .min_writer_version(),
            5
        );

        // Cross-check against a fresh build.
        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        compare_snapshots(&updated, &fresh);

        Ok(())
    }

    // interesting cases for testing Snapshot::new_from:
    // 1. new version < existing version
    // 2. new version == existing version
    // 3. new version > existing version AND
    //   a. log segment hasn't changed
    //   b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
    //   b. log segment for old..=new version has no checkpoint
    //     i. commits have (new protocol, new metadata)
    //     ii. commits have (new protocol, no metadata)
    //     iii. commits have (no protocol, new metadata)
    //     iv. commits have (no protocol, no metadata)
    #[tokio::test]
    async fn test_snapshot_new_from() -> DeltaResult<()> {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let old_snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        // 1. new version < existing version: error
        let snapshot_res = Snapshot::builder_from(old_snapshot.clone())
            .at_version(0)
            .build(&engine);
        assert!(matches!(
            snapshot_res,
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 0 is older than snapshot hint version 1"
        ));

        // 2. new version == existing version
        let snapshot = Snapshot::builder_from(old_snapshot.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        let expected = old_snapshot.clone();
        assert_eq!(snapshot, expected);

        // tests Snapshot::new_from by:
        // 1. creating a snapshot with new API for commits 0..=2 (based on old snapshot at 0)
        // 2. comparing with a snapshot created directly at version 2
        //
        // the commits tested are:
        // - commit 0 -> base snapshot at this version
        // - commit 1 -> final snapshots at this version
        //
        // in each test we will modify versions 1 and 2 to test different scenarios
        fn test_new_from(store: Arc<InMemory>) -> DeltaResult<()> {
            let table_root = "memory:///";
            let engine = SyncEngine::new_with_store(store);
            let base_snapshot = Snapshot::builder_for(table_root)
                .at_version(0)
                .build(&engine)?;
            let snapshot = Snapshot::builder_from(base_snapshot.clone())
                .at_version(1)
                .build(&engine)?;
            let expected = Snapshot::builder_for(table_root)
                .at_version(1)
                .build(&engine)?;
            assert_eq!(snapshot, expected);
            Ok(())
        }

        // for (3) we will just engineer custom log files
        let store = Arc::new(InMemory::new());
        // everything will have a starting 0 commit with commitInfo, protocol, metadata
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];
        let table_root = "memory:///";
        commit(table_root, store.as_ref(), 0, commit0.clone()).await;
        // 3. new version > existing version
        // a. no new log segment
        let engine = SyncEngine::new_with_store(Arc::new(store.fork()));
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        let snapshot = Snapshot::builder_from(base_snapshot.clone()).build(&engine)?;
        let expected = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
        // version exceeds latest version of the table = err
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(1).build(&engine),
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 1 is not available: no new commits were found after existing snapshot version 0"
        ));

        // b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
        let store_3a = store.fork();
        let mut checkpoint1 = commit0.clone();
        commit(table_root, &store_3a, 1, commit0.clone()).await;
        checkpoint1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        checkpoint1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint1
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler
            .parse_json(
                string_array_to_engine_data(json_strings),
                crate::actions::get_commit_schema().clone(),
            )
            .unwrap();
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let checkpoint: RecordBatch = checkpoint.into();

        // Write the record batch to a Parquet file
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        store_3a
            .put(
                &delta_path_for_version(1, "checkpoint.parquet"),
                buffer.into(),
            )
            .await
            .unwrap();
        test_new_from(store_3a.into())?;

        // c. log segment for old..=new version has no checkpoint
        // i. commits have (new protocol, new metadata)
        let store_3c_i = Arc::new(store.fork());
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit(table_root, store_3c_i.as_ref(), 1, commit1).await;
        test_new_from(store_3c_i.clone())?;

        // new commits AND request version > end of log
        let engine = SyncEngine::new_with_store(store_3c_i);
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(2).build(&engine),
            Err(Error::Generic(msg)) if msg == "LogSegment end version 1 not the same as the specified end version 2"
        ));

        // ii. commits have (new protocol, no metadata)
        let store_3c_ii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1.remove(2); // remove metadata
        commit(table_root, &store_3c_ii, 1, commit1).await;
        test_new_from(store_3c_ii.into())?;

        // iii. commits have (no protocol, new metadata)
        let store_3c_iii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit1.remove(1); // remove protocol
        commit(table_root, &store_3c_iii, 1, commit1).await;
        test_new_from(store_3c_iii.into())?;

        // iv. commits have (no protocol, no metadata)
        let store_3c_iv = store.fork();
        let commit1 = vec![commit0[0].clone()];
        commit(table_root, &store_3c_iv, 1, commit1).await;
        test_new_from(store_3c_iv.into())?;

        Ok(())
    }

    // ============================================================================
    // Tests: stale-CRC resolution on the incremental path
    // ============================================================================

    // `resolve_crc_file` when the new listing discovers a checkpoint *at or below* the existing
    // snapshot version must correctly decide which CRC file the combined segment carries on disk.
    // Each case below sets up commits 0-3, writes an existing CRC, builds snapshot_v3, writes
    // a checkpoint, optionally writes a new CRC, then runs the incremental update and checks
    // the segment's `latest_crc_file`. (The checkpoint-*ahead* path is covered separately by
    // `test_incremental_snapshot_multi_hop_replay_then_rebuild_drops_stale_crc`.)
    //
    // The update lands at the same version (v3), so the existing snapshot's in-memory crc@v3
    // always carries forward; only the on-disk `latest_crc_file` differs per case.
    //
    // Cases:
    //   keep_existing:    existing crc@v2 stays when new ckpt@v1 is below it (invariant OK)
    //                     and no newer CRC was listed; segment carries crc@v2.
    //   refresh_to_newer: existing crc@v2 is superseded by a newly-listed crc@v3.
    //   crc_equals_ckpt:  existing crc@v2 and new ckpt@v2 sit at the same version; sanity
    //                     coverage for the invariant boundary `crc.version >= ckpt.version`.
    //   drop_below_ckpt:  existing crc@v1 would violate the LogSegmentFiles invariant with
    //                     new ckpt@v2 (1 < 2), so it is filtered out; segment has no CRC file.
    #[rstest]
    #[case::keep_existing(2, 1, None, Some(2))]
    #[case::refresh_to_newer(2, 1, Some(3), Some(3))]
    #[case::crc_equals_ckpt(2, 2, None, Some(2))]
    #[case::drop_below_ckpt(1, 2, None, None)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_crc_resolution_when_checkpoint_at_or_below_snapshot(
        #[case] existing_crc_v: u64,
        #[case] new_ckpt_v: u64,
        #[case] newly_listed_crc_v: Option<u64>,
        #[case] expected_crc_file_v: Option<u64>,
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 4).await?;

        ctx.store
            .put(
                &delta_path_for_version(existing_crc_v, "crc"),
                make_test_crc_json(300, 3).to_string().into(),
            )
            .await?;

        let snapshot_v3 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(3)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .build(ctx.engine.as_ref())?;
        // A fresh build advances the stale CRC via reverse-replay to a CRC at v3 (file stats
        // Indeterminate, since these synthetic commits carry no commitInfo). The segment's
        // latest CRC file still points at the on-disk CRC version.
        assert_eq!(snapshot_v3.crc().map(|c| c.version), Some(3));
        assert_eq!(
            snapshot_v3
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .map(|f| f.version),
            Some(existing_crc_v)
        );

        // Write the new checkpoint, then optionally plant a newer CRC at `newly_listed_crc_v`.
        Snapshot::builder_for(ctx.url.as_str())
            .at_version(new_ckpt_v)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;
        if let Some(v) = newly_listed_crc_v {
            ctx.store
                .put(
                    &delta_path_for_version(v, "crc"),
                    make_test_crc_json(400, 4).to_string().into(),
                )
                .await?;
        }

        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;

        // Observable equivalence: incremental update matches a fresh rebuild, including
        // `log_segment.listed.latest_crc_file` (so a filter-bypass regression is caught by
        // `compare_snapshots`, not just by the explicit CRC assertions below).
        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        compare_snapshots(&updated, &fresh);
        assert_eq!(updated.version(), 3);
        assert_eq!(updated.log_segment.checkpoint_version, Some(new_ckpt_v));
        assert_eq!(
            updated
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .map(|f| f.version),
            expected_crc_file_v
        );
        assert_eq!(updated.crc().map(|c| c.version), Some(3));

        Ok(())
    }

    #[rstest]
    #[case::disabled(IncrementalReplay::Disabled, None)]
    #[case::unlimited(IncrementalReplay::Unlimited, Some(5))]
    #[case::budget_exact(IncrementalReplay::UpToCommits(2), Some(5))]
    #[case::budget_more_than_enough(IncrementalReplay::UpToCommits(5), Some(5))]
    #[case::budget_too_small(IncrementalReplay::UpToCommits(1), None)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_update_advances_in_memory_crc_within_budget(
        #[case] mode: IncrementalReplay,
        #[case] expected_crc_v: Option<u64>,
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 6).await?;
        ctx.store
            .put(
                &delta_path_for_version(1, "crc"),
                make_test_crc_json(200, 2).to_string().into(),
            )
            .await?;

        let snapshot_a = Snapshot::builder_for(ctx.url.as_str())
            .at_version(3)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_a.crc().map(|c| c.version), Some(3));

        let updated = Snapshot::builder_from(snapshot_a)
            .with_incremental_crc_replay(mode)
            .build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 5);
        assert_eq!(updated.crc().map(|c| c.version), expected_crc_v);
        assert_eq!(
            updated
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .map(|f| f.version),
            Some(1)
        );

        Ok(())
    }

    // Stronger regression for the `resolve_crc_file` invariant filter: set up a metadata change
    // at the checkpoint version so that a stale inherited CRC would produce wrong
    // Metadata, not just wrong bookkeeping. Without the filter, the incremental update
    // would inherit the below-checkpoint CRC and return Metadata from commit 0; the fresh
    // rebuild would return Metadata from commit 2. `compare_snapshots` catches that on
    // `table_configuration`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_drops_stale_crc_preserves_correct_metadata(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        let table_root = ctx.url.as_str();

        // commit 0: initial P&M with empty configuration.
        commit(
            table_root,
            &ctx.store,
            0,
            vec![
                protocol_action(1, 2),
                metadata_action(json!({})),
                add_action("f0.parquet"),
            ],
        )
        .await;
        // commit 1: data-only add.
        commit(table_root, &ctx.store, 1, vec![add_action("f1.parquet")]).await;
        // commit 2: metadata change (new configuration property). A stale CRC@v1 would miss
        // this.
        commit(
            table_root,
            &ctx.store,
            2,
            vec![
                metadata_action(json!({"delta.myKey": "myValue"})),
                add_action("f2.parquet"),
            ],
        )
        .await;
        // commit 3: data-only add.
        commit(table_root, &ctx.store, 3, vec![add_action("f3.parquet")]).await;

        // CRC@v1 captures the BEFORE metadata (no "delta.myKey" property). This is the
        // stale CRC we expect the filter to reject.
        ctx.store
            .put(
                &delta_path_for_version(1, "crc"),
                make_test_crc_json(200, 2).to_string().into(),
            )
            .await?;

        let snapshot_v3 = Snapshot::builder_for(table_root)
            .at_version(3)
            .build(ctx.engine.as_ref())?;
        // snapshot_v3 was built via log replay, which correctly picks up the v2 metadata
        // change (pruned replay over commits after CRC@v1 finds the new metaData action).
        assert_eq!(
            snapshot_v3
                .table_properties()
                .unknown_properties
                .get("delta.myKey"),
            Some(&"myValue".to_string())
        );

        // Write checkpoint@v2, incorporating the new metadata.
        Snapshot::builder_for(table_root)
            .at_version(2)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        // Incremental update. With the filter: CRC@v1 dropped, log replay via checkpoint@v2
        // returns the correct new metadata. Without the filter: the inherited CRC@v1 returns
        // its stale (empty) configuration, and compare_snapshots fails on table_configuration.
        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;
        let fresh = Snapshot::builder_for(table_root).build(ctx.engine.as_ref())?;
        compare_snapshots(&updated, &fresh);

        // Observable contract: the user-visible metadata reflects the v2 change, not the
        // v0 baseline captured by the stale CRC.
        assert_eq!(
            updated
                .table_properties()
                .unknown_properties
                .get("delta.myKey"),
            Some(&"myValue".to_string())
        );

        Ok(())
    }

    // Setup: commits 0-3 with a metadata change at commit 2; stale CRC@v1 captures the
    // pre-change P&M; snapshot_v3 picks up the v2 metadata via log replay. A new
    // checkpoint@v1 is then written (at or below snapshot_v3, so the update takes the
    // P+M-replay path). The pruned replay on commits > v3 is empty, so without the skip
    // it would fall back to CRC@v1 and regress to the pre-v2 configuration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_preserves_metadata_when_below_checkpoint_crc_is_stale(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        let table_root = ctx.url.as_str();

        // commit 0: initial P&M with empty configuration.
        commit(
            table_root,
            &ctx.store,
            0,
            vec![
                protocol_action(1, 2),
                metadata_action(json!({})),
                add_action("f0.parquet"),
            ],
        )
        .await;
        // commit 1: data-only add.
        commit(table_root, &ctx.store, 1, vec![add_action("f1.parquet")]).await;
        // commit 2: metadata change (new configuration property).
        commit(
            table_root,
            &ctx.store,
            2,
            vec![
                metadata_action(json!({"delta.myKey": "myValue"})),
                add_action("f2.parquet"),
            ],
        )
        .await;
        // commit 3: data-only add.
        commit(table_root, &ctx.store, 3, vec![add_action("f3.parquet")]).await;

        // CRC@v1 captures pre-v2 metadata (empty configuration); "stale" relative to the
        // existing snapshot's v2-derived P&M.
        ctx.store
            .put(
                &delta_path_for_version(1, "crc"),
                make_test_crc_json(200, 2).to_string().into(),
            )
            .await?;

        let snapshot_v3 = Snapshot::builder_for(table_root)
            .at_version(3)
            .build(ctx.engine.as_ref())?;
        // Log replay picks up v2's metadata change despite CRC@v1's stale snapshot.
        assert_eq!(
            snapshot_v3
                .table_properties()
                .unknown_properties
                .get("delta.myKey"),
            Some(&"myValue".to_string())
        );

        // Write checkpoint@v1 (at or below snapshot_v3, so incremental update takes the
        // P+M-replay path instead of a full rebuild).
        Snapshot::builder_for(table_root)
            .at_version(1)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        // Incremental update. The pruned replay on commits > 3 is empty. With the stale-
        // CRC skip, the replay returns (None, None) and `TableConfiguration::try_new_from`
        // preserves existing P&M. Without the skip, the inherited CRC@v1 would load its stale
        // metadata and overwrite the existing (correct) v2 configuration.
        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;
        let fresh = Snapshot::builder_for(table_root).build(ctx.engine.as_ref())?;
        compare_snapshots(&updated, &fresh);

        // Observable contract: v2 metadata preserved.
        assert_eq!(
            updated
                .table_properties()
                .unknown_properties
                .get("delta.myKey"),
            Some(&"myValue".to_string())
        );

        Ok(())
    }

    // Multi-hop: v0 -> v5 (incremental P+M replay) -> v10 (checkpoint-ahead full rebuild).
    // Verifies that a CRC carried through the v5 hop is dropped when the new checkpoint
    // invalidates it, so the rebuilt v10 snapshot has no CRC.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_multi_hop_replay_then_rebuild_drops_stale_crc(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;
        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 11).await?;

        // CRC at v5 with matching P&M; it is well above any checkpoint (none exists yet),
        // so every hop inherits it.
        ctx.store
            .put(
                &delta_path_for_version(5, "crc"),
                make_test_crc_json(600, 6).to_string().into(),
            )
            .await?;

        // Hop 1: snapshot at v0.
        let snapshot_v0 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(0)
            .build(ctx.engine.as_ref())?;
        assert!(snapshot_v0.crc().is_none());

        // Hop 2: incremental v0 -> v5 (new commits, no checkpoint -> P+M replay). This picks up
        // CRC@v5 from the listing, which is at the snapshot version.
        let snapshot_v5 = Snapshot::builder_from(snapshot_v0)
            .at_version(5)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v5.log_segment.checkpoint_version, None);
        assert_eq!(snapshot_v5.crc().map(|c| c.version), Some(5));

        // Write checkpoint at v7.
        Snapshot::builder_for(ctx.url.as_str())
            .at_version(7)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref(), None)?;

        // Hop 3: incremental v5 -> v10 (discovers a new checkpoint@v7 ahead of the
        // existing snapshot version, triggering full rebuild). CRC@v5 is below the new
        // checkpoint (5 < 7) so the filter drops it, and the rebuilt snapshot has no CRC.
        let snapshot_v10 = Snapshot::builder_from(snapshot_v5).build(ctx.engine.as_ref())?;
        let fresh_v10 = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        compare_snapshots(&snapshot_v10, &fresh_v10);
        assert_eq!(snapshot_v10.version(), 10);
        assert_eq!(snapshot_v10.log_segment.checkpoint_version, Some(7));
        assert!(snapshot_v10.crc().is_none());

        Ok(())
    }

    // test new CRC in new log segment (old log segment has old CRC)
    #[tokio::test]
    async fn test_snapshot_new_from_crc() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(store.clone());
        let protocol = |reader_version, writer_version| {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": writer_version
                }
            })
        };
        let metadata = json!({
            "metaData": {
                "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        });
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 1),
            metadata.clone(),
        ];
        let commit1 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 2),
        ];

        // commit 0 and 1 jsons
        commit(table_root, &store, 0, commit0.clone()).await;
        commit(table_root, &store, 1, commit1).await;

        // Test CRC handling during incremental snapshot update (v0 -> v1).
        // The new log listing starts at v1, so the new log segment doesn't find 0.crc.
        // a) Only 0.crc exists: resolve_crc_file falls back to old segment's 0.crc.
        // b) Both 0.crc and 1.crc exist: resolve_crc_file picks up 1.crc.
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 1),
        });

        // put the old crc
        let path = delta_path_for_version(0, "crc");
        store.put(&path, crc.to_string().into()).await?;

        // base snapshot is at version 0
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;

        // a) only 0.crc exists -- falls back to old segment's 0.crc
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(
            snapshot
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            0
        );

        // b) both 0.crc and 1.crc exist; resolve_crc_file picks up 1.crc
        let path = delta_path_for_version(1, "crc");
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 2),
        });
        store.put(&path, crc.to_string().into()).await?;
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        let expected = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
        assert_eq!(
            snapshot
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            1
        );

        Ok(())
    }

    // ============================================================================
    // Tests: compaction-file handling on the incremental path
    // ============================================================================

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[tokio::test]
    async fn test_compaction_files_ignored_on_read() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits 0-2 and write compaction files to storage
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 0, 1).await?;
        write_compaction_file(&store, 0, 2).await?;

        // Compaction files exist on disk but should be skipped during listing
        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        assert!(
            snapshot
                .log_segment
                .listed
                .ascending_compaction_files
                .is_empty(),
            "Compaction files should be ignored when log compaction is disabled"
        );

        Ok(())
    }

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[tokio::test]
    async fn test_incremental_snapshot_ignores_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits 0-2 with compaction files in storage
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 0, 1).await?;
        write_compaction_file(&store, 0, 2).await?;

        // Build base snapshot at v2
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(snapshot_v2.version(), 2);
        assert!(snapshot_v2
            .log_segment
            .listed
            .ascending_compaction_files
            .is_empty());

        // Add commit 3
        commit(
            table_root,
            &store,
            3,
            vec![json!({"add": {"path": "file4.parquet", "partitionValues": {}, "size": 400, "modificationTime": 4000, "dataChange": true}})],
        )
        .await;

        // Build v3 incrementally -- compaction files should still be skipped
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;
        assert_eq!(snapshot_v3.version(), 3);
        assert!(snapshot_v3
            .log_segment
            .listed
            .ascending_compaction_files
            .is_empty());

        Ok(())
    }

    /// The incremental snapshot path (try_new_from_impl) re-lists files from the checkpoint
    /// version onwards. We must ensure that it deduplicates compaction files, since producing
    /// duplicates violated the sort invariant in LogSegmentFilesBuilder::build().
    #[tokio::test]
    #[ignore = "log compaction disabled (#2337)"]
    async fn test_incremental_snapshot_with_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits 0-3 and compaction files (1,1) and (1,2)
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 1, 1).await?;
        write_compaction_file(&store, 1, 2).await?;

        // Build snapshot at v2 (includes both compaction files)
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(
            snapshot_v2
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Add commit 3
        commit(
            table_root,
            &store,
            3,
            vec![json!({"add": {"path": "file4.parquet", "partitionValues": {}, "size": 400, "modificationTime": 4000, "dataChange": true}})],
        )
        .await;

        // Build v3 incrementally - before the fix, this panicked due to duplicate compaction files
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;

        assert_eq!(snapshot_v3.version(), 3);
        assert_eq!(
            snapshot_v3
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        Ok(())
    }

    /// This test documents a limitation: When deduplicating compactions, the deduplication logic
    /// only checks the start version (lo), not the hi version. So a new compaction file (1,3)
    /// added after building the base snapshot at v2 gets filtered out because its start version
    /// (1) <= existing_snapshot_version (2).
    #[tokio::test]
    #[ignore = "log compaction disabled (#2337)"]
    async fn test_incremental_snapshot_with_new_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create commits 0-3 and compaction files (1,2) and (2,2)
        setup_test_table_with_commits(table_root, &store, 4).await?;
        write_compaction_file(&store, 1, 2).await?;
        write_compaction_file(&store, 2, 2).await?;

        // Build snapshot at v2
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(
            snapshot_v2
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Add new compaction file (1,3) after building the base snapshot
        write_compaction_file(&store, 1, 3).await?;

        // Build v3 incrementally - the new (1,3) file gets filtered out because
        // the deduplication only looks at start version: 1 <= existing_snapshot_version (2)
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;

        assert_eq!(snapshot_v3.version(), 3);
        assert_eq!(
            snapshot_v3
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Verify we still have the original (1,2) and (2,2) files
        let versions_and_his: Vec<_> = snapshot_v3
            .log_segment
            .listed
            .ascending_compaction_files
            .iter()
            .map(|p| match p.file_type {
                LogPathFileType::CompactedCommit { hi } => (p.version, hi),
                _ => panic!("Expected CompactedCommit"),
            })
            .collect();
        assert_eq!(versions_and_his, vec![(1, 2), (2, 2)]);

        Ok(())
    }
}
