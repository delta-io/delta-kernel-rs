//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::Arc;

use tracing::{info, instrument};

use super::LogSegment;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::actions::CHECKPOINT_ACTION_NAME;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, CHECKPOINT_ACTION_FIELD};
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
#[cfg(feature = "declarative-plans")]
use crate::schema::column_name;
use crate::schema::schema_ref;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing, and the [`ProtocolMetadataSource`] describing how
    /// P&M was resolved.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_opt`], used for fresh
    /// snapshot creation where both Protocol and Metadata must exist.
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> DeltaResult<(Metadata, Protocol, ProtocolMetadataSource)> {
        match self.read_protocol_metadata_opt(engine, crc)? {
            (Some(m), Some(p), source) => Ok((m, p, source)),
            (None, Some(_), _) => Err(Error::MissingMetadata),
            (Some(_), None, _) => Err(Error::MissingProtocol),
            (None, None, _) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns `None` for either if not found.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata`], used for incremental
    /// snapshot updates where the caller can fall back to an existing snapshot's Protocol and
    /// Metadata.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>, ProtocolMetadataSource)> {
        // Case 1: If CRC at target version, use it directly and exit early.
        if let Some(crc) = crc.filter(|c| c.version == self.end_version) {
            info!("P&M from CRC at target version {}", self.end_version);
            return Ok((
                Some(crc.metadata.clone()),
                Some(crc.protocol.clone()),
                ProtocolMetadataSource::CrcAtTarget,
            ));
        }

        // We didn't return above, so we need to do log replay to find P&M.
        //
        // Case 2: CRC exists at an earlier version => Prune the log segment to only replay
        //         commits *after* the CRC version.
        //   (a) If we find new P&M in the pruned replay, return it.
        //   (b) If we don't find new P&M, fall back to the CRC.
        //
        // Case 3: No CRC exists => Full P&M log replay.

        if let Some(crc) = crc.filter(|c| c.version < self.end_version) {
            // Case 2(a): Replay only commits after CRC version
            info!(
                "Pruning log segment to commits after CRC version {}",
                crc.version
            );
            let pruned = self.segment_after_version(crc.version);
            let (metadata_opt, protocol_opt) = pruned.replay_for_pm(engine)?;

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok((
                    metadata_opt,
                    protocol_opt,
                    ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                ));
            }

            // Case 2(b): P&M incomplete from pruned replay, use the CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            return Ok((
                metadata_opt.or_else(|| Some(crc.metadata.clone())),
                protocol_opt.or_else(|| Some(crc.protocol.clone())),
                ProtocolMetadataSource::CrcSeededPmOnlyReplay,
            ));
        }

        // Case 3: Full P&M log replay.
        let (metadata_opt, protocol_opt) = self.replay_for_pm(engine)?;
        Ok((
            metadata_opt,
            protocol_opt,
            ProtocolMetadataSource::FullReplay,
        ))
    }

    /// Replays the log segment for Protocol and Metadata, stopping early once both are found.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        // The plan reader can't unnest a `checkpoint` action, so it rejects one rather than return
        // possibly-stale top-level P&M. The non-plan reader resolves it inline.
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            let mut metadata_opt = None;
            let mut protocol_opt = None;
            for actions_batch in self.read_pm_batches_via_plan(executor.as_ref())? {
                let actions = actions_batch?.actions;
                try_fill_pm(actions.as_ref(), &mut metadata_opt, &mut protocol_opt)?;
                #[cfg(feature = "adaptive-metadata-in-dev")]
                require!(
                    CheckpointAction::try_new_from_data(actions.as_ref())?.is_none(),
                    Error::unsupported(
                        "reading adaptiveMetadata checkpoint actions is unsupported under \
                         declarative-plans execution"
                    )
                );
            }
            return Ok((metadata_opt, protocol_opt));
        }

        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in self.read_pm_batches(engine)? {
            let actions = actions_batch?.actions;
            if try_fill_pm(actions.as_ref(), &mut metadata_opt, &mut protocol_opt)? {
                break;
            }
            #[cfg(feature = "adaptive-metadata-in-dev")]
            if let Some((metadata, protocol)) =
                resolve_pm_from_checkpoint(actions.as_ref(), &metadata_opt, &protocol_opt)?
            {
                return Ok((Some(metadata), Some(protocol)));
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // The `checkpoint` action is projected so a present one is detected; `replay_for_pm` then
        // errors, since the plan can't unnest it.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let versioned_schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            (&CHECKPOINT_ACTION_FIELD),
            not_null "version": LONG,
        };
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let versioned_schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            not_null "version": LONG,
        };

        let commit_files = self.commit_cover_version_tagged_scan_files()?;
        let commits = PlanBuilder::scan_json(commit_files, &["version"], versioned_schema.clone())?;

        // A checkpoint's parts share one format; scan them with the matching operator.
        let checkpoint = self
            .checkpoint_version_tagged_scan_files()?
            .map(|(file_type, checkpoint_files)| {
                let scan = match file_type {
                    FileType::Json => PlanBuilder::scan_json,
                    FileType::Parquet => PlanBuilder::scan_parquet,
                };
                scan(checkpoint_files, &["version"], versioned_schema.clone())
            })
            .transpose()?;

        let plan = PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .aggregate_ungrouped(|a| {
                let a = a
                    .max_non_null_by(
                        column_name!(PROTOCOL_NAME),
                        column_name!(PROTOCOL_NAME),
                        column_name!("version"),
                    )
                    .max_non_null_by(
                        column_name!(METADATA_NAME),
                        column_name!(METADATA_NAME),
                        column_name!("version"),
                    );
                #[cfg(feature = "adaptive-metadata-in-dev")]
                let a = a.max_non_null_by(
                    column_name!(CHECKPOINT_ACTION_NAME),
                    column_name!(CHECKPOINT_ACTION_NAME),
                    column_name!("version"),
                );
                a
            })?
            .build()?;

        // NOTE: The plan dedupes all actions, so mark all results as coming from checkpoint
        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| Ok(ActionsBatch::new(batch?, true)));
        Ok(Box::new(batches))
    }

    // Replay the commit log, projecting rows to Protocol and Metadata action columns, plus the
    // `checkpoint` action under `adaptive-metadata-in-dev` so a single pass also resolves P&M
    // embedded in a manifest-commit checkpoint.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            (&CHECKPOINT_ACTION_FIELD),
        };
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
        };
        self.read_actions(engine, schema)
    }
}

/// Fill `metadata`/`protocol` from `actions` if not already resolved, returning whether both are
/// now present.
fn try_fill_pm(
    actions: &dyn EngineData,
    metadata: &mut Option<Metadata>,
    protocol: &mut Option<Protocol>,
) -> DeltaResult<bool> {
    if metadata.is_none() {
        *metadata = Metadata::try_new_from_data(actions)?;
    }
    if protocol.is_none() {
        *protocol = Protocol::try_new_from_data(actions)?;
    }
    Ok(metadata.is_some() && protocol.is_some())
}

/// Resolve complete P&M from a `checkpoint` action in `actions`, filling any gap in `metadata`/
/// `protocol` from it. Returns `None` when there's no `checkpoint` action.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn resolve_pm_from_checkpoint(
    actions: &dyn EngineData,
    metadata: &Option<Metadata>,
    protocol: &Option<Protocol>,
) -> DeltaResult<Option<(Metadata, Protocol)>> {
    let Some(checkpoint) = CheckpointAction::try_new_from_data(actions)? else {
        return Ok(None);
    };
    let metadata = metadata
        .clone()
        .unwrap_or_else(|| checkpoint.metadata().clone());
    let protocol = protocol
        .clone()
        .unwrap_or_else(|| checkpoint.protocol().clone());
    Ok(Some((metadata, protocol)))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    #[cfg(any(feature = "declarative-plans", feature = "adaptive-metadata-in-dev"))]
    use std::sync::Arc;

    use itertools::Itertools;
    use test_log::test;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use test_utils::add_commit;

    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::object_store::memory::InMemory;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::{DeltaResult, Error};

    // A minimal single-column Delta schema, serialized as `metaData.schemaString` expects.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    const SCHEMA_STRING: &str =
        r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;

    // Build a manifest-commit `checkpoint` action commit line embedding Protocol + Metadata at the
    // given `checkpointMetadata.version`, with the given reader/writer features and schema. The
    // commit has NO top-level protocol/metaData, so P&M can only be resolved from the checkpoint
    // action.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn checkpoint_commit(version: i64, features: &[&str], schema_string: &str) -> String {
        serde_json::json!({ "checkpoint": [
            { "checkpointMetadata": { "version": version } },
            { "contentRoot": { "path": "metadata/root.parquet", "sizeInBytes": 1, "version": version } },
            { "protocol": {
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": features, "writerFeatures": features,
            } },
            { "metaData": {
                "id": "test-table",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
            } },
        ] })
        .to_string()
    }

    // Build a top-level `metaData` commit line with the given schema (no protocol).
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    fn metadata_commit(schema_string: &str) -> String {
        serde_json::json!({ "metaData": {
            "id": "test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {},
        } })
        .to_string()
    }

    // A two-column schema, used to distinguish "newer" metadata from the single-column
    // `SCHEMA_STRING` in override tests.
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    const TWO_COLUMN_SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;

    // Checkpoint-action resolution is a non-plan-path capability; the plan path rejects it.
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    #[tokio::test]
    async fn test_load_resolves_pm_from_manifest_commit_checkpoint_action() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();

        // A successful build means P&M resolved from the checkpoint action; the schema confirms the
        // embedded metaData was used.
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 0);
        assert!(snapshot.schema().field("id").is_some());
    }

    // Top-level actions are seen first in backward replay, so the v1 metaData wins; the protocol
    // still comes from the checkpoint action.
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    #[tokio::test]
    async fn test_later_log_commit_metadata_overrides_checkpoint_action() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();
        // A metadata-only log commit at v1 with a two-column schema.
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            1,
            metadata_commit(TWO_COLUMN_SCHEMA_STRING),
        )
        .await
        .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 1);
        // Newer metadata (two columns) wins over the checkpoint action's single-column metadata.
        assert!(snapshot.schema().field("name").is_some());
    }

    // When a newer checkpoint action carries metadata that differs from an older top-level
    // metaData, the checkpoint's metadata must win.
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    #[tokio::test]
    async fn test_newer_checkpoint_action_metadata_overrides_older_top_level_metadata() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        // v0: a top-level metaData with a single-column schema (the older, subsumed metadata).
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            metadata_commit(SCHEMA_STRING),
        )
        .await
        .unwrap();
        // v1: a manifest-commit checkpoint action embedding a two-column schema.
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            1,
            checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING),
        )
        .await
        .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 1);
        // The checkpoint action's metadata (two columns) wins over the older top-level metaData.
        assert!(snapshot.schema().field("name").is_some());
    }

    // A newer manifest-commit `checkpoint` action that turns on AMT must win over older, complete,
    // non-AMT top-level P&M. The first pass sees only the older top-level protocol/metaData, so the
    // reconcile scan must still run rather than trust the first pass's (stale) view that the table
    // is non-AMT and skip it.
    #[cfg(all(
        feature = "adaptive-metadata-in-dev",
        not(feature = "declarative-plans")
    ))]
    #[tokio::test]
    async fn test_newer_checkpoint_action_overrides_complete_non_amt_top_level_pm() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        // v0: complete, non-AMT top-level protocol (legacy reader 1 / writer 2) + metaData (single
        // column). Actions are separate newline-delimited lines in the commit file.
        let protocol =
            serde_json::json!({ "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 } });
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            format!("{protocol}\n{}", metadata_commit(SCHEMA_STRING)),
        )
        .await
        .unwrap();
        // v1: a manifest commit whose only P&M lives in an AMT checkpoint action (two columns).
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            1,
            checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING),
        )
        .await
        .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 1);
        // The newer checkpoint action's metadata (two columns) wins over the older top-level
        // metaData; a successful build also proves its adaptive protocol was resolved.
        assert!(snapshot.schema().field("name").is_some());
    }

    // The plan reader can't unnest a checkpoint action, so reading one is rejected, not resolved.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[tokio::test]
    async fn test_checkpoint_action_under_plan_execution_is_unsupported() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();

        let err = Snapshot::builder_for(table_root)
            .build(&engine)
            .expect_err("checkpoint action under plan execution must be rejected");
        assert!(
            matches!(err, crate::Error::Unsupported(_)),
            "unexpected error: {err}"
        );
    }

    // A [`PlanExecutor`] whose every operation fails, used to prove that a plan-path failure
    // surfaces from P&M replay rather than falling back to legacy replay.
    #[cfg(feature = "declarative-plans")]
    struct FailingPlanExecutor;

    #[cfg(feature = "declarative-plans")]
    impl PlanExecutor for FailingPlanExecutor {
        fn execute_op(&self, _op: Operation) -> DeltaResult<PlanResult> {
            Err(Error::generic("plan executor deliberately failed"))
        }
    }

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let data: Vec<_> = snapshot
            .log_segment()
            .read_pm_batches(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- row group skipping is
        // disabled for parts missing a projected column, so parts 1 and 5 are read regardless.
        // Under `adaptive-metadata-in-dev` no classic part carries the projected
        // `checkpoint` column, so part 3 no longer skips either -- all five are read
        // instead of two.
        assert_eq!(data.len(), 5);
    }

    // With the `declarative-plans` feature flag on, `SyncEngine` resolves P&M through the
    // declarative plan.
    //
    // This fixture's checkpoint names its map entry fields `entries` where kernel expects
    // `key_value`. Parquet takes that name from the writer's Arrow schema unless the writer sets
    // `WriterProperties::coerce_types`, which is off by default, and Arrow's own
    // `MapFieldNames::default()` is `entries`. So a writer that builds its maps from Arrow defaults
    // produces a file kernel must translate on read. Spark and kernel both write `key_value`,
    // covered by
    // `scan_plan::execution_tests::declarative_metadata_reconciles_checkpoint_with_later_commits`.
    #[test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_entries_named_maps() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 1);
        assert_eq!(snapshot.schema().fields().count(), 3);
    }

    // The array counterpart of the test above. This fixture's checkpoint names its array element
    // fields `item` where kernel expects `element`, so it covers the other half of the naming
    // disagreement. `metaData.partitionColumns` is the array in question, and it is present in
    // every `metaData` action, so its element name is checked on every P&M replay.
    #[test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_item_named_arrays() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 5);
        assert_eq!(snapshot.schema().fields().count(), 5);
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn test_snapshot_build_via_failing_plan_executor_surfaces_error_without_fallback() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_plan_executor(Arc::new(FailingPlanExecutor));

        let result = Snapshot::builder_for(url).build(&engine);

        assert!(
            result.is_err(),
            "plan failure must surface, not fall back to legacy replay"
        );
    }
}
