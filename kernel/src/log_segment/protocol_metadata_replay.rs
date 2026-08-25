//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::Arc;
#[cfg(feature = "declarative-plans")]
use std::sync::LazyLock;

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
#[cfg(feature = "declarative-plans")]
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::Agg;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
#[cfg(feature = "declarative-plans")]
use crate::schema::column_name;
use crate::schema::{schema_ref, StructType};
#[cfg(feature = "declarative-plans")]
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
#[cfg(feature = "declarative-plans")]
use crate::EngineData;
use crate::{DeltaResult, Engine, Error};

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

    /// Replays the log segment for the newest Protocol and Metadata.
    ///
    /// The declarative-plan and imperative paths both produce [`PmCandidate`]s that [`resolve_pm`]
    /// resolves identically: newest version wins, and a checkpoint action's nested P&M is ranked at
    /// its own `checkpointMetadata.version`.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return resolve_pm(self.plan_pm_candidates(executor.as_ref())?);
        }
        resolve_pm(self.identify_pm_candidates(engine)?)
    }

    /// Reads the commit cover and then the checkpoint, yielding one [`PmCandidate`] per batch
    /// tagged with the version it came from.
    fn identify_pm_candidates(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<PmCandidate>> + Send> {
        let (commit_schema, checkpoint_schema) = pm_replay_schemas();
        let commits = self
            .versioned_commit_batches(engine, commit_schema)?
            .map(|batch| {
                let (version, batch) = batch?;
                pm_candidate(&batch, Some(version), Some(version))
            });

        // A base checkpoint's P&M is as of its file version -- distinct from an AMT checkpoint
        // *action*, which `resolve_pm` ranks at its own `checkpointMetadata.version`.
        let base_checkpoint_version = self.checkpoint_version.map(|v| v as i64);
        let checkpoint = self
            .create_checkpoint_stream(engine, checkpoint_schema, None, None, None, None)?
            .actions
            .map(move |b| pm_candidate(&b?, base_checkpoint_version, base_checkpoint_version));

        Ok(commits.chain(checkpoint))
    }

    /// Resolves P&M through the declarative plan, yielding one [`PmCandidate`] per emitted batch.
    ///
    /// The plan can't unnest the checkpoint action's nested P&M, so it projects `checkpoint` as an
    /// opaque column and emits the newest protocol and metaData versions in side columns. The
    /// candidate's P&M versions come from those columns; [`resolve_pm`] reconciles the checkpoint
    /// action by its own version.
    #[cfg(feature = "declarative-plans")]
    fn plan_pm_candidates(
        &self,
        executor: &dyn PlanExecutor,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<PmCandidate>> + Send> {
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
                    )
                    .aggregate_as(
                        Agg::max_non_null_by(
                            column_name!("version"),
                            column_name!(PROTOCOL_NAME),
                            column_name!("version"),
                        ),
                        "protocol_version",
                    )
                    .aggregate_as(
                        Agg::max_non_null_by(
                            column_name!("version"),
                            column_name!(METADATA_NAME),
                            column_name!("version"),
                        ),
                        "metadata_version",
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

        let candidates = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| {
                // The plan deduped every action to one row, so treat the result as a log batch:
                // that is where `pm_candidate` reads the checkpoint action from.
                let batch = ActionsBatch::new(batch?, true);
                let (protocol_version, metadata_version) = read_pm_versions(batch.actions.as_ref())?;
                pm_candidate(&batch, protocol_version, metadata_version)
            });
        Ok(candidates)
    }

    // Replay the commit log, projecting rows to Protocol and Metadata action columns
    #[cfg(test)]
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let (commit_schema, checkpoint_schema) = pm_replay_schemas();
        Ok(self
            .read_actions_with_projected_checkpoint_actions(
                engine,
                commit_schema,
                checkpoint_schema,
                None,
                None,
                None,
                None,
            )?
            .actions)
    }
}

/// Protocol and Metadata parsed from one batch, each tagged with the version it was seen at.
///
/// Under AMT, `checkpoint` carries a checkpoint action whose nested P&M is ranked at its own
/// `checkpointMetadata.version` rather than the version of the commit containing it.
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<CheckpointAction>,
}

/// Resolves the newest Protocol and Metadata across all candidates, ranked by version.
fn resolve_pm(
    candidates: impl Iterator<Item = DeltaResult<PmCandidate>>,
) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
    let mut metadata: Option<(i64, Metadata)> = None;
    let mut protocol: Option<(i64, Protocol)> = None;
    for candidate in candidates {
        let candidate = candidate?;
        take_newer(&mut metadata, candidate.metadata);
        take_newer(&mut protocol, candidate.protocol);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some(checkpoint) = candidate.checkpoint {
            let version = checkpoint.version();
            take_newer(
                &mut metadata,
                Some((version, checkpoint.metadata().clone())),
            );
            take_newer(
                &mut protocol,
                Some((version, checkpoint.protocol().clone())),
            );
        }
    }
    Ok((metadata.map(|(_, m)| m), protocol.map(|(_, p)| p)))
}

/// Replaces `current` when `candidate` is at least as new, keeping the highest version seen.
fn take_newer<T>(current: &mut Option<(i64, T)>, candidate: Option<(i64, T)>) {
    if let Some((version, value)) = candidate {
        if current.as_ref().is_none_or(|(v, _)| version >= *v) {
            *current = Some((version, value));
        }
    }
}

/// The `(commit_schema, checkpoint_schema)` for P&M replay. Commits additionally project the AMT
/// `checkpoint` action; base checkpoint files only carry top-level protocol and metaData.
fn pm_replay_schemas() -> (Arc<StructType>, Arc<StructType>) {
    let checkpoint_schema = schema_ref! {
        (&PROTOCOL_FIELD),
        (&METADATA_FIELD),
    };
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let commit_schema = schema_ref! {
        (&PROTOCOL_FIELD),
        (&METADATA_FIELD),
        (&CHECKPOINT_ACTION_FIELD),
    };
    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    let commit_schema = checkpoint_schema.clone();
    (commit_schema, checkpoint_schema)
}

/// Builds a [`PmCandidate`] from one batch, tagging its top-level protocol and metaData with the
/// versions the caller supplies (`None` when that action is absent from the batch). The AMT
/// checkpoint action is parsed only from log batches, where it can appear.
fn pm_candidate(
    batch: &ActionsBatch,
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
) -> DeltaResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    Ok(PmCandidate {
        protocol: Protocol::try_new_from_data(actions)?
            .zip(protocol_version)
            .map(|(p, v)| (v, p)),
        metadata: Metadata::try_new_from_data(actions)?
            .zip(metadata_version)
            .map(|(m, v)| (v, m)),
        #[cfg(feature = "adaptive-metadata-in-dev")]
        checkpoint: if batch.is_log_batch {
            CheckpointAction::try_new_from_data(actions)?
        } else {
            None
        },
    })
}

/// Reads the `*_version` columns the plan aggregate emits: the version of the newest protocol and
/// metaData (`None` if absent).
#[cfg(feature = "declarative-plans")]
fn read_pm_versions(actions: &dyn EngineData) -> DeltaResult<(Option<i64>, Option<i64>)> {
    #[derive(Default)]
    struct PmVersionsVisitor {
        protocol: Option<i64>,
        metadata: Option<i64>,
    }
    impl RowVisitor for PmVersionsVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                (
                    vec![
                        column_name!("protocol_version"),
                        column_name!("metadata_version"),
                    ],
                    vec![DataType::LONG, DataType::LONG],
                )
                    .into()
            });
            NAMES_AND_TYPES.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            if row_count > 0 {
                self.protocol = getters[0].get_opt(0, "protocol_version")?;
                self.metadata = getters[1].get_opt(0, "metadata_version")?;
            }
            Ok(())
        }
    }
    let mut visitor = PmVersionsVisitor::default();
    visitor.visit_rows_of(actions)?;
    Ok((visitor.protocol, visitor.metadata))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    #[cfg(any(feature = "declarative-plans", feature = "adaptive-metadata-in-dev"))]
    use std::sync::Arc;

    use itertools::Itertools;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use rstest::rstest;
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
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::Engine;
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::{DeltaResult, Error};

    // Single-column schema for a `metaData.schemaString`.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    const SCHEMA_STRING: &str =
        r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;

    // Builds a commit line with a `checkpoint` action that carries protocol and metadata at
    // `version`. The commit has no top-level protocol/metaData, so P&M comes only from that action.
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

    // Builds a top-level `metaData` commit line with the given schema (no protocol).
    #[cfg(feature = "adaptive-metadata-in-dev")]
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

    // Builds a top-level `protocol` commit line with the given reader/writer versions (no
    // features).
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn protocol_commit(min_reader_version: i64, min_writer_version: i64) -> String {
        serde_json::json!({ "protocol": {
            "minReaderVersion": min_reader_version,
            "minWriterVersion": min_writer_version,
        } })
        .to_string()
    }

    // Two-column schema, distinct from `SCHEMA_STRING` so a test can tell which metaData won.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    const TWO_COLUMN_SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;

    // Removes SyncEngine's plan executor so replay uses the non-plan path even when
    // `declarative-plans` is compiled in. Otherwise SyncEngine would use the plan path.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn non_plan_engine(store: Arc<InMemory>) -> impl Engine {
        let engine = SyncEngine::new_with_store(store);
        #[cfg(feature = "declarative-plans")]
        let engine = DelegatingEngine::new(Arc::new(engine)).without_plan_executor();
        engine
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[tokio::test]
    async fn test_load_resolves_pm_from_manifest_commit_checkpoint_action() {
        let store = Arc::new(InMemory::new());
        let engine = non_plan_engine(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();

        // The build succeeds only if P&M came from the checkpoint action, and `id` confirms it used
        // the embedded metaData.
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 0);
        assert!(snapshot.schema().field("id").is_some());
    }

    // The newest protocol and metadata win. Both replay paths must agree here.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[rstest]
    #[case::checkpoint_over_full_pm(
        format!("{}\n{}", protocol_commit(1, 2), metadata_commit(SCHEMA_STRING)),
        checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING)
    )]
    #[case::checkpoint_over_metadata_only(
        metadata_commit(SCHEMA_STRING),
        checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING)
    )]
    #[case::metadata_over_checkpoint(
        checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        metadata_commit(TWO_COLUMN_SCHEMA_STRING)
    )]
    #[tokio::test]
    async fn resolve_pm_newest_action_wins(#[case] v0: String, #[case] v1: String) {
        assert_newest_pm_wins(v0.clone(), v1.clone(), non_plan_engine).await;
        #[cfg(feature = "declarative-plans")]
        assert_newest_pm_wins(v0, v1, |store| SyncEngine::new_with_store(store)).await;
    }

    // Commits v0 then v1, builds a snapshot with `make_engine`, and checks the winner: v1's
    // two-column metaData and the checkpoint's adaptive protocol (reader v3).
    #[cfg(feature = "adaptive-metadata-in-dev")]
    async fn assert_newest_pm_wins<E: Engine>(
        v0: String,
        v1: String,
        make_engine: impl FnOnce(Arc<InMemory>) -> E,
    ) {
        let store = Arc::new(InMemory::new());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(table_root.as_str(), store.as_ref(), 0, v0)
            .await
            .unwrap();
        add_commit(table_root.as_str(), store.as_ref(), 1, v1)
            .await
            .unwrap();

        let engine = make_engine(store);
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 1);
        let schema = snapshot.schema();
        assert!(
            schema.field("name").is_some(),
            "v1's two-column metaData should win"
        );
        assert_eq!(
            schema.num_fields(),
            2,
            "resolved metaData should be exactly the two-column schema"
        );
        assert_eq!(
            snapshot
                .table_configuration()
                .protocol()
                .min_reader_version(),
            3,
            "protocol should resolve from the adaptive checkpoint action"
        );
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[tokio::test]
    async fn test_lagging_checkpoint_ranks_by_checkpoint_version() {
        assert_lagging_checkpoint_loses_to_gap_commit(non_plan_engine).await;
        #[cfg(feature = "declarative-plans")]
        assert_lagging_checkpoint_loses_to_gap_commit(|store| SyncEngine::new_with_store(store))
            .await;
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    async fn assert_lagging_checkpoint_loses_to_gap_commit<E: Engine>(
        make_engine: impl FnOnce(Arc<InMemory>) -> E,
    ) {
        let store = Arc::new(InMemory::new());
        let table_root = url::Url::parse("memory:///").unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            1,
            metadata_commit(TWO_COLUMN_SCHEMA_STRING),
        )
        .await
        .unwrap();
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            2,
            checkpoint_commit(0, &["adaptiveMetadata-preview"], SCHEMA_STRING),
        )
        .await
        .unwrap();

        let engine = make_engine(store);
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 2);
        let schema = snapshot.schema();
        assert!(schema.field("name").is_some());
        assert_eq!(schema.num_fields(), 2);
    }

    // A [`PlanExecutor`] that fails every operation. A test uses it to check that a plan-path
    // failure surfaces from P&M replay instead of falling back to legacy replay.
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
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
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
