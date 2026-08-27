//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

use tracing::{info, instrument};
use url::Url;

use super::LogSegment;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::actions::CHECKPOINT_ACTION_NAME;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, CHECKPOINT_ACTION_FIELD};
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
use crate::path::ParsedLogPath;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::Agg;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::{
    column_name, schema_ref, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    StructField, StructType,
};
use crate::{DeltaResult, Engine, EngineData, Error, Version};

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

    /// Replays the log segment for the latest Protocol and Metadata.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return resolve_pm_batches(self.read_pm_batches_via_plan(executor.as_ref())?);
        }
        resolve_pm_batches(self.read_pm_batches(engine)?)
    }

    /// Reads the P&M commit cover and checkpoint via the declarative plan, tagging each batch with
    /// its version.
    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<VersionedBatch>> + Send> {
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
                let protocol = || column_name!(PROTOCOL_NAME);
                let metadata = || column_name!(METADATA_NAME);
                let version = || column_name!("version");
                // The version aggregates are aliased; unaliased both would be named `version`.
                let a = a
                    .max_non_null_by(protocol(), protocol(), version())
                    .max_non_null_by(metadata(), metadata(), version())
                    .aggregate_as(
                        Agg::max_non_null_by(version(), protocol(), version()),
                        "protocol_version",
                    )
                    .aggregate_as(
                        Agg::max_non_null_by(version(), metadata(), version()),
                        "metadata_version",
                    );
                #[cfg(feature = "adaptive-metadata-in-dev")]
                let a = a.max_non_null_by(
                    column_name!(CHECKPOINT_ACTION_NAME),
                    column_name!(CHECKPOINT_ACTION_NAME),
                    version(),
                );
                a
            })?
            .build()?;

        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| {
                // Mark as a log batch so the checkpoint action is read from it.
                let batch = ActionsBatch::new(batch?, true);
                let (protocol_version, metadata_version) =
                    pm_versions_from_plan_output(batch.actions.as_ref())?;
                Ok(VersionedBatch {
                    protocol_version,
                    metadata_version,
                    batch,
                })
            });
        Ok(batches)
    }

    /// Reads the P&M commit cover and checkpoint, tagging each batch with its version.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<VersionedBatch>> + Send> {
        let (commit_schema, checkpoint_schema) = pm_replay_schemas();
        // Commit schema only: `_file` in the checkpoint schema would break its skipping predicate.
        let file_column =
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath);
        let commit_schema = Arc::new(StructType::try_new(
            commit_schema.fields().cloned().chain([file_column]),
        )?);
        let checkpoint_version = self.checkpoint_version.map(|v| v as i64);
        let batches = self
            .read_actions_with_projected_checkpoint_actions(
                engine,
                commit_schema,
                checkpoint_schema,
                None,
                None,
                None,
                None,
            )?
            .actions;
        Ok(batches.map(move |batch| {
            let batch = batch?;
            // A commit's version is parsed from its `_file`; a checkpoint batch uses the constant.
            let version = if batch.is_log_batch {
                batch_version(batch.actions.as_ref())? as i64
            } else {
                checkpoint_version
                    .ok_or_else(|| Error::internal_error("checkpoint batch without a version"))?
            };
            Ok(VersionedBatch {
                protocol_version: Some(version),
                metadata_version: Some(version),
                batch,
            })
        }))
    }
}

/// Protocol and Metadata parsed from one batch, each with the version it was found at
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    batch: ActionsBatch,
}

/// The newest Protocol and Metadata across `batches`.
fn resolve_pm_batches(
    batches: impl Iterator<Item = DeltaResult<VersionedBatch>>,
) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
    let mut metadata: Option<(i64, Metadata)> = None;
    let mut protocol: Option<(i64, Protocol)> = None;
    for batch in batches {
        let VersionedBatch {
            protocol_version,
            metadata_version,
            batch,
        } = batch?;
        let batch_version = protocol_version.max(metadata_version);
        let candidate = pm_candidate(&batch, protocol_version, metadata_version)?;
        metadata = newer(metadata, candidate.metadata);
        protocol = newer(protocol, candidate.protocol);
        // A checkpoint action's P&M can be older than its commit, so check version not presence.
        if is_final(&protocol, batch_version) && is_final(&metadata, batch_version) {
            break;
        }
    }
    Ok((metadata.map(|(_, m)| m), protocol.map(|(_, p)| p)))
}

/// Parses the log version from a batch's `_file` metadata column.
fn batch_version(data: &dyn EngineData) -> DeltaResult<Version> {
    #[derive(Default)]
    struct FilePathVisitor {
        file: Option<String>,
    }
    impl RowVisitor for FilePathVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
                LazyLock::new(|| (vec![column_name!("_file")], vec![DataType::STRING]).into());
            NAMES_AND_TYPES.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> DeltaResult<()> {
            if self.file.is_none() && row_count > 0 {
                self.file = getters[0].get_opt(0, "_file")?;
            }
            Ok(())
        }
    }
    let mut visitor = FilePathVisitor::default();
    visitor.visit_rows_of(data)?;
    let file = visitor
        .file
        .ok_or_else(|| Error::internal_error("commit batch missing _file column"))?;
    let url = Url::parse(&file)
        .map_err(|e| Error::internal_error(format!("batch has invalid _file {file}: {e}")))?;
    ParsedLogPath::try_from(url)?
        .map(|path| path.version)
        .ok_or_else(|| Error::internal_error(format!("batch from non-log file {file}")))
}

/// Whether `winner` is set at a version at least `batch_version`.
fn is_final<T>(winner: &Option<(i64, T)>, batch_version: Option<i64>) -> bool {
    match (winner, batch_version) {
        (Some((version, _)), Some(bv)) => *version >= bv,
        _ => false,
    }
}

/// The higher-versioned of `a` and `b`; on a tie, `b` wins.
fn newer<T>(a: Option<(i64, T)>, b: Option<(i64, T)>) -> Option<(i64, T)> {
    match (a, b) {
        (Some(a), Some(b)) => {
            if b.0 >= a.0 {
                Some(b)
            } else {
                Some(a)
            }
        }
        (a, b) => a.or(b),
    }
}

/// The commit and checkpoint read schemas for P&M replay; the commit schema also reads the AMT
/// `checkpoint` action.
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

/// The Protocol and Metadata in `batch`, tagged with the given versions, plus any AMT checkpoint
/// action's nested P&M at its own version.
fn pm_candidate(
    batch: &ActionsBatch,
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
) -> DeltaResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = protocol_version.zip(Protocol::try_new_from_data(actions)?);
    let metadata = metadata_version.zip(Metadata::try_new_from_data(actions)?);
    let (checkpoint_protocol, checkpoint_metadata) = match checkpoint_pm(batch)? {
        Some((version, p, m)) => (Some((version, p)), Some((version, m))),
        None => (None, None),
    };
    Ok(PmCandidate {
        protocol: newer(protocol, checkpoint_protocol),
        metadata: newer(metadata, checkpoint_metadata),
    })
}

/// The Protocol and Metadata nested in `batch`'s `checkpoint` action, at the action's own
/// `checkpointMetadata.version`.
fn checkpoint_pm(batch: &ActionsBatch) -> DeltaResult<Option<(i64, Protocol, Metadata)>> {
    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        if !batch.is_log_batch {
            return Ok(None);
        }

        let checkpoint = CheckpointAction::try_new_from_data(batch.actions.as_ref())?;

        Ok(checkpoint.map(|checkpoint| {
            (
                checkpoint.version(),
                checkpoint.protocol().clone(),
                checkpoint.metadata().clone(),
            )
        }))
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    {
        let _ = batch;
        Ok(None)
    }
}

/// Reads the `protocol_version` and `metadata_version` columns the plan aggregate emits.
#[cfg(feature = "declarative-plans")]
fn pm_versions_from_plan_output(
    actions: &dyn EngineData,
) -> DeltaResult<(Option<i64>, Option<i64>)> {
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
    #[cfg(feature = "declarative-plans")]
    use std::sync::Arc;

    use itertools::Itertools;
    use test_log::test;

    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::{DeltaResult, Error};

    #[cfg(feature = "adaptive-metadata-in-dev")]
    mod amt {
        use std::sync::Arc;

        use rstest::rstest;
        use test_utils::add_commit;

        use crate::engine::sync::SyncEngine;
        #[cfg(feature = "declarative-plans")]
        use crate::engine::test_delegating::DelegatingEngine;
        use crate::object_store::memory::InMemory;
        use crate::{Engine, Snapshot};

        const ONE_COLUMN_SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;

        // Builds a commit line with a `checkpoint` action that carries protocol and metadata at
        // `version`. The commit has no top-level protocol/metaData, so P&M comes only from that
        // action.
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
        fn protocol_commit(min_reader_version: i64, min_writer_version: i64) -> String {
            serde_json::json!({ "protocol": {
            "minReaderVersion": min_reader_version,
            "minWriterVersion": min_writer_version,
        } })
            .to_string()
        }

        const TWO_COLUMN_SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;

        // Removes SyncEngine's plan executor so replay uses the non-plan path even when
        // `declarative-plans` is compiled in. Otherwise SyncEngine would use the plan path.
        fn non_plan_engine(store: Arc<InMemory>) -> impl Engine {
            let engine = SyncEngine::new_with_store(store);
            #[cfg(feature = "declarative-plans")]
            let engine = DelegatingEngine::new(Arc::new(engine)).without_plan_executor();
            engine
        }

        #[tokio::test]
        async fn test_load_resolves_pm_from_manifest_commit_checkpoint_action() {
            check_manifest_commit_checkpoint(non_plan_engine).await;
            #[cfg(feature = "declarative-plans")]
            check_manifest_commit_checkpoint(|store| SyncEngine::new_with_store(store)).await;
        }

        // A single commit whose only P&M is a checkpoint action. The build succeeds only if P&M
        // came from that action, and the `id` column confirms it used the embedded
        // metaData.
        async fn check_manifest_commit_checkpoint<E: Engine>(
            make_engine: impl FnOnce(Arc<InMemory>) -> E,
        ) {
            let store = Arc::new(InMemory::new());
            let table_root = url::Url::parse("memory:///").unwrap();
            add_commit(
                table_root.as_str(),
                store.as_ref(),
                0,
                checkpoint_commit(0, &["adaptiveMetadata-preview"], ONE_COLUMN_SCHEMA_STRING),
            )
            .await
            .unwrap();

            let engine = make_engine(store);
            let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
            assert_eq!(snapshot.version(), 0);
            assert!(snapshot.schema().field("id").is_some());
        }

        // The newest protocol and metadata win, ranked by version. Every case runs on both
        // plan/non-plan replay paths. Each case gives its own expected metaData column count and
        // protocol reader-feature count so the assertion states what that case is checking.
        #[rstest]
        // A newer checkpoint action beats the older top-level protocol and metaData.
        #[case::newer_checkpoint_beats_older_pm(
        format!("{}\n{}", protocol_commit(1, 2), metadata_commit(ONE_COLUMN_SCHEMA_STRING)),
        checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING),
        2,
        1
    )]
        // A newer checkpoint action's metaData beats the older top-level metaData.
        #[case::newer_checkpoint_beats_older_metadata(
        metadata_commit(ONE_COLUMN_SCHEMA_STRING),
        checkpoint_commit(1, &["adaptiveMetadata-preview"], TWO_COLUMN_SCHEMA_STRING),
        2,
        1
    )]
        // A newer top-level metaData beats the older checkpoint action's metaData.
        #[case::newer_metadata_beats_older_checkpoint(
        checkpoint_commit(0, &["adaptiveMetadata-preview"], ONE_COLUMN_SCHEMA_STRING),
        metadata_commit(TWO_COLUMN_SCHEMA_STRING),
        2,
        1
    )]
        // Both protocols are reader v3, so only version ordering can pick the winner. The newer
        // checkpoint action lists a second reader feature, and that count is what the assertion
        // checks.
        #[case::newer_checkpoint_protocol_wins(
        checkpoint_commit(0, &["adaptiveMetadata-preview"], ONE_COLUMN_SCHEMA_STRING),
        checkpoint_commit(1, &["adaptiveMetadata-preview", "deletionVectors"], ONE_COLUMN_SCHEMA_STRING),
        1,
        2
    )]
        #[tokio::test]
        async fn resolve_pm_newest_action_wins(
            #[case] v0: String,
            #[case] v1: String,
            #[case] expected_fields: usize,
            #[case] expected_reader_features: usize,
        ) {
            assert_newest_pm_wins(v0, v1, expected_fields, expected_reader_features).await;
        }

        // Runs the check on the non-plan path, then on the plan path when it's compiled in, so
        // every case is verified against both.
        async fn assert_newest_pm_wins(
            v0: String,
            v1: String,
            expected_fields: usize,
            expected_reader_features: usize,
        ) {
            build_and_check_pm(
                &v0,
                &v1,
                expected_fields,
                expected_reader_features,
                non_plan_engine,
            )
            .await;
            #[cfg(feature = "declarative-plans")]
            build_and_check_pm(
                &v0,
                &v1,
                expected_fields,
                expected_reader_features,
                |store| SyncEngine::new_with_store(store),
            )
            .await;
        }

        // Commits v0 then v1 with `make_engine`, then checks the resolved metaData column count and
        // the resolved protocol's reader-feature count.
        async fn build_and_check_pm<E: Engine>(
            v0: &str,
            v1: &str,
            expected_fields: usize,
            expected_reader_features: usize,
            make_engine: impl FnOnce(Arc<InMemory>) -> E,
        ) {
            let store = Arc::new(InMemory::new());
            let table_root = url::Url::parse("memory:///").unwrap();
            add_commit(table_root.as_str(), store.as_ref(), 0, v0.to_string())
                .await
                .unwrap();
            add_commit(table_root.as_str(), store.as_ref(), 1, v1.to_string())
                .await
                .unwrap();

            let engine = make_engine(store);
            let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();

            assert_eq!(snapshot.version(), 1);
            assert_eq!(
                snapshot.schema().num_fields(),
                expected_fields,
                "resolved metaData should have {expected_fields} column(s)"
            );
            let protocol = snapshot.table_configuration().protocol();
            assert_eq!(
                protocol.min_reader_version(),
                3,
                "AMT protocol is reader v3"
            );
            assert_eq!(
                protocol.reader_features().map_or(0, |f| f.len()),
                expected_reader_features,
                "resolved protocol should have {expected_reader_features} reader feature(s)"
            );
        }

        #[tokio::test]
        async fn test_lagging_checkpoint_ranks_by_checkpoint_version() {
            assert_lagging_checkpoint_loses_to_gap_commit(non_plan_engine).await;
            #[cfg(feature = "declarative-plans")]
            assert_lagging_checkpoint_loses_to_gap_commit(|store| {
                SyncEngine::new_with_store(store)
            })
            .await;
        }

        async fn assert_lagging_checkpoint_loses_to_gap_commit<E: Engine>(
            make_engine: impl FnOnce(Arc<InMemory>) -> E,
        ) {
            let store = Arc::new(InMemory::new());
            let table_root = url::Url::parse("memory:///").unwrap();
            add_commit(
                table_root.as_str(),
                store.as_ref(),
                0,
                checkpoint_commit(0, &["adaptiveMetadata-preview"], ONE_COLUMN_SCHEMA_STRING),
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
                checkpoint_commit(0, &["adaptiveMetadata-preview"], ONE_COLUMN_SCHEMA_STRING),
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
    }

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
