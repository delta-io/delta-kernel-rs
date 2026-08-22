//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::Arc;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
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
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::plans::ir::nodes::Agg;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
#[cfg(feature = "declarative-plans")]
use crate::schema::column_name;
use crate::schema::schema_ref;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
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
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            // Plan output is one aggregated row with no arrival order, so `resolve_pm` compares
            // source versions.
            let batches = self.read_pm_batches_via_plan(executor.as_ref())?;
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let version_reader: Option<SourceVersionReader> = Some(read_pm_versions);
            #[cfg(not(feature = "adaptive-metadata-in-dev"))]
            let version_reader: Option<SourceVersionReader> = None;
            return resolve_pm(batches, version_reader);
        }

        // The log stream is newest-first, so `resolve_pm` keeps the first value seen for each
        // field.
        resolve_pm(self.read_pm_batches(engine)?, None)
    }

    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        // The `checkpoint` action is projected as an opaque column; `replay_for_pm` parses and
        // reconciles it by version, since the plan can't unnest the nested P&M itself.
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
                // Also recover the version each value came from, for the newest-wins merge.
                #[cfg(feature = "adaptive-metadata-in-dev")]
                let a = a
                    .max_non_null_by(
                        column_name!(CHECKPOINT_ACTION_NAME),
                        column_name!(CHECKPOINT_ACTION_NAME),
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
                    )
                    .aggregate_as(
                        Agg::max_non_null_by(
                            column_name!("version"),
                            column_name!(CHECKPOINT_ACTION_NAME),
                            column_name!("version"),
                        ),
                        "checkpoint_version",
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

    // Replay the commit log, projecting rows to Protocol and Metadata action columns
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

/// Reads the (protocol, metadata, checkpoint) source versions from a plan-aggregated batch. Only
/// the plan produces these; the non-plan stream ranks the checkpoint by arrival order instead.
type SourceVersionReader =
    fn(&dyn EngineData) -> DeltaResult<(Option<i64>, Option<i64>, Option<i64>)>;

/// Resolves the newest Protocol and Metadata by replaying `batches`.
fn resolve_pm(
    batches: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    #[cfg_attr(not(feature = "adaptive-metadata-in-dev"), allow(unused_variables))]
    version_reader: Option<SourceVersionReader>,
) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
    let mut metadata_opt = None;
    let mut protocol_opt = None;
    for actions_batch in batches {
        let actions = actions_batch?.actions;
        if metadata_opt.is_none() {
            metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
        }
        if protocol_opt.is_none() {
            protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
        }
        // An AMT checkpoint action embeds its own P&M; take it for a field it has at least as new,
        // ranked by version_reader, else by arrival order.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some(checkpoint) = CheckpointAction::try_new_from_data(actions.as_ref())? {
            let (use_checkpoint_metadata, use_checkpoint_protocol) = match version_reader {
                Some(read_versions) => {
                    let (protocol_ver, metadata_ver, checkpoint_ver) =
                        read_versions(actions.as_ref())?;
                    (
                        metadata_ver <= checkpoint_ver,
                        protocol_ver <= checkpoint_ver,
                    )
                }
                // If no version_reader then use the checkpoint action if no P&M found on newer actions
                None => (metadata_opt.is_none(), protocol_opt.is_none()),
            };
            if use_checkpoint_metadata {
                metadata_opt = Some(checkpoint.metadata().clone());
            }
            if use_checkpoint_protocol {
                protocol_opt = Some(checkpoint.protocol().clone());
            }
        }

        if metadata_opt.is_some() && protocol_opt.is_some() {
            break;
        }
    }
    Ok((metadata_opt, protocol_opt))
}

/// Reads the `*_version` columns the plan aggregate emits: the version at which the newest
/// protocol, metaData, and checkpoint action were found (`None` if that action never appeared).
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
fn read_pm_versions(
    actions: &dyn EngineData,
) -> DeltaResult<(Option<i64>, Option<i64>, Option<i64>)> {
    #[derive(Default)]
    struct PmVersionsVisitor {
        protocol: Option<i64>,
        metadata: Option<i64>,
        checkpoint: Option<i64>,
    }
    impl RowVisitor for PmVersionsVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                (
                    vec![
                        column_name!("protocol_version"),
                        column_name!("metadata_version"),
                        column_name!("checkpoint_version"),
                    ],
                    vec![DataType::LONG, DataType::LONG, DataType::LONG],
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
                self.checkpoint = getters[2].get_opt(0, "checkpoint_version")?;
            }
            Ok(())
        }
    }
    let mut visitor = PmVersionsVisitor::default();
    visitor.visit_rows_of(actions)?;
    Ok((visitor.protocol, visitor.metadata, visitor.checkpoint))
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
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::Engine;
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

    // A two-column schema, used to distinguish "newer" metadata from the single-column
    // `SCHEMA_STRING` in override tests.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    const TWO_COLUMN_SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;

    // An engine that forces the non-plan reader (the plan reader rejects checkpoint actions), so
    // checkpoint resolution is exercised in every feature config, not only with plans disabled.
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

        // A successful build means P&M resolved from the checkpoint action; the schema confirms the
        // embedded metaData was used.
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        assert_eq!(snapshot.version(), 0);
        assert!(snapshot.schema().field("id").is_some());
    }

    // Top-level actions are seen first in backward replay, so the v1 metaData wins; the protocol
    // still comes from the checkpoint action.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[tokio::test]
    async fn test_later_log_commit_metadata_overrides_checkpoint_action() {
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
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[tokio::test]
    async fn test_newer_checkpoint_action_metadata_overrides_older_top_level_metadata() {
        let store = Arc::new(InMemory::new());
        let engine = non_plan_engine(store.clone());
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

    // A newer manifest-commit `checkpoint` action must win over older, complete, non-AMT top-level
    // P&M: newest-first replay reaches the checkpoint before the older top-level actions.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[tokio::test]
    async fn test_newer_checkpoint_action_overrides_complete_non_amt_top_level_pm() {
        let store = Arc::new(InMemory::new());
        let engine = non_plan_engine(store.clone());
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

    // Plan path (plain SyncEngine has a plan executor): a checkpoint newer than complete top-level
    // P&M wins both fields, exercising the version reconciliation in the plan branch.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[tokio::test]
    async fn test_plan_newer_checkpoint_overrides_top_level_pm() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = url::Url::parse("memory:///").unwrap();
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
        // The newer checkpoint's two-column metadata wins; a successful build also proves its
        // adaptive protocol resolved over the older non-AMT top-level protocol.
        assert!(snapshot.schema().field("name").is_some());
    }

    // Plan path: a metadata-only commit newer than the checkpoint keeps the top-level metadata,
    // while the protocol still comes from the checkpoint. Covers the keep-top-level merge branch.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[tokio::test]
    async fn test_plan_later_metadata_overrides_checkpoint() {
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
        assert!(snapshot.schema().field("name").is_some());
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
        // Part 3 skips normally (four read), but under `adaptive-metadata-in-dev` the projected
        // `checkpoint` column is also missing from every classic part, so part 3 no longer skips
        // and all five are read.
        let expected_parts = if cfg!(feature = "adaptive-metadata-in-dev") {
            5
        } else {
            4
        };
        assert_eq!(data.len(), expected_parts);
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
