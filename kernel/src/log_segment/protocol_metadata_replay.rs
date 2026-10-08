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
use crate::actions::{CheckpointAction, LastManifestCommit, CHECKPOINT_ACTION_FIELD};
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
#[cfg(feature = "declarative-plans")]
use crate::expressions::{col, Predicate};
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
use crate::path::ParsedLogPath;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::Agg;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::plan::Plan;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
use crate::schema::{
    column_name, schema_ref, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    StructField, StructType,
};
use crate::{Engine, EngineData, KernelError, KernelResult, Result, Version};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// The result's `metadata` and `protocol` are `None` if not found.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    ///
    /// Under `adaptive-metadata-in-dev`, the result's `checkpoint_action` is a
    /// [`CheckpointActionResolution`] describing how the latest AMT checkpoint action was resolved
    /// (captured by replay, hinted by the CRC, or unresolved), for the caller to store on the
    /// [`Snapshot`](crate::Snapshot).
    #[instrument(name = "log_seg.load_p_m", skip_all, fields(enable_call_frame), err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> KernelResult<PmResolution> {
        // Case 1: If CRC at target version, use it directly and exit early.
        if let Some(crc) = crc.filter(|c| c.version == self.end_version) {
            info!("P&M from CRC at target version {}", self.end_version);
            return Ok(PmResolution::from_crc(
                crc,
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
            let candidate = pruned.replay_for_pm(engine)?;
            // Ignore pruned P&M at or below the CRC version: a lagging AMT checkpoint action can
            // carry it, and the CRC's P&M is at least as new.
            let metadata_opt = candidate
                .metadata
                .filter(|(v, _)| *v > crc.version as i64)
                .map(|(_, m)| m);
            let protocol_opt = candidate
                .protocol
                .filter(|(v, _)| *v > crc.version as i64)
                .map(|(_, p)| p);
            // The pruned replay only reads commits after the CRC version, but a hit there is still
            // the latest action: any action at or below the CRC is older. A miss stays `Unresolved`
            // because the latest action may sit at or below the CRC, which this replay never reads.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let checkpoint_action = CheckpointActionResolution::from_replay(candidate.checkpoint)?;

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok(PmResolution {
                    metadata: metadata_opt,
                    protocol: protocol_opt,
                    source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_action,
                });
            }

            // Case 2(b): P&M incomplete or older than the CRC, use the CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            return Ok(PmResolution {
                metadata: metadata_opt.or_else(|| Some(crc.metadata.clone())),
                protocol: protocol_opt.or_else(|| Some(crc.protocol.clone())),
                source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                checkpoint_action,
            });
        }

        // Case 3: Full P&M log replay.
        let candidate = self.replay_for_pm(engine)?;
        Ok(PmResolution {
            metadata: candidate.metadata.map(|(_, m)| m),
            protocol: candidate.protocol.map(|(_, p)| p),
            source: ProtocolMetadataSource::FullReplay,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action: CheckpointActionResolution::from_replay(candidate.checkpoint)?,
        })
    }

    /// Replays the log segment for the latest Protocol and Metadata, each with its version.
    fn replay_for_pm(&self, engine: &dyn Engine) -> KernelResult<PmCandidate> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return resolve_pm_batches(self.read_pm_batches_via_plan(executor.as_ref())?);
        }
        resolve_pm_batches(self.read_pm_batches(engine)?)
    }

    /// Builds the declarative plan that selects the latest Protocol and Metadata actions.
    #[cfg(feature = "declarative-plans")]
    fn build_pm_plan(&self) -> KernelResult<Plan> {
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

        // Required fields are non-null exactly when their Protocol or Metadata action is present.
        // Filter on required leaf fields so readers can use row group skipping.
        let relevant_action = Predicate::or(
            col!(PROTOCOL_NAME, "minReaderVersion").is_not_null(),
            col!(METADATA_NAME, "id").is_not_null(),
        );
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let relevant_action =
            Predicate::or(relevant_action, col!(CHECKPOINT_ACTION_NAME).is_not_null());

        PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .filter(relevant_action)?
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
                let a = {
                    let checkpoint = || column_name!(CHECKPOINT_ACTION_NAME);
                    a.max_non_null_by(checkpoint(), checkpoint(), version())
                        .aggregate_as(
                            Agg::max_non_null_by(version(), checkpoint(), version()),
                            CHECKPOINT_COMMIT_VERSION,
                        )
                };
                a
            })?
            .build()
    }

    /// Reads the P&M commit cover and checkpoint via the declarative plan, tagging each batch with
    /// its version.
    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
    ) -> KernelResult<impl Iterator<Item = KernelResult<VersionedBatch>> + Send> {
        let plan = self.build_pm_plan()?;

        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| {
                // Mark as a log batch so the checkpoint action is read from it.
                let batch = ActionsBatch::new(batch?, true);
                versioned_batch_from_plan_output(batch)
            });
        Ok(batches)
    }

    /// Reads the P&M commit cover and checkpoint, tagging each batch with its version.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<impl Iterator<Item = KernelResult<VersionedBatch>> + Send> {
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
                checkpoint_version.ok_or_else(|| {
                    KernelError::internal_error("checkpoint batch without a version")
                })?
            };
            Ok(VersionedBatch {
                protocol_version: Some(version),
                metadata_version: Some(version),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                checkpoint_commit_version: Some(version),
                batch,
            })
        }))
    }
}

/// How the latest AMT `checkpoint` action was resolved during P&M replay, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without re-scanning the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug)]
pub(crate) enum CheckpointActionResolution {
    /// The latest `checkpoint` action, captured during replay.
    Captured {
        action: Arc<CheckpointAction>,
        /// Pointer to the commit the action was read from.
        last_manifest_commit: LastManifestCommit,
    },
    /// Not captured, but the CRC carries a [`LastManifestCommit`] pointer to the commit that
    /// emitted the latest checkpoint action, letting a consumer resolve it from that one commit
    /// instead of a full log scan.
    //
    // TODO(#3495): `Snapshot::latest_checkpoint_action` still falls back to a full scan on
    // `Hint` instead of resolving the action from the pointed commit.
    Hint(LastManifestCommit),
    /// Replay did not settle it and no pointer is available -- a miss, which does not prove
    /// absence since replay can stop early. Consumers fall back to a log scan.
    Unresolved,
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl CheckpointActionResolution {
    /// Resolves a replay's checkpoint-action find, given as the action paired with the version
    /// of the commit it was read from. A hit is the latest action: replay keeps the one from the
    /// newest commit it reaches, so an earlier-stopping replay can only skip older actions. A miss
    /// does not prove absence (the action may sit in a batch the replay stopped before, or
    /// at/below a seeding CRC version), so it stays [`Unresolved`](Self::Unresolved) for the
    /// accessor to settle by scanning.
    ///
    /// # Errors
    ///
    /// Returns an error if the action's content root is newer than the commit carrying it.
    fn from_replay(checkpoint: Option<(i64, CheckpointAction)>) -> KernelResult<Self> {
        Ok(match checkpoint {
            Some((commit_version, action)) => Self::Captured {
                last_manifest_commit: LastManifestCommit::try_from_checkpoint_action(
                    commit_version,
                    &action,
                )?,
                action: Arc::new(action),
            },
            None => Self::Unresolved,
        })
    }
}

/// Result of a P&M resolution (see [`LogSegment::read_protocol_metadata_opt`]).
pub(crate) struct PmResolution {
    pub(crate) metadata: Option<Metadata>,
    pub(crate) protocol: Option<Protocol>,
    /// How the Protocol and Metadata were resolved.
    pub(crate) source: ProtocolMetadataSource,
    /// How the latest AMT `checkpoint` action was resolved: captured during replay, hinted by the
    /// CRC's manifest-commit pointer, or unresolved. See [`CheckpointActionResolution`].
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) checkpoint_action: CheckpointActionResolution,
}

impl PmResolution {
    /// P&M taken from a CRC that short-circuits replay. With no replay to capture the checkpoint
    /// action, the CRC's [`LastManifestCommit`] pointer becomes a
    /// [`Hint`](CheckpointActionResolution::Hint) when present, else it is `Unresolved`.
    pub(crate) fn from_crc(crc: &Crc, source: ProtocolMetadataSource) -> Self {
        Self {
            metadata: Some(crc.metadata.clone()),
            protocol: Some(crc.protocol.clone()),
            source,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action: crc.last_manifest_commit_opt.clone().map_or(
                CheckpointActionResolution::Unresolved,
                CheckpointActionResolution::Hint,
            ),
        }
    }
}

/// Protocol and Metadata, each tagged with the version it was found at. Holds both a single
/// batch's parse and the newest resolved across batches.
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
    /// The AMT checkpoint action found in this batch, or the latest one across batches, tagged
    /// with the version of the commit it was read from.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<(i64, CheckpointAction)>,
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    /// Version of the log file carrying the batch's checkpoint action, if any.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint_commit_version: Option<i64>,
    batch: ActionsBatch,
}

/// The newest Protocol and Metadata across `batches`.
fn resolve_pm_batches(
    batches: impl Iterator<Item = KernelResult<VersionedBatch>>,
) -> KernelResult<PmCandidate> {
    let mut metadata: Option<(i64, Metadata)> = None;
    let mut protocol: Option<(i64, Protocol)> = None;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut checkpoint: Option<(i64, CheckpointAction)> = None;
    for batch in batches {
        let VersionedBatch {
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_commit_version,
            batch,
        } = batch?;
        let batch_version = protocol_version.max(metadata_version);
        let candidate = pm_candidate(
            &batch,
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_commit_version,
        )?;
        metadata = newer(metadata, candidate.metadata);
        protocol = newer(protocol, candidate.protocol);
        // Keep the checkpoint action from the newest commit, ranked by commit version (not by
        // iteration order). Best-effort: if the loop breaks below before reaching one, it stays
        // `None`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            checkpoint = newer(checkpoint, candidate.checkpoint);
        }
        // A checkpoint action's P&M can be older than its commit, so check version not presence.
        if is_final(&protocol, batch_version) && is_final(&metadata, batch_version) {
            break;
        }
    }
    Ok(PmCandidate {
        protocol,
        metadata,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        checkpoint,
    })
}

/// Parses the log version from a batch's `_file` metadata column.
fn batch_version(data: &dyn EngineData) -> KernelResult<Version> {
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
        fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
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
        .ok_or_else(|| KernelError::internal_error("commit batch missing _file column"))?;
    let url = Url::parse(&file)
        .map_err(|e| KernelError::internal_error(format!("batch has invalid _file {file}: {e}")))?;
    ParsedLogPath::try_from(url)?
        .map(|path| path.version)
        .ok_or_else(|| KernelError::internal_error(format!("batch from non-log file {file}")))
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
    #[cfg(feature = "adaptive-metadata-in-dev")] checkpoint_commit_version: Option<i64>,
) -> KernelResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = protocol_version.zip(Protocol::try_new_from_data(actions)?);
    let metadata = metadata_version.zip(Metadata::try_new_from_data(actions)?);

    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        // A checkpoint action's nested P&M is at its own `checkpointMetadata.version`. Only parse
        // it from batches projected with the checkpoint-action column. The non-plan read
        // (`read_pm_batches`) reads commits with `commit_schema` (which includes
        // `CHECKPOINT_ACTION_FIELD`) and checkpoint parts with `checkpoint_schema` (which omits
        // it), so here `is_log_batch` is exactly "this batch carries the column" and the action is
        // captured solely from commits. The plan path instead projects every union input with one
        // schema that includes the column and marks all batches as log batches, so it can also
        // capture the action from checkpoint parts. Both land on the same answer because a non-plan
        // miss falls back to the full log scan in `latest_checkpoint_action`.
        let checkpoint = if batch.is_log_batch {
            CheckpointAction::try_new_from_data(actions)?
        } else {
            None
        };
        let checkpoint_protocol = checkpoint
            .as_ref()
            .map(|c| (c.version(), c.protocol().clone()));
        let checkpoint_metadata = checkpoint
            .as_ref()
            .map(|c| (c.version(), c.metadata().clone()));
        Ok(PmCandidate {
            protocol: newer(protocol, checkpoint_protocol),
            metadata: newer(metadata, checkpoint_metadata),
            checkpoint: checkpoint_commit_version.zip(checkpoint),
        })
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    Ok(PmCandidate { protocol, metadata })
}

/// Plan aggregate column holding the version of the file the latest checkpoint action came from.
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
const CHECKPOINT_COMMIT_VERSION: &str = "checkpoint_commit_version";

/// Tags the plan aggregate's single batch with the version columns it emits.
#[cfg(feature = "declarative-plans")]
fn versioned_batch_from_plan_output(batch: ActionsBatch) -> KernelResult<VersionedBatch> {
    #[derive(Default)]
    struct PlanVersionsVisitor {
        protocol: Option<i64>,
        metadata: Option<i64>,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        checkpoint_commit: Option<i64>,
    }
    impl RowVisitor for PlanVersionsVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                #[cfg_attr(not(feature = "adaptive-metadata-in-dev"), allow(unused_mut))]
                let mut names = vec![
                    column_name!("protocol_version"),
                    column_name!("metadata_version"),
                ];
                #[cfg(feature = "adaptive-metadata-in-dev")]
                names.push(ColumnName::new([CHECKPOINT_COMMIT_VERSION]));
                let types = vec![DataType::LONG; names.len()];
                (names, types).into()
            });
            NAMES_AND_TYPES.as_ref()
        }
        fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> Result<()> {
            if row_count > 0 {
                self.protocol = getters[0].get_opt(0, "protocol_version")?;
                self.metadata = getters[1].get_opt(0, "metadata_version")?;
                #[cfg(feature = "adaptive-metadata-in-dev")]
                {
                    self.checkpoint_commit = getters[2].get_opt(0, CHECKPOINT_COMMIT_VERSION)?;
                }
            }
            Ok(())
        }
    }
    let mut visitor = PlanVersionsVisitor::default();
    visitor.visit_rows_of(batch.actions.as_ref())?;
    Ok(VersionedBatch {
        protocol_version: visitor.protocol,
        metadata_version: visitor.metadata,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        checkpoint_commit_version: visitor.checkpoint_commit,
        batch,
    })
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
    use crate::expressions::{col, Predicate};
    #[cfg(feature = "declarative-plans")]
    use crate::plans::ir::nodes::Operator;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::{KernelError, Result};

    #[cfg(feature = "declarative-plans")]
    struct FailingPlanExecutor;

    #[cfg(feature = "declarative-plans")]
    impl PlanExecutor for FailingPlanExecutor {
        fn execute_op(&self, _op: Operation) -> Result<PlanResult> {
            Err(KernelError::generic("plan executor deliberately failed"))
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

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn test_declarative_pm_plan_filters_irrelevant_actions() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let snapshot = Snapshot::builder_for(url)
            .build(&SyncEngine::new())
            .unwrap();
        let plan = snapshot.log_segment().build_pm_plan().unwrap();

        let filter = plan
            .nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::Filter(filter) => Some(filter),
                _ => None,
            })
            .expect("P&M plan must filter irrelevant actions");

        let expected = Predicate::or(
            col!("protocol.minReaderVersion").is_not_null(),
            col!("metaData.id").is_not_null(),
        );
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let expected = Predicate::or(expected, col!("checkpoint").is_not_null());
        assert_eq!(filter.predicate.as_ref(), &expected);
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
