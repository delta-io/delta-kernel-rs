//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

#[cfg(feature = "adaptive-metadata-in-dev")]
use itertools::Itertools as _;
use tracing::{info, instrument};
use url::Url;

#[cfg(feature = "adaptive-metadata-in-dev")]
use super::first_checkpoint_action;
use super::LogSegment;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::visitors::LastManifestCommitVisitor;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CheckpointAction, LastManifestCommit, CHECKPOINT_ACTION_FIELD};
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::actions::{CHECKPOINT_ACTION_NAME, COMMIT_INFO_NAME};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::expressions::lit;
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
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::schema::{lazy_schema_ref, SchemaRef};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::utils::require;
use crate::{Engine, EngineData, KernelError, KernelResult, Version};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// The result's `metadata` and `protocol` are `None` if not found.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    ///
    /// Under `adaptive-metadata-in-dev`, the result's `checkpoint_action` is a
    /// [`CheckpointActionResolution`] describing how the latest AMT checkpoint action was resolved
    /// (captured by replay, located by a manifest-commit pointer, or unresolved), for the caller
    /// to store on the
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
            // (or `Located` when the newest commit carries `lastManifestCommit`) because the latest
            // action may sit at or below the CRC, which this replay never reads.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let checkpoint_action = CheckpointActionResolution::from_replay(
                candidate.checkpoint.map(|(_, c)| c),
                candidate.last_manifest_commit.as_ref().map(|(_, p)| p),
            );
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let last_manifest_commit = LastManifestCommitResolution::from_replay(
                candidate.last_manifest_commit.map(|(_, p)| p),
            );

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok(PmResolution {
                    metadata: metadata_opt,
                    protocol: protocol_opt,
                    source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_action,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    last_manifest_commit,
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
                #[cfg(feature = "adaptive-metadata-in-dev")]
                last_manifest_commit,
            });
        }

        // Case 3: Full P&M log replay.
        let candidate = self.replay_for_pm(engine)?;
        Ok(PmResolution {
            metadata: candidate.metadata.map(|(_, m)| m),
            protocol: candidate.protocol.map(|(_, p)| p),
            source: ProtocolMetadataSource::FullReplay,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action: CheckpointActionResolution::from_replay(
                candidate.checkpoint.map(|(_, c)| c),
                candidate.last_manifest_commit.as_ref().map(|(_, p)| p),
            ),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: LastManifestCommitResolution::from_replay(
                candidate.last_manifest_commit.map(|(_, p)| p),
            ),
        })
    }

    /// `end_version` as the `i64` that log actions use for versions.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn end_version_i64(&self) -> KernelResult<i64> {
        i64::try_from(self.end_version).map_err(|_| {
            KernelError::generic(format!(
                "log segment end version {} does not fit in i64",
                self.end_version
            ))
        })
    }

    /// The commit at `end_version` when the commit cover reads a log compaction in its place.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn newest_commit_if_compacted(&self) -> Option<&ParsedLogPath> {
        let newest_cover_is_compaction = self
            .find_commit_cover_paths()
            .first()
            .is_some_and(|path| !path.is_commit());
        self.newest_commit().filter(|_| newest_cover_is_compaction)
    }

    /// The commit at `end_version`, if this segment lists it (it does not when the segment ends
    /// on a checkpoint).
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn newest_commit(&self) -> Option<&ParsedLogPath> {
        self.listed
            .ascending_commit_files
            .last()
            .filter(|commit| commit.version == self.end_version)
    }

    /// Replays the log segment for the latest Protocol and Metadata, each with its version.
    fn replay_for_pm(&self, engine: &dyn Engine) -> KernelResult<PmCandidate> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return resolve_pm_batches(
                self.read_pm_batches_via_plan(executor.as_ref())?,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                |resolved| {
                    self.merge_pointed_checkpoint_action(resolved, |commit, pointer| {
                        Self::read_pointed_checkpoint_action_via_plan(
                            executor.as_ref(),
                            commit,
                            pointer,
                        )
                    })
                },
            );
        }
        let replayed = resolve_pm_batches(
            self.read_pm_batches(engine)?,
            // Replay captures the pointed action itself when the commit cover lists the pointed
            // commit, so only a pointed commit hidden by a compaction is read up front.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            |resolved: PmCandidate| match &resolved.last_manifest_commit {
                Some((_, pointer)) if self.commit_cover_lists(pointer.version) => Ok(resolved),
                _ => self.merge_pointed_checkpoint_action(resolved, |commit, pointer| {
                    Self::read_pointed_checkpoint_action(engine, commit, pointer)
                }),
            },
        )?;
        // Replay read every pointed commit the cover lists, so a pointed action that is still
        // missing when its P&M could win is not in that commit.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some((_, pointer)) = self.required_checkpoint_pointer_to_read(&replayed) {
            return Err(missing_pointed_action(&pointer));
        }
        Ok(replayed)
    }

    /// Whether the commit cover reads the commit at `version` itself, not through a compaction.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn commit_cover_lists(&self, version: i64) -> bool {
        self.find_commit_cover_paths()
            .iter()
            .any(|path| path.is_commit() && i64::try_from(path.version) == Ok(version))
    }

    /// Merges in the checkpoint action that `lastManifestCommit` points at, read with
    /// `read_pointed`, unless replay already captured it.
    ///
    /// # Errors
    /// Returns an error if the pointed commit cannot be read or lacks a matching checkpoint action.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn merge_pointed_checkpoint_action(
        &self,
        resolved: PmCandidate,
        read_pointed: impl FnOnce(&ParsedLogPath, &LastManifestCommit) -> KernelResult<CheckpointAction>,
    ) -> KernelResult<PmCandidate> {
        let Some((commit, pointer)) = self.required_checkpoint_pointer_to_read(&resolved) else {
            return Ok(resolved);
        };
        let checkpoint = read_pointed(commit, &pointer)?;
        // TODO(#2337): a log compaction batch ranks at its start version, so one that spans the
        // pointed commit and also holds a later P&M change loses to the pointed action's older
        // P&M. Once compactions are listed, replay the original commits after the pointed version
        // instead, as delta-io/delta#7825 requires.
        Ok(resolved.with_checkpoint(checkpoint))
    }

    /// The pointed commit and its `lastManifestCommit` when replay still needs the pointed
    /// checkpoint action: it has not been captured, its P&M could still win, and this segment lists
    /// the pointed commit. `None` when there is no pointer or the action is not needed.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn required_checkpoint_pointer_to_read(
        &self,
        resolved: &PmCandidate,
    ) -> Option<(&ParsedLogPath, LastManifestCommit)> {
        let (_, pointer) = resolved.last_manifest_commit.as_ref()?;
        if resolved.checkpoint.is_some() {
            return None;
        }
        // The pointed action's P&M is at or below `pointer.version`, so it can't replace newer P&M.
        let is_newer = |v: Option<i64>| v.is_some_and(|v| v > pointer.version);
        if is_newer(resolved.protocol.as_ref().map(|(v, _)| *v))
            && is_newer(resolved.metadata.as_ref().map(|(v, _)| *v))
        {
            return None;
        }
        // A pointed commit outside this segment is no newer than the segment's P&M.
        let commits = &self.listed.ascending_commit_files;
        let index = commits
            .binary_search_by_key(&Version::try_from(pointer.version).ok()?, |c| c.version)
            .ok()?;
        Some((commits.get(index)?, pointer.clone()))
    }

    /// Reads the checkpoint action `pointer` points at from its manifest `commit`.
    ///
    /// # Errors
    /// Returns an error if the commit cannot be read or carries no matching checkpoint action.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn read_pointed_checkpoint_action(
        engine: &dyn Engine,
        commit: &ParsedLogPath,
        pointer: &LastManifestCommit,
    ) -> KernelResult<CheckpointAction> {
        static SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! { (&CHECKPOINT_ACTION_FIELD) };
        let batches = engine.json_handler().read_json_files(
            std::slice::from_ref(&commit.location),
            SCHEMA.clone(),
            None,
        )?;
        pointed_checkpoint_action(batches, pointer)
    }

    /// Reads the checkpoint action `pointer` points at from its manifest `commit`, with a plan that
    /// keeps the commit's first non-null `checkpoint` row.
    ///
    /// # Errors
    /// Returns an error if the plan fails or the commit carries no matching checkpoint action.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn read_pointed_checkpoint_action_via_plan(
        executor: &dyn PlanExecutor,
        commit: &ParsedLogPath,
        pointer: &LastManifestCommit,
    ) -> KernelResult<CheckpointAction> {
        let schema = schema_ref! {
            (&CHECKPOINT_ACTION_FIELD),
            not_null "version": LONG,
        };
        let files = Self::version_tagged_scan_files(std::slice::from_ref(commit))?;
        let plan = PlanBuilder::scan_json(files, &["version"], schema)?
            .aggregate_ungrouped(|a| {
                let checkpoint = || column_name!(CHECKPOINT_ACTION_NAME);
                a.max_non_null_by(checkpoint(), checkpoint(), column_name!("version"))
            })?
            .build()?;
        pointed_checkpoint_action(
            executor
                .execute_op(Operation::QueryPlan(plan))?
                .into_data()?,
            pointer,
        )
    }

    /// Builds the declarative plan that selects the latest Protocol and Metadata actions.
    #[cfg(feature = "declarative-plans")]
    fn build_pm_plan(&self) -> KernelResult<Plan> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let versioned_schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            (LastManifestCommitVisitor::commit_info_field()),
            not_null "version": LONG,
        };
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let versioned_schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            not_null "version": LONG,
        };

        #[cfg_attr(not(feature = "adaptive-metadata-in-dev"), allow(unused_mut))]
        let mut commit_files = self.commit_cover_version_tagged_scan_files()?;
        // Log compaction files drop commitInfo, so read the newest commit's `lastManifestCommit`
        // from the commit itself when a compaction covers it.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        commit_files.extend(Self::version_tagged_scan_files(
            self.newest_commit_if_compacted(),
        )?);
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
        // Only the newest commit's `lastManifestCommit` describes this segment's version.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let relevant_action = Predicate::or(
            relevant_action,
            Predicate::and(
                col!(COMMIT_INFO_NAME, "lastManifestCommit", "version").is_not_null(),
                col!("version").eq(lit(self.end_version_i64()?)),
            ),
        );

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
                    let commit_info = || column_name!(COMMIT_INFO_NAME);
                    a.max_non_null_by(commit_info(), commit_info(), version())
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
        // The plan reads the newest commit whenever it is listed.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let newest_commit_version = self.newest_commit().map(|_| self.end_version);

        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(move |batch| {
                // Not a log batch: the plan output is one aggregated row, not a commit.
                let batch = ActionsBatch::new(batch?, false);
                let (protocol_version, metadata_version) =
                    pm_versions_from_plan_output(batch.actions.as_ref())?;
                Ok(VersionedBatch {
                    protocol_version,
                    metadata_version,
                    // The plan's filter keeps only the newest commit's `lastManifestCommit`.
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    newest_commit_version,
                    batch,
                })
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
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (end_version, end_version_i64) = (self.end_version, self.end_version_i64()?);
        // Log compaction files drop commitInfo, so read the newest commit's `lastManifestCommit`
        // from the commit itself when a compaction covers it. It is the newest batch, so it goes
        // first.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let newest_commit = self
            .newest_commit_if_compacted()
            .map(|commit| {
                engine.json_handler().read_json_files(
                    std::slice::from_ref(&commit.location),
                    commit_schema.clone(),
                    None,
                )
            })
            .transpose()?
            .into_iter()
            .flatten()
            .map_ok(|batch| ActionsBatch::new(batch, true));
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
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let batches = newest_commit.chain(batches);
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
                newest_commit_version: (batch.is_log_batch && version == end_version_i64)
                    .then_some(end_version),
                batch,
            })
        }))
    }
}

/// How the latest AMT `checkpoint` action was resolved during P&M replay, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without re-scanning the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CheckpointActionResolution {
    /// The latest `checkpoint` action, captured during replay.
    Captured(Arc<CheckpointAction>),
    /// Not captured, but the CRC or the newest replayed commit carries a [`LastManifestCommit`]
    /// naming the commit that emitted the latest checkpoint action. The pointer is authoritative:
    /// a consumer reads that one commit and never scans the log, and a commit that is missing
    /// from the segment or does not carry the described action is an error (the pointer and
    /// the log are inconsistent).
    Located(LastManifestCommit),
    /// Replay did not settle it and no pointer is available -- a miss, which does not prove
    /// absence since replay can stop early. Consumers fall back to a log scan.
    Unresolved,
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl CheckpointActionResolution {
    /// Resolves a replay's checkpoint-action find. A hit is the latest action: replay keeps the
    /// highest `checkpointMetadata.version` across the batches it reaches, and checkpoint versions
    /// strictly increase, so an earlier-stopping replay can only skip older actions. A miss does
    /// not prove absence (the action may sit in a batch the replay stopped before, or at/below a
    /// seeding CRC version), so it becomes [`Located`](Self::Located) if replay found a
    /// `lastManifestCommit`, else [`Unresolved`](Self::Unresolved).
    fn from_replay(
        checkpoint: Option<CheckpointAction>,
        last_manifest_commit: Option<&LastManifestCommit>,
    ) -> Self {
        match (checkpoint, last_manifest_commit) {
            (Some(action), _) => Self::Captured(Arc::new(action)),
            (None, Some(pointer)) => Self::Located(pointer.clone()),
            (None, None) => Self::Unresolved,
        }
    }
}

/// How a snapshot's `lastManifestCommit` was resolved at build time, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without opening the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone)]
pub(crate) enum LastManifestCommitResolution {
    /// Read from the CRC at the snapshot version, the newest commit's commitInfo, or the
    /// committing transaction. `None` means the table has no manifest commit at this version.
    Resolved(Option<LastManifestCommit>),
    /// The build did not find the snapshot version's `lastManifestCommit`; consumers read it from
    /// that version's commit file.
    Unresolved,
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl LastManifestCommitResolution {
    /// Resolves the `lastManifestCommit` that P&M replay read from the newest commit. A miss is
    /// `Unresolved` because replay may not have read that commit (e.g. the segment ends on a
    /// checkpoint).
    fn from_replay(captured: Option<LastManifestCommit>) -> Self {
        match captured {
            Some(pointer) => Self::Resolved(Some(pointer)),
            None => Self::Unresolved,
        }
    }
}

/// Result of a P&M resolution (see [`LogSegment::read_protocol_metadata_opt`]).
pub(crate) struct PmResolution {
    pub(crate) metadata: Option<Metadata>,
    pub(crate) protocol: Option<Protocol>,
    /// How the Protocol and Metadata were resolved.
    pub(crate) source: ProtocolMetadataSource,
    /// How the latest AMT `checkpoint` action was resolved: captured during replay, located by a
    /// manifest-commit pointer, or unresolved. See [`CheckpointActionResolution`].
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) checkpoint_action: CheckpointActionResolution,
    /// How the segment's `lastManifestCommit` was resolved. See [`LastManifestCommitResolution`].
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) last_manifest_commit: LastManifestCommitResolution,
}

impl PmResolution {
    /// P&M taken from a CRC that short-circuits replay. With no replay to capture the checkpoint
    /// action, the CRC's [`LastManifestCommit`] pointer becomes a
    /// [`Located`](CheckpointActionResolution::Located) when present, else it is `Unresolved`.
    pub(crate) fn from_crc(crc: &Crc, source: ProtocolMetadataSource) -> Self {
        Self {
            metadata: Some(crc.metadata.clone()),
            protocol: Some(crc.protocol.clone()),
            source,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_action: crc.last_manifest_commit_opt.clone().map_or(
                CheckpointActionResolution::Unresolved,
                CheckpointActionResolution::Located,
            ),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: LastManifestCommitResolution::Resolved(
                crc.last_manifest_commit_opt.clone(),
            ),
        }
    }
}

/// Protocol and Metadata, each tagged with the version it was found at. Holds both a single
/// batch's parse and the newest resolved across batches.
#[derive(Default)]
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
    /// The AMT checkpoint action `last_manifest_commit` points at, tagged with its
    /// `checkpointMetadata.version` so it ranks the same way as Protocol and Metadata.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<(i64, CheckpointAction)>,
    /// The newest commit's `commitInfo.lastManifestCommit`, if replay read it, tagged with that
    /// commit's version.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    last_manifest_commit: Option<(Version, LastManifestCommit)>,
}

impl PmCandidate {
    /// Folds `other`, parsed from a batch replay reached later, into this result. Each field keeps
    /// its higher-versioned value, so the order batches merge in only matters on a version tie,
    /// where `other` wins.
    fn merge(self, other: PmCandidate) -> Self {
        PmCandidate {
            protocol: newer(self.protocol, other.protocol),
            metadata: newer(self.metadata, other.metadata),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint: newer(self.checkpoint, other.checkpoint),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: newer(self.last_manifest_commit, other.last_manifest_commit),
        }
    }

    /// Merges in the pointed `checkpoint` action and the P&M nested in it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn with_checkpoint(self, checkpoint: CheckpointAction) -> Self {
        let (protocol, metadata) = checkpoint_pm(&checkpoint);
        PmCandidate {
            protocol: newer(self.protocol, Some(protocol)),
            metadata: newer(self.metadata, Some(metadata)),
            checkpoint: Some((checkpoint.version(), checkpoint)),
            ..self
        }
    }
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    /// The version of the segment's newest commit when the batch comes from it; only that
    /// commit's `lastManifestCommit` describes the segment's version.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    newest_commit_version: Option<Version>,
    batch: ActionsBatch,
}

/// The newest Protocol and Metadata across `batches`, plus the checkpoint action that the newest
/// commit's `lastManifestCommit` points at when replay reads the pointed commit. `merge_pointer`
/// can merge in that action from elsewhere; it is called at most once, when replay first sees the
/// pointer.
fn resolve_pm_batches(
    batches: impl Iterator<Item = KernelResult<VersionedBatch>>,
    #[cfg(feature = "adaptive-metadata-in-dev")] merge_pointer: impl FnOnce(
        PmCandidate,
    )
        -> KernelResult<PmCandidate>,
) -> KernelResult<PmCandidate> {
    let mut resolved = PmCandidate::default();
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut merge_pointer = Some(merge_pointer);
    for batch in batches {
        let VersionedBatch {
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            newest_commit_version,
            batch,
        } = batch?;
        let batch_version = protocol_version.max(metadata_version);
        resolved = resolved.merge(pm_candidate(
            &batch,
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            newest_commit_version,
        )?);
        // The newest commit comes first, so the pointer is known before replay reaches the
        // pointed commit. Only the pointed action is trusted; other checkpoint actions are skipped.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if let Some((_, pointer)) = resolved.last_manifest_commit.clone() {
            if resolved.checkpoint.is_none()
                && batch.is_log_batch
                && batch_version == Some(pointer.version)
            {
                if let Some(checkpoint) =
                    CheckpointAction::try_new_from_data(batch.actions.as_ref())?
                {
                    require!(
                        pointer.points_at(&checkpoint),
                        missing_pointed_action(&pointer)
                    );
                    resolved = resolved.with_checkpoint(checkpoint);
                }
            }
            if let Some(merge_pointer) = merge_pointer.take() {
                resolved = merge_pointer(resolved)?;
            }
        }
        // A checkpoint action's P&M can be older than its commit, so check version not presence.
        if is_final(&resolved.protocol, batch_version)
            && is_final(&resolved.metadata, batch_version)
        {
            break;
        }
    }
    Ok(resolved)
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
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> KernelResult<()> {
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

/// The Protocol and Metadata nested in a checkpoint action, each tagged with the action's
/// `checkpointMetadata.version`.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn checkpoint_pm(checkpoint: &CheckpointAction) -> ((i64, Protocol), (i64, Metadata)) {
    let version = checkpoint.version();
    (
        (version, checkpoint.protocol().clone()),
        (version, checkpoint.metadata().clone()),
    )
}

/// The checkpoint action `pointer` points at in `batches`, read from its manifest commit.
///
/// # Errors
/// Returns an error if a batch fails to read or the commit carries no matching checkpoint action.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn pointed_checkpoint_action(
    batches: impl Iterator<Item = KernelResult<Box<dyn EngineData>>>,
    pointer: &LastManifestCommit,
) -> KernelResult<CheckpointAction> {
    first_checkpoint_action(batches)?
        .filter(|checkpoint| pointer.points_at(checkpoint))
        .ok_or_else(|| missing_pointed_action(pointer))
}

/// The error for a manifest commit that lacks the checkpoint action `pointer` points at.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn missing_pointed_action(pointer: &LastManifestCommit) -> KernelError {
    KernelError::generic(format!(
        "lastManifestCommit points at version {} with contentRootVersion {}, but that commit \
         carries no matching checkpoint action",
        pointer.version, pointer.content_root_version
    ))
}

/// The newest commit's `lastManifestCommit` in `actions`, tagged with that commit's `version`.
#[cfg(feature = "adaptive-metadata-in-dev")]
fn newest_commit_pointer(
    actions: &dyn EngineData,
    version: Version,
) -> KernelResult<Option<(Version, LastManifestCommit)>> {
    let Some(pointer) = LastManifestCommit::try_new_from_data(actions)? else {
        return Ok(None);
    };
    pointer.ensure_at_or_before(version)?;
    Ok(Some((version, pointer)))
}

/// The higher-versioned of `a` and `b`; on a tie, `b` wins.
fn newer<V: Ord, T>(a: Option<(V, T)>, b: Option<(V, T)>) -> Option<(V, T)> {
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
/// `checkpoint` action and the `commitInfo.lastManifestCommit` that points at it.
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
        (LastManifestCommitVisitor::commit_info_field()),
    };
    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    let commit_schema = checkpoint_schema.clone();
    (commit_schema, checkpoint_schema)
}

/// The Protocol and Metadata in `batch`, tagged with the given versions, plus the newest
/// commit's `lastManifestCommit` when the batch comes from it.
fn pm_candidate(
    batch: &ActionsBatch,
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    #[cfg(feature = "adaptive-metadata-in-dev")] newest_commit_version: Option<Version>,
) -> KernelResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = protocol_version.zip(Protocol::try_new_from_data(actions)?);
    let metadata = metadata_version.zip(Metadata::try_new_from_data(actions)?);

    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        let last_manifest_commit = match newest_commit_version {
            Some(version) => newest_commit_pointer(actions, version)?,
            None => None,
        };
        Ok(PmCandidate {
            protocol,
            metadata,
            checkpoint: None,
            last_manifest_commit,
        })
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    Ok(PmCandidate { protocol, metadata })
}

/// Reads the `protocol_version` and `metadata_version` columns the plan aggregate emits.
#[cfg(feature = "declarative-plans")]
fn pm_versions_from_plan_output(
    actions: &dyn EngineData,
) -> KernelResult<(Option<i64>, Option<i64>)> {
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
        ) -> KernelResult<()> {
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

    #[cfg(feature = "adaptive-metadata-in-dev")]
    use super::PmCandidate;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    use crate::actions::LastManifestCommit;
    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use crate::expressions::lit;
    #[cfg(feature = "declarative-plans")]
    use crate::expressions::{col, Predicate};
    #[cfg(feature = "declarative-plans")]
    use crate::plans::ir::nodes::Operator;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::{KernelError, KernelResult};

    #[cfg(feature = "declarative-plans")]
    struct FailingPlanExecutor;

    #[cfg(feature = "declarative-plans")]
    impl PlanExecutor for FailingPlanExecutor {
        fn execute_op(&self, _op: Operation) -> KernelResult<PlanResult> {
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
        let expected = Predicate::or(
            expected,
            Predicate::and(
                col!("commitInfo.lastManifestCommit.version").is_not_null(),
                col!("version").eq(lit(snapshot.version() as i64)),
            ),
        );
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

    // Merging keeps the pointer from the newest commit whichever order the batches arrive in.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn test_merge_keeps_newest_last_manifest_commit() {
        let candidate = |version: u64| PmCandidate {
            last_manifest_commit: Some((
                version,
                LastManifestCommit::new(version as i64, 0).unwrap(),
            )),
            ..Default::default()
        };
        let newest = Some((2, LastManifestCommit::new(2, 0).unwrap()));
        assert_eq!(
            candidate(2).merge(candidate(1)).last_manifest_commit,
            newest
        );
        assert_eq!(
            candidate(1).merge(candidate(2)).last_manifest_commit,
            newest
        );
    }
}
