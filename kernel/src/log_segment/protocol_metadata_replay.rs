//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

use tracing::{info, instrument};
use url::Url;

use super::LogSegment;
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use super::{inconsistent_pointer, require_pointed_checkpoint_action};
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
#[cfg(feature = "declarative-plans")]
use crate::schema::SchemaRef;
use crate::schema::{
    column_name, schema_ref, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    StructField, StructType,
};
use crate::{
    version_as_i64, Engine, EngineData, KernelError, KernelResult, KernelResultIterator, Version,
};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// The result's `metadata` and `protocol` are `None` if not found.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    ///
    /// Under `adaptive-metadata-in-dev`, the result's `captured_checkpoint_action` is the latest
    /// AMT checkpoint action when replay captured it, for the caller to store on the
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
            let candidate = pruned.replay_for_pm(
                engine,
                #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
                PmReplayBound::Crc,
            )?;
            // Ignore pruned P&M at or below the CRC version: a lagging AMT checkpoint action can
            // carry it, and the CRC's P&M is at least as new.
            let (protocol_opt, metadata_opt) =
                candidate.pm_newer_than(version_as_i64(crc.version)?);
            let (protocol_opt, metadata_opt) = (protocol_opt.cloned(), metadata_opt.cloned());
            // The pruned replay only reads commits after the CRC version, but a hit there is still
            // the latest action: any action at or below the CRC is older.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let (captured_checkpoint_action, last_manifest_commit) = candidate.amt_resolutions();

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok(PmResolution {
                    metadata: metadata_opt,
                    protocol: protocol_opt,
                    source: ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    captured_checkpoint_action,
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
                captured_checkpoint_action,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                last_manifest_commit,
            });
        }

        // Case 3: Full P&M log replay.
        // A listed checkpoint file truncates the log listing, so the replay starts at it.
        #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
        let bound = match self.checkpoint_version {
            Some(_) => PmReplayBound::CheckpointFile,
            None => PmReplayBound::Unbounded,
        };
        let candidate = self.replay_for_pm(
            engine,
            #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
            bound,
        )?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (captured_checkpoint_action, last_manifest_commit) = candidate.amt_resolutions();
        Ok(PmResolution {
            metadata: candidate.metadata.map(|(_, m)| m),
            protocol: candidate.protocol.map(|(_, p)| p),
            source: ProtocolMetadataSource::FullReplay,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit,
        })
    }

    /// Replays the log segment for the latest Protocol and Metadata, each with its version.
    /// `bound` says what holds the P&M as of where the replay starts.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
        #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
        bound: PmReplayBound,
    ) -> KernelResult<PmCandidate> {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return self.replay_for_pm_via_plan(
                executor.as_ref(),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                bound,
            );
        }
        let candidate = resolve_pm_batches(self.read_pm_batches(engine)?)?;
        // Only a malformed log has a pointer past its own commit; reject it before it is cached.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        candidate.ensure_pointer_at_or_before(self.end_version)?;
        Ok(candidate)
    }

    /// Builds the declarative plan that selects the latest Protocol and Metadata actions. Under
    /// `adaptive-metadata-in-dev`, `segment` picks the files to read and the AMT actions to keep.
    #[cfg(feature = "declarative-plans")]
    fn build_pm_plan(
        &self,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmPlanSegment,
    ) -> KernelResult<Plan> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (commit_paths, extra_actions) =
            (self.pm_plan_commit_paths(segment), segment.extra_actions());
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let (commit_paths, extra_actions) = (self.find_commit_cover_paths(), Vec::new());
        let schema = pm_plan_schema(&extra_actions)?;

        let commits = PlanBuilder::scan_json(
            Self::version_tagged_scan_files(&commit_paths)?,
            &["version"],
            schema.clone(),
        )?;

        // A checkpoint's parts share one format; scan them with the matching operator.
        let checkpoint = self
            .checkpoint_version_tagged_scan_files()?
            .map(|(file_type, checkpoint_files)| {
                let scan = match file_type {
                    FileType::Json => PlanBuilder::scan_json,
                    FileType::Parquet => PlanBuilder::scan_parquet,
                };
                scan(checkpoint_files, &["version"], schema.clone())
            })
            .transpose()?;

        // Required fields are non-null exactly when their Protocol or Metadata action is present.
        // Filter on required leaf fields so readers can use row group skipping.
        let relevant_action = Predicate::or(
            col!(PROTOCOL_NAME, "minReaderVersion").is_not_null(),
            col!(METADATA_NAME, "id").is_not_null(),
        );
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let relevant_action = if segment.reads_commit_info() {
            Predicate::or(relevant_action, col!(COMMIT_INFO_NAME).is_not_null())
        } else {
            relevant_action
        };
        // A compaction tags all its actions with one version, so this matches only the commit's
        // own file.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let relevant_action = match segment.checkpoint_action_version(self.end_version) {
            Some(version) => {
                let checkpoint_action_pred = Predicate::and(
                    col!(CHECKPOINT_ACTION_NAME).is_not_null(),
                    col!("version").eq(lit(version_as_i64(version)?)),
                );
                Predicate::or(relevant_action, checkpoint_action_pred)
            }
            None => relevant_action,
        };

        let relevant = PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .filter(relevant_action)?;
        Self::newest_pm_plan(
            relevant,
            &extra_actions,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            segment.reads_commit_info(),
        )
    }

    /// Aggregates `input` to its newest Protocol, Metadata, and each of `extra_actions`, with the
    /// versions the Protocol and Metadata were found at. When `reads_commit_info`, it also records
    /// the newest `commitInfo` version.
    #[cfg(feature = "declarative-plans")]
    fn newest_pm_plan(
        input: PlanBuilder,
        extra_actions: &[&StructField],
        #[cfg(feature = "adaptive-metadata-in-dev")] reads_commit_info: bool,
    ) -> KernelResult<Plan> {
        input
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
                let a = if reads_commit_info {
                    a.aggregate_as(
                        Agg::max_non_null_by(version(), column_name!(COMMIT_INFO_NAME), version()),
                        "commit_info_version",
                    )
                } else {
                    a
                };
                extra_actions.iter().fold(a, |a, field| {
                    let action = || ColumnName::new([field.name()]);
                    a.max_non_null_by(action(), action(), version())
                })
            })?
            .build()
    }

    /// Runs the declarative P&M plan, tagging each batch with its version. `segment` is as for
    /// [`Self::build_pm_plan`].
    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmPlanSegment,
    ) -> KernelResult<impl KernelResultIterator<VersionedBatch>> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (reads_checkpoint_action, newest_commit_version) = (
            segment.reads_checkpoint_action(),
            segment
                .reads_commit_info()
                .then(|| version_as_i64(self.end_version))
                .transpose()?,
        );
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let reads_checkpoint_action = false;
        let plan = self.build_pm_plan(
            #[cfg(feature = "adaptive-metadata-in-dev")]
            segment,
        )?;
        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(move |batch| {
                // `pm_candidate` parses the checkpoint action only from log batches.
                let batch = ActionsBatch::new(batch?, reads_checkpoint_action);
                versioned_plan_batch(
                    batch,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    newest_commit_version,
                )
            });
        Ok(batches)
    }

    /// Reads the P&M commit cover and checkpoint, tagging each batch with its version.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<impl KernelResultIterator<VersionedBatch>> {
        let (commit_schema, checkpoint_schema) = pm_replay_schemas();
        // Commit schema only: `_file` in the checkpoint schema would break its skipping predicate.
        let file_column =
            StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath);
        let commit_schema = Arc::new(StructType::try_new(
            commit_schema.fields().cloned().chain([file_column]),
        )?);
        let checkpoint_version = self.checkpoint_version.map(|v| v as i64);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let end_version = version_as_i64(self.end_version)?;
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
                is_newest_commit: batch.is_log_batch && version == end_version,
                batch,
            })
        }))
    }

    /// [`Self::replay_for_pm`] through declarative plans. A replay bounded by a CRC or a checkpoint
    /// file runs one P&M plan, which also reads the newest commit's `commitInfo`. An unbounded
    /// replay reads the newest commit first, so its `lastManifestCommit` can bound the rest.
    #[cfg(feature = "declarative-plans")]
    fn replay_for_pm_via_plan(
        &self,
        executor: &dyn PlanExecutor,
        #[cfg(feature = "adaptive-metadata-in-dev")] bound: PmReplayBound,
    ) -> KernelResult<PmCandidate> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if bound == PmReplayBound::Unbounded && self.newest_commit().is_some() {
            return self.replay_for_pm_from_newest_commit(executor);
        }
        let candidate = resolve_pm_batches(self.read_pm_batches_via_plan(
            executor,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            PmPlanSegment::WholeSegment,
        )?)?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        candidate.ensure_pointer_at_or_before(self.end_version)?;
        Ok(candidate)
    }

    /// Reads the newest commit in its own plan. If its P&M are not final, runs one more plan over
    /// the older commits, starting at the pointed content root when the segment lists the
    /// manifest commit.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn replay_for_pm_from_newest_commit(
        &self,
        executor: &dyn PlanExecutor,
    ) -> KernelResult<PmCandidate> {
        let newest = resolve_pm_batches(
            self.read_pm_batches_via_plan(executor, PmPlanSegment::NewestCommit)?,
        )?
        .validated_pointer(self.end_version)?;
        if newest.is_final_at(Some(version_as_i64(self.end_version)?)) {
            return Ok(newest);
        }
        let (oldest_exclusive, manifest_commit) = match self.manifest_commit_to_read(&newest) {
            Some((manifest_commit, content_root)) => {
                // The pointed checkpoint action has the P&M as of its content root, so earlier
                // commits hold nothing newer. The range includes the content root because the
                // manifest commit carrying that action can be at that version.
                (
                    as_version(content_root)?.checked_sub(1),
                    Some(manifest_commit),
                )
            }
            // The newest commit's own checkpoint action holds the P&M as of its version.
            None => match &newest.checkpoint {
                Some((version, _)) => (Some(as_version(*version)?), None),
                None => (None, None),
            },
        };
        let older = PmPlanSegment::OlderCommits {
            oldest_exclusive,
            manifest_commit,
        };
        let resolved = newest.merge(resolve_pm_batches(
            self.read_pm_batches_via_plan(executor, older)?,
        )?);
        // The pointed action must exist when replay lacks Protocol or Metadata without it, and must
        // match the pointer whenever it was read.
        if manifest_commit.is_some()
            && (resolved.checkpoint.is_some()
                || resolved.protocol.is_none()
                || resolved.metadata.is_none())
        {
            return resolved.require_pointed_checkpoint();
        }
        Ok(resolved)
    }

    /// The manifest commit's version and its pointer's content root version, when replay still
    /// needs that commit's checkpoint action: the newest commit's `lastManifestCommit` names it and
    /// the newest commit lacks the action.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn manifest_commit_to_read(&self, newest: &PmCandidate) -> Option<(Version, i64)> {
        if newest.checkpoint.is_some() {
            return None;
        }
        // An unlisted manifest commit is at or below where this segment starts, so whatever covers
        // that start has P&M at least as new. The snapshot reads the action through the pointer.
        let pointer = newest.last_manifest_commit.as_ref()?;
        let commit = self.find_pointed_commit(pointer)?;
        Some((commit.version, pointer.content_root_version))
    }

    /// The commit files `segment` reads.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn pm_plan_commit_paths(&self, segment: PmPlanSegment) -> Vec<ParsedLogPath> {
        match segment {
            PmPlanSegment::WholeSegment => {
                let mut paths = self.find_commit_cover_paths();
                // Log compactions drop commitInfo, so read the newest commit itself when one
                // covers it.
                if let Some(newest_commit) = self.newest_commit() {
                    if !paths.contains(newest_commit) {
                        paths.push(newest_commit.clone());
                    }
                }
                paths
            }
            PmPlanSegment::NewestCommit => Vec::from_iter(self.newest_commit().cloned()),
            PmPlanSegment::OlderCommits {
                oldest_exclusive,
                manifest_commit,
            } => match self.end_version.checked_sub(1) {
                Some(newest_inclusive) => self.find_commit_cover_paths_in_range(
                    oldest_exclusive,
                    newest_inclusive,
                    manifest_commit.as_slice(),
                ),
                None => Vec::new(),
            },
        }
    }

    /// The commit at `end_version`, if this segment lists it (it does not when the segment ends
    /// on a checkpoint).
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn newest_commit(&self) -> Option<&ParsedLogPath> {
        self.listed
            .ascending_commit_files
            .last()
            .filter(|commit| commit.version == self.end_version)
    }
}

/// What holds the Protocol and Metadata as of where a P&M replay starts.
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PmReplayBound {
    /// A CRC just before the replayed commits.
    Crc,
    /// A checkpoint file the replay reads.
    CheckpointFile,
    /// Nothing: the replay may need the checkpoint action that `lastManifestCommit` points at.
    Unbounded,
}

/// The part of the log one P&M plan reads.
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
#[derive(Debug, Clone, Copy, PartialEq)]
enum PmPlanSegment {
    /// The segment's commits and checkpoint.
    WholeSegment,
    /// Only the newest commit.
    NewestCommit,
    /// The commits after `oldest_exclusive` (all when `None`), except the newest. Also keeps
    /// `manifest_commit`'s checkpoint action.
    OlderCommits {
        oldest_exclusive: Option<Version>,
        manifest_commit: Option<Version>,
    },
}

#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
impl PmPlanSegment {
    /// Whether the plan reads `commitInfo`. Only the newest commit's matters.
    fn reads_commit_info(self) -> bool {
        !matches!(self, Self::OlderCommits { .. })
    }

    /// Whether the plan reads the AMT checkpoint action.
    fn reads_checkpoint_action(self) -> bool {
        matches!(
            self,
            Self::NewestCommit
                | Self::OlderCommits {
                    manifest_commit: Some(_),
                    ..
                }
        )
    }

    /// The version of the commit whose checkpoint action the plan keeps, in a segment ending at
    /// `end_version`.
    fn checkpoint_action_version(self, end_version: Version) -> Option<Version> {
        match self {
            Self::WholeSegment => None,
            Self::NewestCommit => Some(end_version),
            Self::OlderCommits {
                manifest_commit, ..
            } => manifest_commit,
        }
    }

    /// The actions the plan reads beyond Protocol and Metadata.
    fn extra_actions(self) -> Vec<&'static StructField> {
        let commit_info = self
            .reads_commit_info()
            .then(LastManifestCommitVisitor::commit_info_field);
        let checkpoint_action = self
            .reads_checkpoint_action()
            .then_some(&*CHECKPOINT_ACTION_FIELD);
        commit_info.into_iter().chain(checkpoint_action).collect()
    }
}

/// How a snapshot's `lastManifestCommit` was resolved at build time, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without opening the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LastManifestCommitResolution {
    /// Read at build time from the CRC or log replay. `None` means the table has no manifest
    /// commit at this version.
    Resolved(Option<LastManifestCommit>),
    /// The build never read the snapshot version's commitInfo; consumers read it from that
    /// version's commit file.
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
    /// The latest AMT `checkpoint` action, when replay happened to read it. This only saves a
    /// read: `lastManifestCommit`, which the CRC or the snapshot version's `commitInfo` always
    /// carries, is the authority on the action and the snapshot follows it when this is `None`.
    /// A hit is the latest action, since replay keeps the highest `checkpointMetadata.version`
    /// it reaches and checkpoint versions strictly increase.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) captured_checkpoint_action: Option<Arc<CheckpointAction>>,
    /// How the segment's `lastManifestCommit` was resolved. See [`LastManifestCommitResolution`].
    #[cfg(feature = "adaptive-metadata-in-dev")]
    pub(crate) last_manifest_commit: LastManifestCommitResolution,
}

impl PmResolution {
    /// P&M taken from a CRC that short-circuits replay. With no replay to capture the checkpoint
    /// action, the CRC's [`LastManifestCommit`] is what later locates it.
    pub(crate) fn from_crc(crc: &Crc, source: ProtocolMetadataSource) -> Self {
        Self {
            metadata: Some(crc.metadata.clone()),
            protocol: Some(crc.protocol.clone()),
            source,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: LastManifestCommitResolution::Resolved(
                crc.last_manifest_commit_opt.clone(),
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
    /// with its `checkpointMetadata.version` so it ranks the same way as Protocol and Metadata.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<(i64, CheckpointAction)>,
    /// The newest commit's `commitInfo.lastManifestCommit`, if replay read it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    last_manifest_commit: Option<LastManifestCommit>,
}

impl PmCandidate {
    /// The Protocol and Metadata found above `version`, each `None` when it is at or below it.
    fn pm_newer_than(&self, version: i64) -> (Option<&Protocol>, Option<&Metadata>) {
        let protocol = self.protocol.as_ref().filter(|(v, _)| *v > version);
        let metadata = self.metadata.as_ref().filter(|(v, _)| *v > version);
        (protocol.map(|(_, p)| p), metadata.map(|(_, m)| m))
    }

    /// Whether both Protocol and Metadata are found at or above `version`.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn is_final_at(&self, version: Option<i64>) -> bool {
        is_final(&self.protocol, version) && is_final(&self.metadata, version)
    }

    /// The AMT checkpoint action replay captured, if any, and how it resolved `lastManifestCommit`,
    /// for the snapshot.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn amt_resolutions(&self) -> (Option<Arc<CheckpointAction>>, LastManifestCommitResolution) {
        (
            self.checkpoint.as_ref().map(|(_, c)| Arc::new(c.clone())),
            LastManifestCommitResolution::from_replay(self.last_manifest_commit.clone()),
        )
    }

    /// Errors unless the `lastManifestCommit` read from the newest commit, at `version`, names a
    /// manifest commit at or before it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn ensure_pointer_at_or_before(&self, version: Version) -> KernelResult<()> {
        self.last_manifest_commit
            .as_ref()
            .map_or(Ok(()), |pointer| pointer.ensure_at_or_before(version))
    }

    /// Combines two replay results, keeping each field's higher-versioned value and either side's
    /// `lastManifestCommit`.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn merge(self, other: PmCandidate) -> Self {
        PmCandidate {
            protocol: newer(self.protocol, other.protocol),
            metadata: newer(self.metadata, other.metadata),
            checkpoint: newer(self.checkpoint, other.checkpoint),
            // Only the newest commit's batch yields a pointer, so at most one side holds it.
            last_manifest_commit: self.last_manifest_commit.or(other.last_manifest_commit),
        }
    }

    /// Checks the `lastManifestCommit` and checkpoint action read from the newest commit, at
    /// `version`: the pointer names a commit at or before it, and the commit carries the pointed
    /// checkpoint action exactly when the pointer names it.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn validated_pointer(self, version: Version) -> KernelResult<Self> {
        self.ensure_pointer_at_or_before(version)?;
        let version = version_as_i64(version)?;
        match &self.last_manifest_commit {
            Some(pointer) if pointer.version == version => self.require_pointed_checkpoint(),
            // The pointer names an older commit, yet this one carries an action of its own.
            Some(pointer) if self.checkpoint.is_some() => Err(inconsistent_pointer(
                pointer,
                &format!("commit {version} carries a checkpoint action"),
            )),
            None if self.checkpoint.is_some() => Err(KernelError::invalid_log_segment(format!(
                "commit {version} carries a checkpoint action but no lastManifestCommit"
            ))),
            _ => Ok(self),
        }
    }

    /// Checks the captured checkpoint action against `last_manifest_commit`, which names the commit
    /// replay read it from.
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    fn require_pointed_checkpoint(mut self) -> KernelResult<Self> {
        let Some(pointer) = &self.last_manifest_commit else {
            return Ok(self);
        };
        let action = require_pointed_checkpoint_action(
            pointer,
            self.checkpoint.take().map(|(_, action)| action),
        )?;
        self.checkpoint = Some((action.version(), action));
        Ok(self)
    }
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    /// Whether the batch comes from the segment's newest commit, whose `lastManifestCommit`
    /// describes the segment's version.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    is_newest_commit: bool,
    batch: ActionsBatch,
}

/// The newest Protocol and Metadata across `batches`.
fn resolve_pm_batches(
    batches: impl KernelResultIterator<VersionedBatch>,
) -> KernelResult<PmCandidate> {
    let mut metadata: Option<(i64, Metadata)> = None;
    let mut protocol: Option<(i64, Protocol)> = None;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut checkpoint: Option<(i64, CheckpointAction)> = None;
    #[cfg(feature = "adaptive-metadata-in-dev")]
    let mut last_manifest_commit: Option<LastManifestCommit> = None;
    for batch in batches {
        let VersionedBatch {
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            is_newest_commit,
            batch,
        } = batch?;
        let batch_version = protocol_version.max(metadata_version);
        let candidate = pm_candidate(
            &batch,
            protocol_version,
            metadata_version,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            is_newest_commit,
        )?;
        metadata = newer(metadata, candidate.metadata);
        protocol = newer(protocol, candidate.protocol);
        // Keep the highest-versioned checkpoint action, ranked by version like P&M (not by
        // iteration order). Best-effort: if the loop breaks below before reaching one, it stays
        // `None`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            checkpoint = newer(checkpoint, candidate.checkpoint);
            last_manifest_commit = last_manifest_commit.or(candidate.last_manifest_commit);
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
        #[cfg(feature = "adaptive-metadata-in-dev")]
        last_manifest_commit,
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

/// Converts a log version read as `i64` to a [`Version`].
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
fn as_version(version: i64) -> KernelResult<Version> {
    Version::try_from(version).map_err(|_| {
        KernelError::invalid_log_segment(format!("Delta log version {version} is negative"))
    })
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
/// `checkpoint` action and `commitInfo.lastManifestCommit`.
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

/// The P&M plan read schema: Protocol, Metadata, `extra_actions`, and the file-constant `version`.
#[cfg(feature = "declarative-plans")]
fn pm_plan_schema(extra_actions: &[&StructField]) -> KernelResult<SchemaRef> {
    let actions = [&*PROTOCOL_FIELD, &*METADATA_FIELD]
        .into_iter()
        .chain(extra_actions.iter().copied())
        .cloned();
    let version = StructField::not_null("version", DataType::LONG);
    Ok(Arc::new(StructType::try_new(actions.chain([version]))?))
}

/// The Protocol and Metadata in `batch`, tagged with the given versions, plus any AMT checkpoint
/// action's nested P&M at its own version and, for the newest commit, its `lastManifestCommit`.
fn pm_candidate(
    batch: &ActionsBatch,
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    #[cfg(feature = "adaptive-metadata-in-dev")] is_newest_commit: bool,
) -> KernelResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = protocol_version.zip(Protocol::try_new_from_data(actions)?);
    let metadata = metadata_version.zip(Metadata::try_new_from_data(actions)?);

    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        // A checkpoint action's nested P&M rank at its `checkpointMetadata.version`. Only batches
        // that project the `checkpoint` column can parse it; others would fail on the missing
        // column.
        let checkpoint = if batch.is_log_batch {
            CheckpointAction::try_new_from_data(actions)?
        } else {
            None
        };
        let (checkpoint_protocol, checkpoint_metadata) =
            checkpoint.as_ref().map(checkpoint_pm).unzip();
        let last_manifest_commit = if is_newest_commit {
            LastManifestCommit::try_new_from_data(actions)?
        } else {
            None
        };
        Ok(PmCandidate {
            protocol: newer(protocol, checkpoint_protocol),
            metadata: newer(metadata, checkpoint_metadata),
            checkpoint: checkpoint.map(|c| (c.version(), c)),
            last_manifest_commit,
        })
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    Ok(PmCandidate { protocol, metadata })
}

/// Tags a P&M plan's output `batch` with the versions its aggregate emits. Under
/// `adaptive-metadata-in-dev`, a batch whose `commitInfo` version equals `newest_commit_version`
/// comes from the newest commit.
#[cfg(feature = "declarative-plans")]
fn versioned_plan_batch(
    batch: ActionsBatch,
    #[cfg(feature = "adaptive-metadata-in-dev")] newest_commit_version: Option<i64>,
) -> KernelResult<VersionedBatch> {
    #[derive(Default)]
    struct PmVersionsVisitor {
        protocol: Option<i64>,
        metadata: Option<i64>,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        reads_commit_info: bool,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        commit_info: Option<i64>,
    }
    fn version_columns(names: Vec<ColumnName>) -> ColumnNamesAndTypes {
        let types = vec![DataType::LONG; names.len()];
        (names, types).into()
    }
    impl RowVisitor for PmVersionsVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            static PM: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                version_columns(vec![
                    column_name!("protocol_version"),
                    column_name!("metadata_version"),
                ])
            });
            #[cfg(feature = "adaptive-metadata-in-dev")]
            static PM_AND_COMMIT_INFO: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
                version_columns(vec![
                    column_name!("protocol_version"),
                    column_name!("metadata_version"),
                    column_name!("commit_info_version"),
                ])
            });
            #[cfg(feature = "adaptive-metadata-in-dev")]
            if self.reads_commit_info {
                return PM_AND_COMMIT_INFO.as_ref();
            }
            PM.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> KernelResult<()> {
            if row_count > 0 {
                self.protocol = getters[0].get_opt(0, "protocol_version")?;
                self.metadata = getters[1].get_opt(0, "metadata_version")?;
                #[cfg(feature = "adaptive-metadata-in-dev")]
                if self.reads_commit_info {
                    self.commit_info = getters[2].get_opt(0, "commit_info_version")?;
                }
            }
            Ok(())
        }
    }
    let mut visitor = PmVersionsVisitor {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        reads_commit_info: newest_commit_version.is_some(),
        ..Default::default()
    };
    visitor.visit_rows_of(batch.actions.as_ref())?;
    Ok(VersionedBatch {
        protocol_version: visitor.protocol,
        metadata_version: visitor.metadata,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        is_newest_commit: newest_commit_version
            .is_some_and(|version| visitor.commit_info == Some(version)),
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

    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use super::PmPlanSegment;
    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use crate::expressions::lit;
    #[cfg(feature = "declarative-plans")]
    use crate::expressions::{col, Predicate};
    #[cfg(feature = "declarative-plans")]
    use crate::log_segment::LogSegment;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::ir::nodes::Operator;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::SnapshotRef;
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

    /// The filter of `segment`'s P&M plan, built from `plan_segment` under
    /// `adaptive-metadata-in-dev`.
    #[cfg(feature = "declarative-plans")]
    fn pm_plan_filter(
        segment: &LogSegment,
        #[cfg(feature = "adaptive-metadata-in-dev")] plan_segment: PmPlanSegment,
    ) -> Predicate {
        let plan = segment
            .build_pm_plan(
                #[cfg(feature = "adaptive-metadata-in-dev")]
                plan_segment,
            )
            .unwrap();
        plan.nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::Filter(filter) => Some(filter.predicate.as_ref().clone()),
                _ => None,
            })
            .expect("P&M plan must filter irrelevant actions")
    }

    #[cfg(feature = "declarative-plans")]
    fn app_txn_checkpoint_snapshot() -> SnapshotRef {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        Snapshot::builder_for(url)
            .build(&SyncEngine::new())
            .unwrap()
    }

    #[cfg(feature = "declarative-plans")]
    fn pm_predicate() -> Predicate {
        let pm = protocol_or_metadata();
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let pm = Predicate::or(pm, col!("commitInfo").is_not_null());
        pm
    }

    #[cfg(feature = "declarative-plans")]
    fn protocol_or_metadata() -> Predicate {
        Predicate::or(
            col!("protocol.minReaderVersion").is_not_null(),
            col!("metaData.id").is_not_null(),
        )
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn test_declarative_pm_plan_filters_irrelevant_actions() {
        let snapshot = app_txn_checkpoint_snapshot();
        let filter = pm_plan_filter(
            snapshot.log_segment(),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            PmPlanSegment::WholeSegment,
        );
        assert_eq!(filter, pm_predicate());
    }

    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[test]
    fn test_declarative_pm_plan_reading_manifest_commit_keeps_its_checkpoint_action() {
        let snapshot = app_txn_checkpoint_snapshot();
        let plan_segment = PmPlanSegment::OlderCommits {
            oldest_exclusive: None,
            manifest_commit: Some(1),
        };
        let filter = pm_plan_filter(snapshot.log_segment(), plan_segment);
        let expected = Predicate::or(
            protocol_or_metadata(),
            Predicate::and(
                col!("checkpoint").is_not_null(),
                col!("version").eq(lit(1i64)),
            ),
        );
        assert_eq!(filter, expected);
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
