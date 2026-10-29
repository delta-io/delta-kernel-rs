//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

#[cfg(feature = "adaptive-metadata-in-dev")]
use itertools::Itertools as _;
use tracing::{info, instrument};
use url::Url;

use super::{LogSegment, checkpoint_action_projection_predicate};
#[cfg(feature = "adaptive-metadata-in-dev")]
use super::{
    last_manifest_commit_inconsistency, missing_pointed_checkpoint_action,
    require_pointed_checkpoint_action,
};
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::visitors::LastManifestCommitVisitor;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::{CHECKPOINT_ACTION_FIELD, CheckpointAction, LastManifestCommit};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::actions::{CHECKPOINT_ACTION_NAME, COMMIT_INFO_NAME};
use crate::actions::{METADATA_FIELD, Metadata, PROTOCOL_FIELD, Protocol};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
use crate::expressions::{Expression, lit};
#[cfg(feature = "declarative-plans")]
use crate::expressions::{Predicate, col};
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
    ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, StructField, StructType,
    column_name, schema_ref,
};
use crate::{
    Engine, EngineData, KernelError, KernelResult, KernelResultIterator, Version, version_as_i64,
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
            // Ignore pruned P&M at or below the CRC version: a lagging AMT checkpoint action can
            // carry it, and the CRC's P&M is at least as new. A captured checkpoint action is still
            // the latest: any action at or below the CRC is older.
            let replayed = pruned.replay_for_pm(engine)?;
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let newest_commit_read = pruned.read_newest_commit_info(&replayed)?;
            let mut resolution = replayed
                .newer_than(version_as_i64(crc.version)?)
                .into_resolution(
                    ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    newest_commit_read,
                );
            if resolution.metadata.is_some() && resolution.protocol.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok(resolution);
            }

            // Case 2(b): P&M incomplete or older than the CRC, use the CRC. Any newer P or M found
            // in the pruned replay takes priority over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            resolution
                .metadata
                .get_or_insert_with(|| crc.metadata.clone());
            resolution
                .protocol
                .get_or_insert_with(|| crc.protocol.clone());
            return Ok(resolution);
        }

        // Case 3: Full P&M log replay.
        // A listed checkpoint file truncates the log listing and holds the P&M as of where the
        // replay starts. Without one, the P&M may be only in the checkpoint action that the newest
        // commit's `lastManifestCommit` points at.
        let candidate = match self.checkpoint_version {
            Some(_) => self.replay_for_pm(engine)?,
            None => self.replay_for_pm_with_unbounded_segment(engine)?,
        };
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let newest_commit_read = self.read_newest_commit_info(&candidate)?;
        Ok(candidate.into_resolution(
            ProtocolMetadataSource::FullReplay,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            newest_commit_read,
        ))
    }

    /// Whether replaying this segment for `candidate` read its newest commit's `commitInfo`. Replay
    /// stops inside that commit only once the commit has both P&M, possibly before its `commitInfo`.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn read_newest_commit_info(&self, candidate: &PmCandidate) -> KernelResult<bool> {
        Ok(self.newest_commit_after_checkpoint().is_some()
            && !candidate.is_final_at(self.end_version)?)
    }

    /// Replays the log segment for the latest Protocol and Metadata, each with its version, in one
    /// pass over the segment.
    fn replay_for_pm(&self, engine: &dyn Engine) -> KernelResult<PmCandidate> {
        PmReader::for_engine(engine).replay(
            engine,
            self,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            PmReplaySegment::WholeSegment,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            PmCandidate::default(),
        )
    }

    /// [`Self::replay_for_pm`] for a segment that no CRC or checkpoint file bounds, whose P&M may
    /// be only in the checkpoint action the newest commit's `lastManifestCommit` points at.
    fn replay_for_pm_with_unbounded_segment(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<PmCandidate> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            let reader = PmReader::for_engine(engine);
            if reader.reads_newest_commit_first(self) {
                let newest_pm_candidate = reader.replay(
                    engine,
                    self,
                    PmReplaySegment::NewestCommit,
                    PmCandidate::default(),
                )?;
                if newest_pm_candidate.is_final_at(version_as_i64(self.end_version)?) {
                    return Ok(newest_pm_candidate);
                }
                let Some(older) = PmReplaySegment::older_commits(self, &newest_pm_candidate)?
                else {
                    // Nothing older to read: the segment has no older commits, or the newest
                    // commit's own checkpoint action has the P&M as of the commit before it. A P
                    // or M that is still missing is reported upstream, by the snapshot build that
                    // consumes this resolution.
                    return Ok(newest_pm_candidate);
                };
                return reader.replay(engine, self, older, newest_pm_candidate);
            }
        }
        self.replay_for_pm(engine)
    }

    /// Builds the declarative plan that selects the latest Protocol and Metadata actions. Under
    /// `adaptive-metadata-in-dev`, `segment` picks the files to read and the AMT actions to keep.
    #[cfg(feature = "declarative-plans")]
    fn build_pm_plan(
        &self,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmReplaySegment,
    ) -> KernelResult<Plan> {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (commit_paths, extra_actions) =
            (segment.commit_paths(self), segment.extra_actions(self)?);
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let (commit_paths, extra_actions) = (self.find_commit_cover_paths(None), Vec::new());
        let schema = pm_plan_schema(&extra_actions)?;

        let commits = PlanBuilder::scan_json(
            Self::version_tagged_scan_files(commit_paths)?,
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
        let pm_action = Predicate::or(
            col!(PROTOCOL_NAME, "minReaderVersion").is_not_null(),
            col!(METADATA_NAME, "id").is_not_null(),
        );
        let relevant_action = extra_actions.iter().fold(pm_action, |filter, action| {
            Predicate::or(filter, action.keep.clone())
        });

        let relevant_actions = PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .filter(relevant_action)?;
        Self::newest_pm_plan(relevant_actions, &extra_actions)
    }

    /// Aggregates `input` to its newest Protocol, Metadata, and each of `extra_actions`, with the
    /// versions the Protocol, Metadata, and each extra action with a `version_column` were found
    /// at.
    #[cfg(feature = "declarative-plans")]
    fn newest_pm_plan(
        input: PlanBuilder,
        extra_actions: &[PmPlanExtraAction],
    ) -> KernelResult<Plan> {
        input
            .aggregate_ungrouped(|aggregate| {
                let protocol = || column_name!(PROTOCOL_NAME);
                let metadata = || column_name!(METADATA_NAME);
                let version = || column_name!("version");
                // The version aggregates are aliased; unaliased both would be named `version`.
                let aggregate = aggregate
                    .max_non_null_by(protocol(), protocol(), version())
                    .max_non_null_by(metadata(), metadata(), version())
                    .aggregate_as(
                        Agg::max_non_null_by(version(), protocol(), version()),
                        PROTOCOL_VERSION_COLUMN,
                    )
                    .aggregate_as(
                        Agg::max_non_null_by(version(), metadata(), version()),
                        METADATA_VERSION_COLUMN,
                    );
                extra_actions.iter().fold(aggregate, |aggregate, action| {
                    let column = || ColumnName::new([action.field.name()]);
                    let aggregate = aggregate.max_non_null_by(column(), column(), version());
                    match action.version_column {
                        Some(version_column) => aggregate.aggregate_as(
                            Agg::max_non_null_by(version(), column(), version()),
                            version_column,
                        ),
                        None => aggregate,
                    }
                })
            })?
            .build()
    }

    /// Runs the declarative P&M plan for `segment` (as for [`Self::build_pm_plan`]) and tags each
    /// output batch with the versions its actions were found at.
    #[cfg(feature = "declarative-plans")]
    fn execute_pm_plan(
        &self,
        executor: &dyn PlanExecutor,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmReplaySegment,
    ) -> KernelResult<impl KernelResultIterator<VersionedBatch> + use<>> {
        let plan = self.build_pm_plan(
            #[cfg(feature = "adaptive-metadata-in-dev")]
            segment,
        )?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let end_version = version_as_i64(self.end_version)?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let checkpoint_commit_version = segment
            .checkpoint_action_version(self)
            .map(version_as_i64)
            .transpose()?;
        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(move |actions| {
                let actions = actions?;
                let (protocol_version, metadata_version) =
                    pm_versions_from_plan_output(actions.as_ref())?;
                // The newest pointer is only the newest commit's if it was found at that version.
                #[cfg(feature = "adaptive-metadata-in-dev")]
                let commit_info_version = (segment.reads_commit_info()
                    && commit_info_version_from_plan_output(actions.as_ref())?
                        == Some(end_version))
                .then_some(end_version);
                Ok(VersionedBatch {
                    protocol_version,
                    metadata_version,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    checkpoint_commit_version,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    commit_info_version,
                    actions,
                })
            });
        Ok(batches)
    }

    /// Reads the commits `segment` covers and the checkpoint, tagging each batch with its version.
    /// Each commit file projects its checkpoint action, so replay captures the pointed one if it
    /// reaches that row before it stops.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmReplaySegment,
    ) -> KernelResult<impl KernelResultIterator<VersionedBatch> + use<>> {
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
        // Read the same files as the plan for `segment`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let commit_files = segment
            .commit_paths(self)
            .into_iter()
            .map(|path| path.location.clone())
            .collect_vec();
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let commit_files = self.find_commit_cover();
        let commits = Self::read_commit_files(engine, &commit_files, commit_schema)?;
        // Let readers skip checkpoint data without a Protocol or Metadata.
        let checkpoint_predicate = checkpoint_action_projection_predicate(&checkpoint_schema);
        let checkpoint = self
            .create_checkpoint_stream(engine, checkpoint_schema, checkpoint_predicate, None, None)?
            .actions;
        let batches = commits.chain(checkpoint);
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
            // Only commit batches can hold a checkpoint action or `commitInfo`; the checkpoint
            // read schema excludes both.
            #[cfg(feature = "adaptive-metadata-in-dev")]
            let commit_version = batch.is_log_batch.then_some(version);
            Ok(VersionedBatch {
                protocol_version: Some(version),
                metadata_version: Some(version),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                checkpoint_commit_version: commit_version,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                commit_info_version: commit_version.filter(|v| *v == end_version),
                actions: batch.actions,
            })
        }))
    }

    /// The commit cover, newest first. When a log compaction covers the newest commit, that commit
    /// is also read as its own file: compactions drop `commitInfo`, and with it the newest
    /// `lastManifestCommit`. Only that `commitInfo` is needed from the newest commit; the
    /// compaction is still generally cheaper to read than the commits it replaces.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn pm_commit_cover_paths(&self) -> Vec<&ParsedLogPath> {
        let mut paths = self.find_commit_cover_paths(None);
        if let Some(newest_commit) = self.newest_commit_after_checkpoint()
            && !paths.contains(&newest_commit)
        {
            // Put it first so replay reads the pointer even if it stops early.
            paths.insert(0, newest_commit);
        }
        paths
    }

    /// The commit at `end_version`, or `None` when the segment ends on a checkpoint. Unlike
    /// `listed.latest_commit_file`, this skips the commit at the checkpoint version, since the
    /// checkpoint already covers it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn newest_commit_after_checkpoint(&self) -> Option<&ParsedLogPath> {
        self.listed
            .ascending_commit_files
            .last()
            .filter(|commit| commit.version == self.end_version)
    }
}

/// How P&M replay reads the log.
enum PmReader {
    /// Through the engine's declarative plan executor.
    #[cfg(feature = "declarative-plans")]
    Plan(Arc<dyn PlanExecutor>),
    /// By reading the log files.
    Files,
}

impl PmReader {
    /// The reader `engine` supports: its plan executor when it has one.
    #[cfg_attr(not(feature = "declarative-plans"), allow(unused_variables))]
    fn for_engine(engine: &dyn Engine) -> Self {
        #[cfg(feature = "declarative-plans")]
        if let Some(executor) = engine.plan_executor() {
            return Self::Plan(executor);
        }
        Self::Files
    }

    /// Replays `segment` of `log_segment` for its newest Protocol and Metadata. Under
    /// `adaptive-metadata-in-dev`, the result is folded onto `newer`, what replay found in the
    /// files newer than `segment`, and fails when its `lastManifestCommit` contradicts the
    /// checkpoint action replay read.
    fn replay(
        &self,
        engine: &dyn Engine,
        log_segment: &LogSegment,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmReplaySegment,
        #[cfg(feature = "adaptive-metadata-in-dev")] newer: PmCandidate,
    ) -> KernelResult<PmCandidate> {
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let newer = PmCandidate::default();
        let batches = self.batches(
            engine,
            log_segment,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            segment,
        )?;
        let replayed = resolve_pm_batches(newer, batches)?;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let replayed = replayed.validated_pointer(log_segment.end_version, |commit| {
            self.keeps_checkpoint_action(log_segment, segment, commit)
        })?;
        Ok(replayed)
    }

    /// The batches replaying `segment` of `log_segment` folds, newest first.
    fn batches(
        &self,
        engine: &dyn Engine,
        log_segment: &LogSegment,
        #[cfg(feature = "adaptive-metadata-in-dev")] segment: PmReplaySegment,
    ) -> KernelResult<Box<dyn KernelResultIterator<VersionedBatch>>> {
        Ok(match self {
            #[cfg(feature = "declarative-plans")]
            Self::Plan(executor) => Box::new(log_segment.execute_pm_plan(
                executor.as_ref(),
                #[cfg(feature = "adaptive-metadata-in-dev")]
                segment,
            )?),
            Self::Files => Box::new(log_segment.read_pm_batches(
                engine,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                segment,
            )?),
        })
    }

    /// Whether replaying `segment` of `log_segment` keeps the checkpoint action of `commit`.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn keeps_checkpoint_action(
        &self,
        log_segment: &LogSegment,
        segment: PmReplaySegment,
        commit: i64,
    ) -> bool {
        let commit = Version::try_from(commit).ok();
        match self {
            // A plan keeps only the action of the commit it targets.
            #[cfg(feature = "declarative-plans")]
            Self::Plan(_) => segment.checkpoint_action_version(log_segment) == commit,
            // File reads keep the action of every commit file they read.
            Self::Files => segment
                .commit_paths(log_segment)
                .iter()
                .any(|path| path.is_commit() && Some(path.version) == commit),
        }
    }

    /// Whether replay must read the newest commit of `log_segment` before it can choose the older
    /// files: a plan fixes its files before it runs, and a log compaction can hide the pointed
    /// commit's checkpoint action from a single pass of file reads.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn reads_newest_commit_first(&self, log_segment: &LogSegment) -> bool {
        log_segment.newest_commit_after_checkpoint().is_some()
            && match self {
                #[cfg(feature = "declarative-plans")]
                Self::Plan(_) => true,
                Self::Files => log_segment
                    .pm_commit_cover_paths()
                    .iter()
                    .any(|path| !path.is_commit()),
            }
    }
}

/// The part of the log one P&M replay reads.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone, Copy, PartialEq)]
enum PmReplaySegment {
    /// The segment's commits and checkpoint.
    WholeSegment,
    /// Only the newest commit.
    NewestCommit,
    /// The commits after `oldest_exclusive` (all when `None`), except the newest. The manifest
    /// commit at `manifest_commit` is read as its own file, so a log compaction cannot hide its
    /// checkpoint action.
    OlderCommits {
        oldest_exclusive: Option<Version>,
        manifest_commit: Option<Version>,
    },
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl PmReplaySegment {
    /// The older commits to read after the newest commit's replay result `newest`, or `None` when
    /// `log_segment` lists none.
    fn older_commits(log_segment: &LogSegment, newest: &PmCandidate) -> KernelResult<Option<Self>> {
        // Replay still needs the pointed checkpoint action when the newest commit's
        // `lastManifestCommit` names it and the newest commit lacks it. An unlisted pointed commit
        // is at or below where this segment starts, so whatever covers that start has P&M at least
        // as new; the snapshot reads that action through the pointer.
        let pointed_commit = match (&newest.checkpoint, &newest.last_manifest_commit) {
            (None, Some(pointer)) => log_segment
                .find_pointed_commit(pointer)
                .map(|commit| (commit.version, pointer.content_root_version)),
            _ => None,
        };
        let (oldest_exclusive, manifest_commit) = match pointed_commit {
            // The pointed checkpoint action has the P&M as of its content root, so earlier commits
            // hold nothing newer. The range includes the content root because the commit
            // containing that action can be at that version.
            Some((manifest_commit, content_root)) => (
                as_version(content_root)?.checked_sub(1),
                Some(manifest_commit),
            ),
            // Without a listed commit containing the pointed action, the newest commit's own
            // action (if any) bounds the range; otherwise every older commit is read.
            None => match &newest.checkpoint {
                Some((_, action)) => (Some(as_version(action.version())?), None),
                None => (None, None),
            },
        };
        let older = Self::OlderCommits {
            oldest_exclusive,
            manifest_commit,
        };
        // An empty range can't include a commit containing the pointed action either.
        Ok((!older.commit_paths(log_segment).is_empty()).then_some(older))
    }

    /// The commit files the replay reads from `log_segment`.
    fn commit_paths(self, log_segment: &LogSegment) -> Vec<&ParsedLogPath> {
        match self {
            Self::WholeSegment => log_segment.pm_commit_cover_paths(),
            Self::NewestCommit => Vec::from_iter(log_segment.newest_commit_after_checkpoint()),
            Self::OlderCommits {
                oldest_exclusive,
                manifest_commit,
            } => match log_segment.end_version.checked_sub(1) {
                Some(newest_inclusive) => log_segment.find_commit_cover_paths_in_range(
                    oldest_exclusive,
                    newest_inclusive,
                    manifest_commit.as_slice(),
                ),
                // A segment ending at version 0 has no commits older than its newest.
                None => Vec::new(),
            },
        }
    }

    /// Whether the plan reads the newest commit's `commitInfo`.
    #[cfg(feature = "declarative-plans")]
    fn reads_commit_info(self) -> bool {
        !matches!(self, Self::OlderCommits { .. })
    }

    /// The commit whose checkpoint action the plan keeps: the newest commit for `WholeSegment` and
    /// `NewestCommit`, or the commit the newest commit's pointer names for `OlderCommits`.
    #[cfg(feature = "declarative-plans")]
    fn checkpoint_action_version(self, log_segment: &LogSegment) -> Option<Version> {
        match self {
            // A plan replays the whole segment only when a CRC or checkpoint file bounds it, so it
            // needs no older commit's action. A segment ending on a checkpoint lists no commit at
            // its end version, and the checkpoint's rows carry that version too.
            Self::WholeSegment | Self::NewestCommit => log_segment
                .newest_commit_after_checkpoint()
                .map(|commit| commit.version),
            // The action the newest commit's pointer names.
            Self::OlderCommits {
                manifest_commit, ..
            } => manifest_commit,
        }
    }

    /// The actions the plan reads from `log_segment` beyond Protocol and Metadata.
    #[cfg(feature = "declarative-plans")]
    fn extra_actions(self, log_segment: &LogSegment) -> KernelResult<Vec<PmPlanExtraAction>> {
        // Keep only `commitInfo` rows with a pointer (none without AMT). Replay uses the newest one
        // only if it is the newest commit's.
        let commit_info = self.reads_commit_info().then(|| PmPlanExtraAction {
            field: LastManifestCommitVisitor::commit_info_field(),
            keep: col!(COMMIT_INFO_NAME, "lastManifestCommit", "version").is_not_null(),
            version_column: Some(COMMIT_INFO_VERSION_COLUMN),
        });
        let checkpoint_action = self
            .checkpoint_action_version(log_segment)
            .map(|version| -> KernelResult<_> {
                Ok(PmPlanExtraAction {
                    field: &CHECKPOINT_ACTION_FIELD,
                    keep: action_at_version(CHECKPOINT_ACTION_NAME, version)?,
                    version_column: None,
                })
            })
            .transpose()?;
        Ok(commit_info.into_iter().chain(checkpoint_action).collect())
    }
}

/// An action a P&M plan reads beyond Protocol and Metadata.
#[cfg(feature = "declarative-plans")]
struct PmPlanExtraAction {
    field: &'static StructField,
    /// Matches the rows to keep for this action.
    keep: Predicate,
    /// The output column for the version the newest action was found at, when replay needs it.
    version_column: Option<&'static str>,
}

/// How a snapshot's `lastManifestCommit` was resolved at build time, stored on the
/// [`Snapshot`](crate::Snapshot) so consumers can read it without opening the log.
#[cfg(feature = "adaptive-metadata-in-dev")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LastManifestCommitResolution {
    /// Read at build time from the CRC or log replay. `None` means the table has no manifest
    /// commit at this version.
    Resolved(Option<LastManifestCommit>),
    /// The build did not find the snapshot version's `lastManifestCommit`; consumers read it from
    /// that version's commit file.
    Unresolved,
}

#[cfg(feature = "adaptive-metadata-in-dev")]
impl LastManifestCommitResolution {
    /// Resolves the `lastManifestCommit` that P&M replay captured from the snapshot version's
    /// commit.
    ///
    /// `newest_commit_read` says whether replay read that commit's `commitInfo`. If so, a missing
    /// `captured` pointer resolves to `Resolved(None)`. Otherwise it is `Unresolved` and is read
    /// from the commit file on first use.
    fn from_replay(captured: Option<LastManifestCommit>, newest_commit_read: bool) -> Self {
        match captured {
            Some(pointer) => Self::Resolved(Some(pointer)),
            None if newest_commit_read => Self::Resolved(None),
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
    /// The AMT `checkpoint` action that `lastManifestCommit` names, if replay read it. This only
    /// saves a read: when it is `None`, the snapshot follows `lastManifestCommit` to the action.
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
#[derive(Default)]
struct PmCandidate {
    protocol: Option<(i64, Protocol)>,
    metadata: Option<(i64, Metadata)>,
    /// The newest AMT checkpoint action replay read, tagged with the version of the commit it was
    /// read from.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint: Option<(i64, CheckpointAction)>,
    /// The newest commit's `commitInfo.lastManifestCommit`, if replay read it.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    last_manifest_commit: Option<LastManifestCommit>,
}

impl PmCandidate {
    /// Drops the Protocol and Metadata found at or below `version`.
    fn newer_than(mut self, version: i64) -> Self {
        self.protocol = self.protocol.filter(|(v, _)| *v > version);
        self.metadata = self.metadata.filter(|(v, _)| *v > version);
        self
    }

    /// The resolution this replay result gives the snapshot, resolved through `source`.
    /// `newest_commit_read` is as for [`LastManifestCommitResolution::from_replay`].
    fn into_resolution(
        self,
        source: ProtocolMetadataSource,
        #[cfg(feature = "adaptive-metadata-in-dev")] newest_commit_read: bool,
    ) -> PmResolution {
        PmResolution {
            metadata: self.metadata.map(|(_, m)| m),
            protocol: self.protocol.map(|(_, p)| p),
            source,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action: self.checkpoint.map(|(_, c)| Arc::new(c)),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit: LastManifestCommitResolution::from_replay(
                self.last_manifest_commit,
                newest_commit_read,
            ),
        }
    }

    /// Whether both Protocol and Metadata are found at or above `version`.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn is_final_at(&self, version: i64) -> bool {
        is_final(&self.protocol, Some(version)) && is_final(&self.metadata, Some(version))
    }

    /// Whether replay can stop after `batch`: it has both P&M at or above the batch's version, and
    /// `batch` is not from a newest commit that names itself but whose checkpoint action is unread.
    fn can_stop_after(&self, batch: &VersionedBatch) -> bool {
        // Only the newest commit's batches carry a `commit_info_version`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let in_self_pointing_newest_commit = self
            .last_manifest_commit
            .as_ref()
            .is_some_and(|pointer| Some(pointer.version) == batch.commit_info_version);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        if in_self_pointing_newest_commit && self.checkpoint.is_none() {
            return false;
        }
        // A checkpoint action's P&M can be older than its commit, so check version, not presence.
        let version = batch.protocol_version.max(batch.metadata_version);
        is_final(&self.protocol, version) && is_final(&self.metadata, version)
    }

    /// Folds `other`, parsed from a batch replay reached later, into this result. Each field keeps
    /// its higher-versioned value, so the order batches merge in only matters on a version tie,
    /// where `other` wins. Once the pointer and the checkpoint action it names are both known, the
    /// action's nested P&M are also candidates.
    fn merge(self, other: PmCandidate) -> Self {
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let brings_amt = other.checkpoint.is_some() || other.last_manifest_commit.is_some();
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let checkpoint = newer(self.checkpoint, other.checkpoint);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let last_manifest_commit = {
            let tagged = |pointer: LastManifestCommit| (pointer.version, pointer);
            newer(
                self.last_manifest_commit.map(tagged),
                other.last_manifest_commit.map(tagged),
            )
            .map(|(_, pointer)| pointer)
        };
        // The pointer and the checkpoint action it names can come from different batches, in
        // either order. Once both are known, use the action's nested P&M as candidates at its
        // `checkpointMetadata.version`.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let (pointed_protocol, pointed_metadata) = match (&checkpoint, &last_manifest_commit) {
            (Some((commit, action)), Some(pointer)) if brings_amt && *commit == pointer.version => {
                let (protocol, metadata) = checkpoint_pm(action);
                (Some(protocol), Some(metadata))
            }
            _ => (None, None),
        };
        #[cfg(not(feature = "adaptive-metadata-in-dev"))]
        let (pointed_protocol, pointed_metadata) = (None, None);
        PmCandidate {
            protocol: newer(newer(self.protocol, other.protocol), pointed_protocol),
            metadata: newer(newer(self.metadata, other.metadata), pointed_metadata),
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit,
        }
    }

    /// Checks the `lastManifestCommit` read from the newest commit at `version` against the
    /// checkpoint action replay read, keeping the action only if it comes from the pointed commit.
    /// `keeps_action_of(commit)` tells whether replay keeps the checkpoint action of `commit` when
    /// it reads that commit. Errors when the log contradicts the pointer.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    fn validated_pointer(
        mut self,
        version: Version,
        keeps_action_of: impl FnOnce(i64) -> bool,
    ) -> KernelResult<Self> {
        let pointer = self.last_manifest_commit.as_ref();
        if let Some(pointer) = pointer {
            pointer.ensure_at_or_before(version)?;
        }
        let version = version_as_i64(version)?;
        // Actions from commits before the pointed one are stale. Replay reads one when a log
        // compaction hides the pointed commit's own action.
        let checkpoint = self
            .checkpoint
            .take()
            .filter(|(commit, _)| pointer.is_none_or(|pointer| *commit >= pointer.version));
        // Replay stops once it has both P&M at or above a commit's version, possibly before that
        // commit's checkpoint action, except in a newest commit that names itself. So only an
        // action missing from a commit replay fully read contradicts the log.
        self.checkpoint = match (checkpoint, pointer) {
            (Some((commit, action)), Some(pointer)) if commit == pointer.version => {
                require_pointed_checkpoint_action(pointer, &action)?;
                Some((commit, action))
            }
            (Some((commit, _)), Some(pointer)) => {
                return Err(last_manifest_commit_inconsistency(
                    pointer,
                    format!(
                        "commit {commit}, which it does not name, also carries a checkpoint action"
                    ),
                ));
            }
            (Some((commit, _)), None) if commit == version && !self.is_final_at(version) => {
                return Err(KernelError::invalid_log_segment(format!(
                    "commit {version} carries a checkpoint action but no lastManifestCommit"
                )));
            }
            (None, Some(pointer))
                if keeps_action_of(pointer.version)
                    && (pointer.version == version || !self.is_final_at(pointer.version)) =>
            {
                return Err(missing_pointed_checkpoint_action(pointer));
            }
            _ => None,
        };
        Ok(self)
    }
}

/// A P&M-projected batch with the versions to rank its Protocol and Metadata at.
struct VersionedBatch {
    protocol_version: Option<i64>,
    metadata_version: Option<i64>,
    /// The version of the commit whose AMT checkpoint action `actions` projects, if any.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    checkpoint_commit_version: Option<i64>,
    /// The newest commit's version, when `actions` holds that commit's `commitInfo`.
    #[cfg(feature = "adaptive-metadata-in-dev")]
    commit_info_version: Option<i64>,
    actions: Box<dyn EngineData>,
}

/// The newest Protocol and Metadata across `seed` and `batches`. `seed` holds what replay already
/// found in newer files, so the fold stops as soon as the two together are final.
fn resolve_pm_batches(
    seed: PmCandidate,
    batches: impl KernelResultIterator<VersionedBatch>,
) -> KernelResult<PmCandidate> {
    let mut resolved = seed;
    for batch in batches {
        let batch = batch?;
        resolved = resolved.merge(pm_candidate(&batch)?);
        if resolved.can_stop_after(&batch) {
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
    ParsedLogPath::new(url)
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
#[cfg(feature = "adaptive-metadata-in-dev")]
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

/// The P&M plan read schema: Protocol, Metadata, `extra_actions`, and the file-constant `version`.
#[cfg(feature = "declarative-plans")]
fn pm_plan_schema(extra_actions: &[PmPlanExtraAction]) -> KernelResult<SchemaRef> {
    let actions = [&*PROTOCOL_FIELD, &*METADATA_FIELD]
        .into_iter()
        .chain(extra_actions.iter().map(|action| action.field))
        .cloned();
    let version = StructField::not_null("version", DataType::LONG);
    Ok(Arc::new(StructType::try_new(actions.chain([version]))?))
}

/// The Protocol and Metadata in `batch`, tagged with their versions. Under AMT, also any checkpoint
/// action, tagged with the version of the commit it was read from, and the newest commit's
/// `lastManifestCommit`.
fn pm_candidate(batch: &VersionedBatch) -> KernelResult<PmCandidate> {
    let actions = batch.actions.as_ref();
    let protocol = batch
        .protocol_version
        .zip(Protocol::try_new_from_data(actions)?);
    let metadata = batch
        .metadata_version
        .zip(Metadata::try_new_from_data(actions)?);

    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        let last_manifest_commit = match batch.commit_info_version {
            Some(_) => LastManifestCommit::try_new_from_data(actions)?,
            None => None,
        };
        // Parsing an action needs its column in the batch's schema.
        let checkpoint = match batch.checkpoint_commit_version {
            Some(commit) => CheckpointAction::try_new_from_data(actions)?.map(|c| (commit, c)),
            None => None,
        };
        Ok(PmCandidate {
            protocol,
            metadata,
            checkpoint,
            last_manifest_commit,
        })
    }

    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    Ok(PmCandidate { protocol, metadata })
}

/// A predicate matching rows that carry the `action` read from the log file at `version`.
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
fn action_at_version(action: &str, version: Version) -> KernelResult<Predicate> {
    Ok(Predicate::and(
        Expression::column([action]).is_not_null(),
        col!("version").eq(lit(version_as_i64(version)?)),
    ))
}

// === P&M plan output columns: the versions the newest of each action was found at ===

#[cfg(feature = "declarative-plans")]
const PROTOCOL_VERSION_COLUMN: &str = "protocol_version";
#[cfg(feature = "declarative-plans")]
const METADATA_VERSION_COLUMN: &str = "metadata_version";
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
const COMMIT_INFO_VERSION_COLUMN: &str = "commit_info_version";

/// Reads the [`PROTOCOL_VERSION_COLUMN`] and [`METADATA_VERSION_COLUMN`] the plan aggregate emits.
#[cfg(feature = "declarative-plans")]
fn pm_versions_from_plan_output(
    actions: &dyn EngineData,
) -> KernelResult<(Option<i64>, Option<i64>)> {
    static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
        (
            vec![
                ColumnName::new([PROTOCOL_VERSION_COLUMN]),
                ColumnName::new([METADATA_VERSION_COLUMN]),
            ],
            vec![DataType::LONG, DataType::LONG],
        )
            .into()
    });
    let [protocol, metadata] = versions_from_plan_output(actions, &COLUMNS)?;
    Ok((protocol, metadata))
}

/// Reads the [`COMMIT_INFO_VERSION_COLUMN`] the plan aggregate emits when it reads `commitInfo`.
#[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
fn commit_info_version_from_plan_output(actions: &dyn EngineData) -> KernelResult<Option<i64>> {
    static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
        (
            vec![ColumnName::new([COMMIT_INFO_VERSION_COLUMN])],
            vec![DataType::LONG],
        )
            .into()
    });
    let [version] = versions_from_plan_output(actions, &COLUMNS)?;
    Ok(version)
}

/// Reads the `columns` version aggregates from the single row the plan aggregate emits.
#[cfg(feature = "declarative-plans")]
fn versions_from_plan_output<const N: usize>(
    actions: &dyn EngineData,
    columns: &'static ColumnNamesAndTypes,
) -> KernelResult<[Option<i64>; N]> {
    struct VersionsVisitor<const N: usize> {
        columns: &'static ColumnNamesAndTypes,
        versions: [Option<i64>; N],
    }
    impl<const N: usize> RowVisitor for VersionsVisitor<N> {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            self.columns.as_ref()
        }
        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> KernelResult<()> {
            if row_count > 0 {
                let (names, _) = self.columns.as_ref();
                for ((version, getter), name) in self.versions.iter_mut().zip(getters).zip(names) {
                    *version = getter.get_opt(0, &name.to_string())?;
                }
            }
            Ok(())
        }
    }
    let mut visitor = VersionsVisitor {
        columns,
        versions: [None; N],
    };
    visitor.visit_rows_of(actions)?;
    Ok(visitor.versions)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    #[cfg(feature = "declarative-plans")]
    use std::sync::Arc;

    use itertools::Itertools;
    use test_log::test;

    #[cfg(feature = "adaptive-metadata-in-dev")]
    use super::PmReplaySegment;
    use crate::Snapshot;
    #[cfg(feature = "declarative-plans")]
    use crate::SnapshotRef;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::DelegatingEngine;
    use crate::engine::sync::SyncEngine;
    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    use crate::expressions::lit;
    #[cfg(feature = "declarative-plans")]
    use crate::expressions::{Predicate, col};
    #[cfg(feature = "declarative-plans")]
    use crate::log_segment::LogSegment;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::ir::nodes::Operator;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
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
            .read_pm_batches(
                &engine,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                PmReplaySegment::WholeSegment,
            )
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
        #[cfg(feature = "adaptive-metadata-in-dev")] plan_segment: PmReplaySegment,
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

    /// The P&M plan filter for the app-txn-checkpoint table.
    #[cfg(feature = "declarative-plans")]
    fn pm_predicate() -> Predicate {
        let pm = protocol_or_metadata();
        #[cfg(feature = "adaptive-metadata-in-dev")]
        let pm = Predicate::or(
            pm,
            col!("commitInfo.lastManifestCommit.version").is_not_null(),
        );
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
            PmReplaySegment::WholeSegment,
        );
        assert_eq!(filter, pm_predicate());
    }

    #[cfg(all(feature = "adaptive-metadata-in-dev", feature = "declarative-plans"))]
    #[test]
    fn test_declarative_pm_plan_reading_manifest_commit_keeps_its_checkpoint_action() {
        let snapshot = app_txn_checkpoint_snapshot();
        let plan_segment = PmReplaySegment::OlderCommits {
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

    // `rstest` cases glob-import their parent module, where `test_log::test` would shadow `test`.
    mod pm_candidate {
        use rstest::rstest;

        use super::super::PmCandidate;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        use crate::actions::LastManifestCommit;
        use crate::actions::{Metadata, Protocol};
        use crate::metrics::ProtocolMetadataSource;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        use crate::unit_test_utils::adaptive_metadata_fixtures::{
            minimal_checkpoint_action, pointer,
        };
        #[cfg(feature = "adaptive-metadata-in-dev")]
        use crate::unit_test_utils::assert_result_error_with_message;

        // A Protocol and Metadata that `writer_version` tells apart, found at the given versions.
        #[cfg_attr(
            not(feature = "adaptive-metadata-in-dev"),
            allow(clippy::needless_update)
        )]
        fn candidate(
            writer_version: i32,
            protocol_version: Option<i64>,
            metadata_version: Option<i64>,
        ) -> PmCandidate {
            PmCandidate {
                protocol: protocol_version.map(|v| (v, protocol(writer_version))),
                metadata: metadata_version.map(|v| (v, metadata(writer_version))),
                ..Default::default()
            }
        }

        fn protocol(writer_version: i32) -> Protocol {
            Protocol::try_new_legacy(1, writer_version).unwrap()
        }

        fn metadata(writer_version: i32) -> Metadata {
            Metadata::default().with_configuration_entry("writer", writer_version.to_string())
        }

        #[rstest]
        #[case::above_version_kept(Some(3), Some(4), Some(3), Some(4))]
        #[case::at_version_dropped(Some(2), Some(3), None, Some(3))]
        #[case::below_version_dropped(Some(1), Some(2), None, None)]
        fn test_newer_than_drops_pm_at_or_below_version(
            #[case] protocol_version: Option<i64>,
            #[case] metadata_version: Option<i64>,
            #[case] expected_protocol_version: Option<i64>,
            #[case] expected_metadata_version: Option<i64>,
        ) {
            let candidate = candidate(1, protocol_version, metadata_version).newer_than(2);
            assert_eq!(
                candidate.protocol.map(|(v, _)| v),
                expected_protocol_version
            );
            assert_eq!(
                candidate.metadata.map(|(v, _)| v),
                expected_metadata_version
            );
        }

        #[rstest]
        #[case::self_newer(2, 1, 1)]
        #[case::other_newer(1, 2, 2)]
        #[case::tie_goes_to_other(1, 1, 2)]
        fn test_merge_keeps_newer_pm(
            #[case] self_version: i64,
            #[case] other_version: i64,
            #[case] expected_writer_version: i32,
        ) {
            let merged = candidate(1, Some(self_version), Some(self_version)).merge(candidate(
                2,
                Some(other_version),
                Some(other_version),
            ));
            let expected_version = self_version.max(other_version);
            assert_eq!(
                merged.protocol,
                Some((expected_version, protocol(expected_writer_version)))
            );
            assert_eq!(
                merged.metadata,
                Some((expected_version, metadata(expected_writer_version)))
            );
        }

        #[test]
        fn test_merge_keeps_pm_missing_from_one_side() {
            let merged = candidate(1, Some(1), None).merge(candidate(2, None, Some(1)));
            assert_eq!(merged.protocol, Some((1, protocol(1))));
            assert_eq!(merged.metadata, Some((1, metadata(2))));
        }

        #[test]
        fn test_into_resolution_drops_versions() {
            let resolution = candidate(1, Some(3), Some(4)).into_resolution(
                ProtocolMetadataSource::FullReplay,
                #[cfg(feature = "adaptive-metadata-in-dev")]
                true,
            );
            assert_eq!(resolution.protocol, Some(protocol(1)));
            assert_eq!(resolution.metadata, Some(metadata(1)));
            assert_eq!(resolution.source, ProtocolMetadataSource::FullReplay);
            #[cfg(feature = "adaptive-metadata-in-dev")]
            assert!(resolution.captured_checkpoint_action.is_none());
        }

        // The pointer ranks by the manifest commit it names and the action by the commit it was
        // read from; `content_root` tells the two sides of a tie apart.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        #[rstest]
        #[case::self_newer(2, 1, 2, 0)]
        #[case::other_newer(1, 2, 2, 1)]
        #[case::tie_goes_to_other(1, 1, 1, 1)]
        fn test_merge_keeps_newer_amt_fields(
            #[case] self_version: i64,
            #[case] other_version: i64,
            #[case] expected_version: i64,
            #[case] expected_content_root: i64,
        ) {
            let amt = |version: i64, content_root: i64| PmCandidate {
                checkpoint: Some((
                    version,
                    minimal_checkpoint_action("root.parquet", content_root as u64).unwrap(),
                )),
                last_manifest_commit: Some(pointer(version, content_root)),
                ..Default::default()
            };
            let merged = amt(self_version, 0).merge(amt(other_version, 1));
            let (checkpoint_version, action) = merged.checkpoint.unwrap();
            assert_eq!(checkpoint_version, expected_version);
            assert_eq!(action.content_root_version(), expected_content_root);
            assert_eq!(
                merged.last_manifest_commit,
                Some(pointer(expected_version, expected_content_root))
            );
        }

        #[cfg(feature = "adaptive-metadata-in-dev")]
        #[rstest]
        fn test_merge_folds_pointed_pm_in_either_order(#[values(false, true)] action_first: bool) {
            let with_pointer = PmCandidate {
                last_manifest_commit: Some(pointer(2, 2)),
                ..Default::default()
            };
            let with_action = PmCandidate {
                checkpoint: Some((2, minimal_checkpoint_action("root.parquet", 2).unwrap())),
                ..Default::default()
            };
            let merged = if action_first {
                with_action.merge(with_pointer)
            } else {
                with_pointer.merge(with_action)
            };
            assert_eq!(merged.protocol.map(|(v, _)| v), Some(2));
            assert_eq!(merged.metadata.map(|(v, _)| v), Some(2));
        }

        // Validation keeps only the pointed commit's action. It errors on an action newer than the
        // pointed commit, an action in the newest commit without a pointer, or a pointer naming a
        // commit replay read and kept actions from when that commit has no action. Replay here
        // reads and keeps the actions of commits 1 and 2. An error about an older commit, or about
        // a newest commit without a pointer, is deferred when replay has both P&M at or above its
        // version (`pm_version`), because replay may have stopped before that commit's action.
        // Replay always reads all of a newest commit that names itself.
        #[cfg(feature = "adaptive-metadata-in-dev")]
        #[rstest]
        #[case::pointed_action(Some(pointer(1, 1)), Some((1, 1)), None, Ok(Some(1)))]
        #[case::older_action(Some(pointer(1, 1)), Some((0, 0)), Some(1), Ok(None))]
        #[case::older_action_without_pointer(None, Some((0, 0)), None, Ok(None))]
        #[case::no_action_for_unread_pointer(Some(pointer(0, 0)), None, None, Ok(None))]
        #[case::no_action_for_older_pointer(
            Some(pointer(1, 1)),
            None,
            None,
            Err("that commit carries no checkpoint action")
        )]
        #[case::other_content_root(
            Some(pointer(1, 1)),
            Some((1, 0)),
            None,
            Err("checkpoint action has content root version 0")
        )]
        #[case::newer_action(
            Some(pointer(1, 1)),
            Some((2, 2)),
            None,
            Err("commit 2, which it does not name, also carries a checkpoint action")
        )]
        #[case::newest_action_without_pointer(
            None,
            Some((2, 2)),
            None,
            Err("commit 2 carries a checkpoint action but no lastManifestCommit")
        )]
        #[case::no_action_for_newest_pointer(
            Some(pointer(2, 2)),
            None,
            None,
            Err("that commit carries no checkpoint action")
        )]
        #[case::older_action_for_newest_pointer(
            Some(pointer(2, 2)),
            Some((0, 0)),
            None,
            Err("that commit carries no checkpoint action")
        )]
        #[case::no_action_for_older_pointer_deferred(Some(pointer(1, 1)), None, Some(1), Ok(None))]
        #[case::newest_action_without_pointer_deferred(None, Some((2, 2)), Some(2), Ok(None))]
        #[case::no_action_for_newest_pointer_with_pm(
            Some(pointer(2, 2)),
            None,
            Some(2),
            Err("that commit carries no checkpoint action")
        )]
        fn test_validated_pointer(
            #[case] pointer: Option<LastManifestCommit>,
            #[case] action: Option<(i64, u64)>,
            #[case] pm_version: Option<i64>,
            #[case] expected: Result<Option<i64>, &str>,
        ) {
            let candidate = PmCandidate {
                checkpoint: action.map(|(commit, content_root)| {
                    let action = minimal_checkpoint_action("root.parquet", content_root).unwrap();
                    (commit, action)
                }),
                last_manifest_commit: pointer,
                ..candidate(1, pm_version, pm_version)
            };
            let result = candidate.validated_pointer(2, |commit| commit >= 1);
            match expected {
                Ok(kept) => {
                    let kept_version = result.unwrap().checkpoint.map(|(commit, _)| commit);
                    assert_eq!(kept_version, kept);
                }
                Err(message) => assert_result_error_with_message(result, message),
            }
        }
    }
}
