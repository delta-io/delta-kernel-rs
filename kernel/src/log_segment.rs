//! Represents a segment of a delta log. [`LogSegment`] wraps a set of checkpoint and commit
//! files.
use std::num::NonZero;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    get_commit_schema, schema_contains_file_actions, Metadata, Protocol, Sidecar, METADATA_NAME,
    PROTOCOL_NAME, SIDECAR_NAME,
};
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_replay::ActionsBatch;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::{SchemaRef, StructField, ToSchema as _};
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, FileMeta, ParquetHandler, Predicate,
    PredicateRef, RowVisitor, StorageHandler, Version,
};
use delta_kernel_derive::internal_api;

#[cfg(feature = "internal-api")]
pub use crate::listed_log_files::ListedLogFiles;
#[cfg(not(feature = "internal-api"))]
use crate::listed_log_files::ListedLogFiles;

use itertools::Itertools;
use tracing::{debug, warn};
use url::Url;

#[cfg(test)]
mod tests;

/// A [`LogSegment`] represents a contiguous section of the log and is made of checkpoint files
/// and commit files and guarantees the following:
///     1. Commit file versions will not have any gaps between them.
///     2. If checkpoint(s) is/are present in the range, only commits with versions greater than the most
///        recent checkpoint version are retained. There will not be a gap between the checkpoint
///        version and the first commit version.
///     3. All checkpoint_parts must belong to the same checkpoint version, and must form a complete
///        version. Multi-part checkpoints must have all their parts.
///
/// [`LogSegment`] is used in [`Snapshot`] when built with [`LogSegment::for_snapshot`], and
/// in `TableChanges` when built with [`LogSegment::for_table_changes`].
///
/// [`Snapshot`]: crate::snapshot::Snapshot
#[derive(Debug, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) struct LogSegment {
    pub end_version: Version,
    pub checkpoint_version: Option<Version>,
    pub log_root: Url,
    /// Sorted commit files in the log segment (ascending)
    pub ascending_commit_files: Vec<ParsedLogPath>,
    /// Sorted (by start version) compaction files in the log segment (ascending)
    pub ascending_compaction_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment.
    pub checkpoint_parts: Vec<ParsedLogPath>,
    /// Latest CRC (checksum) file
    pub latest_crc_file: Option<ParsedLogPath>,
    /// The latest commit file found during listing, which may not be part of the
    /// contiguous segment but is needed for ICT timestamp reading
    pub latest_commit_file: Option<ParsedLogPath>,
}

impl LogSegment {
    #[internal_api]
    pub(crate) fn try_new(
        listed_files: ListedLogFiles,
        log_root: Url,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let ListedLogFiles {
            mut ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
            latest_commit_file,
        } = listed_files;

        // Ensure commit file versions are contiguous
        require!(
            ascending_commit_files
                .windows(2)
                .all(|cfs| cfs[0].version + 1 == cfs[1].version),
            Error::generic(format!(
                "Expected ordered contiguous commit files {ascending_commit_files:?}"
            ))
        );

        // Commit file versions must be greater than the most recent checkpoint version if it exists
        let checkpoint_version = checkpoint_parts.first().map(|checkpoint_file| {
            ascending_commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
            checkpoint_file.version
        });

        // There must be no gap between a checkpoint and the first commit version. Note that
        // that all checkpoint parts share the same version.
        if let (Some(checkpoint_version), Some(commit_file)) =
            (checkpoint_version, ascending_commit_files.first())
        {
            require!(
                checkpoint_version + 1 == commit_file.version,
                Error::InvalidCheckpoint(format!(
                    "Gap between checkpoint version {} and next commit {}",
                    checkpoint_version, commit_file.version,
                ))
            )
        }

        // Get the effective version from chosen files
        let effective_version = ascending_commit_files
            .last()
            .or(checkpoint_parts.first())
            .ok_or(Error::generic("No files in log segment"))?
            .version;
        if let Some(end_version) = end_version {
            require!(
                effective_version == end_version,
                Error::generic(format!(
                    "LogSegment end version {effective_version} not the same as the specified end version {end_version}"
                ))
            );
        }

        Ok(LogSegment {
            end_version: effective_version,
            checkpoint_version,
            log_root,
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
            latest_commit_file,
        })
    }

    /// Constructs a [`LogSegment`] to be used for [`Snapshot`]. For a `Snapshot` at version `n`:
    /// Its LogSegment is made of zero or one checkpoint, and all commits between the checkpoint up
    /// to and including the end version `n`. Note that a checkpoint may be made of multiple
    /// parts. All these parts will have the same checkpoint version.
    ///
    /// The options for constructing a LogSegment for Snapshot are as follows:
    /// - `checkpoint_hint`: a `LastCheckpointHint` to start the log segment from (e.g. from reading the `last_checkpoint` file).
    /// - `time_travel_version`: The version of the log that the Snapshot will be at.
    ///
    /// [`Snapshot`]: crate::snapshot::Snapshot
    #[internal_api]
    pub(crate) fn for_snapshot(
        storage: &dyn StorageHandler,
        log_root: Url,
        log_tail: Vec<ParsedLogPath>,
        time_travel_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let time_travel_version = time_travel_version.into();
        let checkpoint_hint = LastCheckpointHint::try_read(storage, &log_root)?;
        Self::for_snapshot_impl(
            storage,
            log_root,
            log_tail,
            checkpoint_hint,
            time_travel_version,
        )
    }

    // factored out for testing
    pub(crate) fn for_snapshot_impl(
        storage: &dyn StorageHandler,
        log_root: Url,
        log_tail: Vec<ParsedLogPath>,
        checkpoint_hint: Option<LastCheckpointHint>,
        time_travel_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let listed_files = match (checkpoint_hint, time_travel_version) {
            (Some(cp), None) => {
                ListedLogFiles::list_with_checkpoint_hint(&cp, storage, &log_root, log_tail, None)?
            }
            (Some(cp), Some(end_version)) if cp.version <= end_version => {
                ListedLogFiles::list_with_checkpoint_hint(
                    &cp,
                    storage,
                    &log_root,
                    log_tail,
                    Some(end_version),
                )?
            }
            _ => ListedLogFiles::list(storage, &log_root, log_tail, None, time_travel_version)?,
        };

        LogSegment::try_new(listed_files, log_root, time_travel_version)
    }

    /// Constructs a [`LogSegment`] to be used for `TableChanges`. For a TableChanges between versions
    /// `start_version` and `end_version`: Its LogSegment is made of zero checkpoints and all commits
    /// between versions `start_version` (inclusive) and `end_version` (inclusive). If no `end_version`
    /// is specified it will be the most recent version by default.
    #[internal_api]
    pub(crate) fn for_table_changes(
        storage: &dyn StorageHandler,
        log_root: Url,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let end_version = end_version.into();
        if let Some(end_version) = end_version {
            if start_version > end_version {
                return Err(Error::generic(
                    "Failed to build LogSegment: start_version cannot be greater than end_version",
                ));
            }
        }

        // TODO: compactions?
        let listed_files =
            ListedLogFiles::list_commits(storage, &log_root, Some(start_version), end_version)?;
        // - Here check that the start version is correct.
        // - [`LogSegment::try_new`] will verify that the `end_version` is correct if present.
        // - [`ListedLogFiles::list_commits`] also checks that there are no gaps between commits.
        // If all three are satisfied, this implies that all the desired commits are present.
        require!(
            listed_files
                .ascending_commit_files
                .first()
                .is_some_and(|first_commit| first_commit.version == start_version),
            Error::generic(format!(
                "Expected the first commit to have version {start_version}, got {:?}",
                listed_files
                    .ascending_commit_files
                    .first()
                    .map(|c| c.version)
            ))
        );
        LogSegment::try_new(listed_files, log_root, end_version)
    }

    #[allow(unused)]
    /// Constructs a [`LogSegment`] to be used for timestamp conversion. This [`LogSegment`] will
    /// consist only of contiguous commit files up to `end_version` (inclusive). If present,
    /// `limit` specifies the maximum length of the returned log segment. The log segment may be
    /// shorter than `limit` if there are missing commits.
    ///
    // This lists all files starting from `end-limit` if `limit` is defined. For large tables,
    // listing with a `limit` can be a significant speedup over listing _all_ the files in the log.
    pub(crate) fn for_timestamp_conversion(
        storage: &dyn StorageHandler,
        log_root: Url,
        end_version: Version,
        limit: Option<NonZero<usize>>,
    ) -> DeltaResult<Self> {
        // Compute the version to start listing from.
        let start_from = limit
            .map(|limit| match NonZero::<Version>::try_from(limit) {
                Ok(limit) => Ok(Version::saturating_sub(end_version, limit.get() - 1)),
                _ => Err(Error::generic(format!(
                    "Invalid limit {limit} when building log segment in timestamp conversion",
                ))),
            })
            .transpose()?;

        // this is a list of commits with possible gaps, we want to take the latest contiguous
        // chunk of commits
        let mut listed_commits =
            ListedLogFiles::list_commits(storage, &log_root, start_from, Some(end_version))?;

        // remove gaps - return latest contiguous chunk of commits
        let commits = &mut listed_commits.ascending_commit_files;
        if !commits.is_empty() {
            let mut start_idx = commits.len() - 1;
            while start_idx > 0 && commits[start_idx].version == 1 + commits[start_idx - 1].version
            {
                start_idx -= 1;
            }
            commits.drain(..start_idx);
        }

        LogSegment::try_new(listed_commits, log_root, Some(end_version))
    }

    /// Read a stream of actions from this log segment. This returns an iterator of
    /// [`ActionsBatch`]s which includes EngineData of actions + a boolean flag indicating whether
    /// the data was read from a commit file (true) or a checkpoint file (false).
    ///
    /// The log files will be read from most recent to oldest.
    ///
    /// `commit_read_schema` is the (physical) schema to read the commit files with, and
    /// `checkpoint_read_schema` is the (physical) schema to read checkpoint files with. This can be
    /// used to project the log files to a subset of the columns. Having two different
    /// schemas can be useful as a cheap way of doing additional filtering on the checkpoint files
    /// (e.g. filtering out remove actions).
    ///
    ///  The engine data returned might have extra non-log actions (e.g. sidecar
    ///  actions) that are not part of the schema but this is an implementation
    ///  detail that should not be relied on and will likely change.
    ///
    /// `meta_predicate` is an optional expression to filter the log files with. It is _NOT_ the
    /// query's predicate, but rather a predicate for filtering log files themselves.
    #[internal_api]
    pub(crate) fn read_actions_with_projected_checkpoint_actions(
        &self,
        engine: &dyn Engine,
        commit_read_schema: SchemaRef,
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        // `replay` expects commit files to be sorted in descending order, so the return value here is correct
        let commits_and_compactions = self.find_commit_cover();
        let commit_stream = engine
            .json_handler()
            .read_json_files(
                &commits_and_compactions,
                commit_read_schema,
                meta_predicate.clone(),
            )?
            .map_ok(|batch| ActionsBatch::new(batch, true));

        let checkpoint_stream =
            self.create_checkpoint_stream(engine, checkpoint_read_schema, meta_predicate)?;

        Ok(commit_stream.chain(checkpoint_stream))
    }

    // Same as above, but uses the same schema for reading checkpoints and commits.
    #[internal_api]
    pub(crate) fn read_actions(
        &self,
        engine: &dyn Engine,
        action_schema: SchemaRef,
        meta_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        self.read_actions_with_projected_checkpoint_actions(
            engine,
            action_schema.clone(),
            action_schema,
            meta_predicate,
        )
    }

    /// find a minimal set to cover the range of commits we want. This is greedy so not always
    /// optimal, but we assume there are rarely overlapping compactions so this is okay. NB: This
    /// returns files is DESCENDING ORDER, as that's what `replay` expects. This function assumes
    /// that all files in `self.ascending_commit_files` and `self.ascending_compaction_files` are in
    /// range for this log segment. This invariant is maintained by our listing code.
    fn find_commit_cover(&self) -> Vec<FileMeta> {
        // Create an iterator sorted in ascending order by (initial version, end version), e.g.
        // [00.json, 00.09.compacted.json, 00.99.compacted.json, 01.json, 02.json, ..., 10.json,
        //  10.19.compacted.json, 11.json, ...]
        let all_files = itertools::Itertools::merge_by(
            self.ascending_commit_files.iter(),
            self.ascending_compaction_files.iter(),
            |path_a, path_b| path_a.version <= path_b.version,
        );

        let mut last_pushed: Option<&ParsedLogPath> = None;

        let mut selected_files = vec![];
        for next in all_files {
            match last_pushed {
                // Resolve version number ties in favor of the later file (it covers a wider range)
                Some(prev) if prev.version == next.version => {
                    let removed = selected_files.pop();
                    debug!("Selecting {next:?} rather than {removed:?}, it covers a wider range");
                }
                // Skip later files whose start overlaps with the previous end
                Some(&ParsedLogPath {
                    file_type: LogPathFileType::CompactedCommit { hi },
                    ..
                }) if next.version <= hi => {
                    debug!("Skipping log file {next:?}, it's already covered.");
                    continue;
                }
                _ => {} // just fall through
            }
            debug!("Provisionally selecting {next:?}");
            last_pushed = Some(next);
            selected_files.push(next.location.clone());
        }
        selected_files.reverse();
        selected_files
    }

    /// Returns an iterator over checkpoint data, processing sidecar files when necessary.
    ///
    /// By default, `create_checkpoint_stream` checks for the presence of sidecar files, and
    /// reads their contents if present. Checking for sidecar files is skipped if:
    /// - The checkpoint is a multi-part checkpoint
    /// - The checkpoint read schema does not contain a file action
    ///
    /// For single-part checkpoints, any referenced sidecar files are processed. These
    /// sidecar files contain the actual file actions that would otherwise be
    /// stored directly in the checkpoint. The sidecar file batches are chained to the
    /// checkpoint batch in the top level iterator to be returned.
    fn create_checkpoint_stream(
        &self,
        engine: &dyn Engine,
        action_schema: SchemaRef,
        meta_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let need_file_actions = schema_contains_file_actions(&action_schema);

        // Sidecars only contain file actions so don't add it to the schema if not needed
        let checkpoint_read_schema = if !need_file_actions ||
        // Don't duplicate the column if it exists
        action_schema.contains(SIDECAR_NAME) ||
        // With multiple parts the checkpoint can't be v2, so sidecars aren't needed
        self.checkpoint_parts.len() > 1
        {
            action_schema.clone()
        } else {
            Arc::new(
                action_schema.add([StructField::nullable(SIDECAR_NAME, Sidecar::to_schema())])?,
            )
        };

        let checkpoint_file_meta: Vec<_> = self
            .checkpoint_parts
            .iter()
            .map(|f| f.location.clone())
            .collect();

        let parquet_handler = engine.parquet_handler();

        // Historically, we had a shared file reader trait for JSON and Parquet handlers,
        // but it was removed to avoid unnecessary coupling. This is a concrete case
        // where it *could* have been useful, but for now, we're keeping them separate.
        // If similar patterns start appearing elsewhere, we should reconsider that decision.
        let actions = match self.checkpoint_parts.first() {
            Some(parsed_log_path) if parsed_log_path.extension == "json" => {
                engine.json_handler().read_json_files(
                    &checkpoint_file_meta,
                    checkpoint_read_schema.clone(),
                    meta_predicate.clone(),
                )?
            }
            Some(parsed_log_path) if parsed_log_path.extension == "parquet" => parquet_handler
                .read_parquet_files(
                    &checkpoint_file_meta,
                    checkpoint_read_schema.clone(),
                    meta_predicate.clone(),
                )?,
            Some(parsed_log_path) => {
                return Err(Error::generic(format!(
                    "Unsupported checkpoint file type: {}",
                    parsed_log_path.extension,
                )));
            }
            // This is the case when there are no checkpoints in the log segment
            // so we return an empty iterator
            None => Box::new(std::iter::empty()),
        };

        let log_root = self.log_root.clone();

        let actions_iter = actions
            .map(move |checkpoint_batch_result| -> DeltaResult<_> {
                let checkpoint_batch = checkpoint_batch_result?;
                // This closure maps the checkpoint batch to an iterator of batches
                // by chaining the checkpoint batch with sidecar batches if they exist.

                // 1. In the case where the schema does not contain file actions, we return the
                //    checkpoint batch directly as sidecar files only have to be read when the
                //    schema contains add/remove action.
                // 2. Multi-part checkpoint batches never have sidecar actions, so the batch is
                //    returned as-is.
                let sidecar_content = if need_file_actions && checkpoint_file_meta.len() == 1 {
                    Self::process_sidecars(
                        parquet_handler.clone(), // cheap Arc clone
                        log_root.clone(),
                        checkpoint_batch.as_ref(),
                        action_schema.clone(),
                        meta_predicate.clone(),
                    )?
                } else {
                    None
                };

                let combined_batches = std::iter::once(Ok(checkpoint_batch))
                    .chain(sidecar_content.into_iter().flatten())
                    // The boolean flag indicates whether the batch originated from a commit file
                    // (true) or a checkpoint file (false).
                    .map_ok(|sidecar_batch| ActionsBatch::new(sidecar_batch, false));

                Ok(combined_batches)
            })
            .flatten_ok()
            .map(|result| result?); // result-result to result

        Ok(actions_iter)
    }

    /// Processes sidecar files for the given checkpoint batch.
    ///
    /// This function extracts any sidecar file references from the provided batch.
    /// Each sidecar file is read and an iterator of file action batches is returned
    fn process_sidecars(
        parquet_handler: Arc<dyn ParquetHandler>,
        log_root: Url,
        batch: &dyn EngineData,
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<PredicateRef>,
    ) -> DeltaResult<Option<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>> {
        // Visit the rows of the checkpoint batch to extract sidecar file references
        let mut visitor = SidecarVisitor::default();
        visitor.visit_rows_of(batch)?;

        // If there are no sidecar files, return early
        if visitor.sidecars.is_empty() {
            return Ok(None);
        }

        let sidecar_files: Vec<_> = visitor
            .sidecars
            .iter()
            .map(|sidecar| sidecar.to_filemeta(&log_root))
            .try_collect()?;

        // Read the sidecar files and return an iterator of sidecar file batches
        Ok(Some(parquet_handler.read_parquet_files(
            &sidecar_files,
            checkpoint_read_schema,
            meta_predicate,
        )?))
    }

    // Do a lightweight protocol+metadata log replay to find the latest Protocol and Metadata in
    // the LogSegment
    pub(crate) fn protocol_and_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        let actions_batches = self.replay_for_metadata(engine)?;
        let (mut metadata_opt, mut protocol_opt) = (None, None);
        for actions_batch in actions_batches {
            let actions = actions_batch?.actions;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                // we've found both, we can stop
                break;
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    // Get the most up-to-date Protocol and Metadata actions
    pub(crate) fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<(Metadata, Protocol)> {
        match self.protocol_and_metadata(engine)? {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn replay_for_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
            Some(Arc::new(Predicate::or(
                Expression::column([METADATA_NAME, "id"]).is_not_null(),
                Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.read_actions(engine, schema, META_PREDICATE.clone())
    }

    /// How many commits since a checkpoint, according to this log segment
    pub(crate) fn commits_since_checkpoint(&self) -> u64 {
        // we can use 0 as the checkpoint version if there is no checkpoint since `end_version - 0`
        // is the correct number of commits since a checkpoint if there are no checkpoints
        let checkpoint_version = self.checkpoint_version.unwrap_or(0);
        debug_assert!(checkpoint_version <= self.end_version);
        self.end_version - checkpoint_version
    }

    /// How many commits since a log-compaction or checkpoint, according to this log segment
    pub(crate) fn commits_since_log_compaction_or_checkpoint(&self) -> u64 {
        // Annoyingly we have to search all the compaction files to determine this, because we only
        // sort by start version, so technically the max end version could be anywhere in the vec.
        // We can return 0 in the case there is no compaction since end_version - 0 is the correct
        // number of commits since compaction if there are no compactions
        let max_compaction_end = self.ascending_compaction_files.iter().fold(0, |cur, f| {
            if let &ParsedLogPath {
                file_type: LogPathFileType::CompactedCommit { hi },
                ..
            } = f
            {
                Version::max(cur, hi)
            } else {
                warn!("Found invalid ParsedLogPath in ascending_compaction_files: {f:?}");
                cur
            }
        });
        // we want to subtract off the max of the max compaction end or the checkpoint version
        let to_sub = Version::max(self.checkpoint_version.unwrap_or(0), max_compaction_end);
        debug_assert!(to_sub <= self.end_version);
        self.end_version - to_sub
    }

    /// Validates that all commit files in this log segment are not staged commits. We use this in
    /// places like checkpoint writers, where we require all commits to be published.
    pub(crate) fn validate_no_staged_commits(&self) -> DeltaResult<()> {
        require!(
            !self
                .ascending_commit_files
                .iter()
                .any(|commit| matches!(commit.file_type, LogPathFileType::StagedCommit)),
            Error::generic("Found staged commit file in log segment")
        );
        Ok(())
    }
}
