//! Represents a segment of a delta log. [`LogSegment`] wraps a set of  checkpoint and commit
//! files.
use std::collections::HashMap;
use std::convert::identity;
use std::sync::{Arc, LazyLock};

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    get_log_schema, Metadata, Protocol, ADD_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
    SIDECAR_NAME,
};
use crate::log_replay::ActionsBatch;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::snapshot::LastCheckpointHint;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, FileMeta, ParquetHandler, Predicate,
    PredicateRef, RowVisitor, StorageHandler, Version,
};
use delta_kernel_derive::internal_api;

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
/// and in `TableChanges` when built with [`LogSegment::for_table_changes`].
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
        } = listed_files;

        // Commit file versions must be greater than the most recent checkpoint version if it exists
        let checkpoint_version = checkpoint_parts.first().map(|checkpoint_file| {
            ascending_commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
            checkpoint_file.version
        });

        // TODO: consider unifying this with debug_asserts in the ListedLogFiles::new(); issue#995
        // We require that commits that are contiguous. In other words, there must be no gap between commit versions.
        require!(
            ascending_commit_files
                .windows(2)
                .all(|cfs| cfs[0].version + 1 == cfs[1].version),
            Error::generic(format!(
                "Expected ordered contiguous commit files {ascending_commit_files:?}"
            ))
        );

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
        checkpoint_hint: impl Into<Option<LastCheckpointHint>>,
        time_travel_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let time_travel_version = time_travel_version.into();

        let listed_files = match (checkpoint_hint.into(), time_travel_version) {
            (Some(cp), None) => list_log_files_with_checkpoint(&cp, storage, &log_root, None)?,
            (Some(cp), Some(end_version)) if cp.version <= end_version => {
                list_log_files_with_checkpoint(&cp, storage, &log_root, Some(end_version))?
            }
            _ => list_log_files_with_version(storage, &log_root, None, time_travel_version)?,
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

        // todo: compactions?
        let ascending_commit_files: Vec<_> =
            list_log_files(storage, &log_root, start_version, end_version)?
                .filter_ok(|x| x.is_commit())
                .try_collect()?;

        // - Here check that the start version is correct.
        // - [`LogSegment::try_new`] will verify that the `end_version` is correct if present.
        // - [`LogSegment::try_new`] also checks that there are no gaps between commits.
        // If all three are satisfied, this implies that all the desired commits are present.
        require!(
            ascending_commit_files
                .first()
                .is_some_and(|first_commit| first_commit.version == start_version),
            Error::generic(format!(
                "Expected the first commit to have version {start_version}"
            ))
        );
        let listed_files = ListedLogFiles::new(
            ascending_commit_files,
            vec![],
            vec![],
            None, // TODO: use CRC files for table changes?
        );
        LogSegment::try_new(listed_files, log_root, end_version)
    }

    /// Read a stream of actions from this log segment. This returns an iterator of
    /// [`ActionsBatch`]s which includes EngineData of actions + a boolean flag indicating whether
    /// the data was read from a commit file (true) or a checkpoint file (false).
    ///
    /// The log files will be read from most recent to oldest.
    ///
    /// `commit_read_schema` is the (physical) schema to read the commit files with, and
    /// `checkpoint_read_schema` is the (physical) schema to read checkpoint files with. This can be
    /// used to project the log files to a subset of the columns.
    ///
    /// `meta_predicate` is an optional expression to filter the log files with. It is _NOT_ the
    /// query's predicate, but rather a predicate for filtering log files themselves.
    #[internal_api]
    pub(crate) fn read_actions(
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
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let need_file_actions = checkpoint_read_schema.contains(ADD_NAME)
            || checkpoint_read_schema.contains(REMOVE_NAME);
        require!(
            !need_file_actions || checkpoint_read_schema.contains(SIDECAR_NAME),
            Error::invalid_checkpoint(
                "If the checkpoint read schema contains file actions, it must contain the sidecar column"
            )
        );

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
                        checkpoint_read_schema.clone(),
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
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
            Some(Arc::new(Predicate::or(
                Expression::column([METADATA_NAME, "id"]).is_not_null(),
                Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.read_actions(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

/// Returns a fallible iterator of [`ParsedLogPath`] that are between the provided `start_version`
/// (inclusive) and `end_version` (inclusive). [`ParsedLogPath`] may be a commit or a checkpoint.
/// If `start_version` is not specified, the files will begin from version number 0. If
/// `end_version` is not specified, files up to the most recent version will be included.
///
/// Note: this calls [`StorageHandler::list_from`] to get the list of log files.
fn list_log_files(
    storage: &dyn StorageHandler,
    log_root: &Url,
    start_version: impl Into<Option<Version>>,
    end_version: impl Into<Option<Version>>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ParsedLogPath>>> {
    let start_version = start_version.into().unwrap_or(0);
    let end_version = end_version.into();
    let version_prefix = format!("{start_version:020}");
    let start_from = log_root.join(&version_prefix)?;

    Ok(storage
        .list_from(&start_from)?
        .map(|meta| ParsedLogPath::try_from(meta?))
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        .filter_map_ok(identity)
        .take_while(move |path_res| match path_res {
            Ok(path) => end_version.is_none_or(|end_version| end_version >= path.version),
            Err(_) => true,
        }))
}

/// A struct to hold the result of listing log files. The commit and compaction files are guaranteed
/// to be sorted in ascending order by version. The elements of `checkpoint_parts` are all the parts
/// of the same checkpoint. Checkpoint parts share the same version. The `latest_crc_file` includes
/// the latest (highest version) CRC file, if any, which may not correspond to the latest commit.
#[derive(Debug)]
#[internal_api]
pub(crate) struct ListedLogFiles {
    pub(crate) ascending_commit_files: Vec<ParsedLogPath>,
    pub(crate) ascending_compaction_files: Vec<ParsedLogPath>,
    pub(crate) checkpoint_parts: Vec<ParsedLogPath>,
    pub(crate) latest_crc_file: Option<ParsedLogPath>,
}

impl ListedLogFiles {
    #[internal_api]
    pub(crate) fn new(
        ascending_commit_files: Vec<ParsedLogPath>,
        ascending_compaction_files: Vec<ParsedLogPath>,
        checkpoint_parts: Vec<ParsedLogPath>,
        latest_crc_file: Option<ParsedLogPath>,
    ) -> Self {
        // We are adding debug_assertions here since we want to validate invariants that are (relatively) expensive to compute
        #[cfg(debug_assertions)]
        {
            assert!(ascending_compaction_files
                .windows(2)
                .all(|pair| match pair {
                    [ParsedLogPath {
                        version: version0,
                        file_type: LogPathFileType::CompactedCommit { hi: hi0 },
                        ..
                    }, ParsedLogPath {
                        version: version1,
                        file_type: LogPathFileType::CompactedCommit { hi: hi1 },
                        ..
                    }] => version0 < version1 || (version0 == version1 && hi0 <= hi1),
                    _ => false,
                }));

            assert!(checkpoint_parts.iter().all(|part| part.is_checkpoint()));

            // for a multi-part checkpoint, check that they are all same version and all the parts are there
            if checkpoint_parts.len() > 1 {
                assert!(checkpoint_parts
                    .windows(2)
                    .all(|pair| pair[0].version == pair[1].version));

                assert!(checkpoint_parts.iter().all(|part| matches!(
                    part.file_type,
                    LogPathFileType::MultiPartCheckpoint { num_parts, .. }
                    if checkpoint_parts.len() == num_parts as usize
                )));
            }
        }

        ListedLogFiles {
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
        }
    }
}

/// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
/// If successful, this returns a `ListedLogFiles`.
// TODO: encode some of these guarantees in the output types. e.g. we could have:
// - SortedCommitFiles: Vec<ParsedLogPath>, is_ascending: bool, end_version: Version
// - CheckpointParts: Vec<ParsedLogPath>, checkpoint_version: Version (guarantee all same version)
pub(crate) fn list_log_files_with_version(
    storage: &dyn StorageHandler,
    log_root: &Url,
    start_version: Option<Version>,
    end_version: Option<Version>,
) -> DeltaResult<ListedLogFiles> {
    // We expect 10 commit files per checkpoint, so start with that size. We could adjust this based
    // on config at some point

    let log_files = list_log_files(storage, log_root, start_version, end_version)?;

    log_files.process_results(|iter| {
        let mut ascending_commit_files = Vec::with_capacity(10);
        let mut ascending_compaction_files = Vec::with_capacity(2);
        let mut checkpoint_parts = vec![];
        let mut latest_crc_file: Option<ParsedLogPath> = None;

        // Group log files by version
        let log_files_per_version = iter.chunk_by(|x| x.version);

        for (version, files) in &log_files_per_version {
            let mut new_checkpoint_parts = vec![];
            for file in files {
                use LogPathFileType::*;
                match file.file_type {
                    Commit => ascending_commit_files.push(file),
                    CompactedCommit { hi } if end_version.is_none_or(|end| hi <= end) => {
                        ascending_compaction_files.push(file);
                    }
                    CompactedCommit { .. } => (), // Failed the bounds check above
                    SinglePartCheckpoint | UuidCheckpoint(_) | MultiPartCheckpoint { .. } => {
                        new_checkpoint_parts.push(file)
                    }
                    Crc => {
                        let latest_crc_ref = latest_crc_file.as_ref();
                        if latest_crc_ref.is_none_or(|latest| latest.version < file.version) {
                            latest_crc_file = Some(file);
                        }
                    }
                    Unknown => {
                        warn!(
                            "Found file {} with unknown file type {:?} at version {}",
                            file.filename, file.file_type, version
                        );
                    }
                }
            }
            // Group and find the first complete checkpoint for this version.
            // All checkpoints for the same version are equivalent, so we only take one.
            if let Some((_, complete_checkpoint)) = group_checkpoint_parts(new_checkpoint_parts)
                .into_iter()
                // `num_parts` is guaranteed to be non-negative and within `usize` range
                .find(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
            {
                checkpoint_parts = complete_checkpoint;
                // Log replay only uses commits/compactions after a complete checkpoint
                ascending_commit_files.clear();
                ascending_compaction_files.clear();
            }
        }

        ListedLogFiles::new(
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
        )
    })
}

/// Groups all checkpoint parts according to the checkpoint they belong to.
///
/// NOTE: There could be a single-part and/or any number of uuid-based checkpoints. They
/// are all equivalent, and this routine keeps only one of them (arbitrarily chosen).
fn group_checkpoint_parts(parts: Vec<ParsedLogPath>) -> HashMap<u32, Vec<ParsedLogPath>> {
    let mut checkpoints: HashMap<u32, Vec<ParsedLogPath>> = HashMap::new();
    for part_file in parts {
        use LogPathFileType::*;
        match &part_file.file_type {
            SinglePartCheckpoint
            | UuidCheckpoint(_)
            | MultiPartCheckpoint {
                part_num: 1,
                num_parts: 1,
            } => {
                // All single-file checkpoints are equivalent, just keep one
                checkpoints.insert(1, vec![part_file]);
            }
            MultiPartCheckpoint {
                part_num: 1,
                num_parts,
            } => {
                // Start a new multi-part checkpoint with at least 2 parts
                checkpoints.insert(*num_parts, vec![part_file]);
            }
            MultiPartCheckpoint {
                part_num,
                num_parts,
            } => {
                // Continue a new multi-part checkpoint with at least 2 parts.
                // Checkpoint parts are required to be in-order from log listing to build
                // a multi-part checkpoint
                if let Some(part_files) = checkpoints.get_mut(num_parts) {
                    // `part_num` is guaranteed to be non-negative and within `usize` range
                    if *part_num as usize == 1 + part_files.len() {
                        // Safe to append because all previous parts exist
                        part_files.push(part_file);
                    }
                }
            }
            Commit | CompactedCommit { .. } | Crc | Unknown => {}
        }
    }
    checkpoints
}

/// List all commit and checkpoint files after the provided checkpoint. It is guaranteed that all
/// the returned [`ParsedLogPath`]s will have a version less than or equal to the `end_version`.
/// See [`list_log_files_with_version`] for details on the return type.
fn list_log_files_with_checkpoint(
    checkpoint_metadata: &LastCheckpointHint,
    storage: &dyn StorageHandler,
    log_root: &Url,
    end_version: Option<Version>,
) -> DeltaResult<ListedLogFiles> {
    let listed_files = list_log_files_with_version(
        storage,
        log_root,
        Some(checkpoint_metadata.version),
        end_version,
    )?;

    let Some(latest_checkpoint) = listed_files.checkpoint_parts.last() else {
        // TODO: We could potentially recover here
        return Err(Error::invalid_checkpoint(
            "Had a _last_checkpoint hint but didn't find any checkpoints",
        ));
    };
    if latest_checkpoint.version != checkpoint_metadata.version {
        warn!(
            "_last_checkpoint hint is out of date. _last_checkpoint version: {}. Using actual most recent: {}",
            checkpoint_metadata.version,
            latest_checkpoint.version
        );
    } else if listed_files.checkpoint_parts.len() != checkpoint_metadata.parts.unwrap_or(1) {
        return Err(Error::InvalidCheckpoint(format!(
            "_last_checkpoint indicated that checkpoint should have {} parts, but it has {}",
            checkpoint_metadata.parts.unwrap_or(1),
            listed_files.checkpoint_parts.len()
        )));
    }
    Ok(listed_files)
}
