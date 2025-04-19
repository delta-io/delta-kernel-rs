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
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::snapshot::LastCheckpointHint;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, ParquetHandler, RowVisitor,
    StorageHandler, Version,
};
use delta_kernel_derive::internal_api;

use itertools::Itertools;
use tracing::warn;
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
    /// Checkpoint files in the log segment.
    pub checkpoint_parts: Vec<ParsedLogPath>,
}

impl LogSegment {
    pub(crate) fn try_new(
        mut ascending_commit_files: Vec<ParsedLogPath>,
        checkpoint_parts: Vec<ParsedLogPath>,
        log_root: Url,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        // Commit file versions must be greater than the most recent checkpoint version if it exists
        let checkpoint_version = checkpoint_parts.first().map(|checkpoint_file| {
            ascending_commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
            checkpoint_file.version
        });

        // We require that commits that are contiguous. In other words, there must be no gap between commit versions.
        require!(
            ascending_commit_files
                .windows(2)
                .all(|cfs| cfs[0].version + 1 == cfs[1].version),
            Error::generic(format!(
                "Expected ordered contiguous commit files {:?}",
                ascending_commit_files
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
                    "LogSegment end version {} not the same as the specified end version {}",
                    effective_version, end_version
                ))
            );
        }

        Ok(LogSegment {
            end_version: effective_version,
            checkpoint_version,
            log_root,
            ascending_commit_files,
            checkpoint_parts,
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

        let (ascending_commit_files, checkpoint_parts) =
            match (checkpoint_hint.into(), time_travel_version) {
                (Some(cp), None) => list_log_files_with_checkpoint(&cp, storage, &log_root, None)?,
                (Some(cp), Some(end_version)) if cp.version <= end_version => {
                    list_log_files_with_checkpoint(&cp, storage, &log_root, Some(end_version))?
                }
                _ => list_log_files_with_version(storage, &log_root, None, time_travel_version)?,
            };

        LogSegment::try_new(
            ascending_commit_files,
            checkpoint_parts,
            log_root,
            time_travel_version,
        )
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
                "Expected the first commit to have version {}",
                start_version
            ))
        );
        LogSegment::try_new(ascending_commit_files, vec![], log_root, end_version)
    }

    /// Read a stream of actions from this log segment. This returns an iterator of (EngineData,
    /// bool) pairs, where the boolean flag indicates whether the data was read from a commit file
    /// (true) or a checkpoint file (false).
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
        meta_predicate: Option<ExpressionRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        // `replay` expects commit files to be sorted in descending order, so we reverse the sorted
        // commit files
        let commit_files: Vec<_> = self
            .ascending_commit_files
            .iter()
            .rev()
            .map(|f| f.location.clone())
            .collect();
        let commit_stream = engine
            .json_handler()
            .read_json_files(&commit_files, commit_read_schema, meta_predicate.clone())?
            .map_ok(|batch| (batch, true));

        let checkpoint_stream =
            self.create_checkpoint_stream(engine, checkpoint_read_schema, meta_predicate)?;

        Ok(commit_stream.chain(checkpoint_stream))
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
        meta_predicate: Option<ExpressionRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
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
                    .map_ok(|sidecar_batch| (sidecar_batch, false));

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
        meta_predicate: Option<ExpressionRef>,
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
        let data_batches = self.replay_for_metadata(engine)?;
        let (mut metadata_opt, mut protocol_opt) = (None, None);
        for batch in data_batches {
            let (batch, _) = batch?;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(batch.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(batch.as_ref())?;
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
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let schema = get_log_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        // filter out log files that do not contain metadata or protocol information
        static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
            Some(Arc::new(Expression::or(
                Expression::column([METADATA_NAME, "id"]).is_not_null(),
                Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.read_actions(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

/// Returns a fallible iterator of [`ParsedLogPath`] that are between the provided `start_version` (inclusive)
/// and `end_version` (inclusive). [`ParsedLogPath`] may be a commit or a checkpoint.  If `start_version` is
/// not specified, the files will begin from version number 0. If `end_version` is not specified, files up to
/// the most recent version will be included.
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
    let version_prefix = format!("{:020}", start_version);
    let start_from = log_root.join(&version_prefix)?;

    Ok(storage
        .list_from(&start_from)?
        .map(|meta| ParsedLogPath::try_from(meta?))
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        .filter_map_ok(identity)
        .take_while(move |path_res| match path_res {
            Ok(path) => !end_version.is_some_and(|end_version| end_version < path.version),
            Err(_) => true,
        }))
}

/// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
/// If successful, this returns a tuple `(ascending_commit_files, checkpoint_parts)` of type
/// `(Vec<ParsedLogPath>, Vec<ParsedLogPath>)`. The commit files are guaranteed to be sorted in
/// ascending order by version. The elements of `checkpoint_parts` are all the parts of the same
/// checkpoint. Checkpoint parts share the same version.
// TODO: encode some of these guarantees in the output types. e.g. we could have:
// - SortedCommitFiles: Vec<ParsedLogPath>, is_ascending: bool, end_version: Version
// - CheckpointParts: Vec<ParsedLogPath>, checkpoint_version: Version (guarantee all same version)
pub(crate) fn list_log_files_with_version(
    storage: &dyn StorageHandler,
    log_root: &Url,
    start_version: Option<Version>,
    end_version: Option<Version>,
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    // We expect 10 commit files per checkpoint, so start with that size. We could adjust this based
    // on config at some point

    let log_files = list_log_files(storage, log_root, start_version, end_version)?;

    log_files.process_results(|iter| {
        let mut commit_files = Vec::with_capacity(10);
        let mut checkpoint_parts = vec![];

        // Group log files by version
        let log_files_per_version = iter.chunk_by(|x| x.version);

        for (version, files) in &log_files_per_version {
            let mut new_checkpoint_parts = vec![];
            for file in files {
                if file.is_commit() {
                    commit_files.push(file);
                } else if file.is_checkpoint() {
                    new_checkpoint_parts.push(file);
                } else {
                    warn!(
                        "Found a file with unknown file type {:?} at version {}",
                        file.file_type, version
                    );
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
                commit_files.clear(); // Log replay only uses commits after a complete checkpoint
            }
        }
        (commit_files, checkpoint_parts)
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
            Commit | CompactedCommit { .. } | Unknown => {}
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
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    let (commit_files, checkpoint_parts) = list_log_files_with_version(
        storage,
        log_root,
        Some(checkpoint_metadata.version),
        end_version,
    )?;

    let Some(latest_checkpoint) = checkpoint_parts.last() else {
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
    } else if checkpoint_parts.len() != checkpoint_metadata.parts.unwrap_or(1) {
        return Err(Error::InvalidCheckpoint(format!(
            "_last_checkpoint indicated that checkpoint should have {} parts, but it has {}",
            checkpoint_metadata.parts.unwrap_or(1),
            checkpoint_parts.len()
        )));
    }
    Ok((commit_files, checkpoint_parts))
}
