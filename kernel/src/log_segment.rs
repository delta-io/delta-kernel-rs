//! Represents a segment of a delta log. [`LogSegment`] wraps a set of  checkpoint and commit
//! files.

use crate::actions::visitors::SidecarVisitor;
use crate::actions::{
    get_log_add_schema, get_log_schema, Metadata, Protocol, Sidecar, ADD_NAME, METADATA_NAME,
    PROTOCOL_NAME, SIDECAR_NAME,
};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::snapshot::CheckpointMetadata;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileDataReadResultIterator,
    FileMeta, FileSystemClient, ParquetHandler, RowVisitor, Version,
};
use itertools::Either::{Left, Right};
use itertools::Itertools;
use std::collections::HashMap;
use std::convert::identity;
use std::sync::{Arc, LazyLock};
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
#[derive(Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct LogSegment {
    pub end_version: Version,
    pub log_root: Url,
    /// Sorted commit files in the log segment (ascending)
    pub ascending_commit_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment.
    pub checkpoint_parts: Vec<ParsedLogPath>,
}

impl LogSegment {
    fn try_new(
        ascending_commit_files: Vec<ParsedLogPath>,
        checkpoint_parts: Vec<ParsedLogPath>,
        log_root: Url,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
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
        if let (Some(checkpoint_file), Some(commit_file)) =
            (checkpoint_parts.first(), ascending_commit_files.first())
        {
            require!(
                checkpoint_file.version + 1 == commit_file.version,
                Error::InvalidCheckpoint(format!(
                    "Gap between checkpoint version {} and next commit {}",
                    checkpoint_file.version, commit_file.version,
                ))
            )
        }

        // Get the effective version from chosen files
        let version_eff = ascending_commit_files
            .last()
            .or(checkpoint_parts.first())
            .ok_or(Error::generic("No files in log segment"))?
            .version;
        if let Some(end_version) = end_version {
            require!(
                version_eff == end_version,
                Error::generic(format!(
                    "LogSegment end version {} not the same as the specified end version {}",
                    version_eff, end_version
                ))
            );
        }
        Ok(LogSegment {
            end_version: version_eff,
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
    /// - `checkpoint_hint`: a `CheckpointMetadata` to start the log segment from (e.g. from reading the `last_checkpoint` file).
    /// - `time_travel_version`: The version of the log that the Snapshot will be at.
    ///
    /// [`Snapshot`]: crate::snapshot::Snapshot
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn for_snapshot(
        fs_client: &dyn FileSystemClient,
        log_root: Url,
        checkpoint_hint: impl Into<Option<CheckpointMetadata>>,
        time_travel_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let time_travel_version = time_travel_version.into();

        let (mut ascending_commit_files, checkpoint_parts) =
            match (checkpoint_hint.into(), time_travel_version) {
                (Some(cp), None) => {
                    list_log_files_with_checkpoint(&cp, fs_client, &log_root, None)?
                }
                (Some(cp), Some(end_version)) if cp.version <= end_version => {
                    list_log_files_with_checkpoint(&cp, fs_client, &log_root, Some(end_version))?
                }
                _ => list_log_files_with_version(fs_client, &log_root, None, time_travel_version)?,
            };

        // Commit file versions must be greater than the most recent checkpoint version if it exists
        if let Some(checkpoint_file) = checkpoint_parts.first() {
            ascending_commit_files.retain(|log_path| checkpoint_file.version < log_path.version);
        }

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
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn for_table_changes(
        fs_client: &dyn FileSystemClient,
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
            list_log_files(fs_client, &log_root, start_version, end_version)?
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
    /// Read a stream of log data from this log segment.
    ///
    /// The log files will be read from most recent to oldest.
    /// The boolean flags indicates whether the data was read from
    /// a commit file (true) or a checkpoint file (false).
    ///
    /// `read_schema` is the schema to read the log files with. This can be used
    /// to project the log files to a subset of the columns.
    ///
    /// `meta_predicate` is an optional expression to filter the log files with. It is _NOT_ the
    /// query's predicate, but rather a predicate for filtering log files themselves.
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn replay(
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
            .get_json_handler()
            .read_json_files(&commit_files, commit_read_schema, meta_predicate.clone())?
            .map_ok(|batch| (batch, true));

        let checkpoint_parts: Vec<_> = self
            .checkpoint_parts
            .iter()
            .map(|f| f.location.clone())
            .collect();
        let checkpoint_stream = (!checkpoint_parts.is_empty())
            .then(|| {
                Self::create_checkpoint_stream(
                    engine,
                    checkpoint_read_schema,
                    meta_predicate,
                    checkpoint_parts,
                    self.log_root.clone(),
                )
            })
            .transpose()?
            .into_iter()
            .flatten();

        Ok(commit_stream.chain(checkpoint_stream))
    }

    /// Returns an iterator over checkpoint data, processing sidecar files when necessary.
    ///
    /// Checkpoint data is returned directly if:
    /// - Processing a multi-part checkpoint
    /// - Schema does not contain file actions
    ///
    /// For single-part checkpoints, any referenced sidecar files are processed. These
    /// sidecar files contain the actual add/remove actions that would otherwise be
    /// stored directly in the checkpoint. The sidecar file batches replace the checkpoint
    /// batch in the top level iterator to be returned.
    fn create_checkpoint_stream(
        engine: &dyn Engine,
        checkpoint_read_schema: SchemaRef,
        meta_predicate: Option<ExpressionRef>,
        checkpoint_parts: Vec<FileMeta>,
        log_root: Url,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        // We checked that checkpoint_parts is not empty before calling this function
        require!(
            !checkpoint_parts.is_empty(),
            Error::generic("Checkpoint parts should not be empty")
        );

        let need_file_actions = checkpoint_read_schema.contains(ADD_NAME);
        require!(
            !need_file_actions || checkpoint_read_schema.contains(SIDECAR_NAME),
            Error::generic(
                "If the checkpoint read schema contains file actions, it must contain the sidecar column"
            )
        );

        let is_json_checkpoint = checkpoint_parts[0].location.path().ends_with(".json");
        let actions = Self::read_checkpoint_files(
            engine,
            checkpoint_parts.clone(),
            checkpoint_read_schema.clone(),
            meta_predicate,
            is_json_checkpoint,
        )?;

        // 1. In the case where the schema does not contain add/remove actions, we return the checkpoint
        // batch directly as sidecar files only have to be read when the schema contains add/remove actions.
        // 2. Multi-part checkpoint batches never have sidecar actions, so the batch is returned as-is.
        if !need_file_actions || checkpoint_parts.len() > 1 {
            return Ok(Left(actions.map_ok(|batch| (batch, false))));
        }

        let parquet_handler = engine.get_parquet_handler().clone();

        // Process checkpoint batches with potential sidecar file references that need to be read
        // to extract add/remove actions from the sidecar files.
        let actions_iter = actions
            // Flatten the new batches returned. The new batches could be:
            // - the checkpoint batch itself if no sidecar actions are present in the batch
            // - one or more sidecar batches read from the sidecar files if sidecar actions are present
            // After flattenning, perform a map operation to return the actions and a boolean flag indicating
            // that the batch was not read from a log file.
            .map(move |batch_result| {
                let checkpoint_batch = batch_result?;

                Self::process_single_checkpoint_batch(
                    parquet_handler.clone(),
                    log_root.clone(),
                    checkpoint_batch,
                )
            })
            .flatten_ok()
            .map(|result| result.and_then(|inner| inner.map(|actions| (actions, false))));

        return Ok(Right(actions_iter));
    }

    fn process_single_checkpoint_batch(
        parquet_handler: Arc<dyn ParquetHandler>,
        log_root: Url,
        batch: Box<dyn EngineData>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
        let mut visitor = SidecarVisitor::default();

        // Collect sidecars
        visitor.visit_rows_of(batch.as_ref())?;

        // If there are no sidecars, return the batch as is
        if visitor.sidecars.is_empty() {
            return Ok(Left(std::iter::once(Ok(batch))));
        }

        // Convert sidecar actions to sidecar file paths
        let sidecar_files: Vec<_> = visitor
            .sidecars
            .iter()
            .map(|sidecar| Self::sidecar_to_filemeta(sidecar, &log_root))
            .try_collect()?;

        let sidecar_read_schema = get_log_add_schema().clone();

        // If sidecars files exist, read the sidecar files and return the iterator of sidecar batches
        // to replace the checkpoint batch in the top level iterator
        Ok(Right(parquet_handler.read_parquet_files(
            &sidecar_files,
            sidecar_read_schema,
            None,
        )?))
    }

    // Helper function to read checkpoint files based on the file type
    fn read_checkpoint_files(
        engine: &dyn Engine,
        checkpoint_parts: Vec<FileMeta>,
        schema: SchemaRef,
        predicate: Option<ExpressionRef>,
        is_json: bool,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if is_json {
            engine
                .get_json_handler()
                .read_json_files(&checkpoint_parts, schema, predicate)
        } else {
            engine
                .get_parquet_handler()
                .read_parquet_files(&checkpoint_parts, schema, predicate)
        }
    }

    // Helper function to convert a single sidecar action to a FileMeta
    fn sidecar_to_filemeta(sidecar: &Sidecar, log_root: &Url) -> Result<FileMeta, Error> {
        let location = log_root.join("_sidecars/")?.join(&sidecar.path)?;
        Ok(FileMeta {
            location,
            last_modified: sidecar.modification_time,
            size: sidecar.size_in_bytes as usize,
        })
    }

    // Get the most up-to-date Protocol and Metadata actions
    pub(crate) fn read_metadata(&self, engine: &dyn Engine) -> DeltaResult<(Metadata, Protocol)> {
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
        match (metadata_opt, protocol_opt) {
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
        self.replay(engine, schema.clone(), schema, META_PREDICATE.clone())
    }
}

/// Returns a fallible iterator of [`ParsedLogPath`] that are between the provided `start_version` (inclusive)
/// and `end_version` (inclusive). [`ParsedLogPath`] may be a commit or a checkpoint.  If `start_version` is
/// not specified, the files will begin from version number 0. If `end_version` is not specified, files up to
/// the most recent version will be included.
///
/// Note: this calls [`FileSystemClient::list_from`] to get the list of log files.
fn list_log_files(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
    start_version: impl Into<Option<Version>>,
    end_version: impl Into<Option<Version>>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ParsedLogPath>>> {
    let start_version = start_version.into().unwrap_or(0);
    let end_version = end_version.into();
    let version_prefix = format!("{:020}", start_version);
    let start_from = log_root.join(&version_prefix)?;

    Ok(fs_client
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
fn list_log_files_with_version(
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
    start_version: Option<Version>,
    end_version: Option<Version>,
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    // We expect 10 commit files per checkpoint, so start with that size. We could adjust this based
    // on config at some point

    let log_files = list_log_files(fs_client, log_root, start_version, end_version)?;

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
    checkpoint_metadata: &CheckpointMetadata,
    fs_client: &dyn FileSystemClient,
    log_root: &Url,
    end_version: Option<Version>,
) -> DeltaResult<(Vec<ParsedLogPath>, Vec<ParsedLogPath>)> {
    let (commit_files, checkpoint_parts) = list_log_files_with_version(
        fs_client,
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
