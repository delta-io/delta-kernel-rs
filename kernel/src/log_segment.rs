//! Represents a segment of a delta log. [`LogSegment`] wraps a set of  checkpoint and commit
//! files.

use crate::actions::{get_log_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::path::ParsedLogPath;
use crate::schema::SchemaRef;
use crate::snapshot::CheckpointMetadata;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, Expression, ExpressionRef, FileSystemClient, Version,
};
use itertools::Itertools;
use std::cmp::Ordering;
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

        // Return a FileNotFound error if there are no new commits.
        // Callers can then decide how to handle this case.
        require!(
            !ascending_commit_files.is_empty(),
            Error::file_not_found("No new commits.")
        );

        // - Here check that the start version is correct.
        // - [`LogSegment::try_new`] will verify that the `end_version` is correct if present.
        // - [`LogSegment::try_new`] also checks that there are no gaps between commits.
        // If all three are satisfied, this implies that all the desired commits are present.
        require!(
            // safety: we validated the list is not empty above
            ascending_commit_files[0].version == start_version,
            Error::generic(format!(
                "Expected the first commit to have version {}",
                start_version
            ))
        );
        LogSegment::try_new(ascending_commit_files, vec![], log_root, end_version)
    }

    /// Extends this LogSegment with the contents of another LogSegment.
    ///
    /// The other LogSegment must be contiguous with this one, i.e. the end
    /// version of this LogSegment must be one less than the start version of
    /// the other LogSegment and contain only commit files.
    pub(crate) fn extend(&mut self, other: &LogSegment) -> DeltaResult<()> {
        require!(
            self.log_root == other.log_root,
            Error::generic("Cannot merge LogSegments with different log roots")
        );
        require!(
            other.checkpoint_parts.is_empty(),
            Error::generic("Cannot extend by LogSegments with checkpoint parts")
        );

        if other.ascending_commit_files.is_empty() {
            return Ok(());
        }

        require!(
            Some(self.end_version + 1) == other.ascending_commit_files.first().map(|f| f.version),
            Error::generic("Cannot merge non contiguous LogSegments")
        );

        self.ascending_commit_files
            .extend(other.ascending_commit_files.iter().cloned());
        self.end_version = other.end_version;

        Ok(())
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
        let checkpoint_stream = engine
            .get_parquet_handler()
            .read_parquet_files(&checkpoint_parts, checkpoint_read_schema, meta_predicate)?
            .map_ok(|batch| (batch, false));

        Ok(commit_stream.chain(checkpoint_stream))
    }

    // Scan the log segment for metadata and protocol actions
    pub(crate) fn read_metadata_opt(
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
        match self.read_metadata_opt(engine)? {
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
    let mut commit_files = Vec::with_capacity(10);
    let mut checkpoint_parts = vec![];
    let mut max_checkpoint_version = start_version;

    for parsed_path in list_log_files(fs_client, log_root, start_version, end_version)? {
        let parsed_path = parsed_path?;
        if parsed_path.is_commit() {
            commit_files.push(parsed_path);
        } else if parsed_path.is_checkpoint() {
            let path_version = parsed_path.version;
            match max_checkpoint_version {
                None => {
                    checkpoint_parts.push(parsed_path);
                    max_checkpoint_version = Some(path_version);
                }
                Some(checkpoint_version) => match path_version.cmp(&checkpoint_version) {
                    Ordering::Greater => {
                        max_checkpoint_version = Some(path_version);
                        checkpoint_parts.clear();
                        checkpoint_parts.push(parsed_path);
                    }
                    Ordering::Equal => checkpoint_parts.push(parsed_path),
                    Ordering::Less => {}
                },
            }
        }
    }

    Ok((commit_files, checkpoint_parts))
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
