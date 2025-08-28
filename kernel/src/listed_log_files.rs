//! [`ListedLogFiles`] is a struct holding the result of listing the delta log. Currently, it
//! exposes three APIs for listing:
//! 1. [`list_commits`]: Lists all commit files between the provided start and end versions.
//! 2. [`list`]: Lists all commit and checkpoint files between the provided start and end versions.
//! 3. [`list_with_checkpoint_hint`]: Lists all commit and checkpoint files after the provided
//!    checkpoint hint.
//!
//! After listing, one can leverage the [`ListedLogFiles`] to construct a [`LogSegment`].
//!
//! [`list_commits`]: Self::list_commits
//! [`list`]: Self::list
//! [`list_with_checkpoint_hint`]: Self::list_with_checkpoint_hint
//! [`LogSegment`]: crate::log_segment::LogSegment

use std::collections::HashMap;
use std::convert::identity;

use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::{DeltaResult, Error, StorageHandler, Version};

use delta_kernel_derive::internal_api;

use itertools::Itertools;
use tracing::{info, warn};
use url::Url;

/// A struct to hold the result of listing log files. The commit and compaction files are guaranteed
/// to be sorted in ascending order by version. The elements of `checkpoint_parts` are all the parts
/// of the same checkpoint. Checkpoint parts share the same version. The `latest_crc_file` includes
/// the latest (highest version) CRC file, if any, which may not correspond to the latest commit.
///
/// NOTE: the `ascending_commit_files` _may_ contain gaps.
#[derive(Debug)]
#[internal_api]
pub(crate) struct ListedLogFiles {
    pub(crate) ascending_commit_files: Vec<ParsedLogPath>,
    pub(crate) ascending_compaction_files: Vec<ParsedLogPath>,
    pub(crate) checkpoint_parts: Vec<ParsedLogPath>,
    pub(crate) latest_crc_file: Option<ParsedLogPath>,
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

impl ListedLogFiles {
    // Note: for now we expose the constructor as pub(crate) to allow for use in testing. Ideally,
    // we should explore entirely encapsulating ListedLogFiles within LogSegment - currently
    // LogSegment constructor requires a ListedLogFiles.
    #[internal_api]
    pub(crate) fn try_new(
        ascending_commit_files: Vec<ParsedLogPath>,
        ascending_compaction_files: Vec<ParsedLogPath>,
        checkpoint_parts: Vec<ParsedLogPath>,
        latest_crc_file: Option<ParsedLogPath>,
    ) -> DeltaResult<Self> {
        // We are adding debug_assertions here since we want to validate invariants that are
        // (relatively) expensive to compute
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

        Ok(ListedLogFiles {
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
        })
    }

    /// List all commits between the provided `start_version` (inclusive) and `end_version`
    /// (inclusive). All other types are ignored.
    pub(crate) fn list_commits(
        storage: &dyn StorageHandler,
        log_root: &Url,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let commits: Vec<_> = list_log_files(storage, log_root, start_version, end_version)?
            .filter_ok(|log_file| matches!(log_file.file_type, LogPathFileType::Commit))
            .try_collect()?;
        ListedLogFiles::try_new(commits, vec![], vec![], None)
    }

    /// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
    /// If successful, this returns a `ListedLogFiles`.
    // TODO: encode some of these guarantees in the output types. e.g. we could have:
    // - SortedCommitFiles: Vec<ParsedLogPath>, is_ascending: bool, end_version: Version
    // - CheckpointParts: Vec<ParsedLogPath>, checkpoint_version: Version (guarantee all same version)
    pub(crate) fn list(
        storage: &dyn StorageHandler,
        log_root: &Url,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
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

            ListedLogFiles::try_new(
                ascending_commit_files,
                ascending_compaction_files,
                checkpoint_parts,
                latest_crc_file,
            )
        })?
    }

    /// List all commit and checkpoint files after the provided checkpoint. It is guaranteed that all
    /// the returned [`ParsedLogPath`]s will have a version less than or equal to the `end_version`.
    /// See [`list_log_files_with_version`] for details on the return type.
    pub(crate) fn list_with_checkpoint_hint(
        checkpoint_metadata: &LastCheckpointHint,
        storage: &dyn StorageHandler,
        log_root: &Url,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let listed_files = Self::list(
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
            info!(
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
}
