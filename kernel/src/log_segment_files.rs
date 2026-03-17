//! [`LogSegmentFiles`] is a struct holding the result of listing the delta log. Currently, it
//! exposes three APIs for listing:
//! 1. [`list_commits`]: Lists all commit files between the provided start and end versions.
//! 2. [`list`]: Lists all commit and checkpoint files between the provided start and end versions.
//! 3. [`list_with_checkpoint_hint`]: Lists all commit and checkpoint files after the provided
//!    checkpoint hint.
//! 4. [`list_with_backward_checkpoint_scan`]: Scans backward from an end version in
//!    1000-version windows until a complete checkpoint is found or the log is exhausted.
//!
//! After listing, one can leverage the [`LogSegmentFiles`] to construct a [`LogSegment`].
//!
//! [`list_with_backward_checkpoint_scan`]: Self::list_with_backward_checkpoint_scan
//!
//! [`list_commits`]: Self::list_commits
//! [`list`]: Self::list
//! [`list_with_checkpoint_hint`]: Self::list_with_checkpoint_hint
//! [`LogSegment`]: crate::log_segment::LogSegment

use std::collections::HashMap;

use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::{DeltaResult, Error, StorageHandler, Version};

use delta_kernel_derive::internal_api;

use itertools::Itertools;
use tracing::{debug, info, instrument};
use url::Url;

/// Represents the set of log files found during a listing operation in the Delta log directory.
///
/// - `ascending_commit_files`: All commit and staged commit files found, sorted by version. May contain gaps.
/// - `ascending_compaction_files`: All compaction commit files found, sorted by version.
/// - `checkpoint_parts`: All parts of the most recent complete checkpoint (all same version). Empty if no checkpoint found.
/// - `latest_crc_file`: The CRC file with the highest version, only if version >= checkpoint version.
/// - `latest_commit_file`: The commit file with the highest version, or `None` if no commits were found.
/// - `max_published_version`: The highest published commit file version, or `None` if no published commits were found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) struct LogSegmentFiles {
    pub ascending_commit_files: Vec<ParsedLogPath>,
    pub ascending_compaction_files: Vec<ParsedLogPath>,
    pub checkpoint_parts: Vec<ParsedLogPath>,
    pub latest_crc_file: Option<ParsedLogPath>,
    pub latest_commit_file: Option<ParsedLogPath>,
    pub max_published_version: Option<Version>,
}

/// Returns a lazy iterator of [`ParsedLogPath`]s from the filesystem over versions
/// `[start_version, end_version]`. The iterator handles parsing, filtering out non-listable
/// files (e.g. staged commits, dot-prefixed files), and stopping at `end_version`.
///
/// This is a thin wrapper around [`StorageHandler::list_from`] that provides the standard
/// Delta log file discovery pipeline. Callers are responsible for handling the `log_tail`
/// (catalog-provided commits) and tracking `max_published_version`.
fn list_from_storage(
    storage: &dyn StorageHandler,
    log_root: &Url,
    start_version: Version,
    end_version: Version,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ParsedLogPath>>> {
    let start_from = log_root.join(&format!("{start_version:020}"))?;
    let files = storage
        .list_from(&start_from)?
        .map(|meta| ParsedLogPath::try_from(meta?))
        // NOTE: this filters out .crc files etc which start with "." - some engines
        // produce `.something.parquet.crc` corresponding to `something.parquet`. Kernel
        // doesn't care about these files. Critically, note these are _different_ than
        // normal `version.crc` files which are listed + captured normally. Additionally
        // we likely aren't even 'seeing' these files since lexicographically the string
        // "." comes before the string "0".
        .filter_map_ok(|path_opt| path_opt.filter(|p| p.should_list()))
        .take_while(move |path_res| match path_res {
            // discard any path with too-large version; keep errors
            Ok(path) => path.version <= end_version,
            Err(_) => true,
        });
    Ok(files)
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
            | UuidCheckpoint
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
            Commit | StagedCommit | CompactedCommit { .. } | Crc | Unknown => {}
        }
    }
    checkpoints
}

/// Returns `true` if `files` contains at least one complete checkpoint across any version.
///
/// Assumes `files` is in ascending version order, as produced by [`list_from_storage`].
fn has_complete_checkpoint_in(files: &[ParsedLogPath]) -> bool {
    files
        .iter()
        .filter(|f| f.is_checkpoint() && f.location.size > 0)
        .chunk_by(|f| f.version)
        .into_iter()
        .any(|(_, parts)| {
            let owned: Vec<ParsedLogPath> = parts.cloned().collect();
            group_checkpoint_parts(owned)
                .iter()
                .any(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
        })
}

/// Accumulates and groups log files during listing. Each "group" consists of all files that
/// share the same version number (e.g., commit, checkpoint parts, CRC files).
///
/// We need to group by version because:
/// 1. A version may have multiple checkpoint parts that must be collected before we can
///    determine if the checkpoint is complete
/// 2. If a complete checkpoint exists, we can discard all commits before it
///
/// Groups are flushed (processed) when we encounter a file with a different version or
/// reach EOF, at which point we check for complete checkpoints and update our state.
#[derive(Default)]
struct ListingAccumulator {
    /// The result being built up
    output: LogSegmentFiles,
    /// Staging area for checkpoint parts at the current version group; always empty when iteration ends
    pending_checkpoint_parts: Vec<ParsedLogPath>,
    /// End-version bound used in process_file() to filter CompactedCommit files
    end_version: Option<Version>,
    /// The version of the current group being accumulated
    group_version: Option<Version>,
}

impl ListingAccumulator {
    fn process_file(&mut self, file: ParsedLogPath) {
        use LogPathFileType::*;
        match file.file_type {
            Commit | StagedCommit => self.output.ascending_commit_files.push(file),
            CompactedCommit { hi } if self.end_version.is_none_or(|end| hi <= end) => {
                self.output.ascending_compaction_files.push(file);
            }
            CompactedCommit { .. } => (), // Failed the bounds check above
            SinglePartCheckpoint | UuidCheckpoint | MultiPartCheckpoint { .. } => {
                self.pending_checkpoint_parts.push(file)
            }
            Crc => {
                self.output.latest_crc_file.replace(file);
            }
            Unknown => {
                // It is possible that there are other files being stashed away into
                // _delta_log/  This is not necessarily forbidden, but something we
                // want to know about in a debugging scenario
                debug!(
                    "Found file {} with unknown file type {:?} at version {}",
                    file.filename, file.file_type, file.version
                );
            }
        }
    }

    /// Called before processing each new file. If `file_version` differs from the current
    /// `group_version`, finalizes the current group by calling `flush_checkpoint_group`,
    /// then advances `group_version` to the new version. On the first call (when
    /// `group_version` is `None`), simply initializes it.
    fn maybe_flush_and_advance(&mut self, file_version: Version) {
        match self.group_version {
            Some(gv) if file_version != gv => {
                self.flush_checkpoint_group(gv);
                self.group_version = Some(file_version);
            }
            None => {
                self.group_version = Some(file_version);
            }
            _ => {} // same version, no flush needed
        }
    }

    /// Groups and finds the first complete checkpoint for this version.
    /// All checkpoints for the same version are equivalent, so we only take one.
    ///
    /// If this version has a complete checkpoint, we can drop the existing commit and
    /// compaction files we collected so far -- except we must keep the latest commit.
    fn flush_checkpoint_group(&mut self, version: Version) {
        let pending_checkpoint_parts = std::mem::take(&mut self.pending_checkpoint_parts);
        if let Some((_, complete_checkpoint)) = group_checkpoint_parts(pending_checkpoint_parts)
            .into_iter()
            // `num_parts` is guaranteed to be non-negative and within `usize` range
            .find(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
        {
            self.output.checkpoint_parts = complete_checkpoint;
            // Keep the commit at the checkpoint version (if any) before clearing all older commits.
            self.output.latest_commit_file = self
                .output
                .ascending_commit_files
                .last()
                .filter(|c| c.version == version)
                .cloned();
            // Log replay only uses commits/compactions after a complete checkpoint
            self.output.ascending_commit_files.clear();
            self.output.ascending_compaction_files.clear();
            // Drop CRC file if older than checkpoint (CRC must be >= checkpoint version)
            if self
                .output
                .latest_crc_file
                .as_ref()
                .is_some_and(|crc| crc.version < version)
            {
                self.output.latest_crc_file = None;
            }
        }
    }
}

/// Controls how the log_tail lower bound is determined during listing accumulation
#[allow(dead_code)] // DeriveFromCheckpoint used by list_with_backward_checkpoint_scan, not yet wired into the snapshot path
enum LogTailLowerBound {
    /// Include log_tail entries at version >= v
    Explicit(Version),
    /// Derive the lower bound from the most recent complete checkpoint found during the
    /// filesystem phase. Log_tail entries are included at version > cp_version (commits AT
    /// the checkpoint version are subsumed by the checkpoint and must not be replayed).
    /// Falls back to 0 if no checkpoint was found.
    DeriveFromCheckpoint,
}

impl LogSegmentFiles {
    /// Assembles a `LogSegmentFiles` from `fs_files` (an iterator of files
    /// listed from storage) and `log_tail` (catalog-provided commits)
    ///
    /// `lower_bound` controls how the log_tail is filtered:
    /// - [`LogTailLowerBound::Explicit`]: include entries at version >= v
    /// - [`LogTailLowerBound::DeriveFromCheckpoint`]: derive lower bound from the most recent
    ///   complete checkpoint found during the filesystem phase; entries at version > cp_version
    ///   are included (commits AT the checkpoint version are subsumed by the checkpoint)
    fn build_log_segment_files(
        fs_files: impl Iterator<Item = DeltaResult<ParsedLogPath>>,
        log_tail: Vec<ParsedLogPath>,
        lower_bound: LogTailLowerBound,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        // check log_tail is only commits
        // note that LogSegment checks no gaps/duplicates so we don't duplicate that here
        debug_assert!(
            log_tail.iter().all(|entry| entry.is_commit()),
            "log_tail should only contain commits"
        );

        let log_tail_start_version = log_tail.first().map(|f| f.version);
        let mut acc = ListingAccumulator {
            end_version,
            ..Default::default()
        };

        // Phase 1: Stream filesystem files lazily (no collect).
        // We always list from the filesystem even when the log_tail covers the entire commit
        // range, because non-commit files (CRC, checkpoints, compactions) only exist on the
        // filesystem — the log_tail only provides commit files.
        for file_result in fs_files {
            let file = file_result?;

            // Track max published commit version from ALL filesystem Commit files,
            // including those that will be skipped because log_tail takes precedence.
            if matches!(file.file_type, LogPathFileType::Commit) {
                acc.output.max_published_version =
                    acc.output.max_published_version.max(Some(file.version));
            }

            // Skip filesystem commits at versions covered by the log_tail (the log_tail
            // is authoritative for commits). Non-commit files are always kept.
            if file.is_commit()
                && log_tail_start_version.is_some_and(|tail_start| file.version >= tail_start)
            {
                continue;
            }

            acc.maybe_flush_and_advance(file.version);
            acc.process_file(file);
        }

        // Phase 2: resolve upper bound from end version and resolve log_tail lower bound
        let resolved_lower = match lower_bound {
            LogTailLowerBound::Explicit(v) => v,
            LogTailLowerBound::DeriveFromCheckpoint => {
                // Flush the last pending group so that output.checkpoint_parts is populated
                // before we inspect it. Without this flush, a checkpoint whose parts arrived
                // last would remain in pending_checkpoint_parts and the derived lower bound
                // would fall back to 0 instead of the checkpoint version.
                if let Some(gv) = acc.group_version {
                    acc.flush_checkpoint_group(gv);
                    acc.group_version = None;
                }
                // Use cp_version + 1: a commit at the checkpoint version is subsumed by the
                // checkpoint and must not be replayed on top of it. The checkpoint has already
                // been flushed from pending_checkpoint_parts into output.checkpoint_parts, so
                // there are no pending parts in Phase 3 that would naturally discard it.
                acc.output
                    .checkpoint_parts
                    .first()
                    .map(|p| p.version + 1)
                    .unwrap_or(0)
            }
        };
        let upper = end_version.unwrap_or(Version::MAX);

        // Phase 3: Process log_tail entries. We do this after Phase 1 because log_tail commits
        // start at log_tail_start_version and are in ascending version order — they always extend
        // (or overlap with, but supersede) the filesystem-listed commits. Processing them after
        // Phase 1 maintains ascending version order throughout, which is required by the checkpoint
        // grouping logic. Note that Phase 1 already skipped filesystem commits at log_tail
        // versions, so there's no duplication here.
        let filtered_log_tail = log_tail
            .into_iter()
            .filter(|entry| entry.version >= resolved_lower && entry.version <= upper);
        for file in filtered_log_tail {
            // Track max published version for published commits from the log_tail
            if matches!(file.file_type, LogPathFileType::Commit) {
                acc.output.max_published_version =
                    acc.output.max_published_version.max(Some(file.version));
            }

            acc.maybe_flush_and_advance(file.version);
            acc.process_file(file);
        }

        // Flush the final group
        if let Some(gv) = acc.group_version {
            acc.flush_checkpoint_group(gv);
        }

        // Since ascending_commit_files is cleared at each checkpoint, if it's non-empty here
        // it contains only commits after the most recent checkpoint. The last element is the
        // highest version commit overall, so we update latest_commit_file to it. If it's empty,
        // we keep the value set at the checkpoint (if a commit existed at the checkpoint version),
        // or remains None.
        if let Some(commit_file) = acc.output.ascending_commit_files.last() {
            acc.output.latest_commit_file = Some(commit_file.clone());
        }

        Ok(acc.output)
    }

    pub(crate) fn ascending_commit_files(&self) -> &Vec<ParsedLogPath> {
        &self.ascending_commit_files
    }

    pub(crate) fn ascending_commit_files_mut(&mut self) -> &mut Vec<ParsedLogPath> {
        &mut self.ascending_commit_files
    }

    pub(crate) fn checkpoint_parts(&self) -> &Vec<ParsedLogPath> {
        &self.checkpoint_parts
    }

    pub(crate) fn latest_commit_file(&self) -> &Option<ParsedLogPath> {
        &self.latest_commit_file
    }

    /// List all commits between the provided `start_version` (inclusive) and `end_version`
    /// (inclusive). All other types are ignored.
    pub(crate) fn list_commits(
        storage: &dyn StorageHandler,
        log_root: &Url,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        // TODO: plumb through a log_tail provided by our caller
        let start = start_version.unwrap_or(0);
        let end = end_version.unwrap_or(Version::MAX);
        let fs_iter = list_from_storage(storage, log_root, start, end)?;

        let mut listed_commits = Vec::new();
        let mut max_published_version: Option<Version> = None;

        for file_result in fs_iter {
            let file = file_result?;
            if matches!(file.file_type, LogPathFileType::Commit) {
                max_published_version = max_published_version.max(Some(file.version));
                listed_commits.push(file);
            }
        }

        let latest_commit_file = listed_commits.last().cloned();
        Ok(LogSegmentFiles {
            ascending_commit_files: listed_commits,
            latest_commit_file,
            max_published_version,
            ..Default::default()
        })
    }

    /// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
    /// If successful, this returns a `LogSegmentFiles`.
    ///
    /// The `log_tail` is an optional sequence of commits provided by the caller, e.g. via
    /// [`SnapshotBuilder::with_log_tail`]. It may contain either published or staged commits. The
    /// `log_tail` must strictly adhere to being a 'tail' — a contiguous cover of versions `X..=Y`
    /// where `Y` is the latest version of the table. If it overlaps with commits listed from the
    /// filesystem, the `log_tail` will take precedence for commits; non-commit files (CRC,
    /// checkpoints, compactions) are always taken from the filesystem.
    // TODO: encode some of these guarantees in the output types. e.g. we could have:
    // - SortedCommitFiles: Vec<ParsedLogPath>, is_ascending: bool, end_version: Version
    // - CheckpointParts: Vec<ParsedLogPath>, checkpoint_version: Version (guarantee all same version)
    #[instrument(name = "log.list", skip_all, fields(start = ?start_version, end = ?end_version), err)]
    pub(crate) fn list(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let start = start_version.unwrap_or(0);
        let end = end_version.unwrap_or(Version::MAX);
        let fs_iter = list_from_storage(storage, log_root, start, end)?;
        Self::build_log_segment_files(
            fs_iter,
            log_tail,
            LogTailLowerBound::Explicit(start),
            end_version,
        )
    }

    /// List all commit and checkpoint files after the provided checkpoint. It is guaranteed that all
    /// the returned [`ParsedLogPath`]s will have a version less than or equal to the `end_version`.
    /// See [`list_log_files_with_version`] for details on the return type.
    pub(crate) fn list_with_checkpoint_hint(
        checkpoint_metadata: &LastCheckpointHint,
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let listed_files = Self::list(
            storage,
            log_root,
            log_tail,
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

    /// Lists log files by scanning backward from `end_version` in 1000-version windows until a
    /// complete checkpoint is found or the log is exhausted. The resulting files are combined
    /// with the `log_tail` (catalog-provided commits) to build a [`LogSegmentFiles`].
    ///
    /// This avoids a full forward listing when we have an upper-bound version but no checkpoint
    /// hint: instead of listing from version 0, we walk backward from `end_version` in bounded
    /// windows, stopping as soon as we find a complete checkpoint (or reach version 0).
    ///
    /// # Parameters
    /// - `storage`: storage handler for listing files
    /// - `log_root`: URL of the `_delta_log/` directory
    /// - `log_tail`: catalog-provided commit files (must be contiguous and ascending)
    /// - `end_version`: the upper-bound version (inclusive) to scan from
    pub(crate) fn list_with_backward_checkpoint_scan(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        end_version: Version,
    ) -> DeltaResult<Self> {
        // Scan backward in 1000-version windows, collecting ALL file types, until a complete
        // checkpoint is found or the log is exhausted.
        let mut windows: Vec<Vec<ParsedLogPath>> = Vec::new();
        let mut upper = end_version;

        loop {
            let lower = upper.saturating_sub(999);
            let window_files = list_from_storage(storage, log_root, lower, upper)?
                .collect::<DeltaResult<Vec<_>>>()?;

            let checkpoint_found = has_complete_checkpoint_in(&window_files);
            windows.push(window_files);

            if checkpoint_found || lower == 0 {
                break;
            }
            upper = lower - 1;
        }

        // Replay all windows in ascending version order.
        windows.reverse();
        let fs_iter = windows.into_iter().flatten().map(Ok);

        // Use build_log_segment_files with DeriveFromCheckpoint so the log_tail lower bound
        // is derived from the checkpoint found during the fs phase.
        Self::build_log_segment_files(
            fs_iter,
            log_tail,
            LogTailLowerBound::DeriveFromCheckpoint,
            Some(end_version),
        )
    }
}

#[cfg(test)]
mod derive_from_checkpoint_lower_bound_tests {
    use url::Url;

    use super::*;

    fn make_commit(version: Version) -> ParsedLogPath {
        let url = Url::parse(&format!("memory:///_delta_log/{version:020}.json")).unwrap();
        let mut filename_path_segments = url.path_segments().unwrap();
        let filename = filename_path_segments.next_back().unwrap().to_string();
        let extension = filename.split('.').next_back().unwrap().to_string();
        ParsedLogPath {
            location: crate::FileMeta {
                location: url,
                last_modified: 0,
                size: 10,
            },
            filename,
            extension,
            version,
            file_type: LogPathFileType::Commit,
        }
    }

    fn make_checkpoint(version: Version) -> ParsedLogPath {
        let url = Url::parse(&format!(
            "memory:///_delta_log/{version:020}.checkpoint.parquet"
        ))
        .unwrap();
        let mut filename_path_segments = url.path_segments().unwrap();
        let filename = filename_path_segments.next_back().unwrap().to_string();
        let extension = filename.split('.').next_back().unwrap().to_string();
        ParsedLogPath {
            location: crate::FileMeta {
                location: url,
                last_modified: 0,
                size: 100,
            },
            filename,
            extension,
            version,
            file_type: LogPathFileType::SinglePartCheckpoint,
        }
    }

    // Regression test: when log_tail starts before (or at) the checkpoint version,
    // commit@cp_version must NOT appear in ascending_commit_files. It is subsumed by
    // the checkpoint and must not be replayed on top of it.
    #[test]
    fn log_tail_starting_before_checkpoint_excludes_commit_at_checkpoint_version() {
        // Filesystem: commits 0-2 then checkpoint@5 (commits 3-4 skipped; log_tail authoritative)
        // log_tail: commits 3-10 (starts before checkpoint version)
        // Expected: checkpoint@5 found, ascending_commit_files = [6..10], no commit@5
        let fs_files: Vec<DeltaResult<ParsedLogPath>> = vec![
            Ok(make_commit(0)),
            Ok(make_commit(1)),
            Ok(make_commit(2)),
            Ok(make_checkpoint(5)),
        ];

        let log_tail: Vec<ParsedLogPath> = (3..=10).map(make_commit).collect();

        let result = LogSegmentFiles::build_log_segment_files(
            fs_files.into_iter(),
            log_tail,
            LogTailLowerBound::DeriveFromCheckpoint,
            Some(10),
        )
        .unwrap();

        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        // commit@5 must NOT be replayed on top of checkpoint@5
        let commit_versions: Vec<Version> = result
            .ascending_commit_files
            .iter()
            .map(|f| f.version)
            .collect();
        assert!(
            !commit_versions.contains(&5),
            "commit@5 should be excluded (subsumed by checkpoint@5), got: {commit_versions:?}"
        );
        assert_eq!(commit_versions, (6u64..=10).collect::<Vec<_>>());
    }

    // When log_tail starts exactly at cp_version, commit@cp_version is excluded.
    #[test]
    fn log_tail_starting_at_checkpoint_version_excludes_commit_at_that_version() {
        // Filesystem: commits 0-4 then checkpoint@5
        // log_tail: commits 5-8 (starts at checkpoint version)
        let fs_files: Vec<DeltaResult<ParsedLogPath>> = (0..5)
            .map(|v| Ok(make_commit(v)))
            .chain(std::iter::once(Ok(make_checkpoint(5))))
            .collect();

        let log_tail: Vec<ParsedLogPath> = (5..=8).map(make_commit).collect();

        let result = LogSegmentFiles::build_log_segment_files(
            fs_files.into_iter(),
            log_tail,
            LogTailLowerBound::DeriveFromCheckpoint,
            Some(8),
        )
        .unwrap();

        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        let commit_versions: Vec<Version> = result
            .ascending_commit_files
            .iter()
            .map(|f| f.version)
            .collect();
        assert!(
            !commit_versions.contains(&5),
            "commit@5 should be excluded (subsumed by checkpoint@5), got: {commit_versions:?}"
        );
        assert_eq!(commit_versions, vec![6u64, 7, 8]);
    }
}

#[cfg(test)]
mod list_log_files_with_log_tail_tests {
    use std::sync::Arc;

    use url::Url;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore};
    use crate::FileMeta;

    use super::*;

    // size markers used to identify commit sources in tests
    const FILESYSTEM_SIZE_MARKER: u64 = 10;
    const CATALOG_SIZE_MARKER: u64 = 7;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CommitSource {
        Filesystem,
        Catalog,
    }

    // create test storage given list of log files with custom data content
    async fn create_storage(
        log_files: Vec<(Version, LogPathFileType, CommitSource)>,
    ) -> (Box<dyn StorageHandler>, Url) {
        let store = Arc::new(InMemory::new());
        let log_root = Url::parse("memory:///_delta_log/").unwrap();

        for (version, file_type, source) in log_files {
            let path = match file_type {
                LogPathFileType::Commit => {
                    format!("_delta_log/{version:020}.json")
                }
                LogPathFileType::StagedCommit => {
                    let uuid = uuid::Uuid::new_v4();
                    format!("_delta_log/_staged_commits/{version:020}.{uuid}.json")
                }
                LogPathFileType::SinglePartCheckpoint => {
                    format!("_delta_log/{version:020}.checkpoint.parquet")
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                } => {
                    format!(
                        "_delta_log/{version:020}.checkpoint.{part_num:010}.{num_parts:010}.parquet"
                    )
                }
                LogPathFileType::Crc => {
                    format!("_delta_log/{version:020}.crc")
                }
                LogPathFileType::CompactedCommit { hi } => {
                    format!("_delta_log/{version:020}.{hi:020}.compacted.json")
                }
                LogPathFileType::UuidCheckpoint | LogPathFileType::Unknown => {
                    panic!("Unsupported file type in test: {file_type:?}")
                }
            };
            let data = match source {
                CommitSource::Filesystem => bytes::Bytes::from("filesystem"),
                CommitSource::Catalog => bytes::Bytes::from("catalog"),
            };
            store
                .put(&ObjectPath::from(path.as_str()), data.into())
                .await
                .expect("Failed to put test file");
        }

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = Box::new(ObjectStoreStorageHandler::new(store, executor, None));
        (storage, log_root)
    }

    // helper to create a ParsedLogPath with specific source marker
    fn make_parsed_log_path_with_source(
        version: Version,
        file_type: LogPathFileType,
        source: CommitSource,
    ) -> ParsedLogPath {
        let url = Url::parse(&format!("memory:///_delta_log/{version:020}.json")).unwrap();
        let mut filename_path_segments = url.path_segments().unwrap();
        let filename = filename_path_segments.next_back().unwrap().to_string();
        let extension = filename.split('.').next_back().unwrap().to_string();

        let size = match source {
            CommitSource::Filesystem => FILESYSTEM_SIZE_MARKER,
            CommitSource::Catalog => CATALOG_SIZE_MARKER,
        };

        let location = FileMeta {
            location: url,
            last_modified: 0,
            size,
        };

        ParsedLogPath {
            location,
            filename,
            extension,
            version,
            file_type,
        }
    }

    fn assert_source(commit: &ParsedLogPath, expected_source: CommitSource) {
        let expected_size = match expected_source {
            CommitSource::Filesystem => FILESYSTEM_SIZE_MARKER,
            CommitSource::Catalog => CATALOG_SIZE_MARKER,
        };
        assert_eq!(
            commit.location.size, expected_size,
            "Commit version {} should be from {:?}, but size was {}",
            commit.version, expected_source, commit.location.size
        );
    }

    /// Helper to call `LogSegmentFiles::list()` and destructure the result for assertions.
    /// Returns (ascending_commit_files, ascending_compaction_files, checkpoint_parts,
    ///          latest_crc_file, latest_commit_file, max_published_version).
    #[allow(clippy::type_complexity)]
    fn list_and_destructure(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> (
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<Version>,
    ) {
        let r =
            LogSegmentFiles::list(storage, log_root, log_tail, start_version, end_version).unwrap();
        (
            r.ascending_commit_files,
            r.ascending_compaction_files,
            r.checkpoint_parts,
            r.latest_crc_file,
            r.latest_commit_file,
            r.max_published_version,
        )
    }

    #[tokio::test]
    async fn test_empty_log_tail() {
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, vec![], Some(1), Some(2));

        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 1);
        assert_eq!(commits[1].version, 2);
        assert_source(&commits[0], CommitSource::Filesystem);
        assert_source(&commits[1], CommitSource::Filesystem);
        assert_eq!(latest_commit.unwrap().version, 2);
        assert_eq!(max_pub, Some(2));
    }

    #[tokio::test]
    async fn test_log_tail_has_latest_commit_files() {
        // Filesystem has commits 0-2, log_tail has commits 3-5 (the latest)
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, log_tail, Some(0), Some(5));

        assert_eq!(commits.len(), 6);
        // filesystem commits 0-2
        for (i, commit) in commits.iter().enumerate().take(3) {
            assert_eq!(commit.version, i as u64);
            assert_source(commit, CommitSource::Filesystem);
        }
        // catalog commits 3-5
        for (i, commit) in commits.iter().enumerate().skip(3) {
            assert_eq!(commit.version, i as u64);
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(latest_commit.unwrap().version, 5);
        assert_eq!(max_pub, Some(5));
    }

    #[tokio::test]
    async fn test_request_subset_with_log_tail() {
        // Test requesting a subset when log_tail is the latest commits
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        // log_tail represents versions 2-4 (latest commits)
        let log_tail = vec![
            make_parsed_log_path_with_source(2, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        // list for only versions 1-3
        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, log_tail, Some(1), Some(3));

        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].version, 1);
        assert_eq!(commits[1].version, 2);
        assert_eq!(commits[2].version, 3);
        assert_source(&commits[0], CommitSource::Filesystem);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_source(&commits[2], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 3);
        assert_eq!(max_pub, Some(3));
    }

    #[tokio::test]
    async fn test_log_tail_defines_latest_version() {
        // log_tail defines the latest version of the table: if there is file system files after log
        // tail, they are ignored. But we still list all filesystem files to track max_published_version.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem), // <-- max_published_version
        ];
        let (storage, log_root) = create_storage(log_files).await;

        // log_tail is just [1], indicating version 1 is the latest
        let log_tail = vec![make_parsed_log_path_with_source(
            1,
            LogPathFileType::Commit,
            CommitSource::Catalog,
        )];

        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, log_tail, Some(0), None);

        // expect only 0 from file system and 1 from log tail
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_source(&commits[0], CommitSource::Filesystem);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 1);
        // max_published_version should reflect the highest published commit on filesystem
        assert_eq!(max_pub, Some(2));
    }

    #[test]
    fn test_log_tail_covers_entire_range_empty_filesystem() {
        // Test-only storage handler that returns an empty listing.
        // When the log_tail covers the entire commit range, we still call list_from
        // (to pick up non-commit files like CRC/checkpoints), but the filesystem may
        // have nothing — e.g. a purely catalog-managed table.
        struct EmptyStorageHandler;
        impl StorageHandler for EmptyStorageHandler {
            fn list_from(
                &self,
                _path: &Url,
            ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
                Ok(Box::new(std::iter::empty()))
            }
            fn read_files(
                &self,
                _files: Vec<crate::FileSlice>,
            ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<bytes::Bytes>>>> {
                panic!("read_files should not be called during listing");
            }
            fn put(&self, _path: &Url, _data: bytes::Bytes, _overwrite: bool) -> DeltaResult<()> {
                panic!("put should not be called during listing");
            }
            fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
                panic!("copy_atomic should not be called during listing");
            }
            fn head(&self, _path: &Url) -> DeltaResult<crate::FileMeta> {
                panic!("head should not be called during listing");
            }
        }

        // log_tail covers versions 0-2, the entire range
        let log_tail = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(
                2,
                LogPathFileType::StagedCommit,
                CommitSource::Catalog,
            ),
        ];

        let storage = EmptyStorageHandler;
        let url = Url::parse("memory:///anything/_delta_log/").unwrap();
        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(&storage, &url, log_tail, Some(0), Some(2));

        // Only log_tail commits should appear (filesystem is empty)
        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_eq!(commits[2].version, 2);
        assert_source(&commits[0], CommitSource::Catalog);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_source(&commits[2], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 2);
        // Only published (non-staged) commits from log_tail count for max_published_version
        assert_eq!(max_pub, Some(1));
    }

    #[tokio::test]
    async fn test_log_tail_covers_entire_range_with_crc() {
        // When log_tail covers the entire requested range (starts at version 0), commit files
        // from the filesystem should be excluded (log_tail is authoritative for commits), but
        // non-commit files (CRC, checkpoints) should still be picked up from the filesystem.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Crc, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        // log_tail covers versions 0-2, which includes the entire range we'll request
        let log_tail = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(
                2,
                LogPathFileType::StagedCommit,
                CommitSource::Catalog,
            ),
        ];

        let (commits, _, _, latest_crc, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, log_tail, Some(0), Some(2));

        // 3 commits from log_tail: 0, 1, 2
        assert_eq!(commits.len(), 3);
        assert_source(&commits[0], CommitSource::Catalog);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_source(&commits[2], CommitSource::Catalog);

        // CRC at version 2 from filesystem is preserved
        let crc = latest_crc.unwrap();
        assert_eq!(crc.version, 2);
        assert!(matches!(crc.file_type, LogPathFileType::Crc));

        assert_eq!(latest_commit.unwrap().version, 2);
        // Only published commits count: filesystem 0,1 (skipped but tracked) + log_tail 0,1
        assert_eq!(max_pub, Some(1));
    }

    #[tokio::test]
    async fn test_listing_omits_staged_commits() {
        // note that in the presence of staged commits, we CANNOT trust listing to determine which
        // to include in our listing/log segment. This is up to the catalog. (e.g. version
        // 5.uuid1.json and 5.uuid2.json can both exist and only catalog can say which is the 'real'
        // version 5).

        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem), // <-- max_published_version
            (1, LogPathFileType::StagedCommit, CommitSource::Filesystem),
            (2, LogPathFileType::StagedCommit, CommitSource::Filesystem),
        ];

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, vec![], None, None);

        // we must only see two regular commits
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_source(&commits[0], CommitSource::Filesystem);
        assert_source(&commits[1], CommitSource::Filesystem);
        assert_eq!(latest_commit.unwrap().version, 1);
        assert_eq!(max_pub, Some(1));
    }

    #[tokio::test]
    async fn test_listing_with_large_end_version() {
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem), // <-- max_published_version
            (2, LogPathFileType::StagedCommit, CommitSource::Filesystem),
        ];

        let (storage, log_root) = create_storage(log_files).await;
        // note we let you request end version past the end of log. up to consumer to interpret
        let (commits, _, _, _, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, vec![], None, Some(3));

        // we must only see two regular commits
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_eq!(latest_commit.unwrap().version, 1);
        assert_eq!(max_pub, Some(1));
    }

    #[tokio::test]
    async fn test_non_commit_files_at_log_tail_versions_are_preserved() {
        // Filesystem has commits 0-5, a checkpoint at version 7, and a CRC at version 8.
        // Log tail provides commits 6-10. The checkpoint and CRC are on the filesystem
        // at versions covered by the log_tail and must NOT be filtered out.
        //
        // After processing through ListingAccumulator, the checkpoint at version 7
        // causes commits before it to be cleared, keeping only commits after the checkpoint.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
            (3, LogPathFileType::Commit, CommitSource::Filesystem),
            (4, LogPathFileType::Commit, CommitSource::Filesystem),
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                7,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            (8, LogPathFileType::Crc, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![
            make_parsed_log_path_with_source(6, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(7, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(8, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(9, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(10, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let (commits, _, checkpoint_parts, latest_crc, latest_commit, max_pub) =
            list_and_destructure(storage.as_ref(), &log_root, log_tail, Some(0), Some(10));

        // Checkpoint at version 7 is preserved from filesystem
        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 7);
        assert!(checkpoint_parts[0].is_checkpoint());

        // CRC at version 8 is preserved from filesystem
        let crc = latest_crc.unwrap();
        assert_eq!(crc.version, 8);
        assert!(matches!(crc.file_type, LogPathFileType::Crc));

        // After checkpoint processing: commits before checkpoint are cleared,
        // only log_tail commits 6-10 remain (added after checkpoint flush)
        assert_eq!(commits.len(), 5);
        for (i, commit) in commits.iter().enumerate() {
            assert_eq!(commit.version, (i + 6) as u64);
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(latest_commit.unwrap().version, 10);

        // max_published_version reflects all published commits seen (filesystem 0-5 + log_tail 6-10)
        assert_eq!(max_pub, Some(10));
    }
}

#[cfg(test)]
mod list_with_backward_scan_tests {
    use std::sync::Arc;

    use url::Url;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore};
    use crate::FileMeta;

    use super::*;

    const FILESYSTEM_SIZE_MARKER: u64 = 10;
    const CATALOG_SIZE_MARKER: u64 = 7;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CommitSource {
        Filesystem,
        Catalog,
    }

    async fn create_storage(
        log_files: Vec<(Version, LogPathFileType, CommitSource)>,
    ) -> (Box<dyn StorageHandler>, Url) {
        let store = Arc::new(InMemory::new());
        let log_root = Url::parse("memory:///_delta_log/").unwrap();

        for (version, file_type, source) in log_files {
            let path = match file_type {
                LogPathFileType::Commit => {
                    format!("_delta_log/{version:020}.json")
                }
                LogPathFileType::SinglePartCheckpoint => {
                    format!("_delta_log/{version:020}.checkpoint.parquet")
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num,
                    num_parts,
                } => {
                    format!(
                        "_delta_log/{version:020}.checkpoint.{part_num:010}.{num_parts:010}.parquet"
                    )
                }
                LogPathFileType::Crc => {
                    format!("_delta_log/{version:020}.crc")
                }
                _ => panic!("Unsupported file type in test: {file_type:?}"),
            };
            let data = match source {
                CommitSource::Filesystem => bytes::Bytes::from("filesystem"),
                CommitSource::Catalog => bytes::Bytes::from("catalog"),
            };
            store
                .put(&ObjectPath::from(path.as_str()), data.into())
                .await
                .expect("Failed to put test file");
        }

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = Box::new(ObjectStoreStorageHandler::new(store, executor, None));
        (storage, log_root)
    }

    fn make_parsed_log_path_with_source(
        version: Version,
        file_type: LogPathFileType,
        source: CommitSource,
    ) -> ParsedLogPath {
        let url = Url::parse(&format!("memory:///_delta_log/{version:020}.json")).unwrap();
        let mut filename_path_segments = url.path_segments().unwrap();
        let filename = filename_path_segments.next_back().unwrap().to_string();
        let extension = filename.split('.').next_back().unwrap().to_string();

        let size = match source {
            CommitSource::Filesystem => FILESYSTEM_SIZE_MARKER,
            CommitSource::Catalog => CATALOG_SIZE_MARKER,
        };

        let location = FileMeta {
            location: url,
            last_modified: 0,
            size,
        };

        ParsedLogPath {
            location,
            filename,
            extension,
            version,
            file_type,
        }
    }

    fn assert_source(commit: &ParsedLogPath, expected_source: CommitSource) {
        let expected_size = match expected_source {
            CommitSource::Filesystem => FILESYSTEM_SIZE_MARKER,
            CommitSource::Catalog => CATALOG_SIZE_MARKER,
        };
        assert_eq!(
            commit.location.size, expected_size,
            "Commit version {} should be from {:?}, but size was {}",
            commit.version, expected_source, commit.location.size
        );
    }

    #[tokio::test]
    async fn test_backward_scan_finds_checkpoint_in_first_window() {
        // Checkpoint at version 990, commits 0-999, end_version = 999.
        // The checkpoint is within the first backward window (versions 0..=999).
        // Should find the checkpoint and return only commits after it.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0..=999)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            990,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            999,
        )
        .unwrap();

        // Checkpoint at version 990 found
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 990);

        // Commits after checkpoint: 991-999 (9 commits)
        assert_eq!(result.ascending_commit_files.len(), 9);
        assert_eq!(result.ascending_commit_files[0].version, 991);
        assert_eq!(result.ascending_commit_files[8].version, 999);

        assert_eq!(result.latest_commit_file.as_ref().unwrap().version, 999);
    }

    #[tokio::test]
    async fn test_backward_scan_finds_checkpoint_in_second_window() {
        // end_version = 2500. First backward window scans [1501, 2500] -- no checkpoint.
        // Second window scans [501, 2500] -- finds checkpoint at version 800.
        // Should return commits from 801 to 2500.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0..=2500)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            800,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            2500,
        )
        .unwrap();

        // Checkpoint at version 800 found
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 800);

        // Commits after checkpoint: 801-2500 (1700 commits)
        assert_eq!(result.ascending_commit_files.len(), 1700);
        assert_eq!(result.ascending_commit_files[0].version, 801);
        assert_eq!(result.ascending_commit_files.last().unwrap().version, 2500);

        assert_eq!(result.latest_commit_file.as_ref().unwrap().version, 2500);
    }

    #[tokio::test]
    async fn test_backward_scan_no_checkpoint_falls_back_to_version_0() {
        // No checkpoint at all. end_version = 50. Function should scan backward
        // until version 0, find no checkpoint, and return all commits 0-50.
        let log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0..=50)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();

        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            50,
        )
        .unwrap();

        // No checkpoint found
        assert!(result.checkpoint_parts.is_empty());

        // All commits from 0 to 50 (51 commits)
        assert_eq!(result.ascending_commit_files.len(), 51);
        assert_eq!(result.ascending_commit_files[0].version, 0);
        assert_eq!(result.ascending_commit_files[50].version, 50);

        assert_eq!(result.latest_commit_file.as_ref().unwrap().version, 50);
    }

    #[tokio::test]
    async fn test_backward_scan_with_log_tail() {
        // Filesystem has commits 0-10 and a checkpoint at version 5.
        // log_tail provides commits 8-12 (the latest commits).
        // Verify: checkpoint at 5 found, commits after checkpoint include
        // filesystem commits 6-7 and log_tail commits 8-12, and filesystem
        // commits at log_tail versions (8-10) are superseded.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0..=10)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<ParsedLogPath> = (8..=12)
            .map(|v| {
                make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Catalog)
            })
            .collect();

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            12,
        )
        .unwrap();

        // Checkpoint at version 5 found
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        // Commits after checkpoint: 6, 7 from filesystem, 8-12 from log_tail = 7 total
        assert_eq!(result.ascending_commit_files.len(), 7);

        // Versions 6-7 from filesystem
        assert_eq!(result.ascending_commit_files[0].version, 6);
        assert_source(&result.ascending_commit_files[0], CommitSource::Filesystem);
        assert_eq!(result.ascending_commit_files[1].version, 7);
        assert_source(&result.ascending_commit_files[1], CommitSource::Filesystem);

        // Versions 8-12 from catalog (log_tail supersedes filesystem)
        for (i, commit) in result.ascending_commit_files[2..].iter().enumerate() {
            assert_eq!(commit.version, (i + 8) as u64);
            assert_source(commit, CommitSource::Catalog);
        }

        assert_eq!(result.latest_commit_file.as_ref().unwrap().version, 12);
    }

    #[tokio::test]
    async fn test_backward_scan_checkpoint_at_version_0() {
        // Checkpoint at version 0 with commits 0-5.
        // Should find the checkpoint and return only commits after version 0.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            0,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            5,
        )
        .unwrap();

        // Checkpoint at version 0 found
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 0);

        // Commits after checkpoint: 1-5 (5 commits)
        assert_eq!(result.ascending_commit_files.len(), 5);
        assert_eq!(result.ascending_commit_files[0].version, 1);
        assert_eq!(result.ascending_commit_files[4].version, 5);

        assert_eq!(result.latest_commit_file.as_ref().unwrap().version, 5);
    }
}
