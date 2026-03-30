//! [`LogSegmentFiles`] is a struct holding the result of listing the delta log. Currently, it
//! exposes four APIs for listing:
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

#[cfg(test)]
mod tests;

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

/// Returns the version of the latest complete checkpoint in `files`, or `None` if no complete
/// checkpoint exists.
fn find_complete_checkpoint_version(ascending_files: &[ParsedLogPath]) -> Option<Version> {
    ascending_files
        .iter()
        .filter(|f| f.is_checkpoint() && f.location.size > 0)
        .chunk_by(|f| f.version)
        .into_iter()
        .filter_map(|(version, parts)| {
            let owned: Vec<ParsedLogPath> = parts.cloned().collect();
            group_checkpoint_parts(owned)
                .iter()
                .any(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
                .then_some(version)
        })
        .last()
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

/// Number of versions covered by each backward-scan window in
/// `LogSegmentFiles::list_with_backward_checkpoint_scan`
const BACKWARD_SCAN_WINDOW_SIZE: u64 = 1000;

impl LogSegmentFiles {
    /// Assembles a `LogSegmentFiles` from `fs_files` (an iterator of files
    /// listed from storage) and `log_tail` (catalog-provided commits).
    ///
    /// - `fs_files`: files listed from storage in ascending version order
    /// - `log_tail`: list of commits that takes precedence over the filesystem ones
    /// - `start_version`: start version of the entire listing range provided; in practice,
    ///   this is the lower bound (inclusive) for log_tail entries included in the result
    /// - `end_version`: upper bound (inclusive) on versions to include, `None` means no bound
    pub(crate) fn build_log_segment_files(
        fs_files: impl Iterator<Item = DeltaResult<ParsedLogPath>>,
        log_tail: Vec<ParsedLogPath>,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        // check log_tail is only commits
        // note that LogSegment checks no gaps/duplicates so we don't duplicate that here
        debug_assert!(
            log_tail.iter().all(|entry| entry.is_commit()),
            "log_tail should only contain commits"
        );

        let log_tail_start_version = log_tail.first().map(|f| f.version);
        let end = end_version.unwrap_or(Version::MAX);

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

        // Phase 2: Process log_tail entries. We do this after Phase 1 because log_tail commits
        // start at log_tail_start_version and are in ascending version order — they always extend
        // (or overlap with, but supersede) the filesystem-listed commits. Processing them after
        // Phase 1 maintains ascending version order throughout, which is required by the checkpoint
        // grouping logic. Note that Phase 1 already skipped filesystem commits at log_tail
        // versions, so there's no duplication here.
        //
        // log_tail entries at versions before a checkpoint may still be included
        // here - LogSegment::try_new is the safeguard that filters those out unconditionally
        let filtered_log_tail = log_tail
            .into_iter()
            .filter(|entry| entry.version >= start_version && entry.version <= end);
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
        Self::build_log_segment_files(fs_iter, log_tail, start, end_version)
    }

    /// List all commit and checkpoint files after the provided checkpoint. It is guaranteed that all
    /// the returned [`ParsedLogPath`]s will have a version less than or equal to the `end_version`.
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
            // Kernel should not compensate for corrupt tables, so we fail if we can't find a checkpoint
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

    /// Returns a [`LogSegmentFiles`] ending at `end_version`, rooted at the most recent complete
    /// checkpoint at or before `end_version`, or rooted at version 0 if no checkpoint is found.
    ///
    /// To find the checkpoint without a full forward listing from version 0, this scans backward
    /// from `end_version` in windows of size [`BACKWARD_SCAN_WINDOW_SIZE`], stopping as soon as
    /// a complete checkpoint is found (or version 0 is reached).
    /// Then, all files from the windows that were scanned are combined with `log_tail` to produce a log segment
    /// rooted at the checkpoint version (or version 0 if no checkpoint) with all commits after the
    /// checkpoint version. A log_tail commit at exactly the checkpoint version may be included at this
    /// stage but will be filtered out by `LogSegment::try_new`.
    ///
    /// For example, given the desired end_version = 12500 and a checkpoint at v8900:
    /// - Window 1 [11501, 12501): no checkpoint -> continue
    /// - Window 2 [10501, 11501): no checkpoint -> continue
    /// - Window 3 [9501, 10501): no checkpoint -> continue
    /// - Window 4 [8501, 9501): checkpoint at v8900 found -> stop
    /// All files from windows 1-4 are combined with `log_tail` to produce a log segment
    /// rooted at the checkpoint at v8900 with all commits from v8901 to v12500.
    #[instrument(name = "log.list_with_backward_checkpoint_scan", skip_all, fields(end = end_version), err)]
    pub(crate) fn list_with_backward_checkpoint_scan(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        end_version: Version,
    ) -> DeltaResult<Self> {
        // Scan backward in 1000-version windows, collecting ALL file types, until a complete
        // checkpoint is found or the log is exhausted.
        let mut windows: Vec<Vec<ParsedLogPath>> = Vec::new();
        let mut found_checkpoint_version: Option<Version> = None;
        // upper is the exclusive upper bound of the next window; adding 1 includes end_version
        // in the first window. The inclusive range passed to list_from_storage is [lower, upper - 1].
        let mut upper = end_version + 1;
        while upper > 0 {
            let lower = upper.saturating_sub(BACKWARD_SCAN_WINDOW_SIZE);
            let window_files: Vec<_> =
                list_from_storage(storage, log_root, lower, upper - 1)?.try_collect()?;

            found_checkpoint_version = find_complete_checkpoint_version(&window_files);
            windows.push(window_files);

            if found_checkpoint_version.is_some() {
                break;
            }
            upper = lower;
        }

        let fs_iter = windows.into_iter().rev().flatten().map(Ok);
        let start = found_checkpoint_version.unwrap_or(0);
        Self::build_log_segment_files(fs_iter, log_tail, start, Some(end_version))
    }
}
