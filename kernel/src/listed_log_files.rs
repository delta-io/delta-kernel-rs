//! [`ListedLogFiles`] is a struct holding the result of listing the delta log. Currently, it
//! exposes four APIs for listing:
//! 1. [`list_commits`]: Lists all commit files between the provided start and end versions.
//! 2. [`list`]: Lists all commit and checkpoint files between the provided start and end versions.
//! 3. [`list_with_checkpoint_hint`]: Lists all commit and checkpoint files after the provided
//!    checkpoint hint.
//! 4. [`list_with_backward_checkpoint_scan`]: Lists commit and checkpoint files up to a target
//!    version, scanning backward in 1000-version windows until a checkpoint is found.
//!
//! After listing, one can leverage the [`ListedLogFiles`] to construct a [`LogSegment`].
//!
//! [`list_commits`]: Self::list_commits
//! [`list`]: Self::list
//! [`list_with_checkpoint_hint`]: Self::list_with_checkpoint_hint
//! [`list_with_backward_checkpoint_scan`]: list_with_backward_checkpoint_scan
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
#[derive(Debug)]
#[internal_api]
pub(crate) struct ListedLogFiles {
    ascending_commit_files: Vec<ParsedLogPath>,
    ascending_compaction_files: Vec<ParsedLogPath>,
    checkpoint_parts: Vec<ParsedLogPath>,
    latest_crc_file: Option<ParsedLogPath>,
    latest_commit_file: Option<ParsedLogPath>,
    max_published_version: Option<Version>,
}

/// Builder for constructing a validated [`ListedLogFiles`].
///
/// Use struct literal syntax with `..Default::default()` to set only the fields you need,
/// then call `.build()` to validate and produce a `ListedLogFiles`.
#[derive(Debug, Default)]
pub(crate) struct ListedLogFilesBuilder {
    pub ascending_commit_files: Vec<ParsedLogPath>,
    pub ascending_compaction_files: Vec<ParsedLogPath>,
    pub checkpoint_parts: Vec<ParsedLogPath>,
    pub latest_crc_file: Option<ParsedLogPath>,
    pub latest_commit_file: Option<ParsedLogPath>,
    pub max_published_version: Option<Version>,
}

impl ListedLogFilesBuilder {
    /// Validates the builder contents and produces a [`ListedLogFiles`].
    pub(crate) fn build(self) -> DeltaResult<ListedLogFiles> {
        // We are adding debug_assertions here since we want to validate invariants that are
        // (relatively) expensive to compute
        #[cfg(debug_assertions)]
        {
            assert!(self
                .ascending_compaction_files
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

            assert!(self
                .checkpoint_parts
                .iter()
                .all(|part| part.is_checkpoint()));

            // for a multi-part checkpoint, check that they are all same version and all the parts are there
            if self.checkpoint_parts.len() > 1 {
                assert!(self
                    .checkpoint_parts
                    .windows(2)
                    .all(|pair| pair[0].version == pair[1].version));

                assert!(self.checkpoint_parts.iter().all(|part| matches!(
                    part.file_type,
                    LogPathFileType::MultiPartCheckpoint { num_parts, .. }
                    if self.checkpoint_parts.len() == num_parts as usize
                )));
            }
        }

        Ok(ListedLogFiles {
            ascending_commit_files: self.ascending_commit_files,
            ascending_compaction_files: self.ascending_compaction_files,
            checkpoint_parts: self.checkpoint_parts,
            latest_crc_file: self.latest_crc_file,
            latest_commit_file: self.latest_commit_file,
            max_published_version: self.max_published_version,
        })
    }
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

/// Locates the most recent complete checkpoint whose version is strictly less than `version`,
/// searching backward from `version` toward version 0. Returns `None` if no complete checkpoint
/// exists before `version`.
#[cfg(test)]
pub(crate) fn find_last_complete_checkpoint_before(
    storage: &dyn StorageHandler,
    log_root: &Url,
    version: Version,
) -> DeltaResult<Option<Version>> {
    // `upper` is the exclusive upper bound of the current search batch. It starts at the target
    // version and walks backward in steps of 1000 until a complete checkpoint is found or we exhaust all versions
    let mut upper = version;

    while upper > 0 {
        // Each batch covers [lower, upper). saturating_sub prevents underflow when upper < 1000,
        // so that lower is always >= 0 and the final batch covers up to the first version (v0)
        let lower = upper.saturating_sub(1000);
        let start_from = log_root.join(&format!("{lower:020}"))?;

        // List files in [lower, upper) keeping only non-empty checkpoint files
        let checkpoint_files: Vec<ParsedLogPath> = storage
            .list_from(&start_from)?
            .map(|meta| ParsedLogPath::try_from(meta?))
            .filter_map_ok(|opt| opt)
            .take_while(|res| match res {
                Ok(p) => p.version < upper,
                Err(_) => true,
            })
            .filter_ok(|p| p.is_checkpoint() && p.location.size > 0)
            .try_collect()?;

        // Group files by version; storage lists in lexicographic order, and since all Delta log
        // filenames are zero-padded to 20 digits, lexicographic == version order
        // So we can use chunk_by to create one group per version
        let groups: Vec<(Version, Vec<ParsedLogPath>)> = checkpoint_files
            .into_iter()
            .chunk_by(|p| p.version)
            .into_iter()
            .map(|(v, parts)| (v, parts.collect()))
            .collect();

        // Walk from highest to lowest version within this batch and group checkpoint parts
        // Return the version of the first complete checkpoint found
        for (cp_version, parts) in groups.into_iter().rev() {
            let grouped = group_checkpoint_parts(parts);
            if grouped
                .iter()
                .any(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
            {
                return Ok(Some(cp_version));
            }
        }

        // No complete checkpoint in this batch, expand the search window backward
        upper = lower;
    }

    Ok(None)
}

/// Accumulates and groups log files during a listing operation.
///
/// Used by [`list_with_backward_checkpoint_scan`] and [`ListedLogFiles::list`].
///
/// Files are fed in ascending version order via [`ingest_fs_files`] and [`ingest_log_tail`].
/// Files sharing the same version number form a "group" (e.g. multi-part checkpoint parts,
/// or a commit + CRC at the same version). Groups are flushed automatically when a file
/// with a different version is encountered, and the caller must call [`finalize`] after
/// all files have been ingested to flush the last group and produce a [`ListedLogFiles`].
///
/// On each flush, if the current version group contains a complete checkpoint, all
/// previously accumulated commits and compactions are discarded -- log replay only needs
/// files from the most recent checkpoint forward. `latest_commit_file` is set to the
/// commit at the checkpoint version (if any), and overwritten by [`finalize`] with the
/// highest-version commit across both filesystem and log_tail if one exists after the
/// checkpoint.
///
/// The optional `end_version` field filters out compaction files whose upper bound exceeds
/// it. `max_published_version` tracks the highest published commit version seen across
/// both filesystem and log_tail inputs.
///
/// [`ingest_fs_files`]: LogListingGroupBuilder::ingest_fs_files
/// [`ingest_log_tail`]: LogListingGroupBuilder::ingest_log_tail
/// [`finalize`]: LogListingGroupBuilder::finalize
/// [`list_with_backward_checkpoint_scan`]: list_with_backward_checkpoint_scan
#[derive(Default)]
struct LogListingGroupBuilder {
    ascending_commit_files: Vec<ParsedLogPath>,
    ascending_compaction_files: Vec<ParsedLogPath>,
    checkpoint_parts: Vec<ParsedLogPath>,
    latest_crc_file: Option<ParsedLogPath>,
    latest_commit_file: Option<ParsedLogPath>,
    max_published_version: Option<Version>,
    new_checkpoint_parts: Vec<ParsedLogPath>,
    end_version: Option<Version>,
}

impl LogListingGroupBuilder {
    fn process_file(&mut self, file: ParsedLogPath) {
        use LogPathFileType::*;
        match file.file_type {
            Commit | StagedCommit => self.ascending_commit_files.push(file),
            CompactedCommit { hi } if self.end_version.is_none_or(|end| hi <= end) => {
                self.ascending_compaction_files.push(file);
            }
            CompactedCommit { .. } => (), // Failed the bounds check above
            SinglePartCheckpoint | UuidCheckpoint | MultiPartCheckpoint { .. } => {
                self.new_checkpoint_parts.push(file)
            }
            Crc => {
                self.latest_crc_file.replace(file);
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
    fn maybe_flush_and_advance(
        &mut self,
        file_version: Version,
        group_version: &mut Option<Version>,
    ) {
        match *group_version {
            Some(gv) if file_version != gv => {
                self.flush_checkpoint_group(gv);
                *group_version = Some(file_version);
            }
            None => {
                *group_version = Some(file_version);
            }
            _ => {} // same version, no flush needed
        }
    }

    /// Ingests files from a filesystem listing iterator. Filesystem commits at versions
    /// covered by `log_tail_start_version` are skipped (the log_tail is authoritative for
    /// those versions). `max_published_version` is updated from all filesystem Commit files,
    /// including skipped ones. Non-commit files are always processed.
    fn ingest_fs_files(
        &mut self,
        fs_iter: impl Iterator<Item = DeltaResult<ParsedLogPath>>,
        log_tail_start_version: Option<Version>,
        group_version: &mut Option<Version>,
    ) -> DeltaResult<()> {
        for file_result in fs_iter {
            let file = file_result?;
            if matches!(file.file_type, LogPathFileType::Commit) {
                self.max_published_version = self.max_published_version.max(Some(file.version));
            }
            if file.is_commit()
                && log_tail_start_version.is_some_and(|tail_start| file.version >= tail_start)
            {
                continue;
            }
            self.maybe_flush_and_advance(file.version, group_version);
            self.process_file(file);
        }
        Ok(())
    }

    /// Ingests log_tail entries filtered to `[lower_bound, upper_bound]`. Tracks
    /// `max_published_version` for published commits from the log_tail.
    fn ingest_log_tail(
        &mut self,
        log_tail: Vec<ParsedLogPath>,
        lower_bound: Version,
        upper_bound: Version,
        group_version: &mut Option<Version>,
    ) {
        for file in log_tail
            .into_iter()
            .filter(|entry| entry.version >= lower_bound && entry.version <= upper_bound)
        {
            if matches!(file.file_type, LogPathFileType::Commit) {
                self.max_published_version = self.max_published_version.max(Some(file.version));
            }
            self.maybe_flush_and_advance(file.version, group_version);
            self.process_file(file);
        }
    }

    /// Flushes the final version group, sets `latest_commit_file` to the highest-version
    /// commit seen across both filesystem and log_tail inputs, and builds the validated
    /// [`ListedLogFiles`].
    fn finalize(mut self, group_version: Option<Version>) -> DeltaResult<ListedLogFiles> {
        if let Some(gv) = group_version {
            self.flush_checkpoint_group(gv);
        }
        // ascending_commit_files contains only post-checkpoint commits (cleared on each
        // checkpoint flush). Its last element is the highest-version commit overall,
        // spanning both filesystem and log_tail sources.
        if let Some(commit_file) = self.ascending_commit_files.last() {
            self.latest_commit_file = Some(commit_file.clone());
        }
        ListedLogFilesBuilder {
            ascending_commit_files: self.ascending_commit_files,
            ascending_compaction_files: self.ascending_compaction_files,
            checkpoint_parts: self.checkpoint_parts,
            latest_crc_file: self.latest_crc_file,
            latest_commit_file: self.latest_commit_file,
            max_published_version: self.max_published_version,
        }
        .build()
    }

    // Group and find the first complete checkpoint for this version.
    // All checkpoints for the same version are equivalent, so we only take one.
    //
    // If this version has a complete checkpoint, we can drop the existing commit and
    // compaction files we collected so far -- except we must keep the latest commit.
    fn flush_checkpoint_group(&mut self, version: Version) {
        let new_checkpoint_parts = std::mem::take(&mut self.new_checkpoint_parts);
        if let Some((_, complete_checkpoint)) = group_checkpoint_parts(new_checkpoint_parts)
            .into_iter()
            // `num_parts` is guaranteed to be non-negative and within `usize` range
            .find(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
        {
            self.checkpoint_parts = complete_checkpoint;
            // Check if there's a commit file at the same version as this checkpoint. We pop
            // the last element from ascending_commit_files (which is sorted by version) and
            // set latest_commit_file to it only if it matches the checkpoint version. If it
            // doesn't match, we set latest_commit_file to None to discard any older commits
            // from before the checkpoint
            self.latest_commit_file = self
                .ascending_commit_files
                .pop()
                .filter(|commit| commit.version == version);
            // Log replay only uses commits/compactions after a complete checkpoint
            self.ascending_commit_files.clear();
            self.ascending_compaction_files.clear();
            // Drop CRC file if older than checkpoint (CRC must be >= checkpoint version)
            if self
                .latest_crc_file
                .as_ref()
                .is_some_and(|crc| crc.version < version)
            {
                self.latest_crc_file = None;
            }
        }
    }
}

/// Returns `true` if `files` contains at least one complete checkpoint across any version.
///
/// Groups checkpoint files by version, then applies [`group_checkpoint_parts`] to each
/// version's files to detect completeness. A single-part checkpoint is always complete;
/// a multi-part checkpoint is complete when all `num_parts` parts are present.
fn has_complete_checkpoint_in(files: &[ParsedLogPath]) -> bool {
    let mut by_version: HashMap<Version, Vec<ParsedLogPath>> = HashMap::new();
    for file in files {
        if file.is_checkpoint() {
            by_version
                .entry(file.version)
                .or_default()
                .push(file.clone());
        }
    }
    by_version.into_values().any(|parts| {
        group_checkpoint_parts(parts)
            .into_iter()
            .any(|(num_parts, part_files)| part_files.len() == num_parts as usize)
    })
}

/// Lists all commit and checkpoint files up to `end_version`, scanning backward in
/// 1000-version windows until a complete checkpoint is found or version 0 is reached.
///
/// Scans all file types (commits, checkpoints, CRC, compaction) in windows
/// `[end-999, end]`, `[end-1999, end-1000]`, etc., buffering each window. Stops when a
/// complete checkpoint is found in the current window or when the log is exhausted.
/// All buffered windows are then replayed in ascending version order through a single
/// [`LogListingGroupBuilder`] pass -- no second storage listing is needed regardless of
/// how far back the checkpoint lies.
///
/// # Parameters
/// - `storage`: Storage handler for listing.
/// - `log_root`: Root URL of the Delta log directory.
/// - `log_tail`: Caller-provided commits (e.g. from a catalog). Must be a contiguous
///   ascending sequence forming the tail of the log. Only commits are expected.
/// - `end_version`: Inclusive upper bound for the listing.
///
/// # Returns
/// A [`ListedLogFiles`] containing the most recent complete checkpoint (if any) and all
/// commit files from that checkpoint through `end_version`, suitable for constructing a
/// [`LogSegment`].
///
/// # Errors
/// Returns an error if any storage listing fails.
///
/// [`LogSegment`]: crate::log_segment::LogSegment
pub(crate) fn list_with_backward_checkpoint_scan(
    storage: &dyn StorageHandler,
    log_root: &Url,
    log_tail: Vec<ParsedLogPath>,
    end_version: Version,
) -> DeltaResult<ListedLogFiles> {
    let log_tail_start_version = log_tail.first().map(|f| f.version);

    // Scan backward in 1000-version windows, collecting ALL file types, until a complete
    // checkpoint is found or the log is exhausted.
    let mut windows: Vec<Vec<ParsedLogPath>> = Vec::new();
    let mut window_end = end_version;

    loop {
        let window_start = window_end.saturating_sub(999);
        let window_files = list_from_storage(storage, log_root, window_start, window_end)?
            .collect::<DeltaResult<Vec<_>>>()?;

        let checkpoint_found = has_complete_checkpoint_in(&window_files);
        windows.push(window_files);

        if checkpoint_found || window_start == 0 {
            break;
        }
        // Safe: window_start > 0 because the break above didn't fire
        window_end = window_start - 1;
    }

    // Replay all windows in ascending version order. Windows were collected high-to-low,
    // so reverse them. Files within each window are already ascending (from list_from_storage).
    windows.reverse();

    let mut builder = LogListingGroupBuilder {
        end_version: Some(end_version),
        ..Default::default()
    };
    let mut group_version: Option<Version> = None;

    builder.ingest_fs_files(
        windows.into_iter().flatten().map(Ok),
        log_tail_start_version,
        &mut group_version,
    )?;

    if let Some(gv) = group_version {
        builder.flush_checkpoint_group(gv);
    }

    let checkpoint_version = builder.checkpoint_parts.first().map(|p| p.version);
    builder.ingest_log_tail(
        log_tail,
        checkpoint_version.unwrap_or(0),
        end_version,
        &mut group_version,
    );

    builder.finalize(group_version)
}

impl ListedLogFiles {
    #[allow(clippy::type_complexity)] // It's the most readable way to destructure
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<Version>,
    ) {
        (
            self.ascending_commit_files,
            self.ascending_compaction_files,
            self.checkpoint_parts,
            self.latest_crc_file,
            self.latest_commit_file,
            self.max_published_version,
        )
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
        ListedLogFilesBuilder {
            ascending_commit_files: listed_commits,
            latest_commit_file,
            max_published_version,
            ..Default::default()
        }
        .build()
    }

    /// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
    /// If successful, this returns a `ListedLogFiles`.
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
        // check log_tail is only commits
        // note that LogSegment checks no gaps/duplicates so we don't duplicate that here
        debug_assert!(
            log_tail.iter().all(|entry| entry.is_commit()),
            "log_tail should only contain commits"
        );

        let start = start_version.unwrap_or(0);
        let end = end_version.unwrap_or(Version::MAX);
        let log_tail_start_version: Option<Version> = log_tail.first().map(|f| f.version);

        let mut builder = LogListingGroupBuilder {
            end_version,
            ..Default::default()
        };
        let mut group_version: Option<Version> = None;

        // Phase 1: list filesystem files. We always list from the filesystem even when the
        // log_tail covers the entire commit range, because non-commit files (CRC, checkpoints,
        // compactions) only exist on the filesystem.
        let fs_iter = list_from_storage(storage, log_root, start, end)?;
        builder.ingest_fs_files(fs_iter, log_tail_start_version, &mut group_version)?;

        // Phase 2: process log_tail entries after Phase 1 to maintain ascending version order
        // throughout, which is required by the checkpoint grouping logic.
        builder.ingest_log_tail(log_tail, start, end, &mut group_version);

        builder.finalize(group_version)
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
}

#[cfg(test)]
mod list_log_files_with_log_tail_tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore};
    use url::Url;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
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
                    panic!("Unsupported file type in test: {:?}", file_type)
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

    /// Helper to call `ListedLogFiles::list()` and destructure the result for assertions.
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
        ListedLogFiles::list(storage, log_root, log_tail, start_version, end_version)
            .unwrap()
            .into_parts()
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
        // After processing through LogListingGroupBuilder, the checkpoint at version 7
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
mod list_with_backward_checkpoint_scan_tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore};
    use url::Url;

    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::path::LogPathFileType;
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
                LogPathFileType::CompactedCommit { hi } => {
                    format!("_delta_log/{version:020}.{hi:020}.compacted.json")
                }
                _ => panic!("Unsupported file type in test: {:?}", file_type),
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

    fn make_log_tail_entry(version: Version) -> ParsedLogPath {
        let url = Url::parse(&format!("memory:///_delta_log/{version:020}.json")).unwrap();
        let mut filename_path_segments = url.path_segments().unwrap();
        let filename = filename_path_segments.next_back().unwrap().to_string();
        let extension = filename.split('.').next_back().unwrap().to_string();

        ParsedLogPath {
            location: FileMeta {
                location: url,
                last_modified: 0,
                size: CATALOG_SIZE_MARKER,
            },
            filename,
            extension,
            version,
            file_type: LogPathFileType::Commit,
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

    /// Helper to call `list_with_backward_checkpoint_scan` and destructure the result.
    #[allow(clippy::type_complexity)]
    fn scan_and_destructure(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        end_version: Version,
    ) -> (
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Vec<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<ParsedLogPath>,
        Option<Version>,
    ) {
        list_with_backward_checkpoint_scan(storage, log_root, log_tail, end_version)
            .unwrap()
            .into_parts()
    }

    // ---- Fast path: checkpoint found in [end-999, end] ----

    #[tokio::test]
    async fn fast_path_checkpoint_within_window() {
        // Checkpoint at version 5, end_version=10 => window [0, 10], checkpoint found
        let mut log_files: Vec<_> = (0..=10)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, max_pub) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 10);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![6, 7, 8, 9, 10]);
        assert_eq!(latest_commit.unwrap().version, 10);
        assert_eq!(max_pub, Some(10));
    }

    #[tokio::test]
    async fn fast_path_checkpoint_at_end_version() {
        // Checkpoint at exactly end_version=5
        let mut log_files: Vec<_> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, _, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 5);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);
        // No commits after checkpoint at end_version
        assert!(commits.is_empty());
    }

    #[tokio::test]
    async fn fast_path_checkpoint_at_window_boundary() {
        // end_version=999, window=[0, 999], checkpoint at version 0 (= end-999)
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                0,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (999, LogPathFileType::Commit, CommitSource::Filesystem),
        ];

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 999);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 0);
        // Commits after checkpoint: 1 and 999
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![1, 999]);
        assert_eq!(latest_commit.unwrap().version, 999);
    }

    #[tokio::test]
    async fn fast_path_multiple_checkpoints_uses_latest() {
        // Two checkpoints in window: v3 and v7. Should use v7.
        let mut log_files: Vec<_> = (0..=10)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            3,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        log_files.push((
            7,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, _, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 10);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 7);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![8, 9, 10]);
    }

    #[tokio::test]
    async fn fast_path_multipart_checkpoint_in_window() {
        // 2-part checkpoint at v5
        let mut log_files: Vec<_> = (0..=8)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 1,
                num_parts: 2,
            },
            CommitSource::Filesystem,
        ));
        log_files.push((
            5,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 2,
                num_parts: 2,
            },
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, _, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 8);

        assert_eq!(checkpoint_parts.len(), 2);
        assert_eq!(checkpoint_parts[0].version, 5);
        assert_eq!(checkpoint_parts[1].version, 5);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![6, 7, 8]);
    }

    // ---- Multi-window path: checkpoint not in first window ----

    #[tokio::test]
    async fn fallback_checkpoint_outside_window() {
        // Checkpoint at v5, commits up to v1005, end_version=1005
        // Window 1 = [6, 1005], no checkpoint there => window 2 = [0, 5] finds checkpoint at v5
        let mut log_files = vec![
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        for v in 6..=1005 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 1005);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);
        // Commits from v6 through v1005
        assert_eq!(commits.len(), 1000);
        assert_eq!(commits.first().unwrap().version, 6);
        assert_eq!(commits.last().unwrap().version, 1005);
        assert_eq!(latest_commit.unwrap().version, 1005);
    }

    #[tokio::test]
    async fn fallback_no_checkpoint_anywhere_lists_from_v0() {
        let log_files: Vec<_> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, _, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 5);

        assert!(checkpoint_parts.is_empty());
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![0, 1, 2, 3, 4, 5]);
    }

    // ---- Edge cases ----

    #[tokio::test]
    async fn end_version_zero() {
        let log_files = vec![(0, LogPathFileType::Commit, CommitSource::Filesystem)];

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 0);

        assert!(checkpoint_parts.is_empty());
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].version, 0);
        assert_eq!(latest_commit.unwrap().version, 0);
    }

    #[tokio::test]
    async fn end_version_less_than_999_window_starts_at_zero() {
        // end_version=50, window=[0, 50], checkpoint at v10
        let mut log_files: Vec<_> = (0..=50)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            10,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, _, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 50);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 10);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, (11..=50).collect::<Vec<_>>());
    }

    // ---- log_tail interaction ----

    #[tokio::test]
    async fn log_tail_pre_checkpoint_commits_excluded() {
        // This is the key correctness property: log_tail entries at versions before
        // the checkpoint must NOT appear in the output.
        //
        // Setup: checkpoint at v5, filesystem commits 0-5, log_tail provides versions 3-8.
        // The log_tail entries at v3 and v4 are before the checkpoint and must be excluded.
        let mut log_files: Vec<_> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        // Filesystem also has v6-8 but they will be overridden by log_tail
        for v in 6..=8 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<ParsedLogPath> = (3..=8).map(make_log_tail_entry).collect();

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, log_tail, 8);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);

        // Only commits at checkpoint version and above from log_tail should appear
        // The log_tail entries at v3 and v4 must NOT be present
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![5, 6, 7, 8]);

        // Commits at v5-8 should come from the catalog (log_tail)
        for commit in &commits {
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(latest_commit.unwrap().version, 8);
    }

    #[tokio::test]
    async fn log_tail_all_above_checkpoint() {
        // Checkpoint at v5, log_tail provides v6-v8 (all above checkpoint)
        let mut log_files: Vec<_> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<ParsedLogPath> = (6..=8).map(make_log_tail_entry).collect();

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, log_tail, 8);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![6, 7, 8]);
        for commit in &commits {
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(latest_commit.unwrap().version, 8);
    }

    #[tokio::test]
    async fn empty_log_tail_with_backward_scan() {
        // Same as fast_path_checkpoint_within_window but explicitly passing empty log_tail
        let mut log_files: Vec<_> = (0..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            3,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 5);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 3);
        let versions: Vec<_> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![4, 5]);
        assert_eq!(latest_commit.unwrap().version, 5);
    }

    #[tokio::test]
    async fn fast_path_matches_two_listing_fallback_result() {
        // Verify the fast path (1 listing) produces the same result as the fallback
        // path would (via ListedLogFiles::list). This tests correctness of the optimization.
        let mut log_files: Vec<_> = (0..=10)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));

        let (storage, log_root) = create_storage(log_files).await;

        // Fast path result
        let (scan_commits, _, scan_cp, _, scan_latest, scan_max_pub) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 10);

        // Reference result via ListedLogFiles::list (the two-listing path would use)
        let (list_commits, _, list_cp, _, list_latest, list_max_pub) =
            ListedLogFiles::list(storage.as_ref(), &log_root, vec![], Some(5), Some(10))
                .unwrap()
                .into_parts();

        // Both should find checkpoint at v5
        assert_eq!(scan_cp.len(), list_cp.len());
        assert_eq!(scan_cp[0].version, list_cp[0].version);

        // Same commit files after checkpoint
        let scan_versions: Vec<_> = scan_commits.iter().map(|c| c.version).collect();
        let list_versions: Vec<_> = list_commits.iter().map(|c| c.version).collect();
        assert_eq!(scan_versions, list_versions);

        assert_eq!(scan_latest.unwrap().version, list_latest.unwrap().version);
        assert_eq!(scan_max_pub, list_max_pub);
    }

    #[tokio::test]
    async fn log_tail_with_fallback_path_pre_checkpoint_commits_excluded() {
        // Multi-window path with log_tail: checkpoint at v5, >1000 commits after it,
        // log_tail provides versions 3-1006. The log_tail entries at v3 and v4
        // are before the checkpoint and must be excluded even on the multi-window path.
        let mut log_files = vec![
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        for v in 6..=1006 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<ParsedLogPath> = (3..=1006).map(make_log_tail_entry).collect();

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, log_tail, 1006);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 5);

        // Must not contain v3 or v4 from log_tail (pre-checkpoint). The checkpoint
        // flush uses checkpoint_version as the lower bound for ingest_log_tail, so
        // log_tail entries at v3 and v4 are filtered out. v5 (checkpoint version) and
        // above are included, consistent with the fast-path behavior.
        assert!(commits.iter().all(|c| c.version >= 5));
        assert_eq!(commits.first().unwrap().version, 5);
        assert_eq!(commits.last().unwrap().version, 1006);
        assert_eq!(latest_commit.unwrap().version, 1006);
    }

    // ---- Multi-window path: new tests ----

    #[tokio::test]
    async fn second_window_checkpoint_found_without_second_listing() {
        // Checkpoint at v400, end_version=1500.
        // Window 1 = [501, 1500]: no checkpoint.
        // Window 2 = [0, 500]: checkpoint at v400 found.
        // Verifies commits v401..=v1500 returned, checkpoint at v400.
        let mut log_files = vec![
            (400, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                400,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        for v in 401..=1500 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 1500);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 400);
        assert_eq!(commits.len(), 1100);
        assert_eq!(commits.first().unwrap().version, 401);
        assert_eq!(commits.last().unwrap().version, 1500);
        assert_eq!(latest_commit.unwrap().version, 1500);
    }

    #[tokio::test]
    async fn checkpoint_exactly_at_window_boundary() {
        // Checkpoint at version 1000, end_version=2000.
        // Window 1 = [1001, 2000]: no checkpoint.
        // Window 2 = [1, 1000]: checkpoint at v1000 found.
        // Verifies checkpoint at v1000, commits v1001..=v2000.
        let mut log_files = vec![
            (1000, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                1000,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        for v in 1001..=2000 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 2000);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 1000);
        assert_eq!(commits.len(), 1000);
        assert_eq!(commits.first().unwrap().version, 1001);
        assert_eq!(commits.last().unwrap().version, 2000);
        assert_eq!(latest_commit.unwrap().version, 2000);
    }

    #[tokio::test]
    async fn no_checkpoint_spans_multiple_windows() {
        // 2100 commits, no checkpoint anywhere. Verifies all commits returned in ascending
        // order, empty checkpoint_parts.
        let log_files: Vec<_> = (0..=2100)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();

        let (storage, log_root) = create_storage(log_files).await;
        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, vec![], 2100);

        assert!(checkpoint_parts.is_empty());
        assert_eq!(commits.len(), 2101);
        // Ascending order
        for (i, commit) in commits.iter().enumerate() {
            assert_eq!(commit.version, i as u64);
        }
        assert_eq!(latest_commit.unwrap().version, 2100);
    }

    #[tokio::test]
    async fn log_tail_pre_checkpoint_excluded_multi_window() {
        // Mirrors log_tail_pre_checkpoint_commits_excluded but with checkpoint in window 2.
        // Checkpoint at v400, filesystem commits 0-400, log_tail provides versions 350-1500.
        // The log_tail entries at v350..=v399 are before the checkpoint and must be excluded.
        let mut log_files: Vec<_> = (0..=400)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            400,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        for v in 401..=1500 {
            log_files.push((v, LogPathFileType::Commit, CommitSource::Filesystem));
        }

        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<ParsedLogPath> = (350..=1500).map(make_log_tail_entry).collect();

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            scan_and_destructure(storage.as_ref(), &log_root, log_tail, 1500);

        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 400);

        // log_tail entries at v350..=v399 must not appear in output
        assert!(commits.iter().all(|c| c.version >= 400));
        assert_eq!(commits.first().unwrap().version, 400);
        assert_eq!(commits.last().unwrap().version, 1500);
        assert_eq!(latest_commit.unwrap().version, 1500);
    }
}
