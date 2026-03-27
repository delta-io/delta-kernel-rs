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
    /// - `log_tail`: catalog-provided commits
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

#[cfg(test)]
mod list_log_files_with_log_tail_tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use url::Url;

    use rstest::rstest;

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

    /// A [`StorageHandler`] wrapper that counts the number of `list_from` calls.
    /// Used to verify that `list_with_backward_checkpoint_scan` issues the expected
    /// number of storage listing requests.
    struct CountingStorageHandler {
        inner: Box<dyn StorageHandler>,
        list_from_count: AtomicU32,
    }

    impl CountingStorageHandler {
        fn new(inner: Box<dyn StorageHandler>) -> Self {
            Self {
                inner,
                list_from_count: AtomicU32::new(0),
            }
        }

        fn call_count(&self) -> u32 {
            self.list_from_count.load(Ordering::Relaxed)
        }
    }

    impl StorageHandler for CountingStorageHandler {
        fn list_from(
            &self,
            path: &Url,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
            self.list_from_count.fetch_add(1, Ordering::Relaxed);
            self.inner.list_from(path)
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

    // ===== list() tests =====

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

    // ===== list_with_backward_checkpoint_scan() tests =====

    // Log from v0 to v1005. Each case places an optional single-part checkpoint and
    // verifies the expected commits, checkpoint version, and number of storage listings.
    //
    // Window boundaries (window size=1000, end_version=1005, exclusive upper):
    //   Window 1: [6, 1006)  covers v6..=v1005
    //   Window 2: [0, 6)     covers v0..=v5
    //
    // A checkpoint at v6+ is found in window 1 (1 listing); at v5 or lower in window 2
    // (2 listings). A checkpoint beyond end_version is never seen.
    #[rstest]
    // No checkpoint: scan exhausts both windows, all 1006 commits returned
    #[case::no_checkpoint(None, 0..=1005, None, 2)]
    // Checkpoint beyond end_version is never seen; same behavior as no checkpoint
    #[case::checkpoint_beyond_end(Some(1006), 0..=1005, None, 2)]
    // Checkpoint at end_version: found in window 1, no commits after it
    #[case::checkpoint_at_end(Some(1005), 0..0, Some(1005), 1)]
    // Checkpoint at v5: falls in window 2 -> 2 listings; commits 6..=1005 returned.
    // Tests the inclusive window boundary: window 1 covers [6, 1006) or [6, 1005] (lower = 1006 - 1000 = 6),
    // so v5 falls just outside it and requires a second listing, while v6 (next case) does not.
    #[case::checkpoint_in_second_window(Some(5), 6..=1005, Some(5), 2)]
    // Checkpoint at v6: falls in window 1 -> 1 listing; commits 7..=1005 returned
    #[case::checkpoint_in_first_window(Some(6), 7..=1005, Some(6), 1)]
    #[tokio::test]
    async fn backward_scan_single_checkpoint_cases(
        #[case] checkpoint_version: Option<u64>,
        #[case] expected_commits: impl Iterator<Item = u64>,
        #[case] expected_checkpoint: Option<u64>,
        #[case] expected_listings: u32,
    ) {
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=1005)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();

        if let Some(cp) = checkpoint_version {
            log_files.push((
                cp,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ));
        }

        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        let result =
            LogSegmentFiles::list_with_backward_checkpoint_scan(&counter, &log_root, vec![], 1005)
                .unwrap();

        assert_eq!(counter.call_count(), expected_listings);

        assert_eq!(
            result.checkpoint_parts.len(),
            if expected_checkpoint.is_some() { 1 } else { 0 }
        );
        if let Some(cp_version) = expected_checkpoint {
            assert_eq!(result.checkpoint_parts[0].version, cp_version);
        }

        assert!(result
            .ascending_commit_files
            .iter()
            .map(|f| f.version)
            .eq(expected_commits));
    }

    /// end_version=3000. Window 2 contains an incomplete 2-of-2 multipart checkpoint (only
    /// part 1 present). find_complete_checkpoint_version must return None for window 2, causing
    /// the scan to continue to window 3, where a complete single-part checkpoint at v500 is
    /// found. Verifies that incomplete parts from window 2 are discarded and do not pollute
    /// the result's checkpoint_parts.
    ///
    /// Window 1 [2001, 3001): commits v2001..=v3000, no checkpoint -> continue
    /// Window 2 [1001, 2001): commits v1001..=v2000, v1500 (1-of-2 parts) incomplete -> continue
    /// Window 3 [1, 1001):    commits v1..=v1000, v500 (complete) -> checkpoint found -> break
    fn files_incomplete_in_second_window_complete_in_third_window(
    ) -> Vec<(Version, LogPathFileType, CommitSource)> {
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=3000)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            500,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        log_files.push((
            1500,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 1,
                num_parts: 2,
            },
            CommitSource::Filesystem,
        ));
        log_files
    }
    fn multipart_checkpoint_files() -> Vec<(Version, LogPathFileType, CommitSource)> {
        // Log v0..=v52 with a complete 3-part checkpoint at v50.
        // Single window [0, 53): checkpoint found -> stop.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=52)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.extend([
            (
                50,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            (
                50,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            (
                50,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 3,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
        ]);
        log_files
    }

    struct BackwardScanExpected {
        listings: u32,
        checkpoint_parts: usize,
        checkpoint_version: Version,
        commit_count: usize,
        first_commit: Version,
        last_commit: Version,
    }

    // Case 1: complete 3-part checkpoint at v50, single window needed
    // Case 2: incomplete 1-of-2 part at v1500 in window 2, complete checkpoint at v500 in window 3
    #[rstest]
    #[case::multipart_checkpoint(
        multipart_checkpoint_files(),
        52,
        BackwardScanExpected { listings: 1, checkpoint_parts: 3, checkpoint_version: 50, commit_count: 2, first_commit: 51, last_commit: 52 }
    )]
    #[case::incomplete_in_second_window_complete_in_third(
        files_incomplete_in_second_window_complete_in_third_window(),
        3000,
        BackwardScanExpected { listings: 3, checkpoint_parts: 1, checkpoint_version: 500, commit_count: 2500, first_commit: 501, last_commit: 3000 }
    )]
    #[tokio::test]
    async fn backward_scan_multipart_checkpoint_cases(
        #[case] log_files: Vec<(Version, LogPathFileType, CommitSource)>,
        #[case] end_version: Version,
        #[case] expected: BackwardScanExpected,
    ) {
        let BackwardScanExpected {
            listings: expected_listings,
            checkpoint_parts: expected_checkpoint_parts,
            checkpoint_version: expected_checkpoint_version,
            commit_count: expected_commit_count,
            first_commit: expected_first_commit,
            last_commit: expected_last_commit,
        } = expected;
        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            &counter,
            &log_root,
            vec![],
            end_version,
        )
        .unwrap();

        assert_eq!(counter.call_count(), expected_listings);
        assert_eq!(result.checkpoint_parts.len(), expected_checkpoint_parts);
        assert!(result
            .checkpoint_parts
            .iter()
            .all(|p| p.version == expected_checkpoint_version));
        assert_eq!(result.ascending_commit_files.len(), expected_commit_count);
        assert_eq!(
            result.ascending_commit_files.first().unwrap().version,
            expected_first_commit
        );
        assert_eq!(
            result.ascending_commit_files.last().unwrap().version,
            expected_last_commit
        );
        assert_eq!(
            result.latest_commit_file.unwrap().version,
            expected_last_commit
        );
    }

    #[tokio::test]
    async fn backward_scan_with_log_tail_derives_lower_bound_from_checkpoint() {
        // FS: commits v0..=v7 + checkpoint at v5. log_tail: catalog commits v8..=v10.
        // The checkpoint at v5 sets the lower bound to v6, so FS commits v6 and v7 plus all
        // catalog entries v8..=v10 are included.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=7)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<_> = (8u64..=10)
            .map(|v| {
                make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Catalog)
            })
            .collect();

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            10,
        )
        .unwrap();

        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        // FS commits v6, v7 after the checkpoint; catalog commits v8..=v10
        let expected = [
            (6, CommitSource::Filesystem),
            (7, CommitSource::Filesystem),
            (8, CommitSource::Catalog),
            (9, CommitSource::Catalog),
            (10, CommitSource::Catalog),
        ];
        assert_eq!(result.ascending_commit_files.len(), expected.len());
        for (file, (version, source)) in result.ascending_commit_files.iter().zip(expected) {
            assert_eq!(file.version, version);
            assert_source(file, source);
        }
        assert_eq!(result.latest_commit_file.unwrap().version, 10);
    }

    #[tokio::test]
    async fn backward_scan_with_log_tail_starting_before_checkpoint() {
        // FS: commits v0..=v5 + checkpoint at v5 + CRC at v6. log_tail: catalog commits v3..=v8,
        // starting before the checkpoint. The checkpoint at v5 sets the lower bound to v5, so
        // log_tail v3..=v4 are excluded. The log_tail commit at v5 passes through (it is at the
        // checkpoint version). The CRC at v6 is preserved even though v6 is within the log_tail range.
        let mut log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        log_files.push((
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        log_files.push((6, LogPathFileType::Crc, CommitSource::Filesystem));
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail: Vec<_> = (3u64..=8)
            .map(|v| {
                make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Catalog)
            })
            .collect();

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            8,
        )
        .unwrap();

        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        // CRC at v6 is preserved even though v6 is within the log_tail range
        let crc = result.latest_crc_file.unwrap();
        assert_eq!(crc.version, 6);
        assert!(matches!(crc.file_type, LogPathFileType::Crc));

        // v5 passes the start version filter (>= 5) and is included here
        assert_eq!(result.ascending_commit_files.len(), 4);
        for (i, commit) in result.ascending_commit_files.iter().enumerate() {
            assert_eq!(commit.version, (i + 5) as u64);
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(result.latest_commit_file.unwrap().version, 8);
    }

    #[tokio::test]
    async fn backward_scan_log_tail_defines_latest_version() {
        // FS: commits v0..=v5. log_tail: catalog commit v4. end_version=5.
        // FS v4 and v5 are filtered since log_tail_start=4. max_published_version is Some(5),
        // the highest FS commit seen within end_version, even though v5 is not in
        // ascending_commit_files.
        let log_files: Vec<(Version, LogPathFileType, CommitSource)> = (0u64..=5)
            .map(|v| (v, LogPathFileType::Commit, CommitSource::Filesystem))
            .collect();
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![make_parsed_log_path_with_source(
            4,
            LogPathFileType::Commit,
            CommitSource::Catalog,
        )];

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            5,
        )
        .unwrap();

        let expected = [
            (0, CommitSource::Filesystem),
            (1, CommitSource::Filesystem),
            (2, CommitSource::Filesystem),
            (3, CommitSource::Filesystem),
            (4, CommitSource::Catalog),
        ];
        assert_eq!(result.ascending_commit_files.len(), expected.len());
        for (file, (version, source)) in result.ascending_commit_files.iter().zip(expected) {
            assert_eq!(file.version, version);
            assert_source(file, source);
        }
        assert_eq!(result.latest_commit_file.unwrap().version, 4);
        assert_eq!(result.max_published_version, Some(5));
    }

    // ===== find_complete_checkpoint_version direct unit tests (other cases already covered by tests above) =====

    fn zero_size_checkpoint_files() -> Vec<ParsedLogPath> {
        // Commits v0..=5 plus a zero-size checkpoint at v3. make_parsed_log_path_with_source
        // always sets a non-zero size; override here to simulate a corrupt/empty checkpoint
        // file and exercise the size > 0 guard.
        let mut files: Vec<ParsedLogPath> = (0..=5)
            .map(|v| {
                make_parsed_log_path_with_source(
                    v,
                    LogPathFileType::Commit,
                    CommitSource::Filesystem,
                )
            })
            .collect();
        let mut cp = make_parsed_log_path_with_source(
            3,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        );
        cp.location.size = 0;
        files.push(cp);
        files
    }

    fn incomplete_then_complete_files() -> Vec<ParsedLogPath> {
        // Commits v0..=10, an incomplete checkpoint at v5 (1 of 3 parts), and a complete
        // checkpoint at v10. find_complete_checkpoint_version must continue past the failed group
        // and find the complete one.
        let mut files: Vec<ParsedLogPath> = (0..=10)
            .map(|v| {
                make_parsed_log_path_with_source(
                    v,
                    LogPathFileType::Commit,
                    CommitSource::Filesystem,
                )
            })
            .collect();
        files.push(make_parsed_log_path_with_source(
            5,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 1,
                num_parts: 3,
            },
            CommitSource::Filesystem,
        ));
        files.push(make_parsed_log_path_with_source(
            10,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        files
    }

    fn two_complete_checkpoints_files() -> Vec<ParsedLogPath> {
        // Commits v0..=10, complete checkpoint at v5 and complete checkpoint at v10.
        // The function must return the latest (v10), not the first (v5).
        let mut files: Vec<ParsedLogPath> = (0..=10)
            .map(|v| {
                make_parsed_log_path_with_source(
                    v,
                    LogPathFileType::Commit,
                    CommitSource::Filesystem,
                )
            })
            .collect();
        files.push(make_parsed_log_path_with_source(
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        files.push(make_parsed_log_path_with_source(
            10,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        ));
        files
    }

    #[rstest]
    // Commits v0..=5, no checkpoint files
    #[case::no_checkpoint(
        (0u64..=5).map(|v| make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Filesystem)).collect(),
        None
    )]
    // Commits v0..=5 plus a zero-size (corrupt) checkpoint at v3
    #[case::zero_size_checkpoint(zero_size_checkpoint_files(), None)]
    // Commits v0..=10, incomplete checkpoint at v5, complete checkpoint at v10
    #[case::incomplete_then_complete(incomplete_then_complete_files(), Some(10))]
    // Commits v0..=10, complete checkpoint at v5 and v10: must return v10 (latest)
    #[case::two_complete(two_complete_checkpoints_files(), Some(10))]
    fn find_complete_checkpoint_version_cases(
        #[case] files: Vec<ParsedLogPath>,
        #[case] expected: Option<u64>,
    ) {
        assert_eq!(find_complete_checkpoint_version(&files), expected);
    }
}
