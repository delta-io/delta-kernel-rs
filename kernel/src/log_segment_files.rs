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

impl LogSegmentFiles {
    /// Assembles a `LogSegmentFiles` from `fs_files` (an iterator of files
    /// listed from storage) and `log_tail` (catalog-provided commits)
    ///
    /// `start_version` controls how the log_tail is filtered:
    /// - `Some(v)`: include log_tail entries at version >= v
    /// - `None`: derive the lower bound from the most recent complete checkpoint found during
    ///   the filesystem phase; entries at version > cp_version are included (checkpoint wins over
    ///   commits AT the checkpoint version). Falls back to 0 if no checkpoint was found.
    pub(crate) fn build_log_segment_files(
        fs_files: impl Iterator<Item = DeltaResult<ParsedLogPath>>,
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

        // Phase 2: resolve end version upper bound and resolve log_tail start version
        let resolved_start = match start_version {
            Some(v) => v,
            None => {
                // Flush the last pending group so that output.checkpoint_parts is populated
                // before we inspect it. Without this flush, a checkpoint whose parts arrived
                // last would remain in pending_checkpoint_parts and the derived start version
                // would fall back to 0 instead of the checkpoint version.
                if let Some(gv) = acc.group_version {
                    acc.flush_checkpoint_group(gv);
                }
                // Use cp_version + 1: a checkpoint wins over a commit at the checkpoint
                // version and must not be replayed on top of it. The checkpoint has already
                // been flushed from pending_checkpoint_parts into output.checkpoint_parts, so
                // there are no pending parts in Phase 3 that would naturally discard it.
                acc.output
                    .checkpoint_parts
                    .first()
                    .map(|p| p.version + 1)
                    .unwrap_or(0)
            }
        };
        let end = end_version.unwrap_or(Version::MAX);

        // Phase 3: Process log_tail entries. We do this after Phase 1 because log_tail commits
        // start at log_tail_start_version and are in ascending version order — they always extend
        // (or overlap with, but supersede) the filesystem-listed commits. Processing them after
        // Phase 1 maintains ascending version order throughout, which is required by the checkpoint
        // grouping logic. Note that Phase 1 already skipped filesystem commits at log_tail
        // versions, so there's no duplication here.
        let filtered_log_tail = log_tail
            .into_iter()
            .filter(|entry| entry.version >= resolved_start && entry.version <= end);
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
        Self::build_log_segment_files(fs_iter, log_tail, Some(start), end_version)
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

        // Pass None so the log_tail lower bound is derived from the checkpoint found during
        // the fs phase.
        Self::build_log_segment_files(fs_iter, log_tail, None, Some(end_version))
    }
}

#[cfg(test)]
mod list_log_files_with_log_tail_tests {
    use std::sync::atomic::{AtomicU32, Ordering};
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

    /// Helper to call `LogSegmentFiles::build_log_segment_files()` and destructure the result.
    /// Returns (ascending_commit_files, ascending_compaction_files, checkpoint_parts,
    ///          latest_crc_file, latest_commit_file, max_published_version).
    #[allow(clippy::type_complexity)]
    fn build_and_destructure(
        fs_files: impl Iterator<Item = DeltaResult<ParsedLogPath>>,
        log_tail: Vec<ParsedLogPath>,
        lower_bound: Option<Version>,
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
            LogSegmentFiles::build_log_segment_files(fs_files, log_tail, lower_bound, end_version)
                .unwrap();
        (
            r.ascending_commit_files,
            r.ascending_compaction_files,
            r.checkpoint_parts,
            r.latest_crc_file,
            r.latest_commit_file,
            r.max_published_version,
        )
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

    /// Helper to call `LogSegmentFiles::list_with_backward_checkpoint_scan()` and destructure the result.
    /// Returns (ascending_commit_files, ascending_compaction_files, checkpoint_parts,
    ///          latest_crc_file, latest_commit_file, max_published_version).
    #[allow(clippy::type_complexity)]
    fn backward_scan_and_destructure(
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
        let r = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage,
            log_root,
            log_tail,
            end_version,
        )
        .unwrap();
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
        // have nothing - e.g. a purely catalog-managed table.
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

    // ===== list_with_backward_checkpoint_scan correctness =====

    #[tokio::test]
    async fn backward_scan_finds_checkpoint_returns_subsequent_commits() {
        // list_with_backward_checkpoint_scan with a checkpoint at v5 should return the checkpoint
        // and all commits after it (v6..=v10), with no commits before the checkpoint.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
            (3, LogPathFileType::Commit, CommitSource::Filesystem),
            (4, LogPathFileType::Commit, CommitSource::Filesystem),
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            (6, LogPathFileType::Commit, CommitSource::Filesystem),
            (7, LogPathFileType::Commit, CommitSource::Filesystem),
            (8, LogPathFileType::Commit, CommitSource::Filesystem),
            (9, LogPathFileType::Commit, CommitSource::Filesystem),
            (10, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            10,
        )
        .unwrap();

        // Checkpoint at v5 found
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);
        assert!(result.checkpoint_parts[0].is_checkpoint());

        // Only commits after the checkpoint (v6..=v10) are returned
        assert_eq!(result.ascending_commit_files.len(), 5);
        for (i, commit) in result.ascending_commit_files.iter().enumerate() {
            assert_eq!(commit.version, (i + 6) as u64);
        }

        assert_eq!(result.latest_commit_file.unwrap().version, 10);
    }

    #[tokio::test]
    async fn backward_scan_no_checkpoint_returns_all_commits_from_v0() {
        // When no checkpoint exists, list_with_backward_checkpoint_scan scans all the way to v0
        // and returns every commit.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
            (3, LogPathFileType::Commit, CommitSource::Filesystem),
            (4, LogPathFileType::Commit, CommitSource::Filesystem),
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            5,
        )
        .unwrap();

        // No checkpoint
        assert!(result.checkpoint_parts.is_empty());

        // All commits v0..=v5 returned
        assert_eq!(result.ascending_commit_files.len(), 6);
        for (i, commit) in result.ascending_commit_files.iter().enumerate() {
            assert_eq!(commit.version, i as u64);
        }

        assert_eq!(result.latest_commit_file.unwrap().version, 5);
    }

    #[tokio::test]
    async fn backward_scan_multi_window_verifies_result_contents() {
        // end_version=3000, checkpoint at v1500, sparse commits on either side.
        // Window 1 [2001, 3000]: no checkpoint -> continue.
        // Window 2 [1001, 2000]: checkpoint at v1500 -> stop.
        // Verifies both call count AND that the assembled result is correct.
        let log_files = vec![
            (
                1500,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            (1501, LogPathFileType::Commit, CommitSource::Filesystem),
            (3000, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        let result =
            LogSegmentFiles::list_with_backward_checkpoint_scan(&counter, &log_root, vec![], 3000)
                .unwrap();

        assert_eq!(counter.call_count(), 2);

        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 1500);

        assert_eq!(result.ascending_commit_files.len(), 2);
        assert_eq!(result.ascending_commit_files[0].version, 1501);
        assert_eq!(result.ascending_commit_files[1].version, 3000);

        assert_eq!(result.latest_commit_file.unwrap().version, 3000);
    }

    #[tokio::test]
    async fn backward_scan_with_log_tail_derives_lower_bound_from_checkpoint() {
        // Backward scan with a non-empty log_tail: the FS contains a checkpoint at v5 and commits
        // v6-v7; the log_tail provides catalog commits v8-v10. The scan must derive the log_tail
        // lower bound from the checkpoint (v5+1 = 6), including both FS commits after the
        // checkpoint and all log_tail entries.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
            (3, LogPathFileType::Commit, CommitSource::Filesystem),
            (4, LogPathFileType::Commit, CommitSource::Filesystem),
            (5, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            (6, LogPathFileType::Commit, CommitSource::Filesystem),
            (7, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![
            make_parsed_log_path_with_source(8, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(9, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(10, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            10,
        )
        .unwrap();

        // Checkpoint at v5
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        // Commits: v6, v7 from FS; v8, v9, v10 from catalog
        assert_eq!(result.ascending_commit_files.len(), 5);
        assert_eq!(result.ascending_commit_files[0].version, 6);
        assert_eq!(result.ascending_commit_files[1].version, 7);
        assert_eq!(result.ascending_commit_files[2].version, 8);
        assert_eq!(result.ascending_commit_files[3].version, 9);
        assert_eq!(result.ascending_commit_files[4].version, 10);
        assert_source(&result.ascending_commit_files[0], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[1], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[2], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[3], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[4], CommitSource::Catalog);

        assert_eq!(result.latest_commit_file.unwrap().version, 10);
    }

    #[tokio::test]
    async fn backward_scan_with_log_tail_starting_before_checkpoint() {
        // log_tail starts at v3, which is before the checkpoint at v5. The derived lower bound
        // must be cp_version + 1 = 6, so log_tail entries at v3, v4, v5 are excluded even though
        // the log_tail covers those versions.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(6, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(7, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            7,
        )
        .unwrap();

        // Checkpoint at v5 found; lower bound = 6, so log_tail v3/v4/v5 are excluded
        assert_eq!(result.checkpoint_parts.len(), 1);
        assert_eq!(result.checkpoint_parts[0].version, 5);

        assert_eq!(result.ascending_commit_files.len(), 2);
        assert_eq!(result.ascending_commit_files[0].version, 6);
        assert_eq!(result.ascending_commit_files[1].version, 7);
        assert_source(&result.ascending_commit_files[0], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[1], CommitSource::Catalog);

        assert_eq!(result.latest_commit_file.unwrap().version, 7);
    }

    #[tokio::test]
    async fn backward_scan_with_multipart_checkpoint() {
        // A complete 3-part checkpoint in storage. has_complete_checkpoint_in must recognise all
        // three parts as a complete checkpoint and stop the scan.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                5,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            (
                5,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            (
                5,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 3,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            (6, LogPathFileType::Commit, CommitSource::Filesystem),
            (7, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            vec![],
            7,
        )
        .unwrap();

        // All 3 checkpoint parts found; commits before the checkpoint cleared
        assert_eq!(result.checkpoint_parts.len(), 3);
        assert!(result.checkpoint_parts.iter().all(|p| p.version == 5));

        assert_eq!(result.ascending_commit_files.len(), 2);
        assert_eq!(result.ascending_commit_files[0].version, 6);
        assert_eq!(result.ascending_commit_files[1].version, 7);

        assert_eq!(result.latest_commit_file.unwrap().version, 7);
    }

    #[tokio::test]
    async fn backward_scan_no_checkpoint_with_log_tail_uses_lower_bound_zero() {
        // When the backward scan exhausts all windows to v0 without finding a checkpoint,
        // the derived lower bound falls back to 0, so all log_tail entries are included.
        // This exercises the end-to-end path: scan reaches v0, start_version=None is passed to
        // build_log_segment_files, no checkpoint -> lower_bound=0 -> full log_tail included.
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

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            5,
        )
        .unwrap();

        // No checkpoint found; lower bound = 0, so all FS commits (v0-v2) and all log_tail
        // entries (v3-v5) are included
        assert!(result.checkpoint_parts.is_empty());
        assert_eq!(result.ascending_commit_files.len(), 6);
        for (i, commit) in result.ascending_commit_files.iter().enumerate() {
            assert_eq!(commit.version, i as u64);
        }
        assert_source(&result.ascending_commit_files[0], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[1], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[2], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[3], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[4], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[5], CommitSource::Catalog);
        assert_eq!(result.latest_commit_file.unwrap().version, 5);
    }

    #[tokio::test]
    async fn backward_scan_multipart_checkpoint_with_log_tail_derives_lower_bound() {
        // A complete multipart checkpoint is found during backward scan. The derived lower bound
        // must be cp_version + 1, same as for a single-part checkpoint. This verifies that
        // group_checkpoint_parts correctly identifies the complete set and that checkpoint_parts
        // is populated so build_log_segment_files can read the version from it.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (
                3,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 2,
                },
                CommitSource::Filesystem,
            ),
            (
                3,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 2,
                },
                CommitSource::Filesystem,
            ),
            (4, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(6, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let result = LogSegmentFiles::list_with_backward_checkpoint_scan(
            storage.as_ref(),
            &log_root,
            log_tail,
            6,
        )
        .unwrap();

        // Complete 2-part checkpoint at v3; derived lower bound = 4
        assert_eq!(result.checkpoint_parts.len(), 2);
        assert!(result.checkpoint_parts.iter().all(|p| p.version == 3));

        // v4 from FS (= cp_version + 1); v5, v6 from log_tail (all above lower bound)
        assert_eq!(result.ascending_commit_files.len(), 3);
        assert_eq!(result.ascending_commit_files[0].version, 4);
        assert_eq!(result.ascending_commit_files[1].version, 5);
        assert_eq!(result.ascending_commit_files[2].version, 6);
        assert_source(&result.ascending_commit_files[0], CommitSource::Filesystem);
        assert_source(&result.ascending_commit_files[1], CommitSource::Catalog);
        assert_source(&result.ascending_commit_files[2], CommitSource::Catalog);

        assert_eq!(result.latest_commit_file.unwrap().version, 6);
    }

    // ===== list_with_backward_checkpoint_scan() behavioral differences from list() =====
    // Only cases where backward scan produces different results than list() are included here.
    // Identical behavior is already covered by the list() tests above.

    #[tokio::test]
    async fn backward_scan_log_tail_defines_latest_version() {
        // Replicates test_log_tail_defines_latest_version from list() tests.
        // FS v0-v2, log_tail=[v1], end_version=1. Result: v0(FS), v1(catalog), max_pub=2
        // (FS v1 and v2 are skipped from output because log_tail starts at v1, but v2 tracked for max_pub).
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let log_tail = vec![make_parsed_log_path_with_source(
            1,
            LogPathFileType::Commit,
            CommitSource::Catalog,
        )];

        let (commits, _, _, _, latest_commit, max_pub) =
            backward_scan_and_destructure(storage.as_ref(), &log_root, log_tail, 1);

        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_source(&commits[0], CommitSource::Filesystem);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 1);
        // max_published_version reflects the highest published commit the backward scan observed.
        // The scan is bounded by end_version=1, so v2 on filesystem is never seen; max_pub=1.
        assert_eq!(max_pub, Some(1));
    }

    #[tokio::test]
    async fn backward_scan_non_commit_files_at_log_tail_versions_are_preserved() {
        // Replicates test_non_commit_files_at_log_tail_versions_are_preserved from list() tests.
        // FS has commits v0-v5 + checkpoint v7 + CRC v8; log_tail v6-v10; end_version=10.
        // Checkpoint and CRC are preserved even though their versions are covered by log_tail.
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
            backward_scan_and_destructure(storage.as_ref(), &log_root, log_tail, 10);

        // Checkpoint at version 7 is preserved from filesystem
        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 7);
        assert!(checkpoint_parts[0].is_checkpoint());

        // CRC at version 8 is preserved from filesystem
        let crc = latest_crc.unwrap();
        assert_eq!(crc.version, 8);
        assert!(matches!(crc.file_type, LogPathFileType::Crc));

        // After checkpoint at v7: resolved_start = 8 (checkpoint version + 1).
        // Log_tail commits at v6-v7 are below resolved_start and filtered out;
        // only log_tail commits v8-v10 remain (3 commits).
        assert_eq!(commits.len(), 3);
        for (i, commit) in commits.iter().enumerate() {
            assert_eq!(commit.version, (i + 8) as u64);
            assert_source(commit, CommitSource::Catalog);
        }
        assert_eq!(latest_commit.unwrap().version, 10);
        // max_published_version reflects filesystem commits 0-5 + log_tail commits 6-10
        assert_eq!(max_pub, Some(10));
    }

    // ===== has_complete_checkpoint_in direct unit tests =====

    #[test]
    fn has_complete_checkpoint_empty_slice_returns_false() {
        assert!(!has_complete_checkpoint_in(&[]));
    }

    #[test]
    fn has_complete_checkpoint_single_part_returns_true() {
        let files = vec![make_parsed_log_path_with_source(
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        )];
        assert!(has_complete_checkpoint_in(&files));
    }

    #[test]
    fn has_complete_checkpoint_zero_size_filtered_returns_false() {
        // The size > 0 guard must reject zero-size checkpoints to avoid treating empty/corrupt
        // files as complete checkpoints.
        let mut p = make_parsed_log_path_with_source(
            5,
            LogPathFileType::SinglePartCheckpoint,
            CommitSource::Filesystem,
        );
        // make_parsed_log_path_with_source always sets a non-zero size; override here to
        // simulate a corrupt/empty checkpoint file and exercise the size > 0 guard.
        p.location.size = 0;
        assert!(!has_complete_checkpoint_in(&[p]));
    }

    #[test]
    fn has_complete_checkpoint_complete_multipart_returns_true() {
        // A complete 3-part checkpoint: all parts present in order, all non-zero size.
        let files = vec![
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 3,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
        ];
        assert!(has_complete_checkpoint_in(&files));
    }

    #[test]
    fn has_complete_checkpoint_incomplete_multipart_returns_false() {
        // A 3-part checkpoint with only parts 1 and 2 — must NOT be treated as complete.
        let files = vec![
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 2,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
        ];
        assert!(!has_complete_checkpoint_in(&files));
    }

    #[test]
    fn has_complete_checkpoint_incomplete_at_one_version_complete_at_another_returns_true() {
        // An incomplete checkpoint at v5 (1 of 3 parts) followed by a complete checkpoint at v10.
        // has_complete_checkpoint_in must continue past the failed group and find the complete one.
        let files = vec![
            make_parsed_log_path_with_source(
                5,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ];
        assert!(has_complete_checkpoint_in(&files));
    }

    #[test]
    fn has_complete_checkpoint_commits_only_returns_false() {
        // A slice of only commit files contains no checkpoints; must return false.
        let files = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(2, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        assert!(!has_complete_checkpoint_in(&files));
    }

    #[test]
    fn has_complete_checkpoint_zero_size_part_within_multipart_returns_false() {
        // A 3-part checkpoint where part 2 has size=0: the size>0 filter removes part 2, leaving
        // only parts 1 and 3. group_checkpoint_parts requires sequential parts, so part 3 is not
        // appended after part 1 (2 != 1+1 is false, 3 != 1+1 is true so it's skipped), and the
        // checkpoint is incomplete.
        let mut part2 = make_parsed_log_path_with_source(
            10,
            LogPathFileType::MultiPartCheckpoint {
                part_num: 2,
                num_parts: 3,
            },
            CommitSource::Filesystem,
        );
        // make_parsed_log_path_with_source always sets a non-zero size; override here to
        // simulate a missing/corrupt part and exercise the size > 0 guard within a multipart group.
        part2.location.size = 0;
        let files = vec![
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
            part2,
            make_parsed_log_path_with_source(
                10,
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 3,
                    num_parts: 3,
                },
                CommitSource::Filesystem,
            ),
        ];
        assert!(!has_complete_checkpoint_in(&files));
    }

    // ===== window arithmetic / call-counting tests =====

    #[tokio::test]
    async fn backward_scan_window_stops_at_zero_single_call() {
        // end_version=500: lower = 500.saturating_sub(999) = 0, so the loop breaks immediately
        // after the first window. Proves underflow safety via saturating_sub.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (500, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        LogSegmentFiles::list_with_backward_checkpoint_scan(&counter, &log_root, vec![], 500)
            .unwrap();

        assert_eq!(counter.call_count(), 1);
    }

    #[tokio::test]
    async fn backward_scan_window_boundary_999_single_call() {
        // end_version=999: lower = 999 - 999 = 0, so exactly one 1000-version window covers
        // the entire log and the loop exits after one call.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (999, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        LogSegmentFiles::list_with_backward_checkpoint_scan(&counter, &log_root, vec![], 999)
            .unwrap();

        assert_eq!(counter.call_count(), 1);
    }

    #[tokio::test]
    async fn backward_scan_window_boundary_1000_two_calls() {
        // end_version=1000: first window lower = 1000 - 999 = 1 (not 0), so the loop
        // continues. Second window: upper = 0, lower = 0.saturating_sub(999) = 0, breaks.
        // This is the boundary where a second call becomes necessary.
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1000, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;
        let counter = CountingStorageHandler::new(storage);

        LogSegmentFiles::list_with_backward_checkpoint_scan(&counter, &log_root, vec![], 1000)
            .unwrap();

        assert_eq!(counter.call_count(), 2);
    }

    // ===== build_log_segment_files() unit tests (no list() equivalent) =====

    #[test]
    fn build_two_checkpoints_later_supersedes_earlier() {
        // When two complete checkpoints exist in fs_files, flush_checkpoint_group replaces
        // checkpoint_parts each time. The later checkpoint (v7) must win, and all commits
        // accumulated between the two checkpoints must be cleared.
        let fs_files = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(2, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(
                3,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(6, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(
                7,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
            make_parsed_log_path_with_source(8, LogPathFileType::Commit, CommitSource::Filesystem),
        ]
        .into_iter()
        .map(Ok);

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            build_and_destructure(fs_files, vec![], Some(0), Some(8));

        // Checkpoint at v7 supersedes v3; only the commit after v7 remains
        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 7);
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].version, 8);
        assert_eq!(latest_commit.unwrap().version, 8);
    }

    #[test]
    fn build_none_start_version_excludes_log_tail_at_checkpoint_version() {
        // When start_version is None, the lower bound is cp_version + 1, so a log_tail commit AT
        // the checkpoint version is excluded (subsumed by the checkpoint; must not be replayed).
        let fs_files = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(2, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(
                3,
                LogPathFileType::SinglePartCheckpoint,
                CommitSource::Filesystem,
            ),
        ]
        .into_iter()
        .map(Ok);

        let log_tail = vec![
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let (commits, _, checkpoint_parts, _, latest_commit, _) =
            build_and_destructure(fs_files, log_tail, None, Some(5));

        // Checkpoint at v3 found; lower bound = 3+1 = 4, so log_tail v3 is excluded
        assert_eq!(checkpoint_parts.len(), 1);
        assert_eq!(checkpoint_parts[0].version, 3);
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 4);
        assert_eq!(commits[1].version, 5);
        assert_source(&commits[0], CommitSource::Catalog);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 5);
    }

    #[test]
    fn build_none_start_version_no_checkpoint_uses_lower_bound_zero() {
        // When start_version is None and no checkpoint exists, the lower bound defaults to 0
        // and all log_tail entries in [0, end_version] are included.
        let fs_files = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Filesystem),
            make_parsed_log_path_with_source(2, LogPathFileType::Commit, CommitSource::Filesystem),
        ]
        .into_iter()
        .map(Ok);

        let log_tail = vec![
            make_parsed_log_path_with_source(0, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(1, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let (commits, _, checkpoint_parts, _, latest_commit, max_pub) =
            build_and_destructure(fs_files, log_tail, None, Some(2));

        // No checkpoint: lower = 0, log_tail entries v0 and v1 included
        assert!(checkpoint_parts.is_empty());
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].version, 0);
        assert_eq!(commits[1].version, 1);
        assert_source(&commits[0], CommitSource::Catalog);
        assert_source(&commits[1], CommitSource::Catalog);
        assert_eq!(latest_commit.unwrap().version, 1);
        // FS v0, v1, v2 all tracked for max_pub (skipped from accumulator but counted)
        assert_eq!(max_pub, Some(2));
    }
}
