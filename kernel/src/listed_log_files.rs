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

        // Helper that accumulates and groups log files during listing. Each "group" consists of all
        // files that share the same version number (e.g., commit, checkpoint parts, CRC files).
        //
        // We need to group by version because:
        // 1. A version may have multiple checkpoint parts that must be collected before we can
        //    determine if the checkpoint is complete
        // 2. If a complete checkpoint exists, we can discard all commits before it
        //
        // Groups are flushed (processed) when we encounter a file with a different version or
        // reach EOF, at which point we check for complete checkpoints and update our state.
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

        let mut builder = LogListingGroupBuilder {
            end_version,
            ..Default::default()
        };
        let mut group_version: Option<Version> = None;

        // Phase 1: Stream filesystem files lazily (no collect).
        // We always list from the filesystem even when the log_tail covers the entire commit
        // range, because non-commit files (CRC, checkpoints, compactions) only exist on the
        // filesystem — the log_tail only provides commit files.
        let fs_iter = list_from_storage(storage, log_root, start, end)?;
        for file_result in fs_iter {
            let file = file_result?;

            // Track max published commit version from ALL filesystem Commit files,
            // including those that will be skipped because log_tail takes precedence.
            if matches!(file.file_type, LogPathFileType::Commit) {
                builder.max_published_version =
                    builder.max_published_version.max(Some(file.version));
            }

            // Skip filesystem commits at versions covered by the log_tail (the log_tail
            // is authoritative for commits). Non-commit files are always kept.
            if file.is_commit()
                && log_tail_start_version.is_some_and(|tail_start| file.version >= tail_start)
            {
                continue;
            }

            builder.maybe_flush_and_advance(file.version, &mut group_version);
            builder.process_file(file);
        }

        // Phase 2: Process log_tail entries. We do this after Phase 1 because log_tail commits
        // start at log_tail_start_version and are in ascending version order — they always extend
        // (or overlap with, but supersede) the filesystem-listed commits. Processing them after
        // Phase 1 maintains ascending version order throughout, which is required by the checkpoint
        // grouping logic. Note that Phase 1 already skipped filesystem commits at log_tail
        // versions, so there's no duplication here.
        let filtered_log_tail = log_tail
            .into_iter()
            .filter(|entry| entry.version >= start && entry.version <= end);
        for file in filtered_log_tail {
            // Track max published version for published commits from the log_tail
            if matches!(file.file_type, LogPathFileType::Commit) {
                builder.max_published_version =
                    builder.max_published_version.max(Some(file.version));
            }

            builder.maybe_flush_and_advance(file.version, &mut group_version);
            builder.process_file(file);
        }

        // Flush the final group
        if let Some(gv) = group_version {
            builder.flush_checkpoint_group(gv);
        }

        // Since ascending_commit_files is cleared at each checkpoint, if it's non-empty here
        // it contains only commits after the most recent checkpoint. The last element is the
        // highest version commit overall, so we update latest_commit_file to it. If it's empty,
        // we keep the value set at the checkpoint (if a commit existed at the checkpoint version),
        // or remains None.
        if let Some(commit_file) = builder.ascending_commit_files.last() {
            builder.latest_commit_file = Some(commit_file.clone());
        }

        ListedLogFilesBuilder {
            ascending_commit_files: builder.ascending_commit_files,
            ascending_compaction_files: builder.ascending_compaction_files,
            checkpoint_parts: builder.checkpoint_parts,
            latest_crc_file: builder.latest_crc_file,
            latest_commit_file: builder.latest_commit_file,
            max_published_version: builder.max_published_version,
        }
        .build()
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
