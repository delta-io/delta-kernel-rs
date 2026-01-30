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
use tracing::log::*;
use url::Url;

/// Represents the set of log files found during a listing operation in the Delta log directory.
///
/// - `ascending_commit_files`: All commit and staged commit files found, sorted by version. May contain gaps.
/// - `ascending_compaction_files`: All compaction commit files found, sorted by version.
/// - `checkpoint_parts`: All parts of the most recent complete checkpoint (all same version). Empty if no checkpoint found.
/// - `latest_crc_file`: The CRC file with the highest version, if any.
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

struct ListLogFilesResult {
    files: Vec<ParsedLogPath>,
    max_published_version: Option<Version>,
}

/// Lists [`ParsedLogPath`]s over versions [start_version, end_version], taking into account the
/// `log_tail`. If there are fewer files than requested (e.g. `end_version` is past the end of the
/// log), the result will simply end before reaching `end_version`.
///
/// The `log_tail` may originate from a catalog (e.g. from `SnapshotBuilder::with_log_tail`) or
/// from the connector itself, if it cached log state internally (e.g. from `Snapshot::try_new_from`).
/// It may contain either published or staged commits.
///
/// Note that the `log_tail` must strictly adhere to being a 'tail' - that is, it is a contiguous
/// cover of versions `X..=Y` where `Y` is the latest version of the table. If it overlaps with
/// commits listed from the filesystem, the `log_tail` will take precedence.
///
/// If `start_version` is not specified, the listing will begin from version number 0. If
/// `end_version` is not specified, files up to the most recent version will be included.
///
/// Note: this may call [`StorageHandler::list_from`] to get the list of log files unless the
/// provided log_tail covers the entire requested range.
///
/// Note: at a high level we are doing two things:
/// 1. list from the storage handler and filter based on [`ParsedLogPath::should_list`] (to prevent
///    listing staged commits)
/// 2. add the log_tail from the catalog
fn list_log_files(
    storage: &dyn StorageHandler,
    log_root: &Url,
    log_tail: Vec<ParsedLogPath>,
    start_version: impl Into<Option<Version>>,
    end_version: impl Into<Option<Version>>,
) -> DeltaResult<ListLogFilesResult> {
    // check log_tail is only commits
    // note that LogSegment checks no gaps/duplicates so we don't duplicate that here
    debug_assert!(
        log_tail.iter().all(|entry| entry.is_commit()),
        "log_tail should only contain commits"
    );

    // calculate listing bounds
    let start_version = start_version.into().unwrap_or(0);
    let end_version = end_version.into().unwrap_or(Version::MAX);
    // start_from is log path to start listing from: the log root with zero-padded start version
    let start_from = log_root.join(&format!("{start_version:020}"))?;
    // stop before the log_tail or at the requested end, whichever comes first
    let log_tail_start = log_tail.first();
    let list_end_version =
        log_tail_start.map_or(end_version, |first| first.version.saturating_sub(1));

    let mut max_published_version_from_listing: Option<Version> = None;

    // if the log_tail covers the entire requested range (i.e. starts at or before start_version),
    // we skip listing entirely. note that if we don't include this check, we will end up listing
    // and then just filtering out all the files we listed.
    let listed_files: Vec<ParsedLogPath> =
        if log_tail_start.is_none_or(|tail| start_version < tail.version) {
            // NOTE: since engine APIs don't limit listing, we list from start_version and filter.
            // We list up to end_version (not list_end_version) to track max_published_commit_version_from_listing,
            // then filter to list_end_version for the returned files.
            let all_files: Vec<ParsedLogPath> = storage
                .list_from(&start_from)?
                .map(|meta| ParsedLogPath::try_from(meta?))
                // NOTE: this filters out .crc files etc which start with "." - some engines
                // produce `.something.parquet.crc` corresponding to `something.parquet`. Kernel
                // doesn't care about these files. Critically, note these are _different_ than
                // normal `version.crc` files which are listed + captured normally. Additionally
                // we likely aren't even 'seeing' these files since lexicographically the string
                // "." comes before the string "0".
                .filter_map_ok(|path_opt| path_opt.filter(|p| p.should_list()))
                .take_while(|path_res| match path_res {
                    // discard any path with too-large version; keep errors
                    Ok(path) => path.version <= end_version,
                    Err(_) => true,
                })
                .try_collect()?;

            // Track max published commit version from all filesystem-listed files (including those
            // that will be filtered out because log_tail takes precedence at those versions)
            max_published_version_from_listing = all_files
                .iter()
                .filter(|f| matches!(f.file_type, LogPathFileType::Commit))
                .map(|f| f.version)
                .max();

            // Filter to keep only files before log_tail starts
            all_files
                .into_iter()
                .filter(|f| f.version <= list_end_version)
                .collect()
        } else {
            vec![]
        };

    // Chain with filtered log_tail
    let filtered_log_tail: Vec<ParsedLogPath> = log_tail
        .into_iter()
        .filter(|entry| entry.version >= start_version && entry.version <= end_version)
        .collect();

    // Also consider published commits from log_tail
    let max_published_version_from_log_tail = filtered_log_tail
        .iter()
        .filter(|f| matches!(f.file_type, LogPathFileType::Commit))
        .map(|f| f.version)
        .max();

    let files: Vec<ParsedLogPath> = listed_files.into_iter().chain(filtered_log_tail).collect();

    Ok(ListLogFilesResult {
        files,
        max_published_version: max_published_version_from_listing
            .max(max_published_version_from_log_tail),
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
        let log_tail = vec![];
        let result = list_log_files(storage, log_root, log_tail, start_version, end_version)?;
        let listed_commits: Vec<ParsedLogPath> = result
            .files
            .into_iter()
            .filter(|log_file| log_file.is_commit())
            .collect();
        // .last() on a slice is an O(1) operation
        let latest_commit_file = listed_commits.last().cloned();
        ListedLogFilesBuilder {
            ascending_commit_files: listed_commits,
            latest_commit_file,
            max_published_version: result.max_published_version,
            ..Default::default()
        }
        .build()
    }

    /// List all commit and checkpoint files with versions above the provided `start_version` (inclusive).
    /// If successful, this returns a `ListedLogFiles`.
    // TODO: encode some of these guarantees in the output types. e.g. we could have:
    // - SortedCommitFiles: Vec<ParsedLogPath>, is_ascending: bool, end_version: Version
    // - CheckpointParts: Vec<ParsedLogPath>, checkpoint_version: Version (guarantee all same version)
    pub(crate) fn list(
        storage: &dyn StorageHandler,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        start_version: Option<Version>,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        let result = list_log_files(storage, log_root, log_tail, start_version, end_version)?;

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
                }
            }
        }

        let mut builder = LogListingGroupBuilder {
            end_version,
            ..Default::default()
        };

        let mut log_files = result.files.into_iter();
        if let Some(file) = log_files.next() {
            // Process first file to establish an initial group
            let mut group_version = file.version;
            builder.process_file(file);

            // Process remaining files, flushing the previous groups first if the version changed
            for file in log_files {
                if file.version != group_version {
                    builder.flush_checkpoint_group(group_version);
                    group_version = file.version;
                }
                builder.process_file(file);
            }

            // Flush the final group, which must always contain at least one file
            builder.flush_checkpoint_group(group_version);
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
            max_published_version: result.max_published_version,
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
                _ => panic!("Unsupported file type in test"),
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

    #[tokio::test]
    async fn test_empty_log_tail() {
        let log_files = vec![
            (0, LogPathFileType::Commit, CommitSource::Filesystem),
            (1, LogPathFileType::Commit, CommitSource::Filesystem),
            (2, LogPathFileType::Commit, CommitSource::Filesystem),
        ];
        let (storage, log_root) = create_storage(log_files).await;

        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, vec![], Some(1), Some(2)).unwrap();
        let result = total_listing_result.files;

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].version, 1);
        assert_eq!(result[1].version, 2);
        // all should be from filesystem since log_tail is empty
        assert_source(&result[0], CommitSource::Filesystem);
        assert_source(&result[1], CommitSource::Filesystem);
        assert_eq!(total_listing_result.max_published_version, Some(2));
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

        // log_tail is contiguous, only commits, and represents the latest versions
        let log_tail = vec![
            make_parsed_log_path_with_source(3, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(4, LogPathFileType::Commit, CommitSource::Catalog),
            make_parsed_log_path_with_source(5, LogPathFileType::Commit, CommitSource::Catalog),
        ];

        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, log_tail, Some(0), Some(5)).unwrap();
        let result = total_listing_result.files;

        assert_eq!(result.len(), 6);
        // filesystem
        assert_eq!(result[0].version, 0);
        assert_eq!(result[1].version, 1);
        assert_eq!(result[2].version, 2);
        assert_source(&result[0], CommitSource::Filesystem);
        assert_source(&result[1], CommitSource::Filesystem);
        assert_source(&result[2], CommitSource::Filesystem);
        // log_tail
        assert_eq!(result[3].version, 3);
        assert_eq!(result[4].version, 4);
        assert_eq!(result[5].version, 5);
        assert_source(&result[3], CommitSource::Catalog);
        assert_source(&result[4], CommitSource::Catalog);
        assert_source(&result[5], CommitSource::Catalog);
        assert_eq!(total_listing_result.max_published_version, Some(5));
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
        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, log_tail, Some(1), Some(3)).unwrap();
        let result = total_listing_result.files;

        // The result includes version 1 from filesystem, and log_tail until requested version (2-3)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].version, 1);
        assert_eq!(result[1].version, 2);
        assert_eq!(result[2].version, 3);
        assert_source(&result[0], CommitSource::Filesystem);
        assert_source(&result[1], CommitSource::Catalog);
        assert_source(&result[2], CommitSource::Catalog);
        assert_eq!(
            total_listing_result.max_published_version,
            Some(3) // Recall: we listed (with log tail) with end_version=3
        );
    }

    #[tokio::test]
    async fn test_log_tail_defines_latest_version() {
        // log_tail defines the latest version of the table: if there is file system files after log
        // tail, they are ignored
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

        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, log_tail, Some(0), None).unwrap();
        let result = total_listing_result.files;

        // expect only 0 from file system and 1 from log tail
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].version, 0);
        assert_eq!(result[1].version, 1);
        assert_source(&result[0], CommitSource::Filesystem);
        assert_source(&result[1], CommitSource::Catalog);
        assert_eq!(total_listing_result.max_published_version, Some(2));
    }

    #[test]
    fn test_log_tail_covers_entire_range_no_listing() {
        // test-only storage handler that panics if you use it
        struct StorageThatPanics {}
        impl StorageHandler for StorageThatPanics {
            fn list_from(
                &self,
                _path: &Url,
            ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
                panic!("list_from used");
            }
            fn read_files(
                &self,
                _files: Vec<crate::FileSlice>,
            ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<bytes::Bytes>>>> {
                panic!("read_files used");
            }
            fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
                panic!("copy used from {src} to {dest}");
            }
            fn head(&self, _path: &Url) -> DeltaResult<crate::FileMeta> {
                panic!("head used");
            }
        }

        // when log_tail covers the entire requested range, no filesystem listing should occur
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

        let storage = StorageThatPanics {};
        let url = Url::parse("memory:///anything").unwrap();
        let total_listing_result =
            list_log_files(&storage, &url, log_tail, Some(0), Some(2)).unwrap();
        let result = total_listing_result.files;

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].version, 0);
        assert_eq!(result[1].version, 1);
        assert_eq!(result[2].version, 2);
        assert_source(&result[0], CommitSource::Catalog);
        assert_source(&result[1], CommitSource::Catalog);
        assert_source(&result[2], CommitSource::Catalog);
        assert_eq!(total_listing_result.max_published_version, Some(1));
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
        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, vec![], None, None).unwrap();
        let result = total_listing_result.files;

        // we must only see two regular commits
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].version, 0);
        assert_eq!(result[1].version, 1);
        assert_source(&result[0], CommitSource::Filesystem);
        assert_source(&result[1], CommitSource::Filesystem);
        assert_eq!(total_listing_result.max_published_version, Some(1));
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
        let total_listing_result =
            list_log_files(storage.as_ref(), &log_root, vec![], None, Some(3)).unwrap();
        let result = total_listing_result.files;

        // we must only see two regular commits
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].version, 0);
        assert_eq!(result[1].version, 1);
        assert_eq!(total_listing_result.max_published_version, Some(1));
    }
}
