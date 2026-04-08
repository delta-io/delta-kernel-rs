use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use url::Url;

use rstest::rstest;

use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStoreExt as _};
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

fn log_path_for_file_type(version: Version, file_type: &LogPathFileType) -> String {
    match file_type {
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
            format!("_delta_log/{version:020}.checkpoint.{part_num:010}.{num_parts:010}.parquet")
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
    }
}

async fn create_storage(
    log_files: Vec<(Version, LogPathFileType, CommitSource)>,
) -> (Box<dyn StorageHandler>, Url) {
    let store = Arc::new(InMemory::new());
    let log_root = Url::parse("memory:///_delta_log/").unwrap();

    for (version, file_type, source) in log_files {
        let path = log_path_for_file_type(version, &file_type);
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
    let r = LogSegmentFiles::list(storage, log_root, log_tail, start_version, end_version).unwrap();
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
        make_parsed_log_path_with_source(2, LogPathFileType::StagedCommit, CommitSource::Catalog),
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
        make_parsed_log_path_with_source(2, LogPathFileType::StagedCommit, CommitSource::Catalog),
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

// ===== empty (0-byte) log file detection tests =====

/// Creates storage where some files can be empty (0 bytes). Each entry is
/// (version, file_type, is_empty). Non-empty files get placeholder content.
async fn create_storage_with_empty_files(
    log_files: Vec<(Version, LogPathFileType, bool)>,
) -> (Box<dyn StorageHandler>, Url) {
    let store = Arc::new(InMemory::new());
    let log_root = Url::parse("memory:///_delta_log/").unwrap();

    for (version, file_type, is_empty) in log_files {
        let path = log_path_for_file_type(version, &file_type);
        let data = if is_empty {
            bytes::Bytes::new()
        } else {
            bytes::Bytes::from("placeholder")
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

// v2.json is 0 bytes -> listing fails with a clear error
#[tokio::test]
async fn test_zero_byte_commit_errors() {
    let log_files = vec![
        (0, LogPathFileType::Commit, false),
        (1, LogPathFileType::Commit, false),
        (2, LogPathFileType::Commit, true), // empty
    ];
    let (storage, log_root) = create_storage_with_empty_files(log_files).await;

    let result = LogSegmentFiles::list(storage.as_ref(), &log_root, vec![], Some(0), Some(2));
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("empty (0 bytes)"),
        "Expected 'empty (0 bytes)' in error, got: {err}"
    );
}

// 0.4.compacted.json is 0 bytes -> skipped, individual commits v0-v4 used instead
#[rstest]
#[case::forward_list(false)]
#[case::backward_scan(true)]
#[tokio::test]
async fn test_zero_byte_compaction_skipped_commits_used(#[case] use_backward_scan: bool) {
    let log_files = vec![
        (0, LogPathFileType::Commit, false),
        (1, LogPathFileType::Commit, false),
        (2, LogPathFileType::Commit, false),
        (3, LogPathFileType::Commit, false),
        (4, LogPathFileType::Commit, false),
        (
            0,
            LogPathFileType::CompactedCommit { hi: 4 },
            true, // empty compaction
        ),
    ];
    let (storage, log_root) = create_storage_with_empty_files(log_files).await;

    let result = if use_backward_scan {
        LogSegmentFiles::list_with_backward_checkpoint_scan(storage.as_ref(), &log_root, vec![], 4)
            .unwrap()
    } else {
        LogSegmentFiles::list(storage.as_ref(), &log_root, vec![], Some(0), Some(4)).unwrap()
    };

    assert!(
        result.ascending_compaction_files.is_empty(),
        "0-byte compaction should have been skipped"
    );
    assert_eq!(result.ascending_commit_files.len(), 5);
    for (i, commit) in result.ascending_commit_files.iter().enumerate() {
        assert_eq!(commit.version, i as u64);
    }
}

// v10.checkpoint.parquet is 0 bytes -> skipped, falls back to valid checkpoint at v5
#[rstest]
#[case::forward_list(false)]
#[case::backward_scan(true)]
#[tokio::test]
async fn test_zero_byte_checkpoint_skipped_older_used(#[case] use_backward_scan: bool) {
    let log_files = vec![
        (0, LogPathFileType::Commit, false),
        (1, LogPathFileType::Commit, false),
        (2, LogPathFileType::Commit, false),
        (3, LogPathFileType::Commit, false),
        (4, LogPathFileType::Commit, false),
        (5, LogPathFileType::Commit, false),
        (6, LogPathFileType::Commit, false),
        (7, LogPathFileType::Commit, false),
        (8, LogPathFileType::Commit, false),
        (9, LogPathFileType::Commit, false),
        (10, LogPathFileType::Commit, false),
        (5, LogPathFileType::SinglePartCheckpoint, false), // valid checkpoint
        (10, LogPathFileType::SinglePartCheckpoint, true), // empty checkpoint
    ];
    let (storage, log_root) = create_storage_with_empty_files(log_files).await;

    let result = if use_backward_scan {
        LogSegmentFiles::list_with_backward_checkpoint_scan(storage.as_ref(), &log_root, vec![], 10)
            .unwrap()
    } else {
        LogSegmentFiles::list(storage.as_ref(), &log_root, vec![], Some(0), Some(10)).unwrap()
    };

    // Should fall back to checkpoint at v5 (the empty v10 checkpoint is skipped)
    assert_eq!(result.checkpoint_parts.len(), 1);
    assert_eq!(result.checkpoint_parts[0].version, 5);

    // Commits after checkpoint v5: v6 through v10
    assert_eq!(result.ascending_commit_files.len(), 5);
    assert_eq!(result.ascending_commit_files[0].version, 6);
    assert_eq!(result.ascending_commit_files[4].version, 10);
}

// v2.crc is 0 bytes -> skipped, valid v1.crc is kept
#[tokio::test]
async fn test_zero_byte_crc_skipped() {
    let log_files = vec![
        (0, LogPathFileType::Commit, false),
        (1, LogPathFileType::Commit, false),
        (2, LogPathFileType::Commit, false),
        (1, LogPathFileType::Crc, false), // valid CRC
        (2, LogPathFileType::Crc, true),  // empty CRC
    ];
    let (storage, log_root) = create_storage_with_empty_files(log_files).await;

    let result =
        LogSegmentFiles::list(storage.as_ref(), &log_root, vec![], Some(0), Some(2)).unwrap();

    // The 0-byte CRC at v2 is skipped; the valid CRC at v1 is kept
    let crc = result.latest_crc_file.unwrap();
    assert_eq!(crc.version, 1);
}

// ===== find_complete_checkpoint_version direct unit tests (other cases already covered by tests above) =====

fn incomplete_then_complete_files() -> Vec<ParsedLogPath> {
    // Commits v0..=10, an incomplete checkpoint at v5 (1 of 3 parts), and a complete
    // checkpoint at v10. find_complete_checkpoint_version must continue past the failed group
    // and find the complete one.
    let mut files: Vec<ParsedLogPath> = (0..=10)
        .map(|v| {
            make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Filesystem)
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
            make_parsed_log_path_with_source(v, LogPathFileType::Commit, CommitSource::Filesystem)
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
