use url::Url;

use crate::commit_range::CommitRange;
use crate::coroutine::kernel::{static_workflow, Channel};
use crate::coroutine::{drive_workflow, StaticWorkflow};
use crate::log_segment::{validate_catalog_managed_log_tail, LogSegment};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::snapshot::SnapshotRef;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, LogPath, Version};

/// Builder for a [`CommitRange`].
///
/// Created via [`CommitRange::builder_for`] (path-based) or
/// [`CommitRange::builder_from`] (snapshot-based). Supports configuring an end version
/// and the commit ordering. Catalog-managed tables also supply the catalog-ratified log tail and
/// maximum catalog version. [`Self::build`] lists the log for a path-based builder or reuses the
/// commit-file metadata in a snapshot-based builder, then validates contiguity.
#[derive(Clone)]
pub struct CommitRangeBuilder {
    table_root: String,
    start_version: Version,
    end_version: Option<Version>,
    snapshot: Option<SnapshotRef>,
    commit_ordering: CommitOrdering,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
}

impl CommitRangeBuilder {
    pub(crate) fn new_for(table_root: impl AsRef<str>, start_version: Version) -> Self {
        CommitRangeBuilder {
            table_root: table_root.as_ref().to_string(),
            start_version,
            end_version: None,
            snapshot: None,
            commit_ordering: CommitOrdering::AscendingOrder,
            log_tail: Vec::new(),
            max_catalog_version: None,
        }
    }

    pub(crate) fn new_from(snapshot: SnapshotRef, start_version: Version) -> Self {
        CommitRangeBuilder {
            table_root: snapshot.table_root().to_string(),
            start_version,
            end_version: None,
            snapshot: Some(snapshot.clone()),
            commit_ordering: CommitOrdering::AscendingOrder,
            log_tail: Vec::new(),
            max_catalog_version: None,
        }
    }

    /// Pin the end of the range. Without this, the range extends to the latest committed
    /// version observed at build time.
    pub fn with_end_version(mut self, end_version: Version) -> Self {
        self.end_version = Some(end_version);
        self
    }

    /// Set the order in which [`CommitRange::commits`] yields commits. Defaults to
    /// [`CommitOrdering::AscendingOrder`].
    pub fn with_ordering(mut self, commit_ordering: CommitOrdering) -> Self {
        self.commit_ordering = commit_ordering;
        self
    }

    /// Set the catalog-ratified staged commits and maximum catalog-ratified version. The entries
    /// must be sorted in ascending, contiguous version order.
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>, max_catalog_version: Version) -> Self {
        self.log_tail = log_tail;
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Set the maximum version ratified by the catalog. Filesystem commits beyond this version are
    /// ignored.
    pub fn with_max_catalog_version(mut self, max_catalog_version: Version) -> Self {
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Resolve commit-file metadata, validate contiguity, and produce a [`CommitRange`]. A
    /// path-based builder lists `_delta_log/`; a snapshot-based builder reuses the snapshot's log
    /// segment. Neither path reads commit JSON.
    ///
    /// Returns [`Error::MissingVersion`] if a snapshot-derived range requires a commit beyond what
    /// is available from the snapshot's log segment and the supplied catalog tail. Returns an error
    /// if the resolved version range is invalid (start > end), the listed commits are
    /// non-contiguous, or the requested start version is unavailable from both the filesystem and
    /// the supplied catalog tail.
    pub fn build(&self, engine: &dyn Engine) -> DeltaResult<CommitRange> {
        drive_workflow(engine, self.workflow())
    }

    /// Return a lazy workflow that lists and validates this commit range.
    pub fn workflow(&self) -> StaticWorkflow<CommitRange> {
        let builder = self.clone();
        static_workflow!(async move |channel| builder.build_with_channel(channel).await)
    }

    async fn build_with_channel(&self, channel: &Channel) -> DeltaResult<CommitRange> {
        let table_root = Self::parse_table_root(&self.table_root)?;
        let log_root = table_root.join("_delta_log/")?;

        let start_version = self.start_version;
        let requested_end_version = self.end_version;
        if let Some(end_version) = requested_end_version {
            validate_version_range(start_version, end_version)?;
        }
        let log_tail: Vec<ParsedLogPath> =
            self.log_tail.clone().into_iter().map(Into::into).collect();
        self.validate_catalog_managed_inputs(&log_tail)?;
        let configured_end_version = requested_end_version.or(self.max_catalog_version);

        let (mut commit_files, end_version) = if let Some(snapshot) = &self.snapshot {
            let log_segment = snapshot.log_segment();
            let end_version = configured_end_version.unwrap_or(log_segment.end_version);
            let available_end_version = log_tail.last().map_or(log_segment.end_version, |last| {
                last.version.max(log_segment.end_version)
            });
            let tail_start_version = log_tail.first().map(|path| path.version);
            let mut commit_files: Vec<_> = log_segment
                .listed
                .ascending_commit_files
                .iter()
                .filter(|path| tail_start_version.is_none_or(|version| path.version < version))
                .cloned()
                .chain(log_tail)
                .filter(|path| path.version >= start_version && path.version <= end_version)
                .collect();
            commit_files.sort_unstable_by_key(|path| path.version);
            validate_start_version_available(start_version, commit_files.first())?;
            if end_version > available_end_version {
                return Err(Error::MissingVersion(available_end_version + 1));
            }
            (commit_files, end_version)
        } else {
            let log_segment = LogSegment::for_table_changes_with_log_tail(
                channel,
                log_root,
                start_version,
                configured_end_version,
                log_tail,
            )
            .await?;
            let end_version = configured_end_version.unwrap_or(log_segment.end_version);
            let commit_files = log_segment
                .listed
                .ascending_commit_files
                .into_iter()
                .filter(|path| path.version >= start_version && path.version <= end_version)
                .collect();
            (commit_files, end_version)
        };
        validate_number_of_commit_files(start_version, end_version, commit_files.len())?;

        if self.commit_ordering == CommitOrdering::DescendingOrder {
            commit_files.reverse();
        }

        Ok(CommitRange {
            table_root,
            commit_files,
            start_version,
            end_version,
            commit_ordering: self.commit_ordering,
        })
    }

    fn validate_catalog_managed_inputs(&self, log_tail: &[ParsedLogPath]) -> DeltaResult<()> {
        if let Some(max_catalog_version) = self.max_catalog_version {
            require!(
                self.start_version <= max_catalog_version,
                Error::MaxCatalogVersion(format!(
                    "Start version {} exceeds max catalog version {max_catalog_version}",
                    self.start_version
                ))
            );
        }
        require!(
            log_tail
                .iter()
                .all(|path| path.file_type == LogPathFileType::StagedCommit),
            Error::generic("Commit range log tail must contain only staged commits")
        );
        validate_catalog_managed_log_tail(self.end_version, self.max_catalog_version, log_tail)
    }

    /// Parse the stored table-root string into a [`Url`].
    fn parse_table_root(table_root: &str) -> DeltaResult<Url> {
        crate::utils::try_parse_uri(table_root)
    }
}

/// Direction in which [`CommitRange::commits`] yields commits.
/// Default is [`CommitOrdering::AscendingOrder`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOrdering {
    /// Yield commits in increasing version order (e.g. `v=0, v=1, v=2, ...`).
    AscendingOrder,
    /// Yield commits in decreasing version order (e.g. `v=N, v=N-1, ..., v=0`).
    DescendingOrder,
}

fn validate_version_range(start: Version, end: Version) -> DeltaResult<()> {
    if start > end {
        return Err(Error::generic(format!(
            "start_version ({start}) must be <= end_version ({end})",
        )));
    }

    Ok(())
}

/// Ensure `start_version` is the first commit in the snapshot's log segment.
fn validate_start_version_available(
    start_version: Version,
    first_commit: Option<&ParsedLogPath>,
) -> DeltaResult<()> {
    if first_commit.map(|f| f.version) == Some(start_version) {
        return Ok(());
    }
    Err(Error::MissingVersion(start_version))
}

fn validate_number_of_commit_files(
    start: Version,
    end: Version,
    commit_file_count: usize,
) -> DeltaResult<()> {
    let expected = end - start + 1;
    let actual = commit_file_count as u64;
    if expected != actual {
        return Err(Error::generic(format!(
            "The number of commit files: {actual} does not match the expected range (start_version: {start}, end_version: {end}): expected {expected} commit files",
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::commit_range::DeltaAction;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::utils::FoldWithOption as _;
    use crate::{DeltaResultIteratorStatic, Engine, FileMeta, LogPath, Snapshot, StorageHandler};

    struct NoIoStorageHandler;

    impl StorageHandler for NoIoStorageHandler {
        fn list_from(&self, _path: &Url) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
            panic!("snapshot-based commit ranges must not list storage");
        }

        fn read_files(
            &self,
            _files: Vec<crate::FileSlice>,
        ) -> DeltaResult<DeltaResultIteratorStatic<bytes::Bytes>> {
            panic!("commit range construction must not read files");
        }

        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
            panic!("unexpected copy");
        }

        fn put(&self, _path: &Url, _data: bytes::Bytes, _overwrite: bool) -> DeltaResult<()> {
            panic!("unexpected write");
        }

        fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
            panic!("unexpected head");
        }

        fn delete(&self, _path: &Url) -> DeltaResult<()> {
            panic!("unexpected delete");
        }
    }

    /// `table-with-dv-small` has versions 0 and 1 (snapshot version = 1).
    fn dv_small_table_root() -> Url {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        Url::from_directory_path(path).unwrap()
    }

    fn staged_commit(table_root: &Url, version: Version) -> LogPath {
        LogPath::staged_commit(
            table_root.clone(),
            &format!("{version:020}.00000000-0000-4000-8000-{version:012}.json"),
            0,
            1,
        )
        .unwrap()
    }

    #[test]
    fn test_build_path_based_succeeds() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .build(&engine)
            .unwrap();

        assert_eq!(range.start_version(), 0);
        assert_eq!(range.end_version(), 1);
        assert_eq!(range.table_root().as_str(), table_root.as_str());
    }

    #[test]
    fn test_build_snapshot_based_succeeds() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let snapshot_version = snapshot.version();

        let range = CommitRange::builder_from(snapshot, 0)
            .build(&engine)
            .unwrap();

        // Without an explicit end version, the range resolves to the snapshot's version.
        assert_eq!(range.start_version(), 0);
        assert_eq!(range.end_version(), snapshot_version);
    }

    #[rstest::rstest]
    #[case::start_past_snapshot_version(5, None, 5)]
    #[case::end_past_snapshot_version(0, Some(99), 2)]
    #[case::start_error_precedes_end_error(5, Some(99), 5)]
    fn test_build_snapshot_based_reports_unavailable_version(
        #[case] start: Version,
        #[case] end: Option<Version>,
        #[case] expected_missing_version: Version,
    ) {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let err = CommitRange::builder_from(snapshot, start)
            .fold_with(end, CommitRangeBuilder::with_end_version)
            .build(&engine)
            .expect_err("must error");
        assert!(matches!(
            err,
            Error::MissingVersion(version) if version == expected_missing_version
        ));
    }

    #[test]
    fn test_build_snapshot_based_preserves_explicit_reversed_range_error() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let err = CommitRange::builder_from(snapshot, 1)
            .with_end_version(0)
            .build(&engine)
            .expect_err("must error");
        assert!(matches!(
            err,
            Error::Generic(message)
                if message.contains("start_version (1) must be <= end_version (0)")
        ));
    }

    #[test]
    fn test_build_snapshot_based_reports_start_trimmed_by_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let table_root = Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();

        let err = CommitRange::builder_from(snapshot, 1)
            .build(&engine)
            .expect_err("commit at version 1 must be unavailable after checkpoint filtering");
        assert!(matches!(err, Error::MissingVersion(1)));
    }

    #[test]
    fn test_build_snapshot_based_with_explicit_end_version() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let range = CommitRange::builder_from(snapshot, 0)
            .with_end_version(0)
            .build(&engine)
            .unwrap();
        assert_eq!(
            range.end_version(),
            0,
            "explicit end_version must be honored",
        );
    }

    #[test]
    fn test_build_path_based_without_end_version_extends_to_latest() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .build(&engine)
            .unwrap();
        assert_eq!(range.start_version(), 0);
        assert_eq!(
            range.end_version(),
            1,
            "table-with-dv-small latest commit is v=1"
        );
    }

    #[test]
    fn test_build_catalog_managed_range_includes_unpublished_staged_commit() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_log_tail(vec![staged_commit(&table_root, 1)], 1)
            .build(&engine)
            .unwrap();

        assert_eq!(range.end_version(), 1);
        assert_eq!(range.commit_files.len(), 2);
        assert_eq!(range.commit_files[1].version, 1);
        assert_eq!(
            range.commit_files[1].file_type,
            LogPathFileType::StagedCommit
        );
    }

    #[test]
    fn test_build_snapshot_based_merges_catalog_log_tail_without_listing() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(0)
            .build(&engine)
            .unwrap();
        let engine = DelegatingEngine::new(Arc::new(engine))
            .with_storage_handler(Arc::new(NoIoStorageHandler));
        let range = CommitRange::builder_from(snapshot, 0)
            .with_log_tail(vec![staged_commit(&table_root, 1)], 1)
            .build(&engine)
            .unwrap();

        assert_eq!(range.end_version(), 1);
        assert_eq!(range.commit_files.len(), 2);
        assert_eq!(
            range.commit_files[1].file_type,
            LogPathFileType::StagedCommit
        );
    }

    #[rstest::rstest]
    #[case::extends_snapshot(None, 2)]
    #[case::trims_tail(Some(1), 1)]
    fn test_build_snapshot_based_catalog_tail_supersedes_published_commit(
        #[case] end_version: Option<Version>,
        #[case] expected_end: Version,
    ) {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let range = CommitRange::builder_from(snapshot, 0)
            .with_log_tail(
                vec![staged_commit(&table_root, 1), staged_commit(&table_root, 2)],
                2,
            )
            .fold_with(end_version, CommitRangeBuilder::with_end_version)
            .build(&engine)
            .unwrap();

        assert_eq!(range.end_version(), expected_end);
        assert_eq!(range.commit_files.len(), expected_end as usize + 1);
        assert_eq!(
            range.commit_files[1].file_type,
            LogPathFileType::StagedCommit
        );
        assert_eq!(range.commit_files[1].version, 1);
        assert_eq!(range.commit_files.last().unwrap().version, expected_end);
    }

    #[rstest::rstest]
    #[case::tail_covers_explicit_end(Some(1), 2, None)]
    #[case::tail_shorter_than_explicit_end(
        Some(2),
        2,
        Some("Log tail version 1 is less than requested version 2")
    )]
    #[case::tail_does_not_reach_max_catalog_version(
        None,
        2,
        Some("Log tail version 1 does not match max catalog version 2")
    )]
    fn test_catalog_log_tail_end_version_validation(
        #[case] end_version: Option<Version>,
        #[case] max_catalog_version: Version,
        #[case] expected_error: Option<&str>,
    ) {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let result = CommitRange::builder_for(table_root.as_str(), 0)
            .with_log_tail(vec![staged_commit(&table_root, 1)], max_catalog_version)
            .fold_with(end_version, CommitRangeBuilder::with_end_version)
            .build(&engine);

        if let Some(expected_error) = expected_error {
            assert!(matches!(
                result.unwrap_err(),
                Error::MaxCatalogVersion(message) if message.contains(expected_error)
            ));
        } else {
            let range = result.unwrap();
            assert_eq!(range.end_version(), 1);
            assert_eq!(range.commit_files.len(), 2);
        }
    }

    #[test]
    fn test_max_catalog_version_bounds_filesystem_commits() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_max_catalog_version(0)
            .build(&engine)
            .unwrap();

        assert_eq!(range.end_version(), 0);
        assert_eq!(range.commit_files.len(), 1);
        assert_eq!(range.commit_files[0].version, 0);
    }

    #[test]
    fn test_catalog_log_tail_must_be_contiguous() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let err = CommitRange::builder_for(table_root.as_str(), 0)
            .with_log_tail(
                vec![staged_commit(&table_root, 0), staged_commit(&table_root, 2)],
                2,
            )
            .build(&engine)
            .unwrap_err();

        assert!(matches!(err, Error::LogTailVersionsNotContiguous { .. }));
    }

    #[test]
    fn test_catalog_log_tail_must_contain_only_staged_commits() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let published = LogPath::try_new(FileMeta::new(
            table_root
                .join("_delta_log/00000000000000000001.json")
                .unwrap(),
            0,
            1,
        ))
        .unwrap();
        let err = CommitRange::builder_for(table_root.as_str(), 0)
            .with_log_tail(vec![published], 1)
            .build(&engine)
            .unwrap_err();

        assert!(matches!(err, Error::Generic(message) if message.contains("only staged")));
    }

    #[test]
    fn test_catalog_version_bounds_are_validated() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let start_err = CommitRange::builder_for(table_root.as_str(), 2)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap_err();
        assert!(matches!(start_err, Error::MaxCatalogVersion(_)));

        let end_err = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(2)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap_err();
        assert!(matches!(end_err, Error::MaxCatalogVersion(_)));
    }

    #[test]
    fn test_build_descending_ordering_yields_commits_in_reverse_order() {
        let table_root = dv_small_table_root();
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let actions = [DeltaAction::Add, DeltaAction::Remove];

        let asc_range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .build(engine.as_ref())
            .unwrap();
        let desc_range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .with_ordering(CommitOrdering::DescendingOrder)
            .build(engine.as_ref())
            .unwrap();

        let snapshot_at_start = Snapshot::builder_for(table_root.as_str())
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();
        let snapshot_at_end = Snapshot::builder_for(table_root.as_str())
            .at_version(1)
            .build(engine.as_ref())
            .unwrap();

        let asc_versions = asc_range
            .commits(engine.clone(), Some(snapshot_at_start), &actions)
            .unwrap()
            .map(|c| c.unwrap().version())
            .collect::<Vec<_>>();
        let desc_versions = desc_range
            .commits(engine, Some(snapshot_at_end), &actions)
            .unwrap()
            .map(|c| c.unwrap().version())
            .collect::<Vec<_>>();

        assert_eq!(asc_versions, vec![0, 1]);
        assert_eq!(desc_versions, vec![1, 0]);
    }
}
