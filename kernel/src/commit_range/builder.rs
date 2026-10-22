use delta_kernel_derive::internal_api;
use url::Url;

use crate::commit_range::CommitRange;
use crate::log_segment::{
    validate_catalog_managed_log_tail, validate_commit_file_types,
    validate_commit_files_contiguous, validate_commit_files_sorted,
    validate_start_version_available,
};
use crate::log_segment_files::LogSegmentFiles;
use crate::path::ParsedLogPath;
use crate::snapshot::SnapshotRef;
use crate::utils::require;
use crate::{Engine, KernelError, KernelResult, LogPath, Version};

/// Builder for a [`CommitRange`].
///
/// Created via [`CommitRange::builder_for`] (path-based) or
/// [`CommitRange::builder_from`] (snapshot-based). Supports configuring an end version
/// and the commit ordering. Catalog-managed tables also supply the catalog-ratified log tail and
/// maximum catalog version. [`Self::build`] lists the log for a path-based builder or reuses the
/// commit-file metadata in a snapshot-based builder, then checks that the range is complete unless
/// a path-based builder disabled validation with `with_commit_range_validation`. Disabling
/// validation is only for ascending raw commit-file iteration with `CommitRange::commit_files`;
/// [`CommitRange::commits`] is unavailable on such ranges.
pub struct CommitRangeBuilder {
    table_root: String,
    start_version: Version,
    end_version: Option<Version>,
    snapshot: Option<SnapshotRef>,
    commit_ordering: CommitOrdering,
    log_tail: Vec<ParsedLogPath>,
    max_catalog_version: Option<Version>,
    commit_range_validation: bool,
    fail_on_data_loss: bool,
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
            commit_range_validation: true,
            fail_on_data_loss: true,
        }
    }

    pub(crate) fn new_from(snapshot: SnapshotRef, start_version: Version) -> Self {
        CommitRangeBuilder {
            table_root: snapshot.table_root().to_string(),
            start_version,
            end_version: None,
            snapshot: Some(snapshot),
            commit_ordering: CommitOrdering::AscendingOrder,
            log_tail: Vec::new(),
            max_catalog_version: None,
            commit_range_validation: true,
            fail_on_data_loss: true,
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

    /// Set the catalog-ratified commits and maximum catalog-ratified version. The entries may be
    /// staged or published commits and must be sorted in ascending, contiguous version order.
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>, max_catalog_version: Version) -> Self {
        self.log_tail = log_tail.into_iter().map(Into::into).collect();
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Set the maximum version ratified by the catalog. Filesystem commits beyond this version are
    /// ignored.
    pub fn with_max_catalog_version(mut self, max_catalog_version: Version) -> Self {
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Enable or disable build-time commit range validation. Defaults to `true`.
    ///
    /// When `true`, [`Self::build`] requires the requested start version to be present and the
    /// listed commits to be contiguous. When `false`, [`Self::build`] materializes whatever
    /// commits are listed in the range without validating the start version, contiguity, or
    /// end version. Disabling validation is only supported for path-based builders
    /// ([`CommitRange::builder_for`]) with ascending ordering.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn with_commit_range_validation(mut self, commit_range_validation: bool) -> Self {
        self.commit_range_validation = commit_range_validation;
        self
    }

    /// Set whether iterating the built range's commit files fails on missing versions. Defaults
    /// to `true`. Only affects ranges built with commit range validation disabled.
    ///
    /// When `true`, a missing start version or a gap before the end version surfaces as an error
    /// when `CommitRange::commit_files` reaches it. Without an explicit end version, the range ends
    /// at the last listed commit, so a trailing gap cannot be detected. When `false`, available
    /// commit files are yielded across gaps.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn with_fail_on_data_loss(mut self, fail_on_data_loss: bool) -> Self {
        self.fail_on_data_loss = fail_on_data_loss;
        self
    }

    /// Resolve commit-file metadata, validate contiguity (by default), and produce a
    /// [`CommitRange`]. A path-based builder lists `_delta_log/`; a snapshot-based builder reuses
    /// the snapshot's log segment. Neither path reads commit JSON.
    ///
    /// Returns [`KernelError::MissingVersion`] if a snapshot-derived range requires a commit beyond
    /// what is available from the snapshot's log segment and the supplied catalog tail, or if the
    /// listed commits are non-contiguous. Returns [`KernelError::StartVersionNotFound`] (carrying
    /// the earliest still-available version) if the requested start is unavailable but later
    /// versions exist, [`KernelError::EmptyLog`] if nothing is available in the requested range at
    /// all, and [`KernelError::CommitRange`] if the resolved version range is invalid
    /// (start > end).
    pub fn build(self, engine: &dyn Engine) -> KernelResult<CommitRange> {
        self.validate_supported_modes()?;

        let table_root = Self::parse_table_root(&self.table_root)?;
        let log_root = table_root.join("_delta_log/")?;
        self.validate_inputs()?;
        let CommitRangeBuilder {
            table_root: _,
            start_version,
            end_version,
            snapshot,
            commit_ordering,
            log_tail,
            max_catalog_version,
            commit_range_validation,
            fail_on_data_loss,
        } = self;
        let configured_end_version = end_version.or(max_catalog_version);
        let (mut commit_files, end_version) = match snapshot {
            Some(snapshot) => Self::resolve_snapshot_files(
                snapshot,
                log_tail,
                start_version,
                configured_end_version,
            )?,
            None => Self::list_commit_files(
                engine,
                &log_root,
                log_tail,
                start_version,
                configured_end_version,
            )?,
        };

        validate_commit_file_types(&commit_files)?;
        if commit_range_validation {
            Self::validate_complete_range(start_version, end_version, &commit_files)?;
        }

        if commit_ordering == CommitOrdering::DescendingOrder {
            commit_files.reverse();
        }

        Ok(CommitRange {
            table_root,
            commit_files,
            start_version,
            end_version,
            commit_ordering,
            supports_commit_actions: commit_range_validation,
            supports_commit_files: commit_ordering == CommitOrdering::AscendingOrder,
            fail_on_data_loss,
        })
    }

    fn validate_supported_modes(&self) -> KernelResult<()> {
        if self.snapshot.is_some() && !self.commit_range_validation {
            return Err(KernelError::CommitRange(
                "Disabling commit range validation is only supported for path-based commit ranges"
                    .to_string(),
            ));
        }
        if self.commit_ordering == CommitOrdering::DescendingOrder && !self.commit_range_validation
        {
            return Err(KernelError::CommitRange(
                "Disabling commit range validation requires ascending commit ordering".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_inputs(&self) -> KernelResult<()> {
        if let Some(end_version) = self.end_version {
            validate_version_range(self.start_version, end_version)?;
        }
        self.validate_catalog_managed_inputs()
    }

    fn resolve_snapshot_files(
        snapshot: SnapshotRef,
        log_tail: Vec<ParsedLogPath>,
        start_version: Version,
        configured_end_version: Option<Version>,
    ) -> KernelResult<(Vec<ParsedLogPath>, Version)> {
        let log_segment = snapshot.log_segment();
        let end_version = configured_end_version.unwrap_or(log_segment.end_version);
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
        Ok((commit_files, end_version))
    }

    fn list_commit_files(
        engine: &dyn Engine,
        log_root: &Url,
        log_tail: Vec<ParsedLogPath>,
        start_version: Version,
        configured_end_version: Option<Version>,
    ) -> KernelResult<(Vec<ParsedLogPath>, Version)> {
        let commit_files = LogSegmentFiles::list_commits(
            engine.storage_handler().as_ref(),
            log_root,
            log_tail,
            Some(start_version),
            configured_end_version,
            None,
        )?
        .ascending_commit_files;
        let last_version = commit_files.last().ok_or(KernelError::EmptyLog)?.version;
        let end_version = configured_end_version.unwrap_or(last_version);
        Ok((commit_files, end_version))
    }

    fn validate_complete_range(
        start_version: Version,
        end_version: Version,
        commit_files: &[ParsedLogPath],
    ) -> KernelResult<()> {
        validate_start_version_available(start_version, commit_files.first())?;
        validate_commit_files_sorted(commit_files)?;
        validate_commit_files_contiguous(commit_files)?;
        let last_version = commit_files.last().ok_or(KernelError::EmptyLog)?.version;
        validate_terminal_commit_available(end_version, last_version)
    }

    fn validate_catalog_managed_inputs(&self) -> KernelResult<()> {
        if let Some(max_catalog_version) = self.max_catalog_version {
            require!(
                self.start_version <= max_catalog_version,
                KernelError::MaxCatalogVersion(format!(
                    "Start version {} exceeds max catalog version {max_catalog_version}",
                    self.start_version
                ))
            );
        }
        validate_catalog_managed_log_tail(
            self.end_version,
            self.max_catalog_version,
            &self.log_tail,
        )
    }

    /// Parse the stored table-root string into a [`Url`].
    fn parse_table_root(table_root: &str) -> KernelResult<Url> {
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

fn validate_version_range(start: Version, end: Version) -> KernelResult<()> {
    if start > end {
        return Err(KernelError::CommitRange(format!(
            "start_version ({start}) must be <= end_version ({end})",
        )));
    }

    Ok(())
}

fn validate_terminal_commit_available(end: Version, available_end: Version) -> KernelResult<()> {
    if available_end < end {
        let missing_version = available_end.checked_add(1).ok_or_else(|| {
            KernelError::internal_error(
                "Commit version overflow while validating the end of a commit range",
            )
        })?;
        return Err(KernelError::MissingVersion(missing_version));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::commit_range::DeltaAction;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::path::LogPathFileType;
    use crate::utils::FoldWithOption as _;
    use crate::{Engine, FileMeta, KernelResultIterator, LogPath, Snapshot, StorageHandler};

    struct NoIoStorageHandler;

    impl StorageHandler for NoIoStorageHandler {
        fn list_from(&self, _path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
            panic!("snapshot-based commit ranges must not list storage");
        }

        fn read_files(
            &self,
            _files: Vec<crate::FileSlice>,
        ) -> KernelResult<Box<dyn KernelResultIterator<bytes::Bytes>>> {
            panic!("commit range construction must not read files");
        }

        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> KernelResult<()> {
            panic!("unexpected copy");
        }

        fn put(&self, _path: &Url, _data: bytes::Bytes, _overwrite: bool) -> KernelResult<()> {
            panic!("unexpected write");
        }

        fn head(&self, _path: &Url) -> KernelResult<FileMeta> {
            panic!("unexpected head");
        }

        fn delete(&self, _path: &Url) -> KernelResult<()> {
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

    #[test]
    fn test_build_snapshot_based_rejects_disabled_validation() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();

        let result = CommitRange::builder_from(snapshot, 0)
            .with_commit_range_validation(false)
            .build(&engine);

        assert!(
            matches!(result, Err(KernelError::CommitRange(_))),
            "expected CommitRange error"
        );
        assert_result_error_with_message(result, "only supported for path-based commit ranges");
    }

    #[test]
    fn test_build_path_based_rejects_disabled_validation_with_descending_ordering() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let result = CommitRange::builder_for(table_root.as_str(), 0)
            .with_ordering(CommitOrdering::DescendingOrder)
            .with_commit_range_validation(false)
            .build(&engine);

        assert!(
            matches!(result, Err(KernelError::CommitRange(_))),
            "expected CommitRange error"
        );
        assert_result_error_with_message(result, "requires ascending commit ordering");
    }

    #[test]
    fn test_build_relaxed_errors_when_no_commit_is_listed() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let result = CommitRange::builder_for(table_root.as_str(), 5)
            .with_commit_range_validation(false)
            .build(&engine);

        assert!(matches!(result, Err(KernelError::EmptyLog)));
    }

    #[rstest::rstest]
    #[case::start_past_snapshot_version(5, None, None)]
    #[case::end_past_snapshot_version(0, Some(99), Some(2))]
    #[case::start_error_precedes_end_error(5, Some(99), None)]
    fn test_build_snapshot_based_reports_unavailable_version(
        #[case] start: Version,
        #[case] end: Option<Version>,
        #[case] expected_missing_version: Option<Version>,
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
        match expected_missing_version {
            Some(version) => {
                assert!(matches!(err, KernelError::MissingVersion(v) if v == version));
            }
            None => assert!(matches!(err, KernelError::EmptyLog)),
        }
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
            KernelError::CommitRange(message)
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
        assert!(matches!(
            err,
            KernelError::StartVersionNotFound {
                requested: 1,
                earliest: 3
            }
        ));
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
    #[case::range_ends_at_tail(None)]
    #[case::range_ends_before_tail(Some(2))]
    fn test_build_snapshot_based_reports_catalog_tail_gap_as_missing_version(
        #[case] end_version: Option<Version>,
    ) {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let err = CommitRange::builder_from(snapshot, 0)
            .with_log_tail(vec![staged_commit(&table_root, 3)], 3)
            .fold_with(end_version, CommitRangeBuilder::with_end_version)
            .build(&engine)
            .unwrap_err();

        assert!(matches!(err, KernelError::MissingVersion(2)));
    }

    #[rstest::rstest]
    #[case::duplicate_versions(&[0, 0, 1])]
    #[case::out_of_order(&[0, 2, 1])]
    fn test_validate_complete_range_rejects_unsorted_commits(#[case] versions: &[Version]) {
        let table_root = dv_small_table_root();
        let commit_files = versions
            .iter()
            .map(|version| ParsedLogPath::from(staged_commit(&table_root, *version)))
            .collect::<Vec<_>>();

        assert!(matches!(
            CommitRangeBuilder::validate_complete_range(0, 1, &commit_files),
            Err(KernelError::InvalidLogSegment(_))
        ));
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
                KernelError::MaxCatalogVersion(message) if message.contains(expected_error)
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

        assert!(matches!(
            err,
            KernelError::LogTailVersionsNotContiguous { .. }
        ));
    }

    #[rstest::rstest]
    fn test_catalog_log_tail_accepts_published_commits(#[values(false, true)] from_snapshot: bool) {
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
        let builder = if from_snapshot {
            let snapshot = Snapshot::builder_for(table_root.as_str())
                .at_version(0)
                .build(&engine)
                .unwrap();
            CommitRange::builder_from(snapshot, 0)
        } else {
            CommitRange::builder_for(table_root.as_str(), 0)
        };

        let range = builder
            .with_log_tail(vec![published], 1)
            .build(&engine)
            .unwrap();

        assert_eq!(range.end_version(), 1);
        assert_eq!(range.commit_files.len(), 2);
        assert_eq!(range.commit_files[1].file_type, LogPathFileType::Commit);
    }

    #[test]
    fn test_catalog_log_tail_rejects_non_commit_files() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let crc = LogPath::try_new(FileMeta::new(
            table_root
                .join("_delta_log/00000000000000000001.crc")
                .unwrap(),
            0,
            1,
        ))
        .unwrap();
        let err = CommitRange::builder_for(table_root.as_str(), 0)
            .with_log_tail(vec![crc], 1)
            .build(&engine)
            .unwrap_err();

        assert!(matches!(err, KernelError::InvalidLogPath(_)));
    }

    #[test]
    fn test_catalog_version_bounds_are_validated() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let start_err = CommitRange::builder_for(table_root.as_str(), 2)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap_err();
        assert!(matches!(start_err, KernelError::MaxCatalogVersion(_)));

        let end_err = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(2)
            .with_max_catalog_version(1)
            .build(&engine)
            .unwrap_err();
        assert!(matches!(end_err, KernelError::MaxCatalogVersion(_)));
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
