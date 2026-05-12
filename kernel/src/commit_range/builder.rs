use url::Url;

use crate::commit_range::CommitRange;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, Error, Version};

/// Builder for a [`CommitRange`].
///
/// Created via [`CommitRange::builder_for`] (path-based) or
/// [`CommitRange::builder_from`] (snapshot-based). Supports configuring an end version
/// and the commit ordering. [`Self::build`] performs delta-log listing and contiguity
/// validation.
///
/// TODO: support UC catalog commit via `with_log_tail(self, Vec<LogPath>)` and
/// `with_max_catalog_version(self, Version)`
pub struct CommitRangeBuilder {
    table_root: String,
    start_version: Version,
    end_version: Option<Version>,
    snapshot: Option<SnapshotRef>,
    commit_ordering: CommitOrdering,
}

impl CommitRangeBuilder {
    pub(crate) fn new_for(table_root: impl AsRef<str>, start_version: Version) -> Self {
        CommitRangeBuilder {
            table_root: table_root.as_ref().to_string(),
            start_version,
            end_version: None,
            snapshot: None,
            commit_ordering: CommitOrdering::AscendingOrder,
        }
    }

    pub(crate) fn new_from(snapshot: SnapshotRef, start_version: Version) -> Self {
        CommitRangeBuilder {
            table_root: snapshot.table_root().to_string(),
            start_version,
            end_version: None,
            snapshot: Some(snapshot.clone()),
            commit_ordering: CommitOrdering::AscendingOrder,
        }
    }

    /// Pin the end of the range. Without this, the range extends to the latest committed
    /// version observed at build time.
    pub fn with_end_version(mut self, end_version: Version) -> Self {
        self.end_version = Some(end_version);
        self
    }

    /// Set the order in which [`CommitRange::commits`] and [`CommitRange::actions`] yield
    /// commits. Defaults to [`CommitOrdering::AscendingOrder`].
    pub fn with_ordering(mut self, commit_ordering: CommitOrdering) -> Self {
        self.commit_ordering = commit_ordering;
        self
    }

    /// List `_delta_log/`, validate contiguity, and produce a [`CommitRange`]. Performs
    /// filesystem listing but no JSON reads.
    ///
    /// Returns an error if the resolved version range is invalid (start > end), the
    /// listed commits are non-contiguous, or the requested start version is not present
    /// on the filesystem.
    pub fn build(&self, engine: &dyn Engine) -> DeltaResult<CommitRange> {
        let table_root = Self::parse_table_root(&self.table_root)?;
        let log_root = table_root.join("_delta_log/")?;

        let start_version = self.start_version;
        let end_version = self.end_version;

        let log_segment = match &self.snapshot {
            Some(snapshot) => snapshot.log_segment().clone(),
            None => LogSegment::for_table_changes(
                engine.storage_handler().as_ref(),
                log_root,
                start_version,
                end_version,
            )?,
        };

        let end_version = end_version.unwrap_or(log_segment.end_version);

        // Avoid misleading error if snapshot-derived range extends past the snapshot version.
        // Without this you get the error from `validate_number_of_commit_files`.
        if self.snapshot.is_some() && end_version > log_segment.end_version {
            return Err(Error::generic(format!(
                "end_version ({end_version}) cannot exceed snapshot version ({})",
                log_segment.end_version
            )));
        }

        // Snapshot's log segment may extend past [start, end]; filter to the requested range.
        let mut commit_files: Vec<ParsedLogPath> = log_segment
            .listed
            .ascending_commit_files
            .into_iter()
            .filter(|f| f.version >= start_version && f.version <= end_version)
            .collect();

        validate_version_range(start_version, end_version)?;
        if self.snapshot.is_some() {
            validate_start_version_available(
                start_version,
                commit_files.first(),
                log_segment.checkpoint_version,
            )?;
        }
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

    /// Parse the stored table-root string into a [`Url`].
    fn parse_table_root(table_root: &str) -> DeltaResult<Url> {
        crate::utils::try_parse_uri(table_root)
    }
}

/// Direction in which [`CommitRange::commits`] / [`CommitRange::actions`] yield commits.
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

/// Ensure `start_version` is the first commit in the snapshot's log segment; otherwise
/// surface the earliest available commit so the caller knows where the gap begins.
fn validate_start_version_available(
    start_version: Version,
    first_commit: Option<&ParsedLogPath>,
    checkpoint_version: Option<Version>,
) -> DeltaResult<()> {
    if first_commit.map(|f| f.version) == Some(start_version) {
        return Ok(());
    }
    let earliest_available_commit = checkpoint_version
        .map(|v| v + 1)
        .or_else(|| first_commit.map(|f| f.version))
        .ok_or_else(|| {
            Error::generic("snapshot's log segment must have a checkpoint or at least one commit")
        })?;
    Err(Error::generic(format!(
        "start_version {start_version} is not available in the snapshot's log segment \
         (earliest available commit: {earliest_available_commit})",
    )))
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

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::Snapshot;

    /// `table-with-dv-small` has versions 0 and 1 (snapshot version = 1).
    fn dv_small_table_root() -> Url {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        Url::from_directory_path(path).unwrap()
    }

    #[test]
    fn build_path_based_succeeds() {
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
    fn build_snapshot_based_succeeds() {
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

    /// Snapshot-based ranges must reject any version that lands past the snapshot's
    /// version. Both the start-past-snapshot and end-past-snapshot cases surface as a
    /// generic error before the iterator is constructed.
    #[rstest::rstest]
    #[case::start_past_snapshot_version(
        5,
        None,
        &["start_version (5)", "end_version"],
    )]
    #[case::end_past_snapshot_version(
        0,
        Some(99),
        &["99", "snapshot"],
    )]
    fn build_errors_on_version_past_snapshot_version(
        #[case] start: Version,
        #[case] end: Option<Version>,
        #[case] expected_substrings: &[&str],
    ) {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let mut builder = CommitRange::builder_from(snapshot, start);
        if let Some(end) = end {
            builder = builder.with_end_version(end);
        }
        let err = builder.build(&engine).expect_err("must error");
        let msg = format!("{err}");
        for needle in expected_substrings {
            assert!(
                msg.contains(needle),
                "expected message to contain {needle:?}, got: {msg}",
            );
        }
    }

    #[test]
    fn build_snapshot_based_with_explicit_end_version_is_honored() {
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
    fn build_path_based_without_end_version_extends_to_latest() {
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
    fn build_descending_ordering_yields_commits_in_reverse_order() {
        use crate::commit_range::DeltaAction;

        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let actions = [DeltaAction::Add, DeltaAction::Remove];

        let asc_range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .build(&engine)
            .unwrap();
        let desc_range = CommitRange::builder_for(table_root.as_str(), 0)
            .with_end_version(1)
            .with_ordering(CommitOrdering::DescendingOrder)
            .build(&engine)
            .unwrap();

        let snapshot_at_start = Snapshot::builder_for(table_root.as_str())
            .at_version(0)
            .build(&engine)
            .unwrap();
        let snapshot_at_end = Snapshot::builder_for(table_root.as_str())
            .at_version(1)
            .build(&engine)
            .unwrap();

        let asc_versions = asc_range
            .commits(&engine, snapshot_at_start, &actions)
            .unwrap()
            .map(|c| c.unwrap().version())
            .collect::<Vec<_>>();
        let desc_versions = desc_range
            .commits(&engine, snapshot_at_end, &actions)
            .unwrap()
            .map(|c| c.unwrap().version())
            .collect::<Vec<_>>();

        assert_eq!(asc_versions, vec![0, 1]);
        assert_eq!(desc_versions, vec![1, 0]);
    }
}
