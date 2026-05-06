use url::Url;

use crate::commit_range::{CommitBoundary, CommitRange};
use crate::log_segment::LogSegment;
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, Error, Version};

/// Builder for a [`CommitRange`].
///
/// Created via [`CommitRange::builder_for`] (path-based) or
/// [`CommitRange::builder_from`] (snapshot-based). Supports configuring an end boundary
/// and the commit ordering. [`Self::build`] performs boundary resolution, delta-log
/// listing, and contiguity validation.
///
/// TODO: support UC catalog commit via with_log_tail(self, Vec<LogPath>) and
/// with_max_catalog_version(self, Version)
pub struct CommitRangeBuilder {
    pub(crate) table_root: String,
    pub(crate) start_boundary: CommitBoundary,
    pub(crate) end_boundary: Option<CommitBoundary>,
    pub(crate) snapshot: Option<SnapshotRef>,
    pub(crate) commit_ordering: CommitOrdering,
}

impl CommitRangeBuilder {
    pub(crate) fn new_for(table_root: impl AsRef<str>, start_boundary: CommitBoundary) -> Self {
        CommitRangeBuilder {
            table_root: table_root.as_ref().to_string(),
            start_boundary,
            end_boundary: None,
            snapshot: None,
            commit_ordering: CommitOrdering::AscendingOrder,
        }
    }

    pub(crate) fn new_from(snapshot: SnapshotRef, start_boundary: CommitBoundary) -> Self {
        CommitRangeBuilder {
            table_root: snapshot.table_root().to_string(),
            start_boundary,
            end_boundary: None,
            snapshot: Some(snapshot.clone()),
            commit_ordering: CommitOrdering::AscendingOrder,
        }
    }

    /// Pin the end of the range. Without this, the range extends to the latest committed
    /// version observed at build time.
    pub fn with_end_boundary(mut self, end_boundary: CommitBoundary) -> Self {
        self.end_boundary = Some(end_boundary);
        self
    }

    /// Set the order in which [`CommitRange::commits`] and [`CommitRange::actions`] yield
    /// commits. Defaults to [`CommitOrdering::AscendingOrder`].
    pub fn with_ordering(mut self, commit_ordering: CommitOrdering) -> Self {
        self.commit_ordering = commit_ordering;
        self
    }

    /// Resolve boundaries, list `_delta_log/`, validate contiguity, and produce a
    /// [`CommitRange`]. Performs filesystem listing but no JSON reads.
    ///
    /// Returns an error if the resolved version range is invalid (start > end), the
    /// listed commits are non-contiguous, or the requested start version is not present
    /// on the filesystem.
    ///
    /// Currently supports only [`CommitBoundary::Version`] without catalog log tail or
    /// `max_catalog_version`. Calls that supplied either via the builder will error.
    pub fn build(&self, engine: &dyn Engine) -> DeltaResult<CommitRange> {
        let table_root = Self::parse_table_root(&self.table_root)?;
        let log_root = table_root.join("_delta_log/")?;

        let CommitBoundary::Version(start_version) = self.start_boundary;
        let end_version = self.end_boundary.map(|boundary| match boundary {
            CommitBoundary::Version(v) => v,
        });

        // Snapshot-derived ranges skip per-batch protocol validation because the snapshot's
        // log segment is bounded above by the snapshot's version, and protocol immutability
        // guarantees the snapshot's protocol covers every feature live in earlier commits in
        // the range. This invariant must be re-examined if `with_log_tail` or any future
        // option allows extending past the snapshot's version.
        let (log_segment, validate_protocol) = match &self.snapshot {
            Some(snapshot) => (snapshot.log_segment().clone(), false),
            None => (
                LogSegment::for_table_changes(
                    engine.storage_handler().as_ref(),
                    log_root,
                    start_version,
                    end_version,
                )?,
                true,
            ),
        };

        let end_version = end_version.unwrap_or(log_segment.end_version);

        let mut commit_files = log_segment.listed.ascending_commit_files;

        validate_version_range(start_version, end_version)?;
        validate_number_of_commit_files(start_version, end_version, commit_files.len())?;

        if matches!(self.commit_ordering, CommitOrdering::DescendingOrder) {
            commit_files.reverse();
        }

        Ok(CommitRange {
            table_root,
            commit_files,
            start_version,
            end_version,
            start_boundary: self.start_boundary,
            end_boundary: self.end_boundary,
            validate_protocol,
        })
    }

    /// Parse the stored table-root string into a [`Url`].
    fn parse_table_root(table_root: &str) -> DeltaResult<Url> {
        crate::try_parse_uri(table_root)
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

/// Validate that `start_version <= end_version` when an end version is provided.
fn validate_version_range(start: Version, end: Version) -> DeltaResult<()> {
    if start > end {
        return Err(Error::generic(format!(
            "start_version ({start}) must be <= end_version ({end})",
        )));
    }

    Ok(())
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
        let range = CommitRange::builder_for(table_root.as_str(), CommitBoundary::Version(0))
            .with_end_boundary(CommitBoundary::Version(1))
            .build(&engine)
            .unwrap();

        assert_eq!(range.start_version(), 0);
        assert_eq!(range.end_version(), 1);
        assert_eq!(range.table_root().as_str(), table_root.as_str());
        assert!(matches!(range.start_boundary(), CommitBoundary::Version(0)));
        assert!(matches!(
            range.end_boundary(),
            Some(CommitBoundary::Version(1)),
        ));
    }

    #[test]
    fn build_snapshot_based_succeeds() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        let snapshot_version = snapshot.version();

        let range = CommitRange::builder_from(snapshot, CommitBoundary::Version(0))
            .build(&engine)
            .unwrap();

        // Without an explicit end_boundary, the range resolves to the snapshot's version.
        assert_eq!(range.start_version(), 0);
        assert_eq!(range.end_version(), snapshot_version);
        assert!(matches!(range.start_boundary(), CommitBoundary::Version(0)));
        assert!(range.end_boundary().is_none());
    }

    #[test]
    fn build_errors_on_start_past_snapshot_version() {
        let table_root = dv_small_table_root();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .build(&engine)
            .unwrap();
        // The snapshot's log segment ends at snapshot.version() (= 1). Requesting
        // start_version > snapshot.version() makes validate_version_range fire.
        let err = CommitRange::builder_from(snapshot, CommitBoundary::Version(5))
            .build(&engine)
            .expect_err("start past snapshot version must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("start_version (5)") && msg.contains("end_version"),
            "expected start>end error, got: {msg}",
        );
    }
}
