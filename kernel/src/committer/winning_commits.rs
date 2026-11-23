//! Validated file descriptors for winning commits after a version conflict.

use std::ops::Deref;

use url::Url;

use super::WinningCommit;
use crate::log_path::LogPath;
use crate::log_segment::validate_complete_commit_range;
use crate::path::{LogRoot, ParsedLogPath};
use crate::{KernelError, KernelResult, Version};

/// Accepted commit files covering an inclusive version range without gaps.
///
/// Construct this value with [`Self::try_new`] after discovering published and catalog-ratified
/// commits. Construction checks file locations, ordering, and coverage of the observed end version
/// without reading commit actions. The caller establishes catalog acceptance; listing staged
/// proposals alone does not establish that they were ratified.
///
/// Descriptors are immutable and independent of the transaction and committer. Read the actions
/// separately using their published or ratified staged paths. Slice access never performs I/O.
#[derive(Debug, Clone, PartialEq)]
pub struct WinningCommits {
    table_root: Url,
    commits: Vec<WinningCommit>,
    start_version: Version,
    end_version: Version,
}

impl WinningCommits {
    /// Validates accepted commit descriptors from the rejected version through the observed end.
    ///
    /// Entries must belong to this table and already be in ascending order. Every version in the
    /// inclusive range must have an individual published or ratified staged commit file. Missing
    /// history, duplicate versions, reversed bounds, and non-commit paths are errors.
    pub fn try_new(
        table_root: &Url,
        conflict_version: Version,
        end_version: Version,
        commits: Vec<WinningCommit>,
    ) -> KernelResult<Self> {
        let log_root = LogRoot::new(table_root.clone())?;
        let table_root = log_root.table_root();
        if conflict_version > end_version {
            return Err(KernelError::invalid_transaction_state(
                "Winning history ends before the rejected commit version",
            ));
        }
        let commit_files = validate_descriptor_paths(table_root, &commits)?;
        if commits
            .last()
            .is_some_and(|commit| commit.version > end_version)
        {
            return Err(KernelError::invalid_transaction_state(
                "Winning history extends beyond its observed end version",
            ));
        }
        validate_complete_commit_range(conflict_version, end_version, &commit_files)?;
        Ok(Self {
            table_root: table_root.clone(),
            commits,
            start_version: conflict_version,
            end_version,
        })
    }

    /// The rejected commit version, included in this history.
    pub fn start_version(&self) -> Version {
        self.start_version
    }

    /// The observed end version, included in this history.
    pub fn end_version(&self) -> Version {
        self.end_version
    }

    /// Borrows the accepted commit descriptors in ascending order without performing I/O.
    pub fn commits(&self) -> &[WinningCommit] {
        &self.commits
    }

    /// Consumes this range and returns its accepted commit descriptors.
    pub fn into_commits(self) -> Vec<WinningCommit> {
        self.commits
    }

    /// The table whose accepted commit descriptors were validated during construction.
    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    /// Checks that this immutable history belongs to the requested table and rejected version.
    ///
    /// Construction already validated the descriptors and version coverage; this check performs
    /// no file reads or per-descriptor validation.
    pub fn validate_for_conflict(
        &self,
        table_root: &Url,
        conflict_version: Version,
    ) -> KernelResult<()> {
        let log_root = LogRoot::new(table_root.clone())?;
        if &self.table_root != log_root.table_root() {
            return Err(KernelError::internal_error(
                "Committer returned winning history for a different table",
            ));
        }
        if self.start_version != conflict_version {
            return Err(KernelError::StartVersionNotFound {
                requested: conflict_version,
                earliest: self.start_version,
            });
        }
        Ok(())
    }
}

impl Deref for WinningCommits {
    type Target = [WinningCommit];
    fn deref(&self) -> &Self::Target {
        self.commits()
    }
}

impl IntoIterator for WinningCommits {
    type Item = WinningCommit;
    type IntoIter = std::vec::IntoIter<WinningCommit>;
    fn into_iter(self) -> Self::IntoIter {
        self.commits.into_iter()
    }
}

impl<'a> IntoIterator for &'a WinningCommits {
    type Item = &'a WinningCommit;
    type IntoIter = std::slice::Iter<'a, WinningCommit>;
    fn into_iter(self) -> Self::IntoIter {
        self.commits.iter()
    }
}

fn validate_descriptor_paths(
    table_root: &Url,
    commits: &[WinningCommit],
) -> KernelResult<Vec<ParsedLogPath>> {
    commits
        .iter()
        .map(|commit| {
            let parsed: ParsedLogPath = commit.log_path.clone().into();
            // Reconstructing the canonical location also rejects checkpoints, paths from another
            // table, and descriptor versions that disagree with the filename.
            let expected = LogPath::from_commit_filename(
                table_root.clone(),
                commit.version,
                &parsed.filename,
                parsed.location.last_modified,
                parsed.location.size,
            )?;
            if expected != commit.log_path {
                return Err(KernelError::internal_error(
                    "Committer returned winning history for a different table",
                ));
            }
            Ok(parsed)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::FileMeta;

    #[rstest]
    #[case::valid(vec![11, 12], false, false, true)]
    #[case::empty(vec![], false, false, false)]
    #[case::wrong_start(vec![12], false, false, false)]
    #[case::gap(vec![11, 13], false, false, false)]
    #[case::duplicate(vec![11, 11], false, false, false)]
    #[case::descending(vec![11, 10], false, false, false)]
    #[case::wrong_table(vec![11], true, false, false)]
    #[case::checkpoint(vec![11], false, true, false)]
    fn validates_history_descriptors(
        #[case] versions: Vec<Version>,
        #[case] wrong_table: bool,
        #[case] checkpoint: bool,
        #[case] valid: bool,
    ) {
        let root = Url::parse("memory:///table/").unwrap();
        let location_root = if wrong_table {
            Url::parse("memory:///other/").unwrap()
        } else {
            root.clone()
        };
        let suffix = if checkpoint {
            "checkpoint.parquet"
        } else {
            "json"
        };
        let commits = versions
            .into_iter()
            .map(|version| WinningCommit {
                version,
                log_path: LogPath::try_new(FileMeta {
                    location: location_root
                        .join(&format!("_delta_log/{version:020}.{suffix}"))
                        .unwrap(),
                    last_modified: 0,
                    size: 0,
                })
                .unwrap(),
            })
            .collect::<Vec<_>>();
        let end_version = commits.last().map(|commit| commit.version).unwrap_or(11);
        assert_eq!(
            WinningCommits::try_new(&root, 11, end_version, commits).is_ok(),
            valid
        );
    }

    #[test]
    fn rejects_descriptor_version_that_disagrees_with_its_path() {
        let root = Url::parse("memory:///table/").unwrap();
        let commits = vec![WinningCommit {
            version: 11,
            log_path: LogPath::from_commit_filename(
                root.clone(),
                12,
                "00000000000000000012.json",
                0,
                0,
            )
            .unwrap(),
        }];
        assert!(WinningCommits::try_new(&root, 11, 11, commits).is_err());
    }
    #[rstest]
    #[case::complete(11, 12, &[11, 12], true)]
    #[case::missing_end(11, 13, &[11, 12], false)]
    #[case::past_end(11, 11, &[11, 12], false)]
    #[case::reversed_bounds(12, 11, &[12], false)]
    #[case::maximum_version(Version::MAX, Version::MAX, &[Version::MAX], true)]
    fn validates_observed_version_bounds(
        #[case] start: Version,
        #[case] end: Version,
        #[case] versions: &[Version],
        #[case] valid: bool,
    ) {
        let root = Url::parse("memory:///table/").unwrap();
        let commits = versions
            .iter()
            .map(|version| WinningCommit {
                version: *version,
                log_path: LogPath::from_commit_filename(
                    root.clone(),
                    *version,
                    &format!("{version:020}.json"),
                    0,
                    1,
                )
                .unwrap(),
            })
            .collect();
        let result = WinningCommits::try_new(&root, start, end, commits);
        assert_eq!(result.is_ok(), valid);
        if let Ok(history) = result {
            history.validate_for_conflict(&root, start).unwrap();
            history
                .validate_for_conflict(&Url::parse("memory:///table").unwrap(), start)
                .unwrap();
            assert_eq!(history.start_version(), start);
            assert_eq!(history.end_version(), end);
            assert_eq!(
                history
                    .commits()
                    .iter()
                    .map(|c| c.version)
                    .collect::<Vec<_>>(),
                versions
            );
            assert!(
                history
                    .validate_for_conflict(&Url::parse("memory:///other/").unwrap(), start)
                    .is_err()
            );
            if let Some(other_start) = start.checked_add(1) {
                assert!(history.validate_for_conflict(&root, other_start).is_err());
            }
            assert_eq!(history.into_commits().len(), versions.len());
        }
    }
}
