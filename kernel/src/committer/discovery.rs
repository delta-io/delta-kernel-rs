//! Lists published commits and merges ratified catalog history after a confirmed conflict.

use url::Url;

use super::{WinningCommit, WinningCommits};
use crate::log_segment::{validate_catalog_managed_log_tail, validate_catalog_version_bounds};
use crate::log_segment_files::LogSegmentFiles;
use crate::path::{LogRoot, ParsedLogPath};
use crate::utils::require;
use crate::{Engine, KernelError, KernelResult, LogPath, Version};

/// Lists published commits and merges the supplied ratified tail into an ascending, contiguous
/// winning range starting at `conflict_version`, without reading commit JSON.
///
/// `log_tail` contains catalog-ratified published or staged commits for this table in ascending,
/// contiguous order through the observed latest version. A nonempty tail replaces every filesystem
/// commit at or above its first version, including when `max_catalog_version` is `None`.
/// Staged commits require `max_catalog_version`; when a bound and a nonempty tail are supplied,
/// the tail must end at that bound. Files beyond the bound are ignored.
///
/// With a catalog bound, every individual commit through that version must be available.
/// Filesystem-only discovery ends at the last listed JSON commit. Checkpoints and compacted logs
/// cannot fill gaps, and listing the staged directory never establishes that a staged commit was
/// accepted.
pub fn list_published_commits_and_merge_ratified_tail(
    engine: &dyn Engine,
    table_root: &Url,
    conflict_version: Version,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
) -> KernelResult<WinningCommits> {
    let log_root = LogRoot::new(table_root.clone())?;
    let log_tail: Vec<ParsedLogPath> = log_tail.into_iter().map(Into::into).collect();
    validate_catalog_version_bounds(Some(conflict_version), max_catalog_version, None)?;
    validate_catalog_managed_log_tail(None, max_catalog_version, &log_tail)?;
    for path in &log_tail {
        // Reconstruct the expected location using the same filename rules as catalog responses.
        // This also rejects encoded separators and URL components accepted by path parsing alone.
        let expected: ParsedLogPath = LogPath::from_commit_filename(
            log_root.table_root().clone(),
            path.version,
            &path.filename,
            path.location.last_modified,
            path.location.size,
        )?
        .into();
        require!(
            path.location.location == expected.location.location,
            KernelError::invalid_log_path(format!(
                "Catalog log tail path does not belong to table {}: {}",
                log_root.table_root(),
                path.location.location,
            ))
        );
    }

    let commit_files = LogSegmentFiles::list_commits(
        engine.storage_handler().as_ref(),
        log_root.log_root(),
        log_tail,
        Some(conflict_version),
        max_catalog_version,
    )?
    .ascending_commit_files;
    let end_version = max_catalog_version
        .or_else(|| commit_files.last().map(|path| path.version))
        .ok_or(KernelError::EmptyLog)?;
    let commits = commit_files
        .into_iter()
        .map(|path| {
            Ok(WinningCommit {
                version: path.version,
                log_path: LogPath::try_new(path.location)?,
            })
        })
        .collect::<KernelResult<_>>()?;
    WinningCommits::try_new(table_root, conflict_version, end_version, commits)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::FileMeta;
    use crate::engine::sync::SyncEngine;
    use crate::engine::{DelegatingEngine, PanicEngine};

    #[rstest]
    #[case::published_prefix(&[0], &[1, 2], 0, true)]
    #[case::tail_supersedes_published(&[0, 1, 2, 3], &[1, 2], 0, true)]
    #[case::tail_below_start(&[0, 1, 2, 3], &[1, 2], 2, true)]
    #[case::published_tail(&[0], &[1, 2], 0, false)]
    fn discovers_catalog_tail_with_actual_paths(
        #[case] published: &[Version],
        #[case] catalog: &[Version],
        #[case] start: Version,
        #[case] staged: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("_delta_log");
        std::fs::create_dir(&log).unwrap();
        for version in published {
            std::fs::write(log.join(format!("{version:020}.json")), "not JSON").unwrap();
        }
        let root = Url::from_directory_path(directory.path()).unwrap();
        let tail: Vec<_> = catalog
            .iter()
            .map(|version| {
                let filename = if staged {
                    format!("{version:020}.00000000-0000-4000-8000-{version:012}.json")
                } else {
                    format!("{version:020}.json")
                };
                LogPath::from_commit_filename(root.clone(), *version, &filename, 42, 123).unwrap()
            })
            .collect();
        let engine = DelegatingEngine::new(Arc::new(PanicEngine))
            .with_storage_handler(SyncEngine::new().storage_handler());
        let winners = list_published_commits_and_merge_ratified_tail(
            &engine,
            &root,
            start,
            tail.clone(),
            Some(2),
        )
        .unwrap();
        assert_eq!(
            winners
                .iter()
                .map(|commit| commit.version)
                .collect::<Vec<_>>(),
            (start..=2).collect::<Vec<_>>()
        );
        for winner in winners {
            if let Some(index) = catalog
                .iter()
                .position(|version| *version == winner.version)
            {
                assert_eq!(winner.log_path, tail[index]);
            } else {
                let actual: ParsedLogPath = winner.log_path.into();
                assert_eq!(
                    actual.location.location,
                    root.join(&format!("_delta_log/{:020}.json", winner.version))
                        .unwrap()
                );
            }
        }
    }

    #[rstest]
    #[case::unbounded_ignores_checkpoint(&[11, 12], None, Ok(12))]
    #[case::catalog_bound(&[11, 12, 13, 14], Some(12), Ok(12))]
    #[case::complete(&[11, 12, 13], None, Ok(13))]
    #[case::gap(&[11, 13], None, Err("Table version 12 is missing"))]
    #[case::missing_catalog_end(&[11, 12], Some(13), Err("Table version 13 is missing"))]
    #[case::checkpoint_only(&[], None, Err("No table version found"))]
    #[case::missing_start(
        &[12, 13], None,
        Err("Start version 11 is not available; earliest available version is 12")
    )]
    fn discovers_only_complete_individual_commit_ranges(
        #[case] versions: &[Version],
        #[case] max_catalog_version: Option<Version>,
        #[case] expected: Result<Version, &str>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("_delta_log");
        let staged = log.join("_staged_commits");
        std::fs::create_dir_all(&staged).unwrap();
        for version in versions {
            std::fs::write(log.join(format!("{version:020}.json")), "not JSON").unwrap();
        }
        std::fs::write(
            log.join("00000000000000000013.checkpoint.parquet"),
            "checkpoint",
        )
        .unwrap();
        std::fs::write(
            log.join("00000000000000000011.00000000000000000013.compacted.json"),
            "compaction",
        )
        .unwrap();
        for version in 11..=14 {
            std::fs::write(
                staged.join(format!(
                    "{version:020}.00000000-0000-4000-8000-{version:012}.json"
                )),
                "unratified",
            )
            .unwrap();
        }
        let root = Url::from_directory_path(directory.path()).unwrap();
        let result = list_published_commits_and_merge_ratified_tail(
            &SyncEngine::new(),
            &root,
            11,
            vec![],
            max_catalog_version,
        );
        match expected {
            Ok(end) => assert_eq!(
                result
                    .unwrap()
                    .iter()
                    .map(|commit| commit.version)
                    .collect::<Vec<_>>(),
                (11..=end).collect::<Vec<_>>()
            ),
            Err(message) => assert_result_error_with_message(result, message),
        }
    }

    #[rstest]
    #[case::start_above_bound(&[], Some(0), "Requested version 1 exceeds")]
    #[case::staged_without_bound(
        &[concat!("_staged_commits/00000000000000000001.",
            "00000000-0000-4000-8000-000000000001.json")],
        None, "Max catalog version is required"
    )]
    #[case::tail_below_bound(
        &[concat!("_staged_commits/00000000000000000001.",
            "00000000-0000-4000-8000-000000000001.json")],
        Some(2), "does not match max catalog version"
    )]
    #[case::tail_above_bound(
        &["00000000000000000002.json"], Some(1), "does not match max catalog version"
    )]
    #[case::gap(
        &["00000000000000000001.json", "00000000000000000003.json"], Some(3), "not contiguous"
    )]
    #[case::duplicate(
        &["00000000000000000001.json", "00000000000000000001.json"], Some(1), "not contiguous"
    )]
    #[case::unsorted(
        &["00000000000000000002.json", "00000000000000000001.json"], Some(1), "not contiguous"
    )]
    #[case::crc(&["00000000000000000001.crc"], Some(1), "only commits")]
    #[case::checkpoint(&["00000000000000000001.checkpoint.parquet"], Some(1), "only commits")]
    fn rejects_invalid_catalog_tail_before_listing(
        #[case] filenames: &[&str],
        #[case] max_catalog_version: Option<Version>,
        #[case] message: &str,
    ) {
        let root = Url::parse("memory:///table/").unwrap();
        let tail = filenames
            .iter()
            .map(|name| {
                LogPath::try_new(FileMeta::new(
                    root.join(&format!("_delta_log/{name}")).unwrap(),
                    0,
                    1,
                ))
                .unwrap()
            })
            .collect();
        let result = list_published_commits_and_merge_ratified_tail(
            &PanicEngine,
            &root,
            1,
            tail,
            max_catalog_version,
        );
        assert_result_error_with_message(result, message);
    }

    #[rstest]
    #[case::wrong_table("memory:///other/_delta_log/00000000000000000001.json")]
    #[case::wrong_staged_table(concat!(
        "memory:///other/_delta_log/_staged_commits/00000000000000000001.",
        "00000000-0000-4000-8000-000000000001.json"
    ))]
    #[case::query("memory:///table/_delta_log/00000000000000000001.json?other")]
    #[case::fragment("memory:///table/_delta_log/00000000000000000001.json#other")]
    #[case::encoded_separator(concat!(
        "memory:///table/_delta_log/_staged_commits/00000000000000000001.",
        "%2Fother%2F_delta_log%2Fzzzzzzzzzzzz.json"
    ))]
    fn rejects_catalog_paths_outside_expected_location(#[case] location: &str) {
        let root = Url::parse("memory:///table/").unwrap();
        let tail =
            vec![LogPath::try_new(FileMeta::new(Url::parse(location).unwrap(), 0, 1)).unwrap()];
        let result =
            list_published_commits_and_merge_ratified_tail(&PanicEngine, &root, 1, tail, Some(1));
        assert!(
            matches!(result, Err(KernelError::InvalidLogPath(_))),
            "{result:?}"
        );
    }
}
