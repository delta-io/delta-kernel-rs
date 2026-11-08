//! Kernel-side helpers for catalog-managed Delta tables in Unity Catalog.

mod committer;
mod constants;
mod errors;
mod utils;

pub use committer::UCCommitter;
use delta_kernel::snapshot::SnapshotBuilder;
use delta_kernel::{KernelError, KernelResult, LogPath, Snapshot};
use unity_catalog_delta_client_api::{Commit, LoadTableResponse};
use url::Url;
pub use utils::{
    aws_object_store_options, build_uc_create_table_request, get_required_properties_for_disk,
};

/// Convert the inline `Commit`s returned by a UC `load_table` response into
/// kernel `LogPath`s suitable for `Snapshot::builder_for(...).with_log_tail(...)`.
///
/// Sorts commits by version because they may arrive out of order. Each commit may name either a
/// staged or an already-published commit file. A published filename asserts that its version and
/// all lower versions are already published.
///
/// # Errors
///
/// Returns an error if a commit's `version` or `file_size` is negative, or if
/// [`LogPath::from_commit_filename`] rejects its filename (including a filename whose version
/// differs from the commit's `version`).
pub fn log_tail_from_commits(commits: &[Commit], table_root: Url) -> KernelResult<Vec<LogPath>> {
    let mut sorted: Vec<&Commit> = commits.iter().collect();
    sorted.sort_by_key(|c| c.version);
    sorted
        .into_iter()
        .map(|c| {
            let version = c.version.try_into().map_err(|_| {
                KernelError::generic(format!("commit version {} is negative", c.version))
            })?;
            let file_size = c.file_size.try_into().map_err(|_| {
                KernelError::generic(format!(
                    "commit file_size {} does not fit in FileSize",
                    c.file_size
                ))
            })?;
            LogPath::from_commit_filename(
                table_root.clone(),
                version,
                &c.file_name,
                c.file_modification_timestamp,
                file_size,
            )
        })
        .collect()
}

/// Build a [`SnapshotBuilder`] from a UC `load_table` response.
///
/// Parses the table location, converts the inline commits into a log tail via
/// [`log_tail_from_commits`], and pins the catalog's ratified version with
/// `with_max_catalog_version`. The connector still owns the `load_table` call and the final
/// `build(engine)`; add `at_version(v)` to the returned builder to time-travel.
///
/// # Errors
///
/// Returns an error if the response's location is not a valid URL, if [`log_tail_from_commits`]
/// fails, or if `latest_table_version` is negative.
pub fn snapshot_builder_from_load_table(resp: &LoadTableResponse) -> KernelResult<SnapshotBuilder> {
    let table_root = Url::parse(&resp.metadata.location)
        .map_err(|e| KernelError::generic(format!("invalid table location: {e}")))?;
    let log_tail = log_tail_from_commits(&resp.commits, table_root.clone())?;
    let mut builder = Snapshot::builder_for(table_root).with_log_tail(log_tail);
    if let Some(version) = resp.latest_table_version {
        let max_catalog_version: u64 = version
            .try_into()
            .map_err(|_| KernelError::generic("catalog reported a negative table version"))?;
        builder = builder.with_max_catalog_version(max_catalog_version);
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::{FileMeta, Snapshot};
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use unity_catalog_delta_client_api::{Commit, InMemoryUpdateTableClient, TableData};

    use super::*;

    #[tokio::test]
    async fn load_snapshot_errors_on_non_contiguous_commits() {
        let client = InMemoryUpdateTableClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: 3,
                catalog_commits: vec![
                    Commit::new(
                        1,
                        0,
                        "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ),
                    Commit::new(
                        3,
                        0,
                        "00000000000000000003.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ), // gap: version 2 missing
                ],
            },
        );
        let resp = client
            .load_table_response("test_table", "memory:///")
            .unwrap();

        let table_url = Url::parse(&resp.metadata.location).unwrap();
        let log_tail = log_tail_from_commits(&resp.commits, table_url.clone()).unwrap();

        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let max_catalog_version: u64 = resp.latest_table_version.unwrap_or(0).try_into().unwrap();
        let result = Snapshot::builder_for(table_url)
            .with_log_tail(log_tail)
            .with_max_catalog_version(max_catalog_version)
            .build(&engine);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Log tail versions 1 and 3 are not contiguous")
        );
    }

    /// Builds a `LoadTableResponse` with no commits at the given location and ratified version.
    fn load_table_response(location: &str, version: i64) -> LoadTableResponse {
        let client = InMemoryUpdateTableClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: version,
                catalog_commits: vec![],
            },
        );
        client.load_table_response("test_table", location).unwrap()
    }

    #[test]
    fn snapshot_builder_from_load_table_rejects_negative_version() {
        let resp = load_table_response("memory:///my_table", -1);
        let err = snapshot_builder_from_load_table(&resp).unwrap_err();
        assert!(err.to_string().contains("negative table version"));
    }

    #[test]
    fn snapshot_builder_from_load_table_rejects_invalid_location() {
        let resp = load_table_response("not a url", 0);
        let err = snapshot_builder_from_load_table(&resp).unwrap_err();
        assert!(err.to_string().contains("invalid table location"));
    }

    const UUID: &str = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";

    #[rstest]
    fn log_tail_from_commits_sorts_normalizes_and_resolves_commit_forms(
        #[values("memory:///my_table", "memory:///my_table/")] table_root: &str,
    ) {
        let staged_1 = format!("00000000000000000001.{UUID}.json");
        // Out of order, mixing published and staged names.
        let commits = vec![
            Commit::new(2, 0, "00000000000000000002.json", 100, 0),
            Commit::new(0, 0, "00000000000000000000.json", 100, 0),
            Commit::new(1, 0, &staged_1, 100, 0),
        ];

        let log_tail = log_tail_from_commits(&commits, Url::parse(table_root).unwrap()).unwrap();

        let normalized = Url::parse("memory:///my_table/").unwrap();
        let expected: Vec<LogPath> = [
            "_delta_log/00000000000000000000.json".to_string(),
            format!("_delta_log/_staged_commits/{staged_1}"),
            "_delta_log/00000000000000000002.json".to_string(),
        ]
        .iter()
        .map(|rel_path| {
            LogPath::try_new(FileMeta {
                location: normalized.join(rel_path).unwrap(),
                last_modified: 0,
                size: 100,
            })
            .unwrap()
        })
        .collect();
        assert_eq!(log_tail, expected);
    }

    #[test]
    fn log_tail_from_commits_empty_commits_is_empty() {
        let table_root = Url::parse("memory:///my_table/").unwrap();
        assert!(log_tail_from_commits(&[], table_root).unwrap().is_empty());
    }

    #[rstest]
    #[case::negative_file_size(0, "00000000000000000000.json", -1, false)]
    #[case::negative_version(-1, "00000000000000000000.json", 100, false)]
    #[case::version_mismatch(1, "00000000000000000000.json", 100, true)]
    #[case::not_a_commit(0, "00000000000000000000.crc", 100, true)]
    fn log_tail_from_commits_rejects_invalid_commit(
        #[case] version: i64,
        #[case] file_name: &str,
        #[case] file_size: i64,
        #[case] expected_invalid_log_path_error: bool,
    ) {
        let commits = vec![Commit::new(version, 0, file_name, file_size, 0)];
        let table_root = Url::parse("memory:///my_table/").unwrap();

        let err = log_tail_from_commits(&commits, table_root).unwrap_err();

        let expected_kind = if expected_invalid_log_path_error {
            matches!(err, KernelError::InvalidLogPath(_))
        } else {
            matches!(err, KernelError::Generic(_))
        };
        assert!(expected_kind, "{err:?}");
    }
}
