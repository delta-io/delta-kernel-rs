//! Kernel-side helpers for catalog-managed Delta tables in Unity Catalog.

mod committer;
mod constants;
mod errors;
mod utils;

pub use committer::UCCommitter;
use delta_kernel::snapshot::SnapshotBuilder;
use delta_kernel::{DeltaResult, Error, LogPath, Snapshot};
use unity_catalog_delta_client_api::{Commit, LoadTableResponse};
use url::Url;
pub use utils::{build_uc_create_table_request, get_required_properties_for_disk};

/// Convert the inline `Commit`s returned by a UC `load_table` response into
/// kernel `LogPath`s suitable for `Snapshot::builder_for(...).with_log_tail(...)`.
///
/// Sorts commits by version because they may arrive out of order. Normalizes `table_root`
/// with a trailing `/` for staged-commit path resolution.
///
/// # Errors
///
/// Returns an error if a commit's `file_size` is negative (does not fit in `FileSize`) or if
/// [`LogPath::staged_commit`] rejects the resolved path.
pub fn log_tail_from_commits(commits: &[Commit], mut table_root: Url) -> DeltaResult<Vec<LogPath>> {
    // `load_table` returns the location without a trailing slash, but staged-commit path
    // resolution requires the table root to end in `/`.
    if !table_root.path().ends_with('/') {
        table_root.set_path(&format!("{}/", table_root.path()));
    }
    let mut sorted: Vec<&Commit> = commits.iter().collect();
    sorted.sort_by_key(|c| c.version);
    sorted
        .into_iter()
        .map(|c| {
            let file_size = c.file_size.try_into().map_err(|_| {
                Error::generic(format!(
                    "commit file_size {} does not fit in FileSize",
                    c.file_size
                ))
            })?;
            LogPath::staged_commit(
                table_root.clone(),
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
pub fn snapshot_builder_from_load_table(resp: &LoadTableResponse) -> DeltaResult<SnapshotBuilder> {
    let table_root = Url::parse(&resp.metadata.location)
        .map_err(|e| Error::generic(format!("invalid table location: {e}")))?;
    let log_tail = log_tail_from_commits(&resp.commits, table_root.clone())?;
    let mut builder = Snapshot::builder_for(table_root).with_log_tail(log_tail);
    if let Some(version) = resp.latest_table_version {
        let max_catalog_version: u64 = version
            .try_into()
            .map_err(|_| Error::generic("catalog reported a negative table version"))?;
        builder = builder.with_max_catalog_version(max_catalog_version);
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::Snapshot;
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

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("log_tail must be sorted and contiguous"));
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

    #[rstest]
    #[case::missing_trailing_slash("memory:///my_table")]
    #[case::has_trailing_slash("memory:///my_table/")]
    fn log_tail_from_commits_sorts_and_normalizes(#[case] table_root: &str) {
        let names = [
            "00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            "00000000000000000002.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        ];
        // Insert out of order: 2, 0, 1.
        let commits = vec![
            Commit::new(2, 0, names[2], 100, 0),
            Commit::new(0, 0, names[0], 100, 0),
            Commit::new(1, 0, names[1], 100, 0),
        ];
        let normalized = Url::parse("memory:///my_table/").unwrap();

        let log_tail = log_tail_from_commits(&commits, Url::parse(table_root).unwrap()).unwrap();

        let expected: Vec<LogPath> = names
            .iter()
            .map(|name| LogPath::staged_commit(normalized.clone(), name, 0, 100).unwrap())
            .collect();
        assert_eq!(log_tail, expected);
    }

    #[test]
    fn log_tail_from_commits_empty_commits_is_empty() {
        let table_root = Url::parse("memory:///my_table/").unwrap();
        assert!(log_tail_from_commits(&[], table_root).unwrap().is_empty());
    }

    #[test]
    fn log_tail_from_commits_rejects_negative_file_size() {
        let commits = vec![Commit::new(
            0,
            0,
            "00000000000000000000.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            -1,
            0,
        )];
        let table_root = Url::parse("memory:///my_table/").unwrap();

        let err = log_tail_from_commits(&commits, table_root).unwrap_err();

        assert!(err.to_string().contains("does not fit in FileSize"));
    }
}
