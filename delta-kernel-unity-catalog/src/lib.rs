//! Kernel-side helpers for catalog-managed Delta tables under the
//! Unity Catalog API surface.
//!
//! This crate exposes:
//!
//! - [`UCCommitter`]: a `delta-kernel` `Committer` impl that, on v >= 1, dispatches a typed
//!   `requirements + updates` payload through an [`UpdateTableClient`] to the catalog.
//! - [`log_tail_from_commits`]: converts the inline `Commit`s returned by `load_table` into kernel
//!   `LogPath`s. The connector then drives `Snapshot::builder_for(...).with_log_tail(...)`
//!   directly.
//! - [`utils::get_required_properties_for_disk`] and [`utils::build_uc_create_table_request`]:
//!   helpers for the CREATE flow.
//! - [`normalize_table_root`]: normalizes a UC table location to a root URL ending in `/`.
//!
//! [`UpdateTableClient`]: unity_catalog_delta_client_api::UpdateTableClient

mod committer;
mod constants;
mod conversions;
mod errors;
mod intents;
mod utils;

pub use committer::UCCommitter;
use delta_kernel::LogPath;
use unity_catalog_delta_client_api::Commit;
use url::Url;
pub use utils::{build_uc_create_table_request, get_required_properties_for_disk};

/// Boxed dynamic error type used by this crate's free functions. Chosen so
/// callers don't need to depend on a specific error enum; this code path
/// composes failures from kernel, the api crate, and `url::ParseError`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Normalize a UC table location to a root URL ending in `/`. `load_table` returns the location
/// without a trailing slash, but staged-commit/log-path resolution and snapshot building require
/// the table root to end in `/`.
pub fn normalize_table_root(table_url: &Url) -> Url {
    let mut root = table_url.clone();
    if !root.path().ends_with('/') {
        root.set_path(&format!("{}/", root.path()));
    }
    root
}

/// Convert the inline `Commit`s returned by a UC `load_table` response into
/// kernel `LogPath`s suitable for `Snapshot::builder_for(...).with_log_tail(...)`.
///
/// The UC `load_table` endpoint returns unpublished commits inline, so
/// connectors no longer need a separate `get_commits` RPC. They pass the
/// resulting `LogPath`s along with `with_max_catalog_version` to build a
/// snapshot directly.
///
/// # Errors
///
/// Returns a boxed error if any commit's `file_size` does not fit in `usize`
/// or if `LogPath::staged_commit` rejects the inputs.
pub fn log_tail_from_commits(
    commits: &[Commit],
    table_url: &Url,
) -> Result<Vec<LogPath>, BoxError> {
    let table_root = normalize_table_root(table_url);
    let mut sorted: Vec<&Commit> = commits.iter().collect();
    sorted.sort_by_key(|c| c.version);
    sorted
        .into_iter()
        .map(|c| {
            LogPath::staged_commit(
                table_root.clone(),
                &c.file_name,
                c.file_modification_timestamp,
                c.file_size.try_into()?,
            )
            .map_err(Into::into)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::Snapshot;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use unity_catalog_delta_client_api::{Commit, InMemoryUpdateTableClient, TableData};

    use super::*;

    #[tokio::test]
    async fn snapshot_build_errors_on_non_contiguous_commits() {
        // Catalog state has a gap at v2 (commits for v1 and v3 only).
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
                    ),
                ],
            },
        );
        let resp = client
            .load_table_response("test_table", "memory:///")
            .unwrap();

        let table_url = Url::parse(&resp.metadata.location).unwrap();
        let log_tail = log_tail_from_commits(&resp.commits, &table_url).unwrap();

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

    #[test]
    fn normalize_table_root_adds_and_preserves_trailing_slash() {
        let no_slash = Url::parse("s3://bucket/a/b").unwrap();
        assert_eq!(normalize_table_root(&no_slash).as_str(), "s3://bucket/a/b/");

        let with_slash = Url::parse("s3://bucket/a/b/").unwrap();
        assert_eq!(
            normalize_table_root(&with_slash).as_str(),
            "s3://bucket/a/b/"
        );
    }

    #[test]
    fn log_tail_from_commits_resolves_staged_commit_without_trailing_slash() {
        let commits = vec![Commit::new(
            1,
            0,
            "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            10,
            0,
        )];
        // Table URL lacks a trailing slash; normalization inside the helper must still resolve the
        // staged-commit URL.
        let table_url = Url::parse("s3://bucket/a/b").unwrap();
        let log_tail = log_tail_from_commits(&commits, &table_url).unwrap();
        assert_eq!(log_tail.len(), 1);
    }
}
