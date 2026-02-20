//! In-memory implementation of [`UCCommitClient`] and [`UCGetCommitsClient`] for testing.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::RwLock;

use unity_catalog_client::error::{Error, Result};

use crate::models::{Commit, CommitRequest, CommitsRequest, CommitsResponse};

use super::{UCCommitClient, UCGetCommitsClient};

// ============================================================================
// TableData
// ============================================================================

/// In-memory representation of a UC-managed Delta table's commit state.
pub struct TableData {
    /// The highest version that has been ratified (committed) to this table.
    pub max_ratified_version: i64,
    /// Commits that have been registered with UC but not yet published.
    pub catalog_commits: Vec<Commit>,
}

impl TableData {
    pub const MAX_UNPUBLISHED_COMMITS: usize = 20;

    /// Creates a new `TableData` representing a UC Delta table that has just been created.
    /// The table starts with no commits and version 0.
    fn new_post_table_create() -> Self {
        Self {
            max_ratified_version: 0,
            catalog_commits: vec![],
        }
    }

    /// Returns commits within the requested version range.
    fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse> {
        let start = request.start_version.unwrap_or(0);
        let end = request.end_version.unwrap_or(i64::MAX);

        Ok(CommitsResponse {
            commits: Some(
                self.catalog_commits
                    .iter()
                    .filter(|commit| start <= commit.version && commit.version <= end)
                    .cloned()
                    .collect(),
            ),
            latest_table_version: self.max_ratified_version,
        })
    }

    /// Registers a new commit. Returns an error if the version is not the expected next version
    /// or if the number of unpublished commits exceeds the maximum.
    fn commit(&mut self, request: CommitRequest) -> Result<()> {
        let Some(commit) = request.commit_info else {
            return Err(Error::UnsupportedOperation(
                "commit_info is required".to_string(),
            ));
        };

        let expected_version = self.max_ratified_version + 1;

        if commit.version != expected_version {
            return Err(Error::UnsupportedOperation(format!(
                "Expected commit version {} but got {}",
                expected_version, commit.version
            )));
        }

        if self.catalog_commits.len() >= Self::MAX_UNPUBLISHED_COMMITS {
            return Err(Error::MaxUnpublishedCommitsExceeded(
                Self::MAX_UNPUBLISHED_COMMITS as u16,
            ));
        }

        if let Some(v) = request.latest_backfilled_version {
            self.cleanup_published_commits(v);
        }

        self.catalog_commits.push(commit);
        self.max_ratified_version = expected_version;

        Ok(())
    }

    /// Removes commits that have been published (backfilled) to the Delta log.
    fn cleanup_published_commits(&mut self, max_published_version: i64) {
        self.catalog_commits
            .retain(|commit| max_published_version < commit.version);
    }
}

// ============================================================================
// InMemoryCommitsClient
// ============================================================================

/// An in-memory implementation of [`UCCommitClient`] and [`UCGetCommitsClient`] for testing.
pub struct InMemoryCommitsClient {
    // table id -> table data
    tables: RwLock<HashMap<String, TableData>>,
}

impl InMemoryCommitsClient {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_table(&self, table_id: impl Into<String>) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        match tables.entry(table_id.into()) {
            Entry::Vacant(e) => {
                e.insert(TableData::new_post_table_create());
                Ok(())
            }
            Entry::Occupied(e) => Err(Error::UnsupportedOperation(format!(
                "Table {} already exists",
                e.key()
            ))),
        }
    }

    /// Inserts a table with pre-existing state. Useful for testing.
    pub fn insert_table(&self, table_id: impl Into<String>, table_data: TableData) {
        self.tables
            .write()
            .unwrap()
            .insert(table_id.into(), table_data);
    }
}

impl Default for InMemoryCommitsClient {
    fn default() -> Self {
        Self::new()
    }
}

impl UCGetCommitsClient for InMemoryCommitsClient {
    async fn get_commits(&self, request: CommitsRequest) -> Result<CommitsResponse> {
        let tables = self.tables.read().unwrap();
        let table = tables
            .get(&request.table_id)
            .ok_or_else(|| Error::TableNotFound(request.table_id.clone()))?;
        table.get_commits(request)
    }
}

impl UCCommitClient for InMemoryCommitsClient {
    async fn commit(&self, request: CommitRequest) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get_mut(&request.table_id)
            .ok_or_else(|| Error::TableNotFound(request.table_id.clone()))?;
        table.commit(request)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE_ID: &str = "test-table-id";
    const TABLE_URI: &str = "s3://bucket/table";

    fn make_commit(version: i64) -> Commit {
        Commit::new(
            version,
            version * 1000,
            format!("{:020}.json", version),
            100,
            version * 1000,
        )
    }

    fn commit_request(version: i64, latest_backfilled_version: Option<i64>) -> CommitRequest {
        CommitRequest::new(
            TABLE_ID,
            TABLE_URI,
            make_commit(version),
            latest_backfilled_version,
        )
    }

    fn get_commits_request() -> CommitsRequest {
        CommitsRequest::new(TABLE_ID, TABLE_URI)
    }

    fn extract_commit_versions(commits: &[Commit]) -> Vec<i64> {
        commits.iter().map(|c| c.version).collect()
    }

    #[tokio::test]
    async fn test_commit_and_get_commits() {
        let client = InMemoryCommitsClient::new();

        // Create table
        client.create_table(TABLE_ID).unwrap();

        // Insert 10 commits (versions 1-10)
        for v in 1..=10 {
            client.commit(commit_request(v, None)).await.unwrap();
        }

        // Get commits (versions 3-8)
        let commits_request = get_commits_request()
            .with_start_version(3)
            .with_end_version(8);
        let response = client.get_commits(commits_request).await.unwrap();
        let commits = response.commits.unwrap();
        assert_eq!(commits.len(), 6);
        assert_eq!(extract_commit_versions(&commits), vec![3, 4, 5, 6, 7, 8]);
        assert_eq!(response.latest_table_version, 10);

        // Insert commit 11 with latest_backfilled_version = 5
        // This should cleanup commits 1-5 (retain versions >= 6)
        client.commit(commit_request(11, Some(5))).await.unwrap();

        // Get commits again - should return versions 6-11
        let response = client.get_commits(get_commits_request()).await.unwrap();
        let commits = response.commits.unwrap();
        assert_eq!(extract_commit_versions(&commits), vec![6, 7, 8, 9, 10, 11]);
        assert_eq!(response.latest_table_version, 11);
    }

    #[test]
    fn test_create_table_duplicate_throws() {
        let client = InMemoryCommitsClient::new();
        client.create_table(TABLE_ID).unwrap();
        assert!(matches!(
            client.create_table(TABLE_ID),
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[tokio::test]
    async fn test_get_commits_table_not_found() {
        assert!(matches!(
            InMemoryCommitsClient::new()
                .get_commits(get_commits_request())
                .await,
            Err(Error::TableNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_commit_table_not_found() {
        assert!(matches!(
            InMemoryCommitsClient::new()
                .commit(commit_request(1, None))
                .await,
            Err(Error::TableNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_commit_wrong_version() {
        let client = InMemoryCommitsClient::new();
        client.create_table(TABLE_ID).unwrap();
        assert!(matches!(
            client.commit(commit_request(5, None)).await,
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[tokio::test]
    async fn test_get_commits_empty_table() {
        let client = InMemoryCommitsClient::new();
        client.create_table(TABLE_ID).unwrap();
        let response = client.get_commits(get_commits_request()).await.unwrap();
        assert!(response.commits.unwrap().is_empty());
        assert_eq!(response.latest_table_version, 0);
    }

    #[tokio::test]
    async fn test_commit_max_unpublished_commits_exceeded() {
        let client = InMemoryCommitsClient::new();
        client.create_table(TABLE_ID).unwrap();
        for v in 1..=TableData::MAX_UNPUBLISHED_COMMITS as i64 {
            client.commit(commit_request(v, None)).await.unwrap();
        }
        let next_version = TableData::MAX_UNPUBLISHED_COMMITS as i64 + 1;
        assert!(matches!(
            client.commit(commit_request(next_version, None)).await,
            Err(Error::MaxUnpublishedCommitsExceeded(_))
        ));
    }
}
