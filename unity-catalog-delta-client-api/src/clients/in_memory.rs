//! In-memory implementation of [`UpdateTableClient`] for testing.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::RwLock;

use super::UpdateTableClient;
use crate::error::{Error, Result};
use crate::models::{Commit, LoadTableResponse, TableMetadata, TableName, UpdateTableRequest};

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

    /// Apply the typed `add-commit` and (optional) `set-latest-backfilled-version`
    /// updates from an `UpdateTableRequest` against this table's state.
    fn update_table(&mut self, request: &UpdateTableRequest) -> Result<()> {
        let backfilled = request.latest_backfilled_version();

        let commit = request.staged_commit().cloned().ok_or_else(|| {
            Error::UnsupportedOperation("add-commit update is required".to_string())
        })?;

        let expected_version = self.max_ratified_version + 1;
        if commit.version != expected_version {
            return Err(Error::UnsupportedOperation(format!(
                "Expected commit version {expected_version} but got {}",
                commit.version
            )));
        }
        if self.catalog_commits.len() >= Self::MAX_UNPUBLISHED_COMMITS {
            return Err(Error::MaxUnpublishedCommitsExceeded(
                Self::MAX_UNPUBLISHED_COMMITS as u16,
            ));
        }
        if let Some(v) = backfilled {
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
// InMemoryUpdateTableClient
// ============================================================================

/// An in-memory implementation of [`UpdateTableClient`] for testing.
pub struct InMemoryUpdateTableClient {
    // table id -> table data
    tables: RwLock<HashMap<String, TableData>>,
}

impl InMemoryUpdateTableClient {
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

    /// Build a `LoadTableResponse` for the given table id. Lets tests stand in
    /// for the connector's `load_table` HTTP call.
    pub fn load_table_response(
        &self,
        table_id: &str,
        storage_location: impl Into<String>,
    ) -> Result<LoadTableResponse> {
        let tables = self.tables.read().unwrap();
        let table = tables
            .get(table_id)
            .ok_or_else(|| Error::TableNotFound(table_id.to_string()))?;
        let metadata = TableMetadata {
            etag: String::new(),
            table_type: "MANAGED".to_string(),
            table_uuid: table_id.to_string(),
            location: storage_location.into(),
            created_time: 0,
            updated_time: 0,
            columns: serde_json::json!({ "type": "struct", "fields": [] }),
            partition_columns: vec![],
            properties: std::collections::HashMap::new(),
            last_commit_version: Some(table.max_ratified_version),
            last_commit_timestamp_ms: Some(0),
        };
        Ok(LoadTableResponse {
            metadata,
            commits: table.catalog_commits.clone(),
            uniform: None,
            latest_table_version: Some(table.max_ratified_version),
        })
    }
}

impl Default for InMemoryUpdateTableClient {
    fn default() -> Self {
        Self::new()
    }
}

impl UpdateTableClient for InMemoryUpdateTableClient {
    async fn update_table(&self, _target: &TableName, request: UpdateTableRequest) -> Result<()> {
        // Identify the target table from the assert-table-uuid requirement.
        let table_id = request
            .table_uuid()
            .ok_or_else(|| {
                Error::UnsupportedOperation(
                    "InMemoryUpdateTableClient requires an assert-table-uuid requirement"
                        .to_string(),
                )
            })?
            .to_string();

        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get_mut(&table_id)
            .ok_or_else(|| Error::TableNotFound(table_id.clone()))?;
        table.update_table(&request)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DeltaTableRequirement, DeltaTableUpdate};

    const TABLE_ID: &str = "test-table-id";

    fn make_commit(version: i64) -> Commit {
        Commit::new(
            version,
            version * 1000,
            format!("{version:020}.json"),
            100,
            version * 1000,
        )
    }

    fn target() -> TableName {
        TableName::new("test_catalog", "test_schema", "test_table")
    }

    fn commit_request(version: i64, latest_backfilled_version: Option<i64>) -> UpdateTableRequest {
        let mut updates = vec![DeltaTableUpdate::AddCommit {
            commit: make_commit(version),
        }];
        if let Some(v) = latest_backfilled_version {
            updates.push(DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: v,
            });
        }
        UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid {
                uuid: TABLE_ID.to_string(),
            }],
            updates,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_commit_and_load_table_response() {
        let client = InMemoryUpdateTableClient::new();
        client.create_table(TABLE_ID).unwrap();

        for v in 1..=10 {
            client
                .update_table(&target(), commit_request(v, None))
                .await
                .unwrap();
        }

        let resp = client
            .load_table_response(TABLE_ID, "memory:///tbl/")
            .unwrap();
        assert_eq!(resp.latest_table_version, Some(10));
        assert_eq!(resp.commits.len(), 10);

        // Backfill cleanup: commit 11 with backfilled=5 keeps versions 6..=11
        client
            .update_table(&target(), commit_request(11, Some(5)))
            .await
            .unwrap();
        let resp = client
            .load_table_response(TABLE_ID, "memory:///tbl/")
            .unwrap();
        let versions: Vec<i64> = resp.commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![6, 7, 8, 9, 10, 11]);
        assert_eq!(resp.latest_table_version, Some(11));
    }

    #[test]
    fn test_create_table_duplicate_throws() {
        let client = InMemoryUpdateTableClient::new();
        client.create_table(TABLE_ID).unwrap();
        assert!(matches!(
            client.create_table(TABLE_ID),
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[tokio::test]
    async fn test_commit_table_not_found() {
        assert!(matches!(
            InMemoryUpdateTableClient::new()
                .update_table(&target(), commit_request(1, None))
                .await,
            Err(Error::TableNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_commit_wrong_version() {
        let client = InMemoryUpdateTableClient::new();
        client.create_table(TABLE_ID).unwrap();
        assert!(matches!(
            client
                .update_table(&target(), commit_request(5, None))
                .await,
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[tokio::test]
    async fn test_commit_max_unpublished_commits_exceeded() {
        let client = InMemoryUpdateTableClient::new();
        client.create_table(TABLE_ID).unwrap();
        for v in 1..=TableData::MAX_UNPUBLISHED_COMMITS as i64 {
            client
                .update_table(&target(), commit_request(v, None))
                .await
                .unwrap();
        }
        let next_version = TableData::MAX_UNPUBLISHED_COMMITS as i64 + 1;
        assert!(matches!(
            client
                .update_table(&target(), commit_request(next_version, None))
                .await,
            Err(Error::MaxUnpublishedCommitsExceeded(_))
        ));
    }
}
