//! In-memory implementation of [`UpdateTableClient`] for testing.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::{SequenceClient, UpdateTableClient};
use crate::error::{Error, Result};
use crate::models::{
    Commit, CreateIdentitySequences, DropIdentitySequenceResult, DropIdentitySequences,
    DropIdentitySequencesResponse, IdentityIdRange, LoadTableResponse, ReserveIdentityRanges,
    ReserveIdentityRangesResponse, TableIdentifier, TableMetadata, UpdateTableRequest,
};

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
    async fn update_table(
        &self,
        _target: &TableIdentifier,
        request: UpdateTableRequest,
    ) -> Result<()> {
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
// InMemorySequenceClient
// ============================================================================

/// Maximum length of an identity sequence id, matching the service's limit.
const MAX_SEQUENCE_ID_LEN: usize = 64;

/// In-memory state for a single sequence.
struct SequenceState {
    /// The value that will start the next reservation.
    current: i64,
    /// The immutable step (increment) for the sequence.
    step: i64,
    /// The start value the sequence was created with. Retained so create-or-get can detect a
    /// conflicting redefinition.
    start: i64,
}

/// An in-memory implementation of [`SequenceClient`] for testing.
///
/// Thread-safe via `RwLock`. Sequences are keyed by `(table_id, sequence_id)`. Sequence ids are
/// created by the caller, use [`SequenceClient::create_identity_sequences`]
/// (or the sync [`Self::seed_sequence`] setup helper) to create them.
pub struct InMemorySequenceClient {
    // (table_id, sequence_id) -> state
    sequences: RwLock<HashMap<(String, String), SequenceState>>,
}

impl InMemorySequenceClient {
    /// Creates a new empty [`InMemorySequenceClient`].
    pub fn new() -> Self {
        Self {
            sequences: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a single sequence under a table. Returns an error if the id is empty, too long,
    /// the step is zero, or the sequence already exists under the table.
    /// Helper for tests.
    pub fn seed_sequence(
        &self,
        table_id: impl Into<String>,
        sequence_id: impl Into<String>,
        start: i64,
        step: i64,
    ) -> Result<()> {
        let sequence_id = sequence_id.into();
        Self::validate_sequence_id(&sequence_id)?;
        if step == 0 {
            return Err(Error::UnsupportedOperation(
                "step must be non-zero".to_string(),
            ));
        }
        // Acquire the write lock to mutate the sequences map.
        let mut sequences = self.sequences.write().unwrap();
        match sequences.entry((table_id.into(), sequence_id)) {
            Entry::Vacant(e) => {
                e.insert(SequenceState {
                    current: start,
                    step,
                    start,
                });
                Ok(())
            }
            Entry::Occupied(e) => Err(Error::UnsupportedOperation(format!(
                "Sequence {} already exists",
                e.key().1
            ))),
        }
    }

    fn validate_sequence_id(sequence_id: &str) -> Result<()> {
        if sequence_id.is_empty() || sequence_id.len() > MAX_SEQUENCE_ID_LEN {
            return Err(Error::UnsupportedOperation(format!(
                "sequence id must be non-empty and at most {MAX_SEQUENCE_ID_LEN} characters"
            )));
        }
        Ok(())
    }
}

impl Default for InMemorySequenceClient {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceClient for InMemorySequenceClient {
    async fn create_identity_sequences(&self, req: CreateIdentitySequences) -> Result<()> {
        if req.sequences.is_empty() {
            return Err(Error::UnsupportedOperation(
                "create request must contain at least one sequence".to_string(),
            ));
        }

        // Validate the whole batch before mutating so it applies atomically.
        let mut seen = HashSet::new();
        for spec in &req.sequences {
            Self::validate_sequence_id(&spec.sequence_id)?;
            if !seen.insert(spec.sequence_id.as_str()) {
                return Err(Error::UnsupportedOperation(format!(
                    "duplicate sequence id '{}' within request",
                    spec.sequence_id
                )));
            }
            if spec.step == 0 {
                return Err(Error::UnsupportedOperation(
                    "step must be non-zero".to_string(),
                ));
            }
        }

        // Acquire the write lock to mutate the sequences map.
        let mut sequences = self.sequences.write().unwrap();
        // Detect a conflicting redefinition against a stored sequence before applying any insert.
        for spec in &req.sequences {
            let key = (req.table_id.clone(), spec.sequence_id.clone());
            if let Some(existing) = sequences.get(&key) {
                if existing.start != spec.start || existing.step != spec.step {
                    return Err(Error::UnsupportedOperation(format!(
                        "sequence '{}' already exists with a different definition",
                        spec.sequence_id
                    )));
                }
            }
        }
        for spec in &req.sequences {
            sequences
                .entry((req.table_id.clone(), spec.sequence_id.clone()))
                .or_insert(SequenceState {
                    current: spec.start,
                    step: spec.step,
                    start: spec.start,
                });
        }
        Ok(())
    }

    async fn reserve_identity_ranges(
        &self,
        req: ReserveIdentityRanges,
    ) -> Result<ReserveIdentityRangesResponse> {
        if req.reservations.is_empty() {
            return Err(Error::UnsupportedOperation(
                "reserve request must contain at least one reservation".to_string(),
            ));
        }

        let mut seen = HashSet::new();
        for r in &req.reservations {
            if !seen.insert(r.sequence_id.as_str()) {
                return Err(Error::UnsupportedOperation(format!(
                    "duplicate sequence id '{}' within request",
                    r.sequence_id
                )));
            }
            if r.count <= 0 {
                return Err(Error::UnsupportedOperation(
                    "reserve count must be positive".to_string(),
                ));
            }
        }

        // Acquire the write lock to mutate the sequences map.
        let mut sequences = self.sequences.write().unwrap();

        // First validate the reservations, then compute the ranges and advances.
        let mut ranges = Vec::with_capacity(req.reservations.len());
        let mut advances = Vec::with_capacity(req.reservations.len());
        for r in &req.reservations {
            let key = (req.table_id.clone(), r.sequence_id.clone());
            let state = sequences.get(&key).ok_or_else(|| {
                Error::UnsupportedOperation(format!("Sequence not found: {}", r.sequence_id))
            })?;

            if let Some(step) = r.step {
                if step != state.step {
                    return Err(Error::UnsupportedOperation(format!(
                        "reserve step {step} does not match sequence '{}' step {}",
                        r.sequence_id, state.step
                    )));
                }
            }

            let range_start = state.current;
            // Advance by (count - 1) steps for the range end, then one more step past it for the
            // next reservation.
            let stride = state.step.checked_mul(r.count - 1).ok_or_else(|| {
                Error::Generic(format!(
                    "reservation stride overflow (step={}, count={})",
                    state.step, r.count
                ))
            })?;
            let range_end = range_start.checked_add(stride).ok_or_else(|| {
                Error::Generic(format!(
                    "reservation range_end overflow (range_start={range_start}, stride={stride})"
                ))
            })?;
            let next_current = range_end.checked_add(state.step).ok_or_else(|| {
                Error::Generic(format!(
                    "reservation cursor advance overflow -- sequence '{}' is exhausted",
                    r.sequence_id
                ))
            })?;

            ranges.push(IdentityIdRange {
                sequence_id: r.sequence_id.clone(),
                range_start,
                range_end,
                step: state.step,
            });
            advances.push((key, next_current));
        }

        // Now apply the advances.
        for (key, next_current) in advances {
            sequences.get_mut(&key).unwrap().current = next_current;
        }

        Ok(ReserveIdentityRangesResponse { ranges })
    }

    async fn drop_identity_sequences(
        &self,
        req: DropIdentitySequences,
    ) -> Result<DropIdentitySequencesResponse> {
        if req.sequence_ids.is_empty() {
            return Err(Error::UnsupportedOperation(
                "drop request must contain at least one sequence id".to_string(),
            ));
        }
        // Acquire the write lock to mutate the sequences map.
        let mut sequences = self.sequences.write().unwrap();
        let mut seen = HashSet::new();
        let mut results = Vec::new();
        for id in &req.sequence_ids {
            // De-duplicate the results.
            if !seen.insert(id.as_str()) {
                continue;
            }
            let existed = sequences
                .remove(&(req.table_id.clone(), id.clone()))
                .is_some();
            results.push(DropIdentitySequenceResult {
                sequence_id: id.clone(),
                existed,
            });
        }
        Ok(DropIdentitySequencesResponse { results })
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

    fn target() -> TableIdentifier {
        TableIdentifier::new("test_catalog", "test_schema", "test_table")
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

    // ========================================================================
    // InMemorySequenceClient tests
    // ========================================================================

    use crate::models::{IdentityReservation, IdentitySequenceSpec};

    const SEQ_TABLE: &str = "tbl-seq";

    fn spec(id: &str, start: i64, step: i64) -> IdentitySequenceSpec {
        IdentitySequenceSpec {
            sequence_id: id.to_string(),
            start,
            step,
        }
    }

    fn create_req(table_id: &str, specs: Vec<IdentitySequenceSpec>) -> CreateIdentitySequences {
        CreateIdentitySequences {
            table_id: table_id.to_string(),
            sequences: specs,
        }
    }

    /// A single-reservation request with no advisory step.
    fn reserve_one(table_id: &str, sequence_id: &str, count: i64) -> ReserveIdentityRanges {
        ReserveIdentityRanges {
            table_id: table_id.to_string(),
            reservations: vec![IdentityReservation {
                sequence_id: sequence_id.to_string(),
                count,
                step: None,
            }],
        }
    }

    #[tokio::test]
    async fn sequence_create_then_reserve() {
        let client = InMemorySequenceClient::new();
        client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 1)]))
            .await
            .unwrap();

        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-a", 5))
            .await
            .unwrap();
        assert_eq!(resp.ranges.len(), 1);
        assert_eq!(resp.ranges[0].sequence_id, "seq-a");
        assert_eq!(resp.ranges[0].range_start, 1);
        assert_eq!(resp.ranges[0].range_end, 5);
        assert_eq!(resp.ranges[0].step, 1);
    }

    #[tokio::test]
    async fn sequence_create_batch_is_atomic_and_positional_reserve() {
        let client = InMemorySequenceClient::new();
        client
            .create_identity_sequences(create_req(
                SEQ_TABLE,
                vec![spec("seq-a", 0, 2), spec("seq-b", 100, 10)],
            ))
            .await
            .unwrap();

        let resp = client
            .reserve_identity_ranges(ReserveIdentityRanges {
                table_id: SEQ_TABLE.to_string(),
                reservations: vec![
                    IdentityReservation {
                        sequence_id: "seq-a".to_string(),
                        count: 3,
                        step: Some(2),
                    },
                    IdentityReservation {
                        sequence_id: "seq-b".to_string(),
                        count: 2,
                        step: Some(10),
                    },
                ],
            })
            .await
            .unwrap();
        // Ranges are positional with the requested reservations.
        assert_eq!(resp.ranges[0].sequence_id, "seq-a");
        assert_eq!(
            (resp.ranges[0].range_start, resp.ranges[0].range_end),
            (0, 4)
        );
        assert_eq!(resp.ranges[1].sequence_id, "seq-b");
        assert_eq!(
            (resp.ranges[1].range_start, resp.ranges[1].range_end),
            (100, 110)
        );
    }

    #[tokio::test]
    async fn sequence_create_is_idempotent_on_matching_definition() {
        let client = InMemorySequenceClient::new();
        client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 1)]))
            .await
            .unwrap();
        // Reserve to advance the cursor, then re-create with the same definition.
        client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-a", 5))
            .await
            .unwrap();
        client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 1)]))
            .await
            .unwrap();
        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-a", 1))
            .await
            .unwrap();
        assert_eq!(resp.ranges[0].range_start, 6);
    }

    #[tokio::test]
    async fn sequence_create_conflicting_definition_fails() {
        let client = InMemorySequenceClient::new();
        client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 1)]))
            .await
            .unwrap();
        let err = client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 2)]))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::UnsupportedOperation(msg) if msg.contains("different definition")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_create_rejects_empty_batch() {
        let client = InMemorySequenceClient::new();
        let err = client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn sequence_create_rejects_zero_step() {
        let client = InMemorySequenceClient::new();
        let err = client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec("seq-a", 1, 0)]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn sequence_create_rejects_duplicate_id_within_request() {
        let client = InMemorySequenceClient::new();
        let err = client
            .create_identity_sequences(create_req(
                SEQ_TABLE,
                vec![spec("seq-a", 1, 1), spec("seq-a", 2, 1)],
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::UnsupportedOperation(msg) if msg.contains("duplicate sequence id")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_create_rejects_oversized_id() {
        let client = InMemorySequenceClient::new();
        let long_id = "x".repeat(MAX_SEQUENCE_ID_LEN + 1);
        let err = client
            .create_identity_sequences(create_req(SEQ_TABLE, vec![spec(&long_id, 1, 1)]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn sequence_reserve_with_step_greater_than_1() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 0, 10).unwrap();

        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 3))
            .await
            .unwrap();
        // Values: 0, 10, 20
        assert_eq!(resp.ranges[0].range_start, 0);
        assert_eq!(resp.ranges[0].range_end, 20);
        assert_eq!(resp.ranges[0].step, 10);
    }

    #[tokio::test]
    async fn sequence_reserve_with_negative_step() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 10, -2).unwrap();

        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 6))
            .await
            .unwrap();
        // Values: 10, 8, 6, 4, 2, 0. range_end is the last value, not the larger bound.
        assert_eq!(resp.ranges[0].range_start, 10);
        assert_eq!(resp.ranges[0].range_end, 0);
        assert_eq!(resp.ranges[0].step, -2);

        // The next reservation continues descending from -2.
        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 2))
            .await
            .unwrap();
        assert_eq!(resp.ranges[0].range_start, -2);
        assert_eq!(resp.ranges[0].range_end, -4);
        assert_eq!(resp.ranges[0].step, -2);
    }

    #[tokio::test]
    async fn sequence_reserve_nonexistent_fails() {
        let client = InMemorySequenceClient::new();
        let result = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "nonexistent", 5))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sequence_reserve_wrong_table_fails() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1).unwrap();
        // The sequence exists, but under a different table.
        let result = client
            .reserve_identity_ranges(reserve_one("other-table", "seq-1", 5))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sequence_reserve_step_mismatch_fails() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1).unwrap();
        let err = client
            .reserve_identity_ranges(ReserveIdentityRanges {
                table_id: SEQ_TABLE.to_string(),
                reservations: vec![IdentityReservation {
                    sequence_id: "seq-1".to_string(),
                    count: 5,
                    step: Some(2),
                }],
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::UnsupportedOperation(msg) if msg.contains("does not match")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_reserve_batch_is_atomic_on_failure() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-a", 1, 1).unwrap();
        client.seed_sequence(SEQ_TABLE, "seq-b", 1, 1).unwrap();
        // seq-b's step mismatch fails the whole batch. So seq-a must not advance.
        let err = client
            .reserve_identity_ranges(ReserveIdentityRanges {
                table_id: SEQ_TABLE.to_string(),
                reservations: vec![
                    IdentityReservation {
                        sequence_id: "seq-a".to_string(),
                        count: 10,
                        step: None,
                    },
                    IdentityReservation {
                        sequence_id: "seq-b".to_string(),
                        count: 10,
                        step: Some(99),
                    },
                ],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperation(_)));

        // The next reservation for seq-a still starts at 1.
        let resp = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-a", 1))
            .await
            .unwrap();
        assert_eq!(resp.ranges[0].range_start, 1);
    }

    #[test]
    fn sequence_seed_duplicate_fails() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1).unwrap();
        assert!(matches!(
            client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1),
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[tokio::test]
    async fn sequence_successive_reservations_non_overlapping() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1).unwrap();

        let r1 = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 100))
            .await
            .unwrap();
        let r2 = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 100))
            .await
            .unwrap();

        let (r1, r2) = (&r1.ranges[0], &r2.ranges[0]);
        assert!(r1.range_end < r2.range_start || r2.range_end < r1.range_start);
    }

    #[tokio::test]
    async fn sequence_reserve_stride_overflow_distinct_error() {
        let client = InMemorySequenceClient::new();
        client
            .seed_sequence(SEQ_TABLE, "seq-1", 0, i64::MAX)
            .unwrap();
        let err = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 3))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Generic(msg) if msg.contains("stride overflow")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_reserve_range_end_overflow_distinct_error() {
        let client = InMemorySequenceClient::new();
        client
            .seed_sequence(SEQ_TABLE, "seq-1", i64::MAX - 1, 2)
            .unwrap();
        let err = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 2))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Generic(msg) if msg.contains("range_end overflow")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_reserve_cursor_advance_overflow_distinct_error() {
        // First reservation succeeds, but advancing `current` past range_end overflows.
        let client = InMemorySequenceClient::new();
        client
            .seed_sequence(SEQ_TABLE, "seq-1", i64::MAX - 1, 1)
            .unwrap();
        // count=2 -> range_start=i64::MAX-1, range_end=i64::MAX. Cursor advance = i64::MAX + 1
        // -> overflow.
        let err = client
            .reserve_identity_ranges(reserve_one(SEQ_TABLE, "seq-1", 2))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Generic(msg) if msg.contains("cursor advance overflow")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn sequence_drop_reports_existed_and_dedups() {
        let client = InMemorySequenceClient::new();
        client.seed_sequence(SEQ_TABLE, "seq-1", 1, 1).unwrap();

        let resp = client
            .drop_identity_sequences(DropIdentitySequences {
                table_id: SEQ_TABLE.to_string(),
                // "seq-1" exists; "absent" does not; the duplicate "seq-1" is de-duplicated.
                sequence_ids: vec![
                    "seq-1".to_string(),
                    "absent".to_string(),
                    "seq-1".to_string(),
                ],
            })
            .await
            .unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].sequence_id, "seq-1");
        assert!(resp.results[0].existed);
        assert_eq!(resp.results[1].sequence_id, "absent");
        assert!(!resp.results[1].existed);

        // The sequence is really gone, so dropping again reports existed=false.
        let resp = client
            .drop_identity_sequences(DropIdentitySequences {
                table_id: SEQ_TABLE.to_string(),
                sequence_ids: vec!["seq-1".to_string()],
            })
            .await
            .unwrap();
        assert!(!resp.results[0].existed);
    }

    #[tokio::test]
    async fn sequence_drop_rejects_empty_request() {
        let client = InMemorySequenceClient::new();
        let err = client
            .drop_identity_sequences(DropIdentitySequences {
                table_id: SEQ_TABLE.to_string(),
                sequence_ids: vec![],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperation(_)));
    }
}
