use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::credentials::StorageCredential;

// ============================================================================
// update_table: typed requirements + updates
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct Commit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

impl Commit {
    /// Create a new commit to send to UC with the specified version and timestamp.
    pub fn new(
        version: i64,
        timestamp: i64,
        file_name: impl Into<String>,
        file_size: i64,
        file_modification_timestamp: i64,
    ) -> Self {
        Self {
            version,
            timestamp,
            file_name: file_name.into(),
            file_size,
            file_modification_timestamp,
        }
    }
}

/// A precondition that the catalog server must validate before applying a
/// commit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DeltaTableRequirement {
    /// Assert that the table being updated has the given UUID. Used to detect
    /// the case where a table has been dropped and recreated under the same
    /// name between the client's last read and this update.
    AssertTableUuid { uuid: String },
    /// Assert that the table's etag matches the expected value. Used for
    /// optimistic-concurrency control on metadata-only updates.
    AssertEtag { etag: String },
}

/// A typed update to apply atomically as part of an `update_table` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum DeltaTableUpdate {
    /// Register a new staged commit with the catalog.
    AddCommit { commit: Commit },
    /// Inform the catalog of the latest published version.
    SetLatestBackfilledVersion {
        #[serde(rename = "latest-published-version")]
        latest_published_version: i64,
    },
}

/// The three-part name identifying a UC table, used to route requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TableName {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }
}

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}`
/// (`update_table`) request. The target table is routed separately (see
/// [`crate::UpdateTableClient::update_table`]); this struct is purely the serialized body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    /// Preconditions the catalog must validate.
    pub requirements: Vec<DeltaTableRequirement>,
    /// Updates to apply atomically.
    pub updates: Vec<DeltaTableUpdate>,
}

impl UpdateTableRequest {
    /// Build a request from its preconditions and updates.
    ///
    /// # Errors
    ///
    /// Returns an error if `requirements` contains more than one `AssertTableUuid` or more than
    /// one `AssertEtag` requirement.
    pub fn new(
        requirements: Vec<DeltaTableRequirement>,
        updates: Vec<DeltaTableUpdate>,
    ) -> crate::error::Result<Self> {
        let count = |is_variant: fn(&DeltaTableRequirement) -> bool| {
            requirements.iter().filter(|r| is_variant(r)).count()
        };
        if count(|r| matches!(r, DeltaTableRequirement::AssertTableUuid { .. })) > 1 {
            return Err(crate::error::Error::Generic(
                "update_table request must not contain more than one AssertTableUuid requirement"
                    .to_string(),
            ));
        }
        if count(|r| matches!(r, DeltaTableRequirement::AssertEtag { .. })) > 1 {
            return Err(crate::error::Error::Generic(
                "update_table request must not contain more than one AssertEtag requirement"
                    .to_string(),
            ));
        }
        Ok(Self {
            requirements,
            updates,
        })
    }

    /// The table UUID asserted by this request's `AssertTableUuid` requirement, if present.
    /// [`new`](Self::new) guarantees at most one such requirement.
    pub fn table_uuid(&self) -> Option<&str> {
        self.requirements.iter().find_map(|r| match r {
            DeltaTableRequirement::AssertTableUuid { uuid } => Some(uuid.as_str()),
            _ => None,
        })
    }

    /// The `AddCommit` staged-commit reference in this request's updates, if present.
    pub fn staged_commit(&self) -> Option<&Commit> {
        self.updates.iter().find_map(|u| match u {
            DeltaTableUpdate::AddCommit { commit } => Some(commit),
            _ => None,
        })
    }

    /// The latest backfilled (published) version in this request's updates, if present.
    pub fn latest_backfilled_version(&self) -> Option<i64> {
        self.updates.iter().find_map(|u| match u {
            DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version,
            } => Some(*latest_published_version),
            _ => None,
        })
    }
}

// ============================================================================
// load_table: read-side response
// ============================================================================

/// Response from `GET /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}`
/// (`load_table`).
///
/// The server returns the table's full metadata plus any unpublished commits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResponse {
    /// Full table metadata.
    pub metadata: TableMetadata,
    /// Unpublished commits known to the catalog at this read, in descending
    /// version order (newest first).
    #[serde(default)]
    pub commits: Vec<Commit>,
    /// Optional UniForm conversion metadata. Modeled as opaque JSON.
    // TODO: decode into a typed struct once a consumer needs UniForm metadata.
    #[serde(default)]
    pub uniform: Option<serde_json::Value>,
    /// Highest version the catalog knows about, including data-only commits.
    /// Compare with `metadata.last_commit_version`, which only tracks
    /// metadata-changing commits.
    #[serde(default)]
    pub latest_table_version: Option<i64>,
}

/// Full table metadata returned by `load_table`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Entity tag for optimistic-concurrency control.
    pub etag: String,
    /// Table type, e.g. `"MANAGED"` or `"EXTERNAL"`.
    pub table_type: String,
    /// Stable table UUID, matching `Metadata.id` in the Delta log.
    pub table_uuid: String,
    /// Cloud-storage location of the table's `_delta_log/` parent.
    pub location: String,
    /// Creation time in epoch milliseconds.
    pub created_time: i64,
    /// Last update time in epoch milliseconds.
    pub updated_time: i64,
    /// Schema as a Delta `StructType` JSON value.
    pub columns: serde_json::Value,
    /// Partition column names, in declaration order.
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Raw `metaData.configuration` properties.
    pub properties: HashMap<String, String>,
    /// Version of the last commit that changed table metadata.
    #[serde(default)]
    pub last_commit_version: Option<i64>,
    /// Timestamp of the last metadata-changing commit, in epoch milliseconds.
    #[serde(default)]
    pub last_commit_timestamp_ms: Option<i64>,
}

// ============================================================================
// /config: protocol negotiation
// ============================================================================

/// Response from `GET /delta/v1/config`.
///
/// The server returns the list of versioned endpoints the client should use
/// for subsequent calls, plus the negotiated protocol version.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct CatalogConfig {
    /// Supported endpoints, e.g. `"POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables"`.
    pub endpoints: Vec<String>,
    /// Negotiated protocol version, e.g. `"1.0"`.
    pub protocol_version: String,
}

// ============================================================================
// Protocol wire type
// ============================================================================

/// Typed Delta protocol wire model (mirrors kernel's `Protocol` action).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct Protocol {
    /// Minimum reader version required to read the table.
    pub min_reader_version: i32,
    /// Minimum writer version required to write to the table.
    pub min_writer_version: i32,
    /// Reader features (Delta's `readerFeatures` list).
    #[serde(default)]
    pub reader_features: Vec<String>,
    /// Writer features (Delta's `writerFeatures` list).
    #[serde(default)]
    pub writer_features: Vec<String>,
}

// ============================================================================
// create-table: staging-tables and tables (CREATE flow)
// ============================================================================

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/staging-tables` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateStagingTableRequest {
    /// Table name (catalog and schema are in the URL path).
    pub name: String,
}

/// Response from the `staging-tables` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateStagingTableResponse {
    /// Allocated table UUID.
    pub table_id: String,
    /// Table type (always `"MANAGED"` for staging tables).
    pub table_type: String,
    /// Cloud-storage location for the table's `_delta_log/` parent.
    pub location: String,
    /// Temporary credentials for the initial commit.
    pub storage_credentials: Vec<StorageCredential>,
    /// Required protocol that the v0 commit must satisfy.
    pub required_protocol: Protocol,
    /// Suggested protocol the client should consider. Modeled as opaque JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_protocol: Option<serde_json::Value>,
    /// Required raw configuration entries. Null values mean "any valid value".
    pub required_properties: HashMap<String, Option<String>>,
    /// Suggested raw configuration entries.
    #[serde(default)]
    pub suggested_properties: HashMap<String, Option<String>>,
}

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/tables` request, the
/// typed CREATE finalization payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    /// Table name (catalog and schema are in the URL path).
    pub name: String,
    /// Cloud-storage location of the table root, e.g. `"s3://bucket/path/to/table"`.
    pub location: String,
    /// Table type, e.g. `"MANAGED"` or `"EXTERNAL"`.
    pub table_type: String,
    /// Optional table comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Typed Delta schema: a `StructType` JSON value.
    pub columns: serde_json::Value,
    /// Partition column names, in declaration order.
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Typed Delta protocol.
    pub protocol: Protocol,
    /// Raw `metaData.configuration` entries. The server derives the *protocol* properties
    /// (`delta.minReaderVersion`/`minWriterVersion`, `delta.feature.*`) from the typed `protocol`,
    /// so those must not appear here.
    pub properties: HashMap<String, String>,
    /// Per-domain metadata, keyed by domain name. Each value is opaque JSON.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub domain_metadata: HashMap<String, serde_json::Value>,
    /// Timestamp of version 0 (the commit the client wrote before calling this
    /// endpoint), in epoch milliseconds.
    pub last_commit_timestamp_ms: i64,
}

// ============================================================================
// reportMetrics: post-commit telemetry
// ============================================================================

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}/metrics` request.
///
/// Commit telemetry reported to the catalog after a commit succeeds. This is best-effort
/// telemetry, not part of the commit's correctness path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ReportMetricsRequest {
    /// Table UUID; must match the table identified by the URL path.
    pub table_id: String,
    /// The metrics being reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<MetricsReport>,
}

/// Wrapper matching the server's `report` envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricsReport {
    /// Per-commit statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_report: Option<CommitReport>,
}

/// Statistics describing a single commit.
///
/// File and byte counts are derivable from the commit's add/remove actions. Row counts and the
/// file-size histogram are only known to the connector's write engine, so they are optional.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct CommitReport {
    /// Number of files added.
    pub num_files_added: i64,
    /// Number of bytes added.
    pub num_bytes_added: i64,
    /// Number of files removed.
    pub num_files_removed: i64,
    /// Number of bytes removed.
    pub num_bytes_removed: i64,
    /// Rows inserted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_rows_inserted: Option<i64>,
    /// Rows removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_rows_removed: Option<i64>,
    /// Rows updated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_rows_updated: Option<i64>,
    /// File-size distribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size_histogram: Option<FileSizeHistogram>,
}

/// Histogram tracking file counts and total bytes across size ranges.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct FileSizeHistogram {
    /// Sorted bin boundary values.
    pub sorted_bin_boundaries: Vec<i64>,
    /// Count of files in each bin.
    pub file_counts: Vec<i64>,
    /// Total bytes in each bin.
    pub total_bytes: Vec<i64>,
    /// The commit version this histogram is for.
    pub commit_version: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_table_response_decodes_full_body() {
        // Both load_table and update_table return this full table-state body. Decode a
        // representative one and pin the kebab-case field renames on TableMetadata.
        let body = r#"{
            "metadata": {
                "etag": "e1",
                "table-type": "MANAGED",
                "table-uuid": "abc",
                "location": "s3://bucket/t",
                "created-time": 1,
                "updated-time": 2,
                "columns": {"type": "struct", "fields": []},
                "partition-columns": ["p"],
                "properties": {},
                "last-commit-version": 5,
                "last-commit-timestamp-ms": 6
            },
            "commits": [],
            "latest-table-version": 7
        }"#;
        let resp: LoadTableResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.metadata.table_type, "MANAGED");
        assert_eq!(resp.metadata.table_uuid, "abc");
        assert_eq!(resp.metadata.last_commit_timestamp_ms, Some(6));
        assert_eq!(resp.latest_table_version, Some(7));
    }

    // === Serde golden tests: pin the wire tag + renamed keys for the tagged enums. ===

    fn sample_commit() -> Commit {
        Commit::new(7, 1234, "00000000000000000007.uuid.json", 100, 5678)
    }

    #[test]
    fn update_add_commit_wire_shape() {
        let v = serde_json::to_value(DeltaTableUpdate::AddCommit {
            commit: sample_commit(),
        })
        .unwrap();
        assert_eq!(v["action"], "add-commit");
        assert!(v.get("commit").is_some());
    }

    #[test]
    fn update_set_latest_backfilled_version_wire_shape() {
        let v = serde_json::to_value(DeltaTableUpdate::SetLatestBackfilledVersion {
            latest_published_version: 9,
        })
        .unwrap();
        assert_eq!(v["action"], "set-latest-backfilled-version");
        assert_eq!(v["latest-published-version"], 9);
    }

    #[test]
    fn requirement_assert_table_uuid_wire_shape() {
        let v = serde_json::to_value(DeltaTableRequirement::AssertTableUuid {
            uuid: "abc".to_string(),
        })
        .unwrap();
        assert_eq!(v["type"], "assert-table-uuid");
        assert_eq!(v["uuid"], "abc");
    }

    #[test]
    fn requirement_assert_etag_wire_shape() {
        let v = serde_json::to_value(DeltaTableRequirement::AssertEtag {
            etag: "e1".to_string(),
        })
        .unwrap();
        assert_eq!(v["type"], "assert-etag");
        assert_eq!(v["etag"], "e1");
    }

    #[test]
    fn commit_wire_shape() {
        let v = serde_json::to_value(sample_commit()).unwrap();
        assert_eq!(v["version"], 7);
        assert_eq!(v["timestamp"], 1234);
        assert_eq!(v["file-name"], "00000000000000000007.uuid.json");
        assert_eq!(v["file-size"], 100);
        assert_eq!(v["file-modification-timestamp"], 5678);
    }

    #[test]
    fn update_table_request_serializes_only_body_fields() {
        let req = UpdateTableRequest::new(vec![], vec![]).unwrap();
        let v = serde_json::to_value(&req).unwrap();
        let obj = v.as_object().unwrap();
        assert_eq!(
            obj.len(),
            2,
            "body should have exactly requirements + updates"
        );
        assert!(obj.contains_key("requirements"));
        assert!(obj.contains_key("updates"));
    }

    #[test]
    fn table_uuid_returns_none_when_only_etag_requirement_present() {
        let req = UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertEtag { etag: "e1".into() }],
            vec![],
        )
        .unwrap();
        assert_eq!(req.table_uuid(), None);
    }

    #[test]
    fn table_uuid_returns_uuid_when_assert_table_uuid_present() {
        let req = UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid { uuid: "abc".into() }],
            vec![],
        )
        .unwrap();
        assert_eq!(req.table_uuid(), Some("abc"));
    }

    #[test]
    fn new_rejects_duplicate_requirements() {
        for dup in [
            vec![
                DeltaTableRequirement::AssertTableUuid { uuid: "a".into() },
                DeltaTableRequirement::AssertTableUuid { uuid: "b".into() },
            ],
            vec![
                DeltaTableRequirement::AssertEtag { etag: "e1".into() },
                DeltaTableRequirement::AssertEtag { etag: "e2".into() },
            ],
        ] {
            assert!(
                UpdateTableRequest::new(dup, vec![]).is_err(),
                "duplicate requirement of the same type must be rejected"
            );
        }

        // One of each type is allowed.
        assert!(UpdateTableRequest::new(
            vec![
                DeltaTableRequirement::AssertTableUuid { uuid: "a".into() },
                DeltaTableRequirement::AssertEtag { etag: "e1".into() },
            ],
            vec![],
        )
        .is_ok());
    }

    #[test]
    fn staged_commit_returns_commit_when_add_commit_present() {
        let req = UpdateTableRequest::new(
            vec![],
            vec![DeltaTableUpdate::AddCommit {
                commit: sample_commit(),
            }],
        )
        .unwrap();
        assert_eq!(req.staged_commit(), Some(&sample_commit()));
    }

    #[test]
    fn staged_commit_returns_none_when_no_add_commit_present() {
        let req = UpdateTableRequest::new(
            vec![],
            vec![DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: 9,
            }],
        )
        .unwrap();
        assert_eq!(req.staged_commit(), None);
    }

    #[test]
    fn catalog_config_decodes_kebab_protocol_version() {
        let config: CatalogConfig = serde_json::from_str(
            r#"{"endpoints": ["POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables"], "protocol-version": "1.0"}"#,
        )
        .unwrap();
        assert_eq!(config.protocol_version, "1.0");
        assert_eq!(config.endpoints.len(), 1);
    }

    #[test]
    fn create_staging_table_request_wire_shape() {
        let v = serde_json::to_value(CreateStagingTableRequest {
            name: "t".to_string(),
        })
        .unwrap();
        assert_eq!(v["name"], "t");
    }

    #[test]
    fn create_staging_table_response_decodes_renamed_fields() {
        // Pin the `table-id` rename and that storage-credentials / optional advertisement
        // fields decode.
        let body = r#"{
            "table-id": "abc",
            "table-type": "MANAGED",
            "location": "s3://bucket/t",
            "storage-credentials": [
                {"prefix": "s3://bucket/t/", "operation": "READ_WRITE",
                 "expiration-time-ms": 123, "config": {"s3.access-key-id": "ak"}}
            ],
            "required-protocol": {"min-reader-version": 3, "min-writer-version": 7},
            "required-properties": {}
        }"#;
        let resp: CreateStagingTableResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.table_id, "abc");
        assert_eq!(resp.location, "s3://bucket/t");
        assert_eq!(resp.storage_credentials.len(), 1);
        assert_eq!(resp.required_protocol.min_reader_version, 3);
        assert!(resp.suggested_protocol.is_none());
        assert!(resp.required_properties.is_empty());
    }

    #[test]
    fn create_staging_table_response_decodes_property_maps_and_suggested_protocol() {
        let body = r#"{
            "table-id": "abc",
            "table-type": "MANAGED",
            "location": "s3://bucket/t",
            "storage-credentials": [],
            "required-protocol": {"min-reader-version": 3, "min-writer-version": 7},
            "suggested-protocol": {"min-reader-version": 1, "min-writer-version": 2},
            "required-properties": {"delta.appendOnly": "true", "delta.columnMapping.mode": null},
            "suggested-properties": {"delta.checkpointInterval": null}
        }"#;
        let resp: CreateStagingTableResponse = serde_json::from_str(body).unwrap();
        assert_eq!(
            resp.required_properties["delta.appendOnly"],
            Some("true".to_string())
        );
        assert_eq!(resp.required_properties["delta.columnMapping.mode"], None);
        assert_eq!(resp.suggested_properties["delta.checkpointInterval"], None);
        assert!(resp.suggested_protocol.is_some());
    }

    #[test]
    fn create_table_request_wire_shape() {
        let req = CreateTableRequest {
            name: "t".to_string(),
            location: "s3://bucket/t".to_string(),
            table_type: "MANAGED".to_string(),
            comment: None,
            columns: serde_json::json!({"type": "struct", "fields": []}),
            partition_columns: vec![],
            protocol: Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: vec!["catalogManaged".to_string()],
                writer_features: vec!["catalogManaged".to_string()],
            },
            properties: HashMap::from([("delta.appendOnly".to_string(), "true".to_string())]),
            domain_metadata: HashMap::new(),
            last_commit_timestamp_ms: 42,
        };
        let v = serde_json::to_value(&req).unwrap();
        // Field renames.
        assert_eq!(v["location"], "s3://bucket/t");
        assert_eq!(v["table-type"], "MANAGED");
        assert_eq!(v["last-commit-timestamp-ms"], 42);
        // Protocol is sent typed and separate; properties carry only raw configuration (the server
        // derives `delta.feature.*` / versions itself), so no derived keys leak into `properties`.
        assert_eq!(v["protocol"]["min-reader-version"], 3);
        assert_eq!(v["properties"]["delta.appendOnly"], "true");
        assert!(v["properties"].get("delta.minReaderVersion").is_none());
        assert!(v["properties"]
            .get("delta.feature.catalogManaged")
            .is_none());
        // Empty domain metadata is omitted from the wire body.
        assert!(v.get("domain-metadata").is_none());
    }

    #[test]
    fn create_table_request_serializes_optional_fields_when_present() {
        let req = CreateTableRequest {
            name: "t".to_string(),
            location: "s3://bucket/t".to_string(),
            table_type: "MANAGED".to_string(),
            comment: Some("hello".to_string()),
            columns: serde_json::json!({"type": "struct", "fields": []}),
            partition_columns: vec!["p1".to_string(), "p2".to_string()],
            protocol: Protocol::default(),
            properties: HashMap::new(),
            domain_metadata: HashMap::from([(
                "delta.clustering".to_string(),
                serde_json::json!({"clusteringColumns": [["c1"]]}),
            )]),
            last_commit_timestamp_ms: 42,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["comment"], "hello");
        assert_eq!(v["partition-columns"], serde_json::json!(["p1", "p2"]));
        assert_eq!(
            v["domain-metadata"]["delta.clustering"]["clusteringColumns"][0],
            serde_json::json!(["c1"])
        );
    }

    #[test]
    fn report_metrics_request_wire_shape() {
        let req = ReportMetricsRequest {
            table_id: "abc".to_string(),
            report: Some(MetricsReport {
                commit_report: Some(CommitReport {
                    num_files_added: 3,
                    num_bytes_added: 300,
                    num_files_removed: 1,
                    num_bytes_removed: 100,
                    num_rows_inserted: Some(42),
                    num_rows_removed: None,
                    num_rows_updated: None,
                    file_size_histogram: Some(FileSizeHistogram {
                        sorted_bin_boundaries: vec![0, 1000],
                        file_counts: vec![2, 1],
                        total_bytes: vec![150, 150],
                        commit_version: 7,
                    }),
                }),
            }),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["table-id"], "abc");
        let cr = &v["report"]["commit-report"];
        assert_eq!(cr["num-files-added"], 3);
        assert_eq!(cr["num-bytes-removed"], 100);
        assert_eq!(cr["num-rows-inserted"], 42);
        // Absent optional row counts are omitted from the wire body.
        assert!(cr.get("num-rows-removed").is_none());
        assert!(cr.get("num-rows-updated").is_none());
        assert_eq!(cr["file-size-histogram"]["commit-version"], 7);
        assert_eq!(
            cr["file-size-histogram"]["sorted-bin-boundaries"],
            serde_json::json!([0, 1000])
        );
    }

    #[test]
    fn report_metrics_request_omits_absent_report() {
        let req = ReportMetricsRequest {
            table_id: "abc".to_string(),
            report: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["table-id"], "abc");
        assert!(v.get("report").is_none());
    }
}
