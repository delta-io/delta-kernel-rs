use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::credentials::StorageCredential;

// ============================================================================
// Delta-Commits API (legacy)
//
// These types back the current Delta-Commits read/commit path. They are superseded by the
// Delta-Tables types below (update_table / load_table) and will be deleted once the read and
// commit paths are swapped onto the new API. Kept here so the crate and its downstream consumers
// keep compiling during the migration.
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitsRequest {
    pub table_id: String,
    pub table_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_version: Option<i64>,
}

impl CommitsRequest {
    pub fn new(table_id: impl Into<String>, table_uri: impl Into<String>) -> Self {
        Self {
            table_id: table_id.into(),
            table_uri: table_uri.into(),
            start_version: None,
            end_version: None,
        }
    }

    pub fn with_start_version(mut self, version: i64) -> Self {
        self.start_version = Some(version);
        self
    }

    pub fn with_end_version(mut self, version: i64) -> Self {
        self.end_version = Some(version);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commits: Option<Vec<Commit>>,
    pub latest_table_version: i64,
}

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

    pub fn timestamp_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.timestamp)
    }

    pub fn file_modification_as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(self.file_modification_timestamp)
    }
}

/// Request to commit a new version to the table. It must include either a `commit_info` or
/// `latest_backfilled_version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub table_id: String,
    pub table_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_backfilled_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<serde_json::Value>,
}

impl CommitRequest {
    pub fn new(
        table_id: impl Into<String>,
        table_uri: impl Into<String>,
        commit_info: Commit,
        latest_backfilled_version: Option<i64>,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            table_uri: table_uri.into(),
            commit_info: Some(commit_info),
            latest_backfilled_version,
            metadata: None,
            protocol: None,
        }
    }

    // TODO: expose metadata/protocol (with_metadata, with_protocol)
}

// ============================================================================
// update_table: typed requirements + updates
// ============================================================================

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

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}`
/// (`update_table`) request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    /// Catalog name. Used for URL routing.
    #[serde(skip)]
    pub catalog: String,
    /// Schema name. Used for URL routing.
    #[serde(skip)]
    pub schema: String,
    /// Table name. Used for URL routing.
    #[serde(skip)]
    pub table_name: String,
    /// Preconditions the catalog must validate.
    pub requirements: Vec<DeltaTableRequirement>,
    /// Updates to apply atomically.
    pub updates: Vec<DeltaTableUpdate>,
}

impl UpdateTableRequest {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table_name: impl Into<String>,
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
            catalog: catalog.into(),
            schema: schema.into(),
            table_name: table_name.into(),
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
    fn update_table_request_skips_routing_fields() {
        let req = UpdateTableRequest::new("cat", "sch", "tbl", vec![], vec![]).unwrap();
        let v = serde_json::to_value(&req).unwrap();
        let obj = v.as_object().unwrap();
        for key in ["catalog", "schema", "table_name", "table-name"] {
            assert!(
                !obj.contains_key(key),
                "routing field {key} must not be serialized"
            );
        }
        assert!(obj.contains_key("requirements"));
        assert!(obj.contains_key("updates"));
    }

    #[test]
    fn table_uuid_returns_none_when_only_etag_requirement_present() {
        let req = UpdateTableRequest::new(
            "c",
            "s",
            "t",
            vec![DeltaTableRequirement::AssertEtag { etag: "e1".into() }],
            vec![],
        )
        .unwrap();
        assert_eq!(req.table_uuid(), None);
    }

    #[test]
    fn table_uuid_returns_uuid_when_assert_table_uuid_present() {
        let req = UpdateTableRequest::new(
            "c",
            "s",
            "t",
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
                UpdateTableRequest::new("c", "s", "t", dup, vec![]).is_err(),
                "duplicate requirement of the same type must be rejected"
            );
        }

        // One of each type is allowed.
        assert!(UpdateTableRequest::new(
            "c",
            "s",
            "t",
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
            "c",
            "s",
            "t",
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
            "c",
            "s",
            "t",
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
}
