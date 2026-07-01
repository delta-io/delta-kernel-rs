use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// update_table: typed requirements + updates
// ============================================================================

/// A precondition that the catalog server must validate before applying a
/// commit.
///
/// Kernel emits `AssertTableUuid`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Requirement {
    /// Assert that the table being updated has the given UUID. Used to detect
    /// the case where a table has been dropped and recreated under the same
    /// name between the client's last read and this update.
    AssertTableUuid {
        /// Table UUID, matching `Metadata.id` from the Delta log.
        uuid: String,
    },
    /// Assert that the table's etag matches the expected value. Used for
    /// optimistic-concurrency control on metadata-only updates.
    AssertEtag {
        /// Expected etag value, obtained from a prior `load_table` response.
        etag: String,
    },
}

/// A typed update to apply atomically as part of an `update_table` call.
///
/// Variants and field names mirror the OpenAPI `TableUpdate` discriminator
/// (`action` tag, kebab-case on the wire).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum Update {
    /// Register a new staged commit with the catalog.
    AddCommit {
        /// Reference to the staged commit file just written to cloud storage.
        commit: Commit,
    },
    /// Inform the catalog of the latest published version. Catalog uses this
    /// to clean up unpublished commits whose data has been backfilled into
    /// the Delta log.
    SetLatestBackfilledVersion {
        /// The most recent version that has been published to the Delta log.
        #[serde(rename = "latest-published-version")]
        latest_published_version: i64,
    },
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
}

/// Body of a `POST /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}`
/// (`update_table`) request. The catalog applies all updates atomically iff
/// every requirement holds.
///
/// The `catalog`, `schema`, and `table_name` fields identify the target table
/// for URL routing only; they are not serialized into the request body
/// because the wire protocol carries them in the URL path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    /// Catalog name. Used for URL path only; not serialized.
    #[serde(skip)]
    pub catalog: String,
    /// Schema name. Used for URL path only; not serialized.
    #[serde(skip)]
    pub schema: String,
    /// Table name. Used for URL path only; not serialized.
    #[serde(skip)]
    pub table_name: String,
    /// Preconditions the catalog must validate.
    pub requirements: Vec<Requirement>,
    /// Updates to apply atomically.
    pub updates: Vec<Update>,
}

impl UpdateTableRequest {
    /// Construct an `UpdateTableRequest` from its fields.
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table_name: impl Into<String>,
        requirements: Vec<Requirement>,
        updates: Vec<Update>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table_name: table_name.into(),
            requirements,
            updates,
        }
    }

    /// The table UUID asserted by this request's `AssertTableUuid` requirement, if present.
    pub fn table_uuid(&self) -> Option<&str> {
        self.requirements.iter().find_map(|r| match r {
            Requirement::AssertTableUuid { uuid } => Some(uuid.as_str()),
            Requirement::AssertEtag { .. } => None,
        })
    }

    /// The `AddCommit` staged-commit reference in this request's updates, if present.
    pub fn add_commit(&self) -> Option<&Commit> {
        self.updates.iter().find_map(|u| match u {
            Update::AddCommit { commit } => Some(commit),
            _ => None,
        })
    }
}

/// Response from the `update_table` endpoint.
///
/// The server returns the full post-commit table state; these fields are optional and unused by
/// this client.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateTableResponse {
    /// Committed version. `None` when the server omits the field from its response.
    #[serde(default)]
    pub committed_version: Option<i64>,
    /// Optimistic-concurrency token returned by the catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

// ============================================================================
// load_table: read-side response
// ============================================================================

/// Response from `GET /delta/v1/catalogs/{c}/schemas/{s}/tables/{t}`
/// (`load_table`).
///
/// In the UC API the read path is a single RPC: the server returns the
/// table's full metadata plus any unpublished commits inline so the connector
/// can build a `Snapshot` without any further RPCs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResponse {
    /// Full table metadata (required by the spec).
    pub metadata: TableMetadata,
    /// Unpublished commits known to the catalog at this read, sorted by
    /// version ascending. Optional per spec.
    #[serde(default)]
    pub commits: Vec<Commit>,
    /// Optional UniForm conversion metadata. Modeled as opaque JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uniform: Option<serde_json::Value>,
    /// Highest version the catalog knows about, including data-only commits.
    /// Compare with `metadata.last_commit_version`, which only tracks
    /// metadata-changing commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_table_version: Option<i64>,
}

/// Full table metadata returned by `load_table`.
///
/// Mirrors the OpenAPI `TableMetadata` schema; all listed fields are required
/// per the spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Entity tag for optimistic-concurrency control.
    pub etag: String,
    /// Table type, e.g. `"MANAGED"` or `"EXTERNAL"`. Wire field: `table-type`.
    pub table_type: String,
    /// Stable table UUID, matching `Metadata.id` in the Delta log. Wire field:
    /// `table-uuid`.
    pub table_uuid: String,
    /// Cloud-storage location of the table's `_delta_log/` parent.
    pub location: String,
    /// Creation time in epoch milliseconds. Wire field: `created-time`.
    pub created_time: i64,
    /// Last update time in epoch milliseconds. Wire field: `updated-time`.
    pub updated_time: i64,
    /// Schema as a Delta `StructType` JSON value.
    pub columns: serde_json::Value,
    /// Partition column names, in declaration order. Wire field:
    /// `partition-columns`.
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Raw `metaData.configuration` properties.
    pub properties: HashMap<String, String>,
    /// Version of the last commit that changed table metadata. Wire field:
    /// `last-commit-version`.
    #[serde(default)]
    pub last_commit_version: i64,
    /// Timestamp of the last metadata-changing commit, in epoch milliseconds.
    /// Wire field: `last-commit-timestamp-ms`.
    #[serde(default)]
    pub last_commit_timestamp_ms: i64,
}

// ============================================================================
// /config: protocol negotiation
// ============================================================================

/// Response from `GET /delta/v1/config`.
///
/// The server returns the list of versioned endpoints the client should use
/// for subsequent calls, plus the negotiated protocol version.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogConfig {
    /// Supported endpoints, e.g. `"POST /v1/catalogs/{catalog}/schemas/{schema}/tables"`.
    pub endpoints: Vec<String>,
    /// Negotiated protocol version, e.g. `"1.0"`. Wire field: `protocol-version`.
    #[serde(rename = "protocol-version")]
    pub protocol_version: String,
}

// ============================================================================
// Protocol wire type
// ============================================================================

/// Typed protocol fields used in read responses and credential vending.
///
/// Mirrors Delta's `Protocol` action. Reader/writer feature lists are sent as
/// strings on the wire; using `Vec<String>` keeps the api crate kernel-free.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_table_response_decodes_full_load_table_body_as_none() {
        // The real server returns a full table state with no top-level `committed-version`;
        // decoding must tolerate the missing key (and unknown extras) and leave the fields
        // `None`.
        let body = r#"{
            "metadata": {"table-uuid": "abc", "location": "s3://bucket/t"},
            "commits": [],
            "latest-table-version": 7
        }"#;
        let resp: UpdateTableResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.committed_version, None);
        assert_eq!(resp.etag, None);
    }

    #[test]
    fn update_table_response_decodes_committed_version_and_etag() {
        let resp: UpdateTableResponse =
            serde_json::from_str(r#"{"committed-version":42,"etag":"e1"}"#).unwrap();
        assert_eq!(resp.committed_version, Some(42));
        assert_eq!(resp.etag.as_deref(), Some("e1"));
    }

    // === Serde golden tests: pin the wire tag + renamed keys for the tagged enums. ===
    // Assert only the discriminator tag and any kebab-case-renamed keys, not the whole blob,
    // to avoid over-fitting to field values.

    fn sample_commit() -> Commit {
        Commit::new(7, 1234, "00000000000000000007.uuid.json", 100, 5678)
    }

    #[test]
    fn update_add_commit_wire_shape() {
        let v = serde_json::to_value(Update::AddCommit {
            commit: sample_commit(),
        })
        .unwrap();
        assert_eq!(v["action"], "add-commit");
        assert!(v.get("commit").is_some());
    }

    #[test]
    fn update_set_latest_backfilled_version_wire_shape() {
        let v = serde_json::to_value(Update::SetLatestBackfilledVersion {
            latest_published_version: 9,
        })
        .unwrap();
        assert_eq!(v["action"], "set-latest-backfilled-version");
        assert_eq!(v["latest-published-version"], 9);
    }

    #[test]
    fn requirement_assert_table_uuid_wire_shape() {
        let v = serde_json::to_value(Requirement::AssertTableUuid {
            uuid: "abc".to_string(),
        })
        .unwrap();
        assert_eq!(v["type"], "assert-table-uuid");
        assert_eq!(v["uuid"], "abc");
    }

    #[test]
    fn requirement_assert_etag_wire_shape() {
        let v = serde_json::to_value(Requirement::AssertEtag {
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
        let req = UpdateTableRequest::new("cat", "sch", "tbl", vec![], vec![]);
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
}
