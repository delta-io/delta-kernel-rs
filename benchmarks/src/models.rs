//! Data models for workload specifications

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::schema::Schema;
use serde::Deserialize;
use url::Url;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// ReadConfig represents a specific configuration for a read operation
/// A config represents configurations for a specific benchmark that aren't specified in the spec JSON file
#[derive(Clone, Debug)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

/// Provides a default set of read configs for a given table, read spec, and operation
pub fn default_read_configs() -> Vec<ReadConfig> {
    vec![ReadConfig {
        name: "serial".into(),
        parallel_scan: ParallelScan::Disabled,
    }]
}

#[derive(Clone, Debug)]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
}

/// Info needed to access a UC-managed table via credential vending.
/// This covers both catalog-managed and non-catalog-managed UC tables.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogInfo {
    /// Fully-qualified table name: "catalog.schema.table"
    pub table_name: String,
}

/// Table info JSON files are located at the root of each table directory
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    /// Table name is a short identifier for the table (part of the final benchmark name), e.g. 100Adds0Chkpts
    pub name: String,
    /// Human-readable description of the table. Use this to capture context that the name alone
    /// doesn't convey (e.g. "A table with 1 commit with 1M add actions. This includes a commit file
    /// in the delta log, but no actual Parquet data files and no CRC files").
    /// The description is a free-form more verbose description for human readers.
    pub description: String,
    /// URL to the table. Used for remote tables (e.g. `s3://my-bucket/my-table`) or (rarely)
    /// absolute local paths. If `None`, the table is assumed to be in the `delta/` subdirectory
    /// next to `tableInfo.json`. Mutually exclusive with `catalog_info`.
    pub table_path: Option<Url>,
    /// Info needed to access a UC-managed table via credential vending.
    /// When present, the engine is set up with UC-vended credentials instead of local/S3 access.
    /// Whether to use `UCKernelClient` (catalog-managed) or standard snapshot builder is
    /// determined by the `delta.feature.catalogManaged` property.
    /// Mutually exclusive with `table_path`.
    /// TODO(#2303): Create an enum type that ensures table_path and catalog_info are mutually exclusive
    pub catalog_info: Option<CatalogInfo>,
    /// Schema at the latest version of the table, in Delta protocol JSON format
    /// e.g. `{"type": "struct", "fields": [...]}`
    pub schema: Schema,
    /// Delta protocol requirements at the latest version of the table
    /// e.g. `{"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": [], "writerFeatures": []}`
    pub protocol: Protocol,
    /// Log-level statistics giving a quick overview of the table without requiring a full log
    /// replay. See [`LogInfo`] for field details.
    pub log_info: LogInfo,
    /// Table properties from the Delta metadata action (string key-value pairs). Use `{}` if
    /// none. e.g. `{"delta.enableDeletionVector": "true", "delta.columnMapping.mode": "none"}`
    pub properties: HashMap<String, String>,
    /// Physical data layout of the table. Use `{}` for unpartitioned/unclustered tables.
    /// See [`DataLayout`] for partitioned and clustered variants.
    pub data_layout: DataLayout,
    /// Tags for filtering which tables are benchmarked via `BENCH_TAGS`. Use `[]` if none.
    /// Built-in tag: `base` (run in CI). e.g. `["base", "my-feature"]`
    #[serde(default)]
    pub tags: Vec<String>,
    /// Path to the directory containing the `tableInfo.json` file
    #[serde(skip, default)]
    pub table_info_dir: PathBuf,
}

impl TableInfo {
    /// Returns true if the table has at least one tag in common with `required`
    /// Tag matching uses union semantics; any single matching tag is sufficient
    pub fn matches_tags(&self, required: &[String]) -> bool {
        required.iter().any(|r| self.tags.contains(r))
    }

    pub fn resolved_table_root(&self) -> Url {
        self.table_path.clone().unwrap_or_else(|| {
            // If table path is not provided, assume that the Delta table is in a delta/ subdirectory at the same level as tableInfo.json
            Url::from_file_path(self.table_info_dir.join("delta"))
                .expect("table_info_dir must be an absolute path")
        })
    }

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let mut table_info: TableInfo = serde_json::from_str(&content)?;
        if table_info.catalog_info.is_some() && table_info.table_path.is_some() {
            return Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "catalog_info and table_path are mutually exclusive",
            )));
        }
        // Stores the parent directory of the `tableInfo.json` file
        if let Some(parent) = path.as_ref().parent() {
            table_info.table_info_dir = parent.to_path_buf();
        }
        Ok(table_info)
    }
}

/// Log-level information describing the history and structure of a Delta table
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogInfo {
    /// Number of active Add file actions in the table
    pub num_add_files: u64,
    /// Number of Remove file actions in the table
    pub num_remove_files: u64,
    /// Total on-disk size of all data files in bytes
    pub size_in_bytes: Option<u64>,
    /// Number of commits (JSON log files) in the table history
    pub num_commits: u64,
    /// Total number of actions across all commits
    pub num_actions: u64,
    /// Version of the most recent checkpoint, if any
    pub last_checkpoint_version: Option<u64>,
    /// Version of the most recent CRC file, if any
    pub last_crc_version: Option<u64>,
    /// Number of part files in the most recent multi-part checkpoint, if any.
    /// For classic multi-part checkpoints this is the number of parquet parts; for V2 checkpoints this is the number of sidecar files
    /// For workloads that don't have multi-part checkpoints/sidecars, this is `None`
    pub num_checkpoint_files: Option<u32>,
}

/// Physical data layout of a Delta table
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum DataLayout {
    /// Partitioned table. Two tables with the same number of partition columns can differ
    /// significantly in cardinality (e.g. 1 column with 100 distinct values vs. 10000), so both
    /// are tracked. e.g. `{"numPartitionColumns": 2, "numDistinctPartitions": 100}`
    Partitioned {
        /// Number of partition columns
        #[serde(rename = "numPartitionColumns")]
        num_partition_columns: u32,
        /// Number of distinct partition values observed in the table
        #[serde(rename = "numDistinctPartitions")]
        num_distinct_partitions: u64,
    },
    /// Clustered table, e.g. `{"numClusteringColumns": 1}`
    Clustered {
        /// Number of clustering columns
        #[serde(rename = "numClusteringColumns")]
        num_clustering_columns: u32,
    },
    /// No special data organization (unpartitioned, unclustered). Serializes as `{}`
    None {},
}

/// Time travel parameter. Either version or timestamp.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum TimeTravel {
    /// Timetravel to a specific snapshot/read version specified by the workload JSON.
    /// It is i64 so that negative values from the test spec can be deserialized correctly.
    /// [`TimeTravel::as_version`] will reject values below zero.
    Version { version: i64 },
    /// Timetravel to a specific timestamp specified by the workload JSON.
    /// (not yet supported)
    Timestamp { timestamp: String },
}

impl TimeTravel {
    /// Returns the version if this is version-based time travel and the version is non-negative.
    ///
    /// Returns an error for negative versions or for timestamp-based time travel (which is not
    /// yet supported).
    pub fn as_version(&self) -> Result<u64, &'static str> {
        match self {
            TimeTravel::Version { version } => u64::try_from(*version)
                .map_err(|_| "Only non-negative snapshot versions are supported"),
            TimeTravel::Timestamp { .. } => Err("Timestamp-based time travel is not yet supported"),
        }
    }
}

/// Spec defines the operation performed on a table - defines what operation at what version (e.g. read at version 0)
/// There will be multiple specs for a given table
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Spec {
    Read(ReadSpec),
    #[serde(alias = "snapshot_construction")]
    SnapshotConstruction(Box<SnapshotConstructionSpec>),
}

/// Specification for a read workload.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSpec {
    // Time travel version or timestamp for the read
    #[serde(flatten)]
    pub time_travel: Option<TimeTravel>,
    /// SQL WHERE clause expression (e.g. "id < 500"). Parsed into a kernel `Predicate`
    /// and passed to the scan builder for data skipping.
    pub predicate: Option<String>,
    // Column projections to read
    pub columns: Option<Vec<String>>,
    /// Expected outcome - either success with row count or error with code.
    #[serde(flatten)]
    pub expected: Option<ReadExpected>,
}

impl ReadSpec {
    pub fn as_str(&self) -> &str {
        "read"
    }
}

/// Specification for a snapshot construction workload.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotConstructionSpec {
    // Time travel version or timestamp for the read
    #[serde(flatten)]
    pub time_travel: Option<TimeTravel>,
    /// Expected outcome - either success with protocol/metadata or error with code.
    #[serde(flatten)]
    pub expected: Option<SnapshotExpected>,
}

impl SnapshotConstructionSpec {
    pub fn as_str(&self) -> &str {
        "snapshotConstruction"
    }
}

impl Spec {
    pub fn as_str(&self) -> &str {
        match self {
            Spec::Read(read_spec) => read_spec.as_str(),
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                snapshot_construction_spec.as_str()
            }
        }
    }

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let spec: Spec = serde_json::from_str(&content)?;
        Ok(spec)
    }
}

/// Expected error outcome for a workload.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedError {
    pub error_code: String,
    pub error_message: Option<String>,
}

/// Expected success outcome for a read workload.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadExpectedSuccess {
    pub row_count: u64,
    pub file_count: Option<u64>,
    pub files_skipped: Option<u64>,
}

/// Expected result for read operations - either success or error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ReadExpected {
    Success { expected: ReadExpectedSuccess },
    Error { error: ExpectedError },
}

/// Expected success outcome for a snapshot construction workload.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotExpectedSuccess {
    pub protocol: Box<Protocol>,
    pub metadata: Box<Metadata>,
}

/// Expected result for snapshot operations - either success or error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SnapshotExpected {
    Success { expected: SnapshotExpectedSuccess },
    Error { error: ExpectedError },
}

/// For Read specs, we will either run a read data operation or a read metadata operation
#[derive(Clone, Copy, Debug)]
pub enum ReadOperation {
    ReadData,
    ReadMetadata,
}

impl ReadOperation {
    pub fn as_str(&self) -> &str {
        match self {
            ReadOperation::ReadData => "readData",
            ReadOperation::ReadMetadata => "readMetadata",
        }
    }
}

/// Partial workload specification loaded from JSON - table, case name, and spec only
#[derive(Clone, Debug)]
pub struct Workload {
    pub table_info: TableInfo,
    /// Spec filename without extension; used as the benchmark case label
    pub case_name: String,
    pub spec: Spec,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn make_table_info(tags: &[&str]) -> TableInfo {
        let tags_json = serde_json::to_string(tags).unwrap();
        let json = format!(
            r#"{{"name":"t","description":"d","schema":{{"type":"struct","fields":[]}},"protocol":{{"minReaderVersion":1,"minWriterVersion":2}},"logInfo":{{"numAddFiles":0,"numRemoveFiles":0,"sizeInBytes":0,"numCommits":1,"numActions":1}},"properties":{{}},"dataLayout":{{}},"tags":{}}}"#,
            tags_json
        );
        serde_json::from_str(&json).unwrap()
    }

    #[rstest]
    #[case(&["ci", "checkpoints"], &["ci"], true)]
    #[case(&["ci", "checkpoints"], &["ci", "large"], true)]
    #[case(&["large"], &["ci", "checkpoints"], false)]
    #[case(&[], &["ci"], false)]
    fn test_matches_tags(
        #[case] table_tags: &[&str],
        #[case] required: &[&str],
        #[case] expected: bool,
    ) {
        let info = make_table_info(table_tags);
        let required: Vec<String> = required.iter().map(|s| s.to_string()).collect();
        assert_eq!(info.matches_tags(&required), expected);
    }

    #[rstest]
    #[case(
        r#"{
            "name": "test_table",
            "description": "A test table",
            "tablePath": "s3://bucket/test_table",
            "schema": {"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}]},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 3, "numRemoveFiles": 1, "sizeInBytes": 1535, "numCommits": 100, "numActions": 10000},
            "properties": {},
            "dataLayout": {"numPartitionColumns": 2, "numDistinctPartitions": 4},
            "tags": ["base"]
        }"#,
        "test_table",
        "A test table",
        Some(Url::parse("s3://bucket/test_table").unwrap()),
        &["base"]
    )]
    #[case(
        r#"{
            "name": "no_path_table",
            "description": "No path specified",
            "schema": {"type": "struct", "fields": []},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 1},
            "properties": {},
            "dataLayout": {"numClusteringColumns": 2},
            "tags": ["base"]
        }"#,
        "no_path_table",
        "No path specified",
        None,
        &["base"]
    )]
    #[case(
        r#"{
            "name": "extra_fields_table",
            "description": "Has extra fields",
            "extraField": "should be ignored",
            "schema": {"type": "struct", "fields": []},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 1},
            "properties": {},
            "dataLayout": {"numClusteringColumns": 1},
            "tags": []
        }"#,
        "extra_fields_table",
        "Has extra fields",
        None,
        &[]
    )]
    fn test_deserialize_table_info(
        #[case] json: &str,
        #[case] expected_name: &str,
        #[case] expected_description: &str,
        #[case] expected_table_path: Option<Url>,
        #[case] expected_tags: &[&str],
    ) {
        let table_info: TableInfo =
            serde_json::from_str(json).expect("Failed to deserialize table info");

        assert_eq!(table_info.name, expected_name);
        assert_eq!(table_info.description, expected_description);
        assert_eq!(table_info.table_path, expected_table_path);
        let expected_tags: Vec<String> = expected_tags.iter().map(|s| s.to_string()).collect();
        assert_eq!(table_info.tags, expected_tags);
    }

    #[rstest]
    #[case(
        r#"{
            "name": "catalog_table", "description": "A catalog-managed table",
            "catalogInfo": {"tableName": "main.schema.table"},
            "schema": {"type": "struct", "fields": []},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 1},
            "properties": {}, "dataLayout": {}, "tags": []
        }"#,
        true,
        "main.schema.table"
    )]
    #[case(
        r#"{
            "name": "local_table", "description": "A local table",
            "schema": {"type": "struct", "fields": []},
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 1},
            "properties": {}, "dataLayout": {}, "tags": []
        }"#,
        false,
        ""
    )]
    fn test_deserialize_catalog_info_field(
        #[case] json: &str,
        #[case] expect_present: bool,
        #[case] expected_table_name: &str,
    ) {
        let table_info: TableInfo =
            serde_json::from_str(json).expect("Failed to deserialize table info");
        assert_eq!(table_info.catalog_info.is_some(), expect_present);
        if expect_present {
            assert_eq!(
                table_info.catalog_info.unwrap().table_name,
                expected_table_name
            );
        }
    }

    #[rstest]
    #[case(r#"{"description": "missing name"}"#, "missing field")]
    #[case(
        r#"{"name": "missing_schema", "description": "d",
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
            "logInfo": {"numAddFiles": 0, "numRemoveFiles": 0, "sizeInBytes": 0, "numCommits": 1, "numActions": 1},
            "properties": {}, "dataLayout": {"numClusteringColumns": 1}, "tags": []}"#,
        "missing field"
    )]
    fn test_deserialize_table_info_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<TableInfo>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }

    #[rstest]
    #[case(r#"{"type": "read", "version": 5}"#, "read", Some(5))]
    #[case(r#"{"type": "read"}"#, "read", None)]
    #[case(
        r#"{"type": "read", "version": 7, "extraField": "should be ignored"}"#,
        "read",
        Some(7)
    )]
    #[case(
        r#"{"type": "snapshotConstruction", "version": 5}"#,
        "snapshotConstruction",
        Some(5)
    )]
    #[case(r#"{"type": "snapshotConstruction"}"#, "snapshotConstruction", None)]
    #[case(
        r#"{"type": "snapshotConstruction", "version": 7, "extraField": "should be ignored"}"#,
        "snapshotConstruction",
        Some(7)
    )]
    #[case(
        r#"{"type": "snapshotConstruction", "version": -1}"#,
        "snapshotConstruction",
        Some(-1)
    )]
    #[case(r#"{"type": "read", "version": -1}"#, "read", Some(-1))]
    fn test_deserialize_spec(
        #[case] json: &str,
        #[case] expected_type: &str,
        #[case] expected_version: Option<i64>,
    ) {
        let spec: Spec = serde_json::from_str(json).expect("Failed to deserialize spec");
        assert_eq!(spec.as_str(), expected_type);
        let version = match &spec {
            Spec::Read(read_spec) => match &read_spec.time_travel {
                Some(TimeTravel::Version { version }) => Some(*version),
                _ => None,
            },
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                match &snapshot_construction_spec.time_travel {
                    Some(TimeTravel::Version { version }) => Some(*version),
                    _ => None,
                }
            }
        };

        assert_eq!(version, expected_version);
    }

    #[test]
    fn test_time_travel_as_version_rejects_negative() {
        let tt = TimeTravel::Version { version: -1 };
        assert_eq!(
            tt.as_version(),
            Err("Only non-negative snapshot versions are supported")
        );
    }

    #[rstest]
    #[case(r#"{"version": 10}"#, "missing field")]
    #[case(r#"{"type": "write", "version": 3}"#, "unknown variant")]
    fn test_deserialize_spec_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<Spec>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }
}
