//! Data models for workload specifications

use delta_kernel::actions::Protocol;
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

/// Table info JSON files are located at the root of each table directory
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    /// Table name used for identifying the table (part of the final benchmark name) - e.g. 100Adds0Chkpts
    pub name: String,
    /// Human-readable description of the table
    pub description: String,
    /// URL to the table. Used for remote tables or (rarely) absolute local paths.
    /// If `None`, the table is assumed to be in the `delta/` subdirectory next to `tableInfo.json`
    pub table_path: Option<Url>,
    /// Schema at the latest version of the table
    pub schema: Schema,
    /// Delta Protocol struct, containing version requirements and table features at the latest version of the table
    pub protocol: Protocol,
    /// Log-level statistics for the table
    pub log_info: LogInfo,
    /// Table properties from the Delta metadata (e.g. `{"delta.enableDeletionVector": "true", "delta.columnMapping.mode": "none"}`)
    pub properties: HashMap<String, String>,
    /// Physical data layout of the table
    pub data_layout: DataLayout,
    /// Tags for filtering which tables are benchmarked via `BENCH_TAGS`
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
    pub size_in_bytes: u64,
    /// Number of commits (JSON log files) in the table history
    pub num_commits: u64,
    /// Total number of actions across all commits
    pub num_actions: u64,
    /// Version of the most recent checkpoint, if any
    pub last_checkpoint_version: Option<u64>,
    /// Version of the most recent CRC file, if any
    pub last_crc_version: Option<u64>,
    /// Number of parquet part files in the most recent multi-part checkpoint, if any
    pub num_parallel_checkpoint_files: Option<u32>,
}

/// Physical data layout of a Delta table
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum DataLayout {
    Partitioned {
        /// Number of partition columns
        #[serde(rename = "numPartitionColumns")]
        num_partition_columns: u32,
        /// Number of distinct partition values observed in the table
        #[serde(rename = "numDistinctPartitions")]
        num_distinct_partitions: u64,
    },
    Clustered {
        /// Number of clustering columns
        #[serde(rename = "numClusteringColumns")]
        num_clustering_columns: u32,
    },
    /// No special data organization (default)
    None {},
}

/// Spec defines the operation performed on a table - defines what operation at what version (e.g. read at version 0)
/// There will be multiple specs for a given table
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Spec {
    Read(ReadSpec),
    SnapshotConstruction(SnapshotConstructionSpec),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReadSpec {
    /// Version to read; if `None`, reads the latest version
    pub version: Option<u64>,
}

impl ReadSpec {
    pub fn as_str(&self) -> &str {
        "read"
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotConstructionSpec {
    /// Version to construct the snapshot at; if `None`, uses the latest version
    pub version: Option<u64>,
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
    fn test_deserialize_spec(
        #[case] json: &str,
        #[case] expected_type: &str,
        #[case] expected_version: Option<u64>,
    ) {
        let spec: Spec = serde_json::from_str(json).expect("Failed to deserialize spec");
        assert_eq!(spec.as_str(), expected_type);
        let version = match &spec {
            Spec::Read(read_spec) => read_spec.version,
            Spec::SnapshotConstruction(snapshot_construction_spec) => {
                snapshot_construction_spec.version
            }
        };

        assert_eq!(version, expected_version);
    }

    #[rstest]
    #[case(r#"{"version": 10}"#, "missing field")]
    #[case(r#"{"type": "write", "version": 3}"#, "unknown variant")]
    fn test_deserialize_spec_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<Spec>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }
}
