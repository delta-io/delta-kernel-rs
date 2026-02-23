//! Data models for workload specifications

use serde::Deserialize;

use std::path::PathBuf;

// ReadConfig represents a specific configuration for a read operation
// A config represents configurations for a specific benchmark that aren't specified in the spec JSON file
#[derive(Clone, Debug)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

// Provides a default set of read configs for a given table, read spec, and operation
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

//Table info JSON files are located at the root of each table directory
#[derive(Clone, Debug, Deserialize)]
pub struct TableInfo {
    pub name: String,                //Table name used for identifying the table
    pub description: Option<String>, //Human-readable description of the table
    pub table_path: Option<String>,  //Path or URL to the table
    #[serde(skip, default)]
    pub table_info_dir: PathBuf, //Path to the directory containing the table info JSON file
}

impl TableInfo {
    pub fn resolved_table_root(&self) -> String {
        //If table path is not provided, assume that the Delta table is in a delta/ subdirectory at the same level as table_info.json
        self.table_path.clone().unwrap_or_else(|| {
            self.table_info_dir
                .join("delta")
                .to_string_lossy()
                .to_string()
        })
    }
}

// Spec defines the operation performed on a table - defines what operation at what version (e.g. read at version 0)
// There will be multiple specs for a given table
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Spec {
    Read {
        version: Option<u64>, //If version is None, read at latest version
    },
}

impl Spec {
    pub fn as_str(&self) -> &str {
        match self {
            Spec::Read { .. } => "read",
        }
    }
}

//For Read specs, we will either run a read data operation or a read metadata operation
#[derive(Clone, Copy, Debug)]
pub enum ReadOperation {
    ReadData,
    ReadMetadata,
}

impl ReadOperation {
    pub fn as_str(&self) -> &str {
        match self {
            ReadOperation::ReadData => "read_data",
            ReadOperation::ReadMetadata => "read_metadata",
        }
    }
}

// Partial workload specification loaded from JSON - table, case name, and spec only
#[derive(Clone, Debug)]
pub struct Workload {
    pub table_info: TableInfo,
    pub case_name: String, //Name of the spec JSON file
    pub spec: Spec,
}

impl Workload {
    pub fn name(&self) -> String {
        format!("{}/{}", self.table_info.name, self.case_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        r#"{"name": "basic_append", "description": "A basic table with two append writes"}"#,
        "basic_append",
        Some("A basic table with two append writes")
    )]
    #[case(
        r#"{"name": "table_without_description"}"#,
        "table_without_description",
        None
    )]
    #[case(
       r#"{"name": "table_with_extra_fields", "description": "A table with extra fields", "extra_field": "should be ignored"}"#,
       "table_with_extra_fields",
       Some("A table with extra fields")
   )]
    fn test_deserialize_table_info(
        #[case] json: &str,
        #[case] expected_name: &str,
        #[case] expected_description: Option<&str>,
    ) {
        let table_info: TableInfo =
            serde_json::from_str(json).expect("Failed to deserialize table info");

        assert_eq!(table_info.name, expected_name);
        assert_eq!(table_info.description.as_deref(), expected_description);
    }

    #[rstest]
    #[case(
        r#"{"description": "A table missing the required name field"}"#,
        "missing field"
    )]
    fn test_deserialize_table_info_errors(#[case] json: &str, #[case] expected_msg: &str) {
        let error = serde_json::from_str::<TableInfo>(json).unwrap_err();
        assert!(error.to_string().contains(expected_msg));
    }

    #[rstest]
    #[case(r#"{"type": "read", "version": 5}"#, Some(5))]
    #[case(r#"{"type": "read"}"#, None)]
    #[case(
        r#"{"type": "read", "version": 7, "extra_field": "should be ignored"}"#,
        Some(7)
    )]
    fn test_deserialize_spec_read(#[case] json: &str, #[case] expected_version: Option<u64>) {
        let spec: Spec = serde_json::from_str(json).expect("Failed to deserialize read spec");

        let Spec::Read { version } = spec;
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
