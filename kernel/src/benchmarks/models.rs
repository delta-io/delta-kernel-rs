//! Data models for workload specifications

use serde::Deserialize;

use std::path::{Path, PathBuf};

// A config represents configurations for a specific benchmark that aren't specified in the spec JSON file
// Wraps operation-specific configs
#[derive(Clone, Debug)]
pub enum Config {
    Read(ReadConfig),
}

impl Config {
    pub fn name(&self) -> &str {
        match self {
            Config::Read(c) => &c.name,
        }
    }
}

// ReadConfig represents a specific configuration for a read operation
#[derive(Clone, Debug)]
pub struct ReadConfig {
    name: String,
    pub parallel_scan: ParallelScan,
}

// Provides a default set of read configs for a given table, read spec, and operation
pub fn default_read_configs() -> Vec<Config> {
    vec![Config::Read(ReadConfig {
        name: "serial".into(),
        parallel_scan: ParallelScan::Disabled,
    })]
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

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let mut table_info: TableInfo = serde_json::from_str(&content)?;
        //Stores the parent directory of the table info JSON file
        if let Some(parent) = path.as_ref().parent() {
            table_info.table_info_dir = parent.to_path_buf();
        }
        Ok(table_info)
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

    pub fn from_json_path<P: AsRef<Path>>(path: P) -> Result<Self, serde_json::Error> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(serde_json::Error::io)?;
        let spec: Spec = serde_json::from_str(&content)?;
        Ok(spec)
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

// Partial workload specification loaded from JSON - table, case name, and kind only
// This is then used to construct a WorkloadVariant with the operation and config before running a benchmark
#[derive(Clone, Debug)]
pub struct Workload {
    pub table_info: TableInfo,
    pub case_name: String, //Name of the spec JSON file
    pub spec: Spec,
}

// Fully specified workload - ready to run. Constructed in the benchmark harness by
// combining a Workload with a concrete ReadConfig and (for read specs) a ReadOperation
#[derive(Clone, Debug)]
pub struct WorkloadVariant {
    pub table_info: TableInfo,
    pub case_name: String,
    pub spec: Spec,
    pub operation: Option<ReadOperation>, // required for Read specs; None for other spec types
    pub config: Config,
}

impl WorkloadVariant {
    /// Validates that this variant is ready to run.
    /// For Read specs, ensures that operation is set.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.spec {
            Spec::Read { .. } => {
                if self.operation.is_none() {
                    return Err(format!(
                        "Invalid workload variant specification: '{}' must have read operation specified",
                        self.case_name
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    pub fn name(&self) -> Result<String, Box<dyn std::error::Error>> {
        // For Read specs, use the operation (read_data vs read_metadata)
        // For other specs, use the spec type name itself (e.g. write) - this will be added when other specs are implemented
        let workload_str = match &self.spec {
            Spec::Read { .. } => self
                .operation
                .as_ref()
                .ok_or_else(|| -> Box<dyn std::error::Error> {
                    format!("Workload '{}' must have read operation set", self.case_name).into()
                })?
                .as_str(),
        };
        Ok(format!(
            "{}/{}/{}/{}",
            self.table_info.name,
            self.case_name,
            workload_str,
            self.config.name(),
        ))
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

    fn make_variant(operation: Option<ReadOperation>, config_name: &str) -> WorkloadVariant {
        WorkloadVariant {
            table_info: TableInfo {
                name: "test_table".into(),
                description: None,
                table_path: None,
                table_info_dir: PathBuf::from("/tmp"),
            },
            case_name: "append_10k".into(),
            spec: Spec::Read { version: Some(1) },
            operation,
            config: Config::Read(ReadConfig {
                name: config_name.into(),
                parallel_scan: ParallelScan::Disabled,
            }),
        }
    }

    #[rstest]
    #[case(Some(ReadOperation::ReadMetadata), true, "")]
    #[case(None, false, "must have read operation specified")]
    fn test_workload_spec_variant_validate(
        #[case] operation: Option<ReadOperation>,
        #[case] should_succeed: bool,
        #[case] expected_error_msg: &str,
    ) {
        let result = make_variant(operation, "serial").validate();
        if should_succeed {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected_error_msg));
        }
    }

    #[rstest]
    #[case(
        Some(ReadOperation::ReadMetadata),
        Ok("test_table/append_10k/read_metadata/serial")
    )]
    #[case(None, Err("Workload 'append_10k' must have read operation set"))]
    fn test_workload_spec_variant_name(
        #[case] operation: Option<ReadOperation>,
        #[case] expected: Result<&str, &str>,
    ) {
        let result = make_variant(operation, "serial").name();
        match expected {
            Ok(expected_name) => assert_eq!(result.unwrap(), expected_name),
            Err(expected_err) => assert_eq!(result.unwrap_err().to_string(), expected_err),
        }
    }
}
