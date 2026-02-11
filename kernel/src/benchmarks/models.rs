//! Data models for workload specifications

use serde::Deserialize;

// ReadConfig represents a specific configuration for a read operation
// A config represents configurations for a specific benchmark that aren't specified in the spec JSON file
#[derive(Clone)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

impl ReadConfig {
    pub fn name(&self) -> &str {
        &self.name
    }
}

// Provides a default set of read configs for a given table, read spec, and operation
pub fn default_read_configs() -> Vec<ReadConfig> {
    vec![
        ReadConfig {
            name: "serial".into(),
            parallel_scan: ParallelScan::Disabled,
        },
        ReadConfig {
            name: "parallel_4".into(),
            parallel_scan: ParallelScan::Enabled { num_threads: 4 },
        },
    ]
}

#[derive(Clone)]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
}

//Table info JSON files are located at the root of each table directory
#[derive(Clone, Debug, Deserialize)]
pub struct TableInfo {
    pub name: String,                //Table name used for identifying the table
    pub description: Option<String>, //Human-readable description of the table
}

// Specs define the operation performed on a table - defines what operation at what version (e.g. read at version 0)
// There will be multiple specs for a given table
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Spec {
    Read {
        version: Option<i64>, //If version is None, read at latest version
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
#[derive(Clone, Copy)]
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

// Complete workload specification - for a given table, spec, operation, and config
//
// Created from JSON with table_info, case_name, and spec populated
// with_read_operation and with_config are used to set the operation and config
// validate is then used to ensure that the workload spec variant is ready to run
#[derive(Clone)]
pub struct WorkloadSpecVariant {
    pub table_info: TableInfo,
    pub case_name: String, //Name of the spec JSON file
    pub spec: Spec,
    pub operation: Option<ReadOperation>, //operation is optional because WorkloadSpecVariant is used for all specs, not just reads
    pub config: Option<ReadConfig>, //config is optional because WorkloadSpecVariant structs will have no config upon creation, but config will be set before running a benchmark
}

impl WorkloadSpecVariant {
    // Validates that this variant is ready to run - ensures that config and operation (operation required for read specs only) are set
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.config.is_none() {
            return Err(format!(
                "Invalid workload variant specification: '{}' must have config specified",
                self.case_name
            )
            .into());
        }
        match &self.spec {
            Spec::Read { .. } => {
                if self.operation.is_none() {
                    return Err(format!(
                       "Invalid workload variant specification: '{}' must have read operation specified",
                       self.case_name
                   ).into());
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

        let config_str = self
            .config
            .as_ref()
            .ok_or_else(|| -> Box<dyn std::error::Error> {
                format!("Workload '{}' must have config set", self.case_name).into()
            })?
            .name();

        Ok(format!(
            "{}/{}/{}/{}",
            self.table_info.name, self.case_name, workload_str, config_str
        ))
    }

    pub fn with_read_operation(mut self, operation: ReadOperation) -> Self {
        self.operation = Some(operation);
        self
    }

    pub fn with_config(mut self, config: ReadConfig) -> Self {
        self.config = Some(config);
        self
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
    fn test_deserialize_spec_read(#[case] json: &str, #[case] expected_version: Option<i64>) {
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

    #[rstest]
    #[case(
        None,
        Some(ReadOperation::ReadMetadata),
        false,
        "must have config specified"
    )]
    #[case(Some("serial"), None, false, "must have read operation specified")]
    #[case(Some("serial"), Some(ReadOperation::ReadMetadata), true, "")]
    fn test_workload_spec_variant_validate(
        #[case] config_name: Option<&str>,
        #[case] operation: Option<ReadOperation>,
        #[case] should_succeed: bool,
        #[case] expected_error_msg: &str,
    ) {
        let table_info = TableInfo {
            name: "test_table".into(),
            description: None,
        };
        let spec = Spec::Read { version: Some(1) };
        let config = config_name.map(|name| ReadConfig {
            name: name.into(),
            parallel_scan: ParallelScan::Disabled,
        });
        let variant = WorkloadSpecVariant {
            table_info,
            case_name: "test_case".into(),
            spec,
            operation,
            config,
        };

        let result = variant.validate();
        if should_succeed {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected_error_msg));
        }
    }

    #[rstest]
    #[case(
        Some("serial"),
        Some(ReadOperation::ReadMetadata),
        Ok("test_table/append_10k/read_metadata/serial")
    )]
    #[case(
        None,
        Some(ReadOperation::ReadData),
        Err("Workload 'append_10k' must have config set")
    )]
    #[case(
        Some("serial"),
        None,
        Err("Workload 'append_10k' must have read operation set")
    )]
    fn test_workload_spec_variant_name(
        #[case] config_name: Option<&str>,
        #[case] operation: Option<ReadOperation>,
        #[case] expected: Result<&str, &str>,
    ) {
        let table_info = TableInfo {
            name: "test_table".into(),
            description: None,
        };
        let spec = Spec::Read { version: Some(1) };
        let config = config_name.map(|name| ReadConfig {
            name: name.into(),
            parallel_scan: ParallelScan::Disabled,
        });
        let variant = WorkloadSpecVariant {
            table_info,
            case_name: "append_10k".into(),
            spec,
            operation,
            config,
        };

        match expected {
            Ok(expected_name) => {
                assert_eq!(variant.name().unwrap(), expected_name);
            }
            Err(expected_error) => {
                let error = variant.name().unwrap_err();
                assert_eq!(error.to_string(), expected_error);
            }
        }
    }

    #[test]
    fn test_workload_spec_variant_builder_pattern() {
        let table_info = TableInfo {
            name: "test_table".into(),
            description: None,
        };
        let spec = Spec::Read { version: Some(1) };
        let config = ReadConfig {
            name: "parallel_4".into(),
            parallel_scan: ParallelScan::Enabled { num_threads: 4 },
        };

        let variant = WorkloadSpecVariant {
            table_info,
            case_name: "test_case".into(),
            spec,
            operation: None,
            config: None,
        }
        .with_read_operation(ReadOperation::ReadMetadata)
        .with_config(config);

        assert!(variant.validate().is_ok());
        assert_eq!(
            variant.name().unwrap(),
            "test_table/test_case/read_metadata/parallel_4"
        );
    }
}
