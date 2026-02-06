//! Data models for workload specifications

use serde::Deserialize;

// ReadConfig represents a specific configuration for a read operation
// A config represents configurations for a specific benchmark that would not be specified in the spec
#[derive(Clone)]
pub struct ReadConfig {
    pub name: String,
    pub parallel_scan: ParallelScan,
}

#[derive(Clone)]
pub enum ParallelScan {
    Disabled,
    Enabled { num_threads: usize },
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

//Table info JSON files are located at the root of each table directory and act as documentation for the table
#[derive(Clone, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub description: Option<String>,
}

// Specs define the operation performed on a table - defines what operation at what version (e.g. read at version 0)
// There will be multiple specs for a given table
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Spec {
    Read {
        version: Option<i64>, //If version is None, read at latest version
    },
}

//For Read specs, we will either run a read data operation or a read metadata operation
pub enum ReadOperation {
    ReadData,
    ReadMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_table_info() {
        let json_content = r#"{
    "name": "basic_append",
    "description": "A basic table with two append writes"
}"#;
        let table_info: TableInfo =
            serde_json::from_str(json_content).expect("Failed to deserialize table_info.json");

        assert_eq!(table_info.name, "basic_append");
        assert_eq!(
            table_info.description,
            Some("A basic table with two append writes".to_string())
        );
    }

    #[test]
    fn test_deserialize_table_info_missing_description() {
        let json_content = r#"{
    "name": "table_without_description"
}"#;
        let table_info: TableInfo = serde_json::from_str(json_content)
            .expect("Failed to deserialize table_info_missing_description.json");

        assert_eq!(table_info.name, "table_without_description");
        assert_eq!(table_info.description, None);
    }

    #[test]
    fn test_deserialize_table_info_missing_name() {
        let json_content = r#"{
    "description": "A table missing the required name field"
}"#;
        let result: Result<TableInfo, _> = serde_json::from_str(json_content);

        assert!(
            result.is_err(),
            "Expected deserialization to fail when name is missing"
        );
    }

    #[test]
    fn test_deserialize_table_info_extra_fields() {
        let json_content = r#"{
    "name": "table_with_extras",
    "description": "A table with extra fields",
    "extra_field": "should be ignored"
}"#;
        let table_info: TableInfo = serde_json::from_str(json_content)
            .expect("Failed to deserialize table_info_extra_fields.json");

        assert_eq!(table_info.name, "table_with_extras");
        assert_eq!(
            table_info.description,
            Some("A table with extra fields".to_string())
        );
    }

    #[test]
    fn test_deserialize_spec_read_with_version() {
        let json_content = r#"{
    "type": "read",
    "version": 5
}"#;
        let spec: Spec = serde_json::from_str(json_content)
            .expect("Failed to deserialize spec_read_with_version.json");

        let Spec::Read { version } = spec;
        assert_eq!(version, Some(5));
    }

    #[test]
    fn test_deserialize_spec_read_without_version() {
        let json_content = r#"{
    "type": "read"
}"#;
        let spec: Spec = serde_json::from_str(json_content)
            .expect("Failed to deserialize spec_read_without_version.json");

        let Spec::Read { version } = spec;
        assert_eq!(version, None);
    }

    #[test]
    fn test_deserialize_spec_missing_type() {
        let json_content = r#"{
    "version": 10
}"#;
        let result: Result<Spec, _> = serde_json::from_str(json_content);

        assert!(
            result.is_err(),
            "Expected deserialization to fail when type field is missing"
        );
    }

    #[test]
    fn test_deserialize_spec_invalid_type() {
        let json_content = r#"{
    "type": "write",
    "version": 3
}"#;
        let result: Result<Spec, _> = serde_json::from_str(json_content);

        assert!(
            result.is_err(),
            "Expected deserialization to fail with invalid type value"
        );
    }

    #[test]
    fn test_deserialize_spec_extra_fields() {
        let json_content = r#"{
    "type": "read",
    "version": 7,
    "extra_field": "should be ignored"
}"#;
        let spec: Spec = serde_json::from_str(json_content)
            .expect("Failed to deserialize spec_extra_fields.json");

        let Spec::Read { version } = spec;
        assert_eq!(version, Some(7));
    }
}
