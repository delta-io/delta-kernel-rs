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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_table_info() {
        let json_content = include_str!("test_data/table_info.json");
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
        let json_content = include_str!("test_data/table_info_missing_description.json");
        let table_info: TableInfo = serde_json::from_str(json_content)
            .expect("Failed to deserialize table_info_missing_description.json");

        assert_eq!(table_info.name, "table_without_description");
        assert_eq!(table_info.description, None);
    }

    #[test]
    fn test_deserialize_table_info_missing_name() {
        let json_content = include_str!("test_data/table_info_missing_name.json");
        let result: Result<TableInfo, _> = serde_json::from_str(json_content);

        assert!(
            result.is_err(),
            "Expected deserialization to fail when name is missing"
        );
    }

    #[test]
    fn test_deserialize_table_info_extra_fields() {
        let json_content = include_str!("test_data/table_info_extra_fields.json");
        let table_info: TableInfo = serde_json::from_str(json_content)
            .expect("Failed to deserialize table_info_extra_fields.json");

        assert_eq!(table_info.name, "table_with_extras");
        assert_eq!(
            table_info.description,
            Some("A table with extra fields".to_string())
        );
    }
}
