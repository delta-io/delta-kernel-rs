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
#[derive(Clone, Deserialize, Debug)]
pub struct TableInfo {
    pub name: String,
    pub description: Option<String>,
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
}
