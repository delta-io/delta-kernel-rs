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
