//! This module defines the `Statistics` struct used in Add and Remove actions.

use delta_kernel_derive::ToSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Statistics {
    /// The number of records in this data file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_records: Option<u64>,

    /// Whether per-column statistics are currently tight or wide.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,

    /// The number of `null` value for this column or an estimate thereof (depending on the `tight_bounds` value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_count: Option<PerColumnStatistics>,

    /// The minimum value per column in this file or an estimate thereof (depending on the `tight_bounds` value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_values: Option<PerColumnStatistics>,

    /// The maximum value per column in this file or an estimate thereof (depending on the `tight_bounds` value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_values: Option<PerColumnStatistics>,
}

impl Statistics {
    /// Creates a new `Statistics` instance with the given number of records.
    pub(crate) fn new(num_records: u64) -> Self {
        // TODO: We only support num_records for now
        Self {
            num_records: Some(num_records),
            tight_bounds: None,
            null_count: None,
            min_values: None,
            max_values: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PerColumnStatistics {
    // TODO: Implement per-column statistics
    // See (https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics)
}
