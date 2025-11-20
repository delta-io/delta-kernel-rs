//! Statistics types for Delta tables
//!
//! This module contains types for representing file statistics in both JSON and parsed formats.
//! Statistics are used for data skipping during query execution and are stored in checkpoint files.

use std::collections::HashMap;

use crate::expressions::Scalar;

/// Parsed statistics for a file (alternative to JSON stats string)
///
/// This represents the structured form of file statistics that can be stored
/// directly in checkpoints as `stats_parsed` instead of as a JSON string.
///
/// The minValues, maxValues, and nullCount fields contain dynamic structs
/// whose schema matches the table's data columns (using physical column names).
///
/// # Schema
///
/// The stats_parsed schema in checkpoints is:
/// ```text
/// struct<
///   numRecords: long,
///   minValues: struct<col1: type1, col2: type2, ...>,  // Dynamic per table
///   maxValues: struct<col1: type1, col2: type2, ...>,  // Dynamic per table
///   nullCount: struct<col1: long, col2: long, ...>,    // Dynamic per table
///   tightBounds: boolean
/// >
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StatsParsed {
    /// Number of records in the file
    pub num_records: Option<i64>,

    /// Minimum values per column (physical column names)
    /// Dynamic based on table schema
    pub min_values: Option<HashMap<String, Scalar>>,

    /// Maximum values per column (physical column names)
    /// Dynamic based on table schema
    pub max_values: Option<HashMap<String, Scalar>>,

    /// Null count per column (physical column names)
    pub null_count: Option<HashMap<String, i64>>,

    /// Whether statistics are exact (tight bounds)
    pub tight_bounds: Option<bool>,
}
