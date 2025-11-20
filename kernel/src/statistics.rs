//! Statistics types for Delta tables
//!
//! This module contains types for representing file statistics in both JSON and parsed formats.
//! Statistics are used for data skipping during query execution and are stored in checkpoint files.

use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::{DataType, StructField};

/// Parsed statistics for a file (alternative to JSON stats string)
///
/// This represents the structured form of file statistics that can be stored
/// directly in checkpoints as `stats_parsed` instead of as a JSON string.
///
/// The minValues, maxValues, and nullCount fields contain dynamic structs
/// whose schema matches the table's data columns (using physical column names).
///
/// # Usage
///
/// This type is used when reading checkpoint parquet files that contain the
/// `stats_parsed` column. It provides direct access to statistics without
/// needing to parse JSON strings, improving performance for data skipping.
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
///
/// # Example
///
/// ```rust,ignore
/// use delta_kernel::statistics::StatsParsed;
/// use std::collections::HashMap;
///
/// let stats = StatsParsed {
///     num_records: Some(1000),
///     min_values: Some(HashMap::from([
///         ("id".to_string(), Scalar::Integer(1)),
///         ("name".to_string(), Scalar::String("alice".to_string())),
///     ])),
///     max_values: Some(HashMap::from([
///         ("id".to_string(), Scalar::Integer(1000)),
///         ("name".to_string(), Scalar::String("zoe".to_string())),
///     ])),
///     null_count: Some(HashMap::from([
///         ("id".to_string(), 0),
///         ("name".to_string(), 5),
///     ])),
///     tight_bounds: Some(true),
/// };
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

// Manual implementation of Eq for StatsParsed
// Note: This is a shallow equality check. For HashMap<String, Scalar>, we use PartialEq
// even though Scalar doesn't implement Eq, we can still provide this for compatibility.
impl Eq for StatsParsed {}

// Implementation of ToDataType for schema derivation
// Returns a base schema structure for stats_parsed. The actual minValues/maxValues/nullCount
// schemas are dynamic and depend on the table schema.
impl ToDataType for StatsParsed {
    fn to_data_type() -> DataType {
        // Return the stats_parsed schema structure
        // The inner struct schemas (minValues, maxValues, nullCount) are table-dependent
        // and will be determined at runtime during checkpoint reading/writing
        DataType::struct_type_unchecked(vec![
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("minValues", DataType::struct_type_unchecked(vec![])), // Dynamic
            StructField::nullable("maxValues", DataType::struct_type_unchecked(vec![])), // Dynamic
            StructField::nullable("nullCount", DataType::struct_type_unchecked(vec![])), // Dynamic
            StructField::nullable("tightBounds", DataType::BOOLEAN),
        ])
    }
}
