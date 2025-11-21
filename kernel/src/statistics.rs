//! Parsed statistics for Delta table files.
//!
//! Delta Lake supports two formats for storing file statistics:
//! 1. JSON format (`stats` field) - String containing JSON
//! 2. Parsed format (`stats_parsed` field) - Structured data (this module)
//!
//! Parsed statistics avoid JSON parsing overhead during data skipping operations.

use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::schema::{DataType, StructField, StructType, ToSchema};

/// Parsed statistics for a data file.
///
/// This structure mirrors the `stats_parsed` column format in Delta checkpoints.
/// Column names in the maps are PHYSICAL names (not logical names).
/// Use [`crate::schema::StructField::physical_name`] for column mapping.
///
/// # Example
/// ```rust,ignore
/// use delta_kernel::statistics::StatsParsed;
/// use delta_kernel::expressions::Scalar;
/// use std::collections::HashMap;
///
/// let stats = StatsParsed {
///     num_records: 100,
///     min_values: HashMap::from([
///         ("id".to_string(), Some(Scalar::Integer(1))),
///     ]),
///     max_values: HashMap::from([
///         ("id".to_string(), Some(Scalar::Integer(100))),
///     ]),
///     null_count: HashMap::from([
///         ("id".to_string(), 0),
///     ]),
///     tight_bounds: None,
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StatsParsed {
    /// Number of records in the file.
    pub num_records: i64,

    /// Minimum values for each column.
    ///
    /// Keys are PHYSICAL column names.
    /// Value is `None` for all-null columns.
    pub min_values: HashMap<String, Option<Scalar>>,

    /// Maximum values for each column.
    ///
    /// Keys are PHYSICAL column names.
    /// Value is `None` for all-null columns.
    pub max_values: HashMap<String, Option<Scalar>>,

    /// Null count for each column.
    ///
    /// Keys are PHYSICAL column names.
    pub null_count: HashMap<String, i64>,

    /// Whether bounds are tight (affected by deletion vectors).
    ///
    /// When deletion vectors are present, the statistics may not reflect
    /// the deleted rows, making the bounds "loose" rather than "tight".
    pub tight_bounds: Option<bool>,
}

// Implement ToSchema for StatsParsed
// This is a placeholder implementation since the actual schema is complex
impl ToSchema for StatsParsed {
    fn to_schema() -> StructType {
        // For now, return a simple placeholder schema
        // The actual schema would be generated based on table schema
        StructType::new_unchecked(vec![
            StructField::new("numRecords", DataType::LONG, false),
            // In reality, minValues/maxValues/nullCount are dynamic based on table schema
            // For now just return a simple placeholder
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_parsed_creation() {
        let stats = StatsParsed {
            num_records: 100,
            min_values: HashMap::from([
                ("id".to_string(), Some(Scalar::Integer(1))),
                (
                    "name".to_string(),
                    Some(Scalar::String("Alice".to_string())),
                ),
            ]),
            max_values: HashMap::from([
                ("id".to_string(), Some(Scalar::Integer(100))),
                ("name".to_string(), Some(Scalar::String("Zoe".to_string()))),
            ]),
            null_count: HashMap::from([("id".to_string(), 0), ("name".to_string(), 2)]),
            tight_bounds: Some(true),
        };

        assert_eq!(stats.num_records, 100);
        assert_eq!(stats.min_values.len(), 2);
        assert_eq!(stats.max_values.len(), 2);
        assert_eq!(stats.null_count.len(), 2);
        assert_eq!(stats.tight_bounds, Some(true));

        // Verify min value access
        if let Some(Some(Scalar::Integer(min_id))) = stats.min_values.get("id") {
            assert_eq!(*min_id, 1);
        } else {
            panic!("Expected min id to be Some(Integer(1))");
        }

        // Verify null value
        assert_eq!(stats.null_count.get("id"), Some(&0));
    }

    #[test]
    fn test_stats_parsed_with_nulls() {
        let stats = StatsParsed {
            num_records: 50,
            min_values: HashMap::from([
                ("col1".to_string(), Some(Scalar::Integer(10))),
                ("col2".to_string(), None), // All nulls
            ]),
            max_values: HashMap::from([
                ("col1".to_string(), Some(Scalar::Integer(100))),
                ("col2".to_string(), None), // All nulls
            ]),
            null_count: HashMap::from([("col1".to_string(), 5), ("col2".to_string(), 50)]),
            tight_bounds: None,
        };

        assert_eq!(stats.num_records, 50);
        assert_eq!(stats.min_values.get("col2"), Some(&None));
        assert_eq!(stats.max_values.get("col2"), Some(&None));
        assert_eq!(stats.null_count.get("col2"), Some(&50));
    }

    #[test]
    fn test_stats_parsed_clone() {
        let stats = StatsParsed {
            num_records: 10,
            min_values: HashMap::from([("x".to_string(), Some(Scalar::Integer(1)))]),
            max_values: HashMap::from([("x".to_string(), Some(Scalar::Integer(10)))]),
            null_count: HashMap::from([("x".to_string(), 0)]),
            tight_bounds: None,
        };

        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }
}
