//! Parsed statistics for Delta table files.
//!
//! Delta Lake supports two formats for storing file statistics:
//! 1. JSON format (`stats` field) - String containing JSON
//! 2. Parsed format (`stats_parsed` field) - Structured data (this module)
//!
//! Parsed statistics avoid JSON parsing overhead during data skipping operations.

use std::collections::HashMap;

#[cfg(test)]
use serde::{Deserialize, Serialize};

use crate::schema::{DataType, StructField, StructType, ToSchema};

/// Parsed statistics for a data file.
///
/// This structure mirrors the `stats_parsed` column format in Delta checkpoints.
/// Column names in the maps are PHYSICAL names (not logical names).
/// Use [`crate::schema::StructField::physical_name`] for column mapping.
///
/// # Example
/// ```rust,ignore
/// use delta_kernel::statistics::{StatsParsed, StatValue};
/// use std::collections::HashMap;
///
/// let stats = StatsParsed {
///     num_records: 100,
///     min_values: HashMap::from([
///         ("id".to_string(), Some(StatValue::Int(1))),
///     ]),
///     max_values: HashMap::from([
///         ("id".to_string(), Some(StatValue::Int(100))),
///     ]),
///     null_count: HashMap::from([
///         ("id".to_string(), 0),
///     ]),
///     tight_bounds: None,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(Serialize, Deserialize))]
pub struct StatsParsed {
    /// Number of records in the file.
    pub num_records: i64,

    /// Minimum values for each column.
    ///
    /// Keys are PHYSICAL column names.
    /// Value is `None` for all-null columns.
    pub min_values: HashMap<String, Option<StatValue>>,

    /// Maximum values for each column.
    ///
    /// Keys are PHYSICAL column names.
    /// Value is `None` for all-null columns.
    pub max_values: HashMap<String, Option<StatValue>>,

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

/// A typed statistic value.
///
/// Represents min/max values which can be of various types.
/// This enum provides type-safe access to statistics values.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(Serialize, Deserialize))]
pub enum StatValue {
    /// Boolean value
    Boolean(bool),
    /// 8-bit signed integer
    Byte(i8),
    /// 16-bit signed integer
    Short(i16),
    /// 32-bit signed integer
    Int(i32),
    /// 64-bit signed integer
    Long(i64),
    /// 32-bit floating point
    Float(f32),
    /// 64-bit floating point
    Double(f64),
    /// UTF-8 string
    String(String),
    /// Binary data
    Binary(Vec<u8>),
    /// Date as days since Unix epoch (1970-01-01)
    Date(i32),
    /// Timestamp as microseconds since Unix epoch, with timezone
    Timestamp(i64),
    /// Timestamp as microseconds since Unix epoch, without timezone
    TimestampNtz(i64),
}

// Manual implementation of Eq for StatValue
// Note: For floats, we treat them as equal if their bits are equal (not NaN-safe)
impl Eq for StatValue {}

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

impl StatValue {
    /// Convert a StatValue to i64 if possible.
    ///
    /// Useful for comparing integer-like values.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            StatValue::Byte(v) => Some(*v as i64),
            StatValue::Short(v) => Some(*v as i64),
            StatValue::Int(v) => Some(*v as i64),
            StatValue::Long(v) => Some(*v),
            StatValue::Date(v) => Some(*v as i64),
            StatValue::Timestamp(v) => Some(*v),
            StatValue::TimestampNtz(v) => Some(*v),
            _ => None,
        }
    }

    /// Convert a StatValue to f64 if possible.
    ///
    /// Useful for comparing numeric values.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            StatValue::Float(v) => Some(*v as f64),
            StatValue::Double(v) => Some(*v),
            _ => self.as_i64().map(|v| v as f64),
        }
    }

    /// Get a reference to the string value if this is a String.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            StatValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Get a reference to the binary data if this is Binary.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            StatValue::Binary(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Get the boolean value if this is a Boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            StatValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_value_as_i64() {
        assert_eq!(StatValue::Byte(42).as_i64(), Some(42));
        assert_eq!(StatValue::Short(1000).as_i64(), Some(1000));
        assert_eq!(StatValue::Int(100000).as_i64(), Some(100000));
        assert_eq!(StatValue::Long(1000000).as_i64(), Some(1000000));
        assert_eq!(StatValue::Date(18000).as_i64(), Some(18000));
        assert_eq!(StatValue::Timestamp(1234567890).as_i64(), Some(1234567890));
        assert_eq!(
            StatValue::TimestampNtz(1234567890).as_i64(),
            Some(1234567890)
        );
        assert_eq!(StatValue::String("test".to_string()).as_i64(), None);
        assert_eq!(StatValue::Boolean(true).as_i64(), None);
    }

    #[test]
    fn test_stat_value_as_f64() {
        assert_eq!(StatValue::Float(3.14).as_f64(), Some(3.14f32 as f64));
        assert_eq!(StatValue::Double(2.718).as_f64(), Some(2.718));
        assert_eq!(StatValue::Int(42).as_f64(), Some(42.0));
        assert_eq!(StatValue::String("test".to_string()).as_f64(), None);
    }

    #[test]
    fn test_stat_value_as_str() {
        assert_eq!(
            StatValue::String("hello".to_string()).as_str(),
            Some("hello")
        );
        assert_eq!(StatValue::Int(42).as_str(), None);
    }

    #[test]
    fn test_stat_value_as_bytes() {
        let data = vec![1, 2, 3, 4];
        assert_eq!(
            StatValue::Binary(data.clone()).as_bytes(),
            Some(data.as_slice())
        );
        assert_eq!(StatValue::Int(42).as_bytes(), None);
    }

    #[test]
    fn test_stat_value_as_bool() {
        assert_eq!(StatValue::Boolean(true).as_bool(), Some(true));
        assert_eq!(StatValue::Boolean(false).as_bool(), Some(false));
        assert_eq!(StatValue::Int(1).as_bool(), None);
    }

    #[test]
    fn test_stats_parsed_creation() {
        let stats = StatsParsed {
            num_records: 100,
            min_values: HashMap::from([
                ("id".to_string(), Some(StatValue::Int(1))),
                (
                    "name".to_string(),
                    Some(StatValue::String("Alice".to_string())),
                ),
            ]),
            max_values: HashMap::from([
                ("id".to_string(), Some(StatValue::Int(100))),
                (
                    "name".to_string(),
                    Some(StatValue::String("Zoe".to_string())),
                ),
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
        if let Some(Some(StatValue::Int(min_id))) = stats.min_values.get("id") {
            assert_eq!(*min_id, 1);
        } else {
            panic!("Expected min id to be Some(Int(1))");
        }

        // Verify null value
        assert_eq!(stats.null_count.get("id"), Some(&0));
    }

    #[test]
    fn test_stats_parsed_with_nulls() {
        let stats = StatsParsed {
            num_records: 50,
            min_values: HashMap::from([
                ("col1".to_string(), Some(StatValue::Int(10))),
                ("col2".to_string(), None), // All nulls
            ]),
            max_values: HashMap::from([
                ("col1".to_string(), Some(StatValue::Int(100))),
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
            min_values: HashMap::from([("x".to_string(), Some(StatValue::Int(1)))]),
            max_values: HashMap::from([("x".to_string(), Some(StatValue::Int(10)))]),
            null_count: HashMap::from([("x".to_string(), 0)]),
            tight_bounds: None,
        };

        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }
}
