//! This module defines the `Statistics` struct used in Add and Remove actions.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    /// The number of records in this data file.
    // Even though num_records is non-negative, we implement it as i64 since the Delta spec does
    // not define unsigned data types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_records: Option<i64>,

    /// Whether per-column statistics are currently tight, i.e. the min/maxValue actually exists in the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,

    // TODO: Implement per-column statistics
    // See (https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics)
    /// The number of `null` value for this column or an estimate thereof (depending on the `tight_bounds` value).
    /// For now, kernel ignores this value.
    #[serde(skip_serializing_if = "Option::is_none", skip_deserializing)]
    pub null_count: Option<String>,

    /// The minimum value per column in this file or an estimate thereof (depending on the `tight_bounds` value).
    /// For now, kernel ignores this value.
    #[serde(skip_serializing_if = "Option::is_none", skip_deserializing)]
    pub min_values: Option<String>,

    /// The maximum value per column in this file or an estimate thereof (depending on the `tight_bounds` value).
    /// For now, kernel ignores this value.
    #[serde(skip_serializing_if = "Option::is_none", skip_deserializing)]
    pub max_values: Option<String>,
}

impl Statistics {
    /// Creates a new `Statistics` instance with the given number of records.
    ///
    /// This is a convenience method since `num_records` as the only statistic used by kernel at this point and
    /// Parquet writers might expose this value as `usize`, which is not an official data type in the Delta spec.
    #[allow(unused)]
    pub fn new(num_records: usize) -> Self {
        Self {
            num_records: Some(num_records as i64),
            tight_bounds: None,
            null_count: None,
            min_values: None,
            max_values: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_serialization_valid_cases() {
        // Test case: numRecords only
        let stats = Statistics {
            num_records: Some(100),
            tight_bounds: None,
            null_count: None,
            min_values: None,
            max_values: None,
        };
        let json_str = serde_json::to_string(&stats).unwrap();
        assert_eq!(json_str, r#"{"numRecords":100}"#);

        // Test case: empty statistics
        let stats = Statistics {
            num_records: None,
            tight_bounds: None,
            null_count: None,
            min_values: None,
            max_values: None,
        };
        let json_str = serde_json::to_string(&stats).unwrap();
        assert_eq!(json_str, "{}");

        // Test case: tightBounds and numRecords
        let stats = Statistics {
            num_records: Some(0),
            tight_bounds: Some(true),
            null_count: None,
            min_values: None,
            max_values: None,
        };
        let json_str = serde_json::to_string(&stats).unwrap();
        assert_eq!(json_str, r#"{"numRecords":0,"tightBounds":true}"#);
    }

    #[test]
    fn test_statistics_deserialization_valid_cases() {
        // Only numRecords
        let json_str = r#"{"numRecords":100}"#;
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, Some(100));
        assert_eq!(stats.tight_bounds, None);
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);

        // Empty stats
        let json_str = "{}";
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, None);
        assert_eq!(stats.tight_bounds, None);
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);

        // numRecords and tightBounds
        let json_str = r#"{"tightBounds":true, "numRecords":0}"#;
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, Some(0));
        assert_eq!(stats.tight_bounds, Some(true));
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);

        // Test cases for statistics fields that we currently ignore
        let json_str = r#"{"minValues":{"whatever":"in_here"}, "numRecords":100}"#;
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, Some(100));
        assert_eq!(stats.tight_bounds, None);
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);

        let json_str = r#"{
            "numRecords": 200,
            "minValues": {
                "as_int": 0,
                "as_long": 0,
                "as_byte": 0,
                "as_short": 0,
                "as_float": 0.0,
                "as_double": 0.0,
                "as_string": "0",
                "as_date": "2000-01-01",
                "as_timestamp": "2000-01-01T00:00:00.000-08:00",
                "as_big_decimal": 0
            },
            "maxValues": {
                "as_int": 0,
                "as_long": 0,
                "as_byte": 0,
                "as_short": 0,
                "as_float": 0.0,
                "as_double": 0.0,
                "as_string": "0",
                "as_date": "2000-01-01",
                "as_timestamp": "2000-01-01T00:00:00.000-08:00",
                "as_big_decimal": 0
            },
            "nullCount": {
                "as_int": 0,
                "as_long": 0,
                "as_byte": 0,
                "as_short": 0,
                "as_float": 0,
                "as_double": 0,
                "as_string": 0,
                "as_date": 0,
                "as_timestamp": 0,
                "as_big_decimal": 0
            }
        }"#;
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, Some(200));
        assert_eq!(stats.tight_bounds, None);
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);
    }

    #[test]
    fn test_statistics_roundtrip() {
        // Test that serialization -> deserialization produces the same result
        let original = Statistics {
            num_records: Some(42),
            tight_bounds: Some(true),
            null_count: None,
            min_values: None,
            max_values: None,
        };

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Statistics = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_statistics_deserialization_invalid_cases() {
        // Test case: unknown field
        let json_str = r#"{"tightBounds":true, "wrongKey":0}"#;
        let result: Result<Statistics, _> = serde_json::from_str(json_str);
        // If we want strict validation, we'd need to add #[serde(deny_unknown_fields)]
        assert!(
            result.is_ok(),
            "Unknown fields are currently ignored by serde"
        );

        // Test case: invalid numRecords type (string instead of number)
        let json_str = r#"{"numRecords":"not_a_number"}"#;
        let result: Result<Statistics, _> = serde_json::from_str(json_str);
        assert!(
            result.is_err(),
            "Should fail when numRecords is not a number"
        );

        // Test case: invalid tightBounds type (string instead of boolean)
        let json_str = r#"{"tightBounds":"not_a_boolean"}"#;
        let result: Result<Statistics, _> = serde_json::from_str(json_str);
        assert!(
            result.is_err(),
            "Should fail when tightBounds is not a boolean"
        );

        // Test case: malformed JSON
        let json_str = r#"{"numRecords":100"#; // Missing closing brace
        let result: Result<Statistics, _> = serde_json::from_str(json_str);
        assert!(result.is_err(), "Should fail on malformed JSON");
    }

    #[test]
    fn test_statistics_new_constructor() {
        let stats = Statistics::new(150);
        assert_eq!(stats.num_records, Some(150));
        assert_eq!(stats.tight_bounds, None);
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);

        // Test serialization of constructed stats
        let json_str = serde_json::to_string(&stats).unwrap();
        assert_eq!(json_str, r#"{"numRecords":150}"#);
    }

    #[test]
    fn test_statistics_with_empty_objects() {
        // Test with empty objects for min/max/null values
        let json_str = r#"{"numRecords":50, "nullCount":{}, "minValues":{}, "maxValues":{}}"#;
        let stats: Statistics = serde_json::from_str(json_str).unwrap();
        assert_eq!(stats.num_records, Some(50));
        assert_eq!(stats.null_count, None);
        assert_eq!(stats.min_values, None);
        assert_eq!(stats.max_values, None);
    }

    #[test]
    fn test_statistics_skip_serializing_none_fields() {
        // Verify that None fields are not included in serialization
        let stats = Statistics {
            num_records: Some(123),
            tight_bounds: None,
            null_count: None,
            min_values: None,
            max_values: None,
        };

        let json_str = serde_json::to_string(&stats).unwrap();
        // Should only contain numRecords, not the None fields
        assert!(json_str.contains("numRecords"));
        assert!(!json_str.contains("tightBounds"));
        assert!(!json_str.contains("nullCount"));
        assert!(!json_str.contains("minValues"));
        assert!(!json_str.contains("maxValues"));
    }
}
