//! Partition key and type validation for the write path.
//!
//! Validates that the connector provided the correct partition column names and value types
//! before serialization. Called internally by [`Transaction::partitioned_write_context`].
//!
//! - [`validate_partition_keys`]: checks key completeness (case-insensitive matching,
//!   normalizes to schema case, detects post-normalization duplicates).
//! - [`validate_partition_value_types`]: checks that each `Scalar`'s type matches the
//!   partition column's schema type. Null scalars skip the type check.
//!
//! [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context

use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::schema::StructType;
use crate::{DeltaResult, Error};

/// Validates that `partition_values` contains exactly the expected partition columns,
/// with case-insensitive key matching. Returns the map re-keyed to schema case.
///
/// Errors if:
/// - A partition column is missing from the map
/// - An extra key is present that is not a partition column
/// - Two keys collide after case normalization (e.g., "COL" and "col" both provided)
pub(crate) fn validate_partition_keys(
    logical_partition_columns: &[String],
    partition_values: HashMap<String, Scalar>,
) -> DeltaResult<HashMap<String, Scalar>> {
    // Build a case-insensitive lookup from the schema's partition column names.
    let schema_lookup: HashMap<String, &str> = logical_partition_columns
        .iter()
        .map(|name| (name.to_lowercase(), name.as_str()))
        .collect();

    let mut normalized = HashMap::with_capacity(partition_values.len());
    for (key, value) in partition_values {
        let lower_key = key.to_lowercase();
        let schema_name = schema_lookup.get(&lower_key).ok_or_else(|| {
            Error::generic(format!(
                "unknown partition column '{key}'. Expected one of: [{}]",
                logical_partition_columns.join(", ")
            ))
        })?;
        // Detect post-normalization duplicates (e.g., "COL" and "col" both provided).
        if normalized.contains_key(*schema_name) {
            return Err(Error::generic(format!(
                "duplicate partition column '{key}' (case-insensitive match with existing key)"
            )));
        }
        normalized.insert(schema_name.to_string(), value);
    }

    // Check that all partition columns are present.
    for col in logical_partition_columns {
        if !normalized.contains_key(col.as_str()) {
            return Err(Error::generic(format!(
                "missing partition column '{col}'. Provided: [{}]",
                normalized.keys().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
    }

    Ok(normalized)
}

/// Validates that each [`Scalar`] value's type is compatible with the corresponding
/// partition column's type in the table schema. Null scalars skip the type check
/// (null is valid for any partition column type).
pub(crate) fn validate_partition_value_types(
    logical_schema: &StructType,
    partition_values: &HashMap<String, Scalar>,
) -> DeltaResult<()> {
    for (col_name, value) in partition_values {
        if value.is_null() {
            continue;
        }
        let field = logical_schema.field(col_name).ok_or_else(|| {
            Error::generic(format!(
                "partition column '{col_name}' not found in table schema"
            ))
        })?;
        let expected_type = field.data_type();
        let actual_type = value.data_type();
        if *expected_type != actual_type {
            return Err(Error::generic(format!(
                "partition column '{col_name}' has type {expected_type:?} but got \
                 value of type {actual_type:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Scalar;
    use crate::schema::{DataType, StructField};

    // === validate_partition_keys tests ===

    #[test]
    fn test_validate_partition_keys_matching_keys_returns_ok() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_partition_keys(&cols, values).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("year"), Some(&Scalar::Integer(2024)));
    }

    #[test]
    fn test_validate_partition_keys_missing_key_returns_error() {
        let cols = vec!["year".to_string(), "region".to_string()];
        let values = HashMap::from([("year".to_string(), Scalar::Integer(2024))]);
        let result = validate_partition_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing partition column 'region'"), "{err}");
    }

    #[test]
    fn test_validate_partition_keys_extra_key_returns_error() {
        let cols = vec!["year".to_string()];
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        let result = validate_partition_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown partition column 'region'"), "{err}");
    }

    #[test]
    fn test_validate_partition_keys_case_normalizes_to_schema_case() {
        let cols = vec!["Year".to_string()];
        let values = HashMap::from([("YEAR".to_string(), Scalar::Integer(2024))]);
        let result = validate_partition_keys(&cols, values).unwrap();
        // Key should be normalized to "Year" (the schema case), not "YEAR"
        assert!(result.contains_key("Year"));
        assert!(!result.contains_key("YEAR"));
    }

    #[test]
    fn test_validate_partition_keys_duplicate_after_normalization_returns_error() {
        let cols = vec!["col".to_string()];
        let values = HashMap::from([
            ("COL".to_string(), Scalar::Integer(1)),
            ("col".to_string(), Scalar::Integer(2)),
        ]);
        let result = validate_partition_keys(&cols, values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
    }

    // === validate_partition_value_types tests ===

    #[test]
    fn test_validate_partition_value_types_matching_types_returns_ok() {
        let schema = StructType::new_unchecked(vec![
            StructField::not_null("year", DataType::INTEGER),
            StructField::nullable("region", DataType::STRING),
        ]);
        let values = HashMap::from([
            ("year".to_string(), Scalar::Integer(2024)),
            ("region".to_string(), Scalar::String("US".into())),
        ]);
        validate_partition_value_types(&schema, &values).unwrap();
    }

    #[test]
    fn test_validate_partition_value_types_mismatch_returns_error() {
        let schema =
            StructType::new_unchecked(vec![StructField::not_null("year", DataType::INTEGER)]);
        let values = HashMap::from([("year".to_string(), Scalar::String("2024".into()))]);
        let result = validate_partition_value_types(&schema, &values);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("year"), "{err}");
    }

    #[test]
    fn test_validate_partition_value_types_null_skips_type_check() {
        let schema =
            StructType::new_unchecked(vec![StructField::not_null("year", DataType::INTEGER)]);
        let values = HashMap::from([("year".to_string(), Scalar::Null(DataType::STRING))]);
        // Null skips the type check even though the null's inner type is STRING, not INTEGER.
        validate_partition_value_types(&schema, &values).unwrap();
    }
}
