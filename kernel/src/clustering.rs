//! Clustering column support for Delta tables.
//!
//! This module provides functionality for reading and writing clustering columns
//! via domain metadata. Per the Delta protocol, writers MUST write per-file statistics
//! for clustering columns.
//!
//! Clustering columns are stored in domain metadata under the `delta.clustering` domain
//! as a JSON object with a `clusteringColumns` field containing an array of column paths,
//! where each path is an array of field names (to handle nested columns).

use serde::{Deserialize, Serialize};

use crate::actions::domain_metadata::domain_metadata_configuration;
use crate::actions::DomainMetadata;
use crate::expressions::ColumnName;
use crate::log_segment::LogSegment;
use crate::scan::data_skipping::stats_schema::is_skipping_eligible_datatype;
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Engine, Error};

/// Domain metadata structure for clustering columns.
///
/// This is deserialized from the JSON configuration stored in the
/// `delta.clustering` domain metadata. Each clustering column is represented
/// as an array of field names to support nested columns.
///
/// The column names are physical names. If column mapping is enabled, these will be
/// the physical column identifiers (e.g., `col-uuid`); otherwise, they match the logical names.
///
/// Example JSON:
/// ```json
/// {"clusteringColumns": [["col1"], ["user", "address", "city"]]}
/// ```
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusteringDomainMetadata {
    clustering_columns: Vec<Vec<String>>,
}

/// The domain name for clustering metadata.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";

/// Validates clustering columns against the table schema.
///
/// This function performs comprehensive validation of clustering columns:
///
/// **Structural validations:**
/// 1. At least one column must be specified
/// 2. No duplicate columns
///
/// **Schema validations:**
/// 3. Column paths must resolve in the schema (including nested paths through structs)
/// 4. Leaf field must have a data type eligible for statistics collection
///
/// Both top-level and nested columns are supported. For nested columns, all intermediate
/// fields must be struct types and the leaf field must be a stats-eligible primitive.
///
/// # Errors
///
/// Returns an error if any validation fails.
pub(crate) fn validate_clustering_columns(
    schema: &StructType,
    columns: &[ColumnName],
) -> DeltaResult<()> {
    use std::collections::HashSet;

    // Structural validation: at least one column required
    if columns.is_empty() {
        return Err(Error::generic("Clustering requires at least one column"));
    }

    // Validate each column and check for duplicates
    let mut seen = HashSet::new();
    for col in columns {
        if !seen.insert(col) {
            return Err(Error::generic(format!(
                "Duplicate clustering column: '{}'",
                col
            )));
        }

        // Walk the column path through nested structs and validate the leaf type.
        // walk_column_fields validates: non-empty path, each field exists, intermediates are structs.
        let fields = schema.walk_column_fields(col)?;
        let leaf_type = fields
            .last()
            .ok_or_else(|| Error::generic(format!("Could not resolve column '{col}' in schema")))?
            .data_type();
        match leaf_type {
            DataType::Primitive(ptype) if is_skipping_eligible_datatype(ptype) => {}
            dt => {
                return Err(Error::generic(format!(
                    "Clustering column '{}' has unsupported type '{}'. \
                     Supported types: Byte, Short, Integer, Long, Float, Double, \
                     Decimal, Date, Timestamp, TimestampNtz, String",
                    col, dt
                )));
            }
        }
    }
    Ok(())
}

/// Creates domain metadata for clustering configuration.
///
/// Converts the given clustering columns into the JSON format required by the Delta protocol
/// and wraps it in a `DomainMetadata` action.
///
/// # Format
///
/// The JSON format is: `{"clusteringColumns": [["col1"], ["col2"]]}`
/// Each column is represented as an array of path components to support nested columns.
pub(crate) fn create_clustering_domain_metadata(columns: &[ColumnName]) -> DomainMetadata {
    let metadata = ClusteringDomainMetadata {
        clustering_columns: columns
            .iter()
            .map(|c| c.path().iter().map(|s| s.to_string()).collect())
            .collect(),
    };
    // ClusteringDomainMetadata serialization cannot fail (only contains Vec<Vec<String>>)
    #[allow(clippy::unwrap_used)]
    let config = serde_json::to_string(&metadata).unwrap();

    DomainMetadata::new(CLUSTERING_DOMAIN_NAME.to_string(), config)
}

/// Parses clustering columns from a JSON configuration string.
///
/// Returns `Ok(columns)` if the configuration is valid, or an error if malformed.
fn parse_clustering_columns(json_str: &str) -> DeltaResult<Vec<ColumnName>> {
    let metadata: ClusteringDomainMetadata = serde_json::from_str(json_str)?;
    Ok(metadata
        .clustering_columns
        .into_iter()
        .map(ColumnName::new)
        .collect())
}

/// Reads clustering columns from the log segment's domain metadata.
///
/// This function performs a log scan to find the clustering domain metadata.
/// Callers should first check if the `ClusteredTable` feature is enabled via
/// the protocol before calling this function to avoid unnecessary I/O.
/// See [`Snapshot::get_clustering_columns`] which performs this check.
///
/// Returns `Ok(Some(columns))` if clustering domain metadata exists,
/// `Ok(None)` if no clustering domain metadata is found, or an error if the
/// metadata is malformed.
///
/// [`Snapshot::get_clustering_columns`]: crate::snapshot::Snapshot::get_clustering_columns
pub(crate) fn get_clustering_columns(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<Option<Vec<ColumnName>>> {
    let config = domain_metadata_configuration(log_segment, CLUSTERING_DOMAIN_NAME, engine)?;
    match config {
        Some(json_str) => Ok(Some(parse_clustering_columns(&json_str)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField};

    #[rstest::rstest]
    #[case::simple(
        r#"{"clusteringColumns": [["col1"], ["col2"]]}"#,
        vec![vec!["col1"], vec!["col2"]]
    )]
    #[case::empty(
        r#"{"clusteringColumns": []}"#,
        vec![]
    )]
    #[case::nested(
        r#"{"clusteringColumns": [["id"], ["user", "address", "city"], ["a", "b", "c", "d", "e"]]}"#,
        vec![vec!["id"], vec!["user", "address", "city"], vec!["a", "b", "c", "d", "e"]]
    )]
    #[case::special_characters(
        r#"{"clusteringColumns": [["col.with.dot"], ["`backticks`", "nested"]]}"#,
        vec![vec!["col.with.dot"], vec!["`backticks`", "nested"]]
    )]
    #[case::tolerates_unknown_fields(
        r#"{"clusteringColumns": [["col1"]], "foo": "bar", "futureField": 123}"#,
        vec![vec!["col1"]]
    )]
    fn test_parse_clustering_columns(#[case] json: &str, #[case] expected: Vec<Vec<&str>>) {
        let columns = parse_clustering_columns(json).unwrap();
        let expected_cols: Vec<ColumnName> = expected.into_iter().map(ColumnName::new).collect();
        assert_eq!(columns, expected_cols);
    }

    #[test]
    fn test_validate_clustering_columns_valid() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let columns = vec![ColumnName::new(["id"])];
        assert!(validate_clustering_columns(&schema, &columns).is_ok());
    }

    #[test]
    fn test_validate_clustering_columns_not_found() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)]);
        let columns = vec![ColumnName::new(["nonexistent"])];
        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }

    #[test]
    fn test_validate_clustering_columns_nested_valid() {
        let address_struct = StructType::new_unchecked(vec![
            StructField::new("city", DataType::STRING, true),
            StructField::new("zip", DataType::STRING, true),
        ]);
        let user_struct = StructType::new_unchecked(vec![
            StructField::new("name", DataType::STRING, true),
            StructField::new("address", DataType::Struct(Box::new(address_struct)), true),
        ]);
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("user", DataType::Struct(Box::new(user_struct)), true),
        ]);

        // Nested leaf column with eligible type should succeed
        let columns = vec![ColumnName::new(["user", "address", "city"])];
        assert!(validate_clustering_columns(&schema, &columns).is_ok());
    }

    #[test]
    fn test_validate_clustering_nested_struct_leaf_rejected() {
        let inner_struct =
            StructType::new_unchecked(vec![StructField::new("field", DataType::STRING, false)]);
        let schema = StructType::new_unchecked(vec![StructField::new(
            "parent",
            DataType::Struct(Box::new(inner_struct)),
            false,
        )]);

        // Clustering on an entire struct (not a leaf primitive) should fail
        let columns = vec![ColumnName::new(["parent"])];
        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported type"));
    }

    #[test]
    fn test_validate_clustering_nested_intermediate_not_struct() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("flat_col", DataType::STRING, false)]);

        // Trying to traverse into a non-struct field should fail
        let columns = vec![ColumnName::new(["flat_col", "child"])];
        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("is not a struct type"));
    }

    #[test]
    fn test_validate_clustering_nested_path_not_found() {
        let inner_struct =
            StructType::new_unchecked(vec![StructField::new("field", DataType::STRING, false)]);
        let schema = StructType::new_unchecked(vec![StructField::new(
            "parent",
            DataType::Struct(Box::new(inner_struct)),
            false,
        )]);

        // Nested field that doesn't exist should fail
        let columns = vec![ColumnName::new(["parent", "nonexistent"])];
        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }

    #[test]
    fn test_create_clustering_domain_metadata() {
        let columns = vec![ColumnName::new(["col1"]), ColumnName::new(["col2"])];
        let dm = create_clustering_domain_metadata(&columns);

        assert_eq!(dm.domain(), CLUSTERING_DOMAIN_NAME);

        // Verify roundtrip: the JSON we create should be parseable back
        let parsed = parse_clustering_columns(dm.configuration()).unwrap();
        assert_eq!(parsed, columns);
    }

    #[test]
    fn test_create_and_parse_roundtrip() {
        // Test that create and parse are inverses
        let original = vec![
            ColumnName::new(["id"]),
            ColumnName::new(["timestamp"]),
            ColumnName::new(["region"]),
        ];
        let dm = create_clustering_domain_metadata(&original);
        let parsed = parse_clustering_columns(dm.configuration()).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_validate_clustering_columns_supported_types() {
        // All supported primitive types
        let schema = StructType::new_unchecked(vec![
            StructField::new("byte_col", DataType::BYTE, false),
            StructField::new("short_col", DataType::SHORT, false),
            StructField::new("int_col", DataType::INTEGER, false),
            StructField::new("long_col", DataType::LONG, false),
            StructField::new("float_col", DataType::FLOAT, false),
            StructField::new("double_col", DataType::DOUBLE, false),
            StructField::new("date_col", DataType::DATE, false),
            StructField::new("timestamp_col", DataType::TIMESTAMP, false),
            StructField::new("timestamp_ntz_col", DataType::TIMESTAMP_NTZ, false),
            StructField::new("string_col", DataType::STRING, false),
            StructField::new("decimal_col", DataType::decimal(10, 2).unwrap(), false),
        ]);

        // Each supported type should be valid for clustering
        for field in schema.fields() {
            let columns = vec![ColumnName::new([field.name()])];
            assert!(
                validate_clustering_columns(&schema, &columns).is_ok(),
                "Type {} should be supported for clustering",
                field.data_type()
            );
        }
    }

    #[test]
    fn test_validate_clustering_columns_unsupported_primitive_types() {
        // Boolean and Binary are primitives but not supported for clustering
        let schema = StructType::new_unchecked(vec![
            StructField::new("bool_col", DataType::BOOLEAN, false),
            StructField::new("binary_col", DataType::BINARY, false),
        ]);

        for field in schema.fields() {
            let columns = vec![ColumnName::new([field.name()])];
            let result = validate_clustering_columns(&schema, &columns);
            assert!(
                result.is_err(),
                "Type {} should NOT be supported for clustering",
                field.data_type()
            );
            assert!(result.unwrap_err().to_string().contains("unsupported type"));
        }
    }

    #[test]
    fn test_validate_clustering_columns_complex_types_rejected() {
        use crate::schema::{ArrayType, MapType};

        let inner_struct =
            StructType::new_unchecked(vec![StructField::new("inner", DataType::STRING, false)]);

        let schema = StructType::new_unchecked(vec![
            StructField::new(
                "struct_col",
                DataType::Struct(Box::new(inner_struct)),
                false,
            ),
            StructField::new(
                "array_col",
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                false,
            ),
            StructField::new(
                "map_col",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::INTEGER,
                    false,
                ))),
                false,
            ),
        ]);

        for field in schema.fields() {
            let columns = vec![ColumnName::new([field.name()])];
            let result = validate_clustering_columns(&schema, &columns);
            assert!(
                result.is_err(),
                "Complex type {} should NOT be supported for clustering",
                field.data_type()
            );
            assert!(result.unwrap_err().to_string().contains("unsupported type"));
        }
    }

    // Structural validation tests - parameterized with rstest

    /// Test that any number of clustering columns is allowed (no protocol-imposed limit).
    #[rstest::rstest]
    #[case::four(4)]
    #[case::five(5)]
    #[case::ten(10)]
    fn test_validate_clustering_column_count(#[case] num_columns: usize) {
        let fields: Vec<StructField> = (0..num_columns)
            .map(|i| StructField::new(format!("col{}", i), DataType::INTEGER, false))
            .collect();
        let schema = StructType::new_unchecked(fields);

        let columns: Vec<ColumnName> = (0..num_columns)
            .map(|i| ColumnName::new([format!("col{}", i)]))
            .collect();

        assert!(validate_clustering_columns(&schema, &columns).is_ok());
    }

    /// Test various structural validation error cases.
    #[rstest::rstest]
    #[case::empty_columns(vec![], "at least one column")]
    #[case::duplicate_columns(vec!["id", "id"], "Duplicate clustering column")]
    fn test_validate_clustering_structural_errors(
        #[case] column_names: Vec<&str>,
        #[case] expected_error: &str,
    ) {
        let schema =
            StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)]);
        let columns: Vec<ColumnName> = column_names
            .into_iter()
            .map(|s| ColumnName::new([s]))
            .collect();

        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(expected_error),
            "Expected error containing '{}'",
            expected_error
        );
    }

    #[test]
    fn test_validate_clustering_columns_empty_name_rejected() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)]);
        // Create a ColumnName with empty path (can't easily express in rstest case)
        let columns: Vec<ColumnName> = vec![ColumnName::new(Vec::<String>::new())];
        let result = validate_clustering_columns(&schema, &columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }
}
