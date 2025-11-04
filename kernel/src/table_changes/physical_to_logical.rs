use std::collections::HashMap;

use crate::expressions::Scalar;
use crate::scan::state_info::StateInfo;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::transforms::{get_transform_expr, parse_partition_values};
use crate::{DeltaResult, Error, ExpressionRef};

use super::scan_file::{CdfScanFile, CdfScanFileType};
use super::{CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME};

/// Gets CDF metadata columns from the logical schema and scan file.
///
/// This function directly looks up CDF columns in the schema and generates their values
/// based on the scan file metadata, returning an iterator over the metadata.
fn get_cdf_columns(
    logical_schema: &SchemaRef,
    scan_file: &CdfScanFile,
) -> DeltaResult<impl Iterator<Item = (usize, (String, Scalar))>> {
    // Handle _change_type
    let change_type_field = logical_schema.field_with_index(CHANGE_TYPE_COL_NAME);
    let change_type_metadata = match (change_type_field, &scan_file.scan_type) {
        (Some((idx, field)), CdfScanFileType::Add | CdfScanFileType::Remove) => {
            let name = field.name().to_string();
            let value = Scalar::String(scan_file.scan_type.get_cdf_string_value().to_string());
            Some((idx, (name, value)))
        }
        (Some(_), CdfScanFileType::Cdc) | (None, _) => {
            // Cdc files contain the `change_type_` column physically, so we do not insert a metadata-derived value
            None
        }
    };

    // Handle _commit_timestamp
    let timestamp_field = logical_schema.field_with_index(COMMIT_TIMESTAMP_COL_NAME);
    let timestamp_metadata = if let Some((idx, field)) = timestamp_field {
        let value = Scalar::timestamp_from_millis(scan_file.commit_timestamp)
            .map_err(|e| Error::generic(format!("Failed to process {}: {e}", scan_file.path)))?;
        Some((idx, (field.name().to_string(), value)))
    } else {
        None
    };

    // Handle _commit_version
    let version_field = logical_schema.field_with_index(COMMIT_VERSION_COL_NAME);
    let version_metadata = version_field.map(|(idx, field)| {
        let name = field.name().to_string();
        let value = Scalar::Long(scan_file.commit_version);
        (idx, (name, value))
    });

    Ok(change_type_metadata
        .into_iter()
        .chain(timestamp_metadata)
        .chain(version_metadata))
}

/// Gets the physical schema that will be used to read data in the `scan_file` path.
pub(crate) fn scan_file_physical_schema(
    scan_file: &CdfScanFile,
    physical_schema: &StructType,
) -> SchemaRef {
    if scan_file.scan_type == CdfScanFileType::Cdc {
        let change_type = StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING);
        let fields = physical_schema.fields().cloned().chain(Some(change_type));
        // NOTE: We don't validate the fields again because CHANGE_TYPE_COL_NAME should never be used anywhere else
        StructType::new_unchecked(fields).into()
    } else {
        physical_schema.clone().into()
    }
}

// Get the transform expression for a CDF scan file
//
// Returns None when no transformation is needed (identity transform), otherwise returns Some(expr).
//
// Note: parse_partition_values returns null values for missing partition columns,
// and CDF metadata columns (commit_timestamp, commit_version, change_type) are then
// added to overwrite any conflicting values. This behavior can be made more strict by changing
// the parse_partition_values function to return an error for missing partition values,
// and adding cdf values to the partition_values map

// Note: Delta doesn't support row-tracking for CDF (see:
// https://docs.databricks.com/aws/en/delta/row-tracking#limitations)
pub(crate) fn get_cdf_transform_expr(
    scan_file: &CdfScanFile,
    state_info: &StateInfo,
    physical_schema: &StructType,
) -> DeltaResult<Option<ExpressionRef>> {
    let mut partition_values = HashMap::new();

    // Get the transform spec from StateInfo (if present)
    let empty_spec = Vec::new();
    let transform_spec = state_info
        .transform_spec
        .as_ref()
        .map(|ts| ts.as_ref())
        .unwrap_or(&empty_spec);

    // Return None for identity transforms to avoid unnecessary expression evaluation
    if transform_spec.is_empty() {
        return Ok(None);
    }

    // Handle regular partition values using parse_partition_values
    let parsed_values = parse_partition_values(
        &state_info.logical_schema,
        transform_spec,
        &scan_file.partition_values,
    )?;
    partition_values.extend(parsed_values);

    // Handle CDF metadata columns
    let cdf_values = get_cdf_columns(&state_info.logical_schema, scan_file)?;
    partition_values.extend(cdf_values);

    get_transform_expr(
        transform_spec,
        partition_values,
        physical_schema,
        None, /* base_row_id */
    )
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Expression;
    use crate::scan::state::DvInfo;
    use crate::scan::state_info::StateInfo;
    use crate::scan::PhysicalPredicate;
    use crate::schema::{DataType, StructField, StructType};
    use crate::transforms::FieldTransformSpec;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_logical_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("age", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
            StructField::nullable("_commit_version", DataType::LONG),
            StructField::nullable("_commit_timestamp", DataType::TIMESTAMP),
        ]))
    }

    fn create_test_physical_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("name", DataType::STRING),
        ])
    }

    fn create_test_cdf_scan_file() -> CdfScanFile {
        CdfScanFile {
            path: "test/file.parquet".to_string(),
            partition_values: {
                let mut map = HashMap::new();
                map.insert("age".to_string(), "30".to_string());
                map
            },
            scan_type: CdfScanFileType::Add,
            commit_version: 100,
            commit_timestamp: 1000000000000,
            dv_info: DvInfo::default(),
            remove_dv: None,
        }
    }

    // Helper to create StateInfo with a custom transform spec for tests
    fn create_test_state_info(
        logical_schema: SchemaRef,
        transform_spec: Vec<FieldTransformSpec>,
    ) -> StateInfo {
        let physical_schema = create_test_physical_schema();
        StateInfo {
            logical_schema,
            physical_schema: physical_schema.into(),
            physical_predicate: PhysicalPredicate::None,
            transform_spec: Some(Arc::new(transform_spec)),
        }
    }

    #[test]
    fn test_get_cdf_transform_expr_add_file_with_cdf_metadata() {
        // Add files need _change_type metadata injected
        let scan_file = create_test_cdf_scan_file(); // Default is Add type
        let logical_schema = create_test_logical_schema();
        let physical_schema = create_test_physical_schema();

        // Request CDF metadata columns in transform
        // _change_type should be DynamicColumn (physical in CDC, metadata in Add/Remove)
        let transform_spec = vec![
            FieldTransformSpec::DynamicColumn {
                field_index: 3, // _change_type
                physical_name: "_change_type".to_string(),
                insert_after: Some("id".to_string()),
            },
            FieldTransformSpec::MetadataDerivedColumn {
                field_index: 4, // _commit_version
                insert_after: Some("id".to_string()),
            },
        ];

        let state_info = create_test_state_info(logical_schema, transform_spec);

        let result = get_cdf_transform_expr(&scan_file, &state_info, &physical_schema);
        assert!(result.is_ok());

        let expr_opt = result.unwrap();
        assert!(expr_opt.is_some(), "Expected Some(expr) but got None");
        let expr = expr_opt.unwrap();
        let Expression::Transform(transform) = expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Should have transform for "id" field with CDF metadata
        assert!(transform.field_transforms.contains_key("id"));
        let id_transform = &transform.field_transforms["id"];
        assert!(!id_transform.is_replace);
        assert_eq!(id_transform.exprs.len(), 2);

        // Verify _change_type is "insert" for Add files
        let Expression::Literal(change_type) = id_transform.exprs[0].as_ref() else {
            panic!("Expected literal for _change_type");
        };
        assert_eq!(change_type, &Scalar::String("insert".to_string()));

        // Verify _commit_version
        let Expression::Literal(version) = id_transform.exprs[1].as_ref() else {
            panic!("Expected literal for _commit_version");
        };
        assert_eq!(version, &Scalar::Long(100));
    }

    #[test]
    fn test_get_cdf_transform_expr_remove_file_with_cdf_metadata() {
        // Remove files need _change_type metadata with "delete" value
        let mut scan_file = create_test_cdf_scan_file();
        scan_file.scan_type = CdfScanFileType::Remove;

        let logical_schema = create_test_logical_schema();
        let physical_schema = create_test_physical_schema();

        let transform_spec = vec![FieldTransformSpec::DynamicColumn {
            field_index: 3, // _change_type
            physical_name: "_change_type".to_string(),
            insert_after: Some("name".to_string()),
        }];

        let state_info = create_test_state_info(logical_schema, transform_spec);

        let result = get_cdf_transform_expr(&scan_file, &state_info, &physical_schema);
        assert!(result.is_ok());

        let expr_opt = result.unwrap();
        assert!(expr_opt.is_some(), "Expected Some(expr) but got None");
        let expr = expr_opt.unwrap();
        let Expression::Transform(transform) = expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        let name_transform = &transform.field_transforms["name"];
        assert_eq!(name_transform.exprs.len(), 1);

        // Verify _change_type is "delete" for Remove files
        let Expression::Literal(change_type) = name_transform.exprs[0].as_ref() else {
            panic!("Expected literal for _change_type");
        };
        assert_eq!(change_type, &Scalar::String("delete".to_string()));
    }

    #[test]
    fn test_get_cdf_transform_expr_cdc_file_with_partition() {
        // CDC files with partitions - should get partition values but not _change_type metadata
        let mut scan_file = create_test_cdf_scan_file();
        scan_file.scan_type = CdfScanFileType::Cdc;

        let logical_schema = create_test_logical_schema();
        // For CDC, physical schema needs _change_type column
        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
        ]);

        // Request both partition and CDF columns
        let transform_spec = vec![
            FieldTransformSpec::MetadataDerivedColumn {
                field_index: 1, // age partition
                insert_after: Some("id".to_string()),
            },
            FieldTransformSpec::DynamicColumn {
                field_index: 3, // _change_type - physical in CDC files
                physical_name: "_change_type".to_string(),
                insert_after: Some("name".to_string()),
            },
        ];

        let state_info = create_test_state_info(logical_schema, transform_spec);

        let result = get_cdf_transform_expr(&scan_file, &state_info, &physical_schema);
        assert!(result.is_ok());

        let expr_opt = result.unwrap();
        assert!(expr_opt.is_some(), "Expected Some(expr) but got None");
        let expr = expr_opt.unwrap();
        let Expression::Transform(transform) = expr.as_ref() else {
            panic!("Expected Transform expression");
        };

        // Should have age partition value
        let id_transform = &transform.field_transforms["id"];
        assert_eq!(id_transform.exprs.len(), 1);
        let Expression::Literal(age_value) = id_transform.exprs[0].as_ref() else {
            panic!("Expected literal for age");
        };
        assert_eq!(age_value, &Scalar::Long(30));

        // For CDC files with DynamicColumn, _change_type is handled as physical
        // The transform spec has DynamicColumn which becomes a Column expression for CDC files
        // (not a literal metadata value)
        let name_transform = &transform.field_transforms["name"];
        assert_eq!(name_transform.exprs.len(), 1);
        // Should be a Column expression, not a Literal
        assert!(matches!(
            name_transform.exprs[0].as_ref(),
            Expression::Column(_)
        ));
    }

    #[test]
    fn test_scan_file_physical_schema_for_cdc() {
        // CDC files need _change_type added to physical schema
        let physical_schema = create_test_physical_schema();
        let mut scan_file = create_test_cdf_scan_file();
        scan_file.scan_type = CdfScanFileType::Cdc;

        let result = scan_file_physical_schema(&scan_file, &physical_schema);

        assert_eq!(result.fields().len(), 3); // Original 2 + _change_type
        let change_type_field = result.field_at_index(2).unwrap();
        assert_eq!(change_type_field.name(), "_change_type");
        assert_eq!(change_type_field.data_type(), &DataType::STRING);
        assert!(!change_type_field.is_nullable()); // Should be non-nullable
    }

    #[test]
    fn test_scan_file_physical_schema_for_add_remove() {
        // Add/Remove files don't modify physical schema
        let physical_schema = create_test_physical_schema();
        let scan_file = create_test_cdf_scan_file();

        // Test Add file
        let result = scan_file_physical_schema(&scan_file, &physical_schema);
        assert_eq!(result.fields().len(), 2); // No change

        // Test Remove file
        let mut remove_file = scan_file.clone();
        remove_file.scan_type = CdfScanFileType::Remove;
        let result = scan_file_physical_schema(&remove_file, &physical_schema);
        assert_eq!(result.fields().len(), 2); // No change
    }

    #[test]
    fn test_get_cdf_transform_expr_returns_none_for_identity() {
        // When there's no transform spec and no CDF metadata columns in the schema,
        // the function should return None (identity transform)
        let scan_file = CdfScanFile {
            path: "test/file.parquet".to_string(),
            partition_values: HashMap::new(),
            scan_type: CdfScanFileType::Add,
            commit_version: 100,
            commit_timestamp: 1000000000000,
            dv_info: DvInfo::default(),
            remove_dv: None,
        };

        // Create a simple schema without CDF metadata columns
        let logical_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("name", DataType::STRING),
        ]));

        let physical_schema = StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("name", DataType::STRING),
        ]);

        // Empty transform spec - no transformations needed
        let transform_spec = vec![];

        let state_info = StateInfo {
            logical_schema,
            physical_schema: physical_schema.clone().into(),
            physical_predicate: PhysicalPredicate::None,
            transform_spec: Some(Arc::new(transform_spec)),
        };

        let result = get_cdf_transform_expr(&scan_file, &state_info, &physical_schema);
        assert!(result.is_ok());

        let expr_opt = result.unwrap();
        assert!(
            expr_opt.is_none(),
            "Expected None for identity transform but got Some(expr)"
        );
    }
}
