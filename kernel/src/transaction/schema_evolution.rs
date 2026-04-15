//! Schema evolution operations for ALTER TABLE.
//!
//! This module defines the [`SchemaOperation`] enum and the [`apply_schema_operations`] function
//! that validates and applies schema changes to produce an evolved schema.

use indexmap::IndexMap;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_features::{assign_field_column_mapping, ColumnMappingMode};
use crate::DeltaResult;

/// A schema evolution operation to be applied during ALTER TABLE.
///
/// Operations are validated and applied in order during
/// [`apply_schema_operations`]. Each operation sees the schema state after all prior operations
/// have been applied.
#[derive(Debug, Clone)]
pub(crate) enum SchemaOperation {
    /// Add a top-level column.
    AddColumn { field: StructField },

    /// Drop a column by path. Supports nested columns (e.g. `column_name!("address.city")`).
    /// Requires column mapping.
    DropColumn { path: ColumnName },

    /// Rename a column by path. Supports nested columns. Requires column mapping.
    RenameColumn { path: ColumnName, new_name: String },

    /// Change a column's nullability from NOT NULL to nullable.
    SetNullable { path: ColumnName },
}

// === Schema tree manipulation helpers ===

// Modify a field at the given path using the provided function. For nested paths, rebuilds
// parent structs along the way since `StructType.fields` is private. `full_path` is threaded
// through unchanged for error messages; `remaining` drives the traversal.
fn modify_field_at_path(
    mut fields: Vec<StructField>,
    full_path: &[String],
    remaining: &[String],
    modifier: &dyn Fn(StructField) -> DeltaResult<StructField>,
) -> DeltaResult<Vec<StructField>> {
    let (first, rest) = remaining
        .split_first()
        .ok_or_else(|| Error::internal_error("modify_field_at_path called with empty path"))?;

    // Delta column names are case-insensitive
    let idx = fields
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(first))
        .ok_or_else(|| {
            Error::generic(format!(
                "Column '{}' does not exist",
                ColumnName::new(full_path.iter())
            ))
        })?;

    // Take ownership of the field to avoid cloning
    let mut field = fields.remove(idx);

    if rest.is_empty() {
        fields.insert(idx, modifier(field)?);
    } else {
        let DataType::Struct(inner) = field.data_type else {
            return Err(Error::generic(format!(
                "Column '{}' is not a struct; cannot traverse into it",
                ColumnName::new(full_path.iter())
            )));
        };
        // into_fields() gives owned fields -- no cloning
        let new_inner_fields =
            modify_field_at_path(inner.into_fields().collect(), full_path, rest, modifier)?;
        field.data_type = DataType::Struct(Box::new(StructType::try_new(new_inner_fields)?));
        fields.insert(idx, field);
    }
    Ok(fields)
}

/// Remove a field at the given path from a list of fields. For nested paths, rebuilds parent
/// structs along the way. Returns the updated fields.
fn remove_field_at_path(fields: &[StructField], path: &[String]) -> DeltaResult<Vec<StructField>> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| Error::internal_error("remove_field_at_path called with empty path"))?;

    if rest.is_empty() {
        let pos = fields
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(first))
            .ok_or_else(|| {
                Error::generic(format!(
                    "Column '{}' does not exist",
                    ColumnName::new(path.iter())
                ))
            })?;
        if fields.len() <= 1 {
            return Err(Error::generic(format!(
                "Cannot drop '{}': it is the last field at its level",
                ColumnName::new(path.iter())
            )));
        }
        let mut result: Vec<StructField> = fields.to_vec();
        result.remove(pos);
        Ok(result)
    } else {
        let mut result: Vec<StructField> = fields.to_vec();
        let field = result
            .iter_mut()
            .find(|f| f.name().eq_ignore_ascii_case(first))
            .ok_or_else(|| {
                Error::generic(format!(
                    "Column '{}' does not exist",
                    ColumnName::new(path.iter())
                ))
            })?;
        let inner = match &field.data_type {
            DataType::Struct(s) => s,
            _ => {
                return Err(Error::generic(format!(
                    "Column '{}' is not a struct; cannot traverse into it",
                    first
                )));
            }
        };
        let new_inner_fields =
            remove_field_at_path(&inner.fields().cloned().collect::<Vec<_>>(), rest)?;
        let new_inner = StructType::try_new(new_inner_fields)?;
        field.data_type = DataType::Struct(Box::new(new_inner));
        Ok(result)
    }
}

/// Get sibling field names at the same level as the target path, excluding the target itself.
/// Used by rename to check whether the new name conflicts with an existing sibling.
fn sibling_names_at_path(fields: &[StructField], path: &[String]) -> DeltaResult<Vec<String>> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| Error::internal_error("sibling_names_at_path called with empty path"))?;

    if rest.is_empty() {
        Ok(fields
            .iter()
            .filter(|f| !f.name().eq_ignore_ascii_case(first))
            .map(|f| f.name().to_string())
            .collect())
    } else {
        let field = fields
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(first))
            .ok_or_else(|| Error::generic(format!("Column '{}' does not exist", first)))?;
        let inner = match &field.data_type {
            DataType::Struct(s) => s,
            _ => {
                return Err(Error::generic(format!(
                    "Column '{}' is not a struct; cannot traverse into it",
                    first
                )));
            }
        };
        sibling_names_at_path(&inner.fields().cloned().collect::<Vec<_>>(), rest)
    }
}

/// The result of applying schema operations, including any updates needed for column mapping.
#[derive(Debug)]
pub(crate) struct SchemaEvolutionResult {
    /// The evolved schema after all operations are applied.
    pub schema: SchemaRef,
    /// The new max column ID (if column mapping is active and columns were added).
    /// Used to update `delta.columnMapping.maxColumnId` in table properties.
    pub new_max_column_id: Option<i64>,
}

/// Applies a sequence of schema operations to the given schema, returning the evolved schema.
///
/// Each operation is validated against the current schema state (after prior operations have been
/// applied).
///
/// # Errors
///
/// Returns an error if any operation fails validation. The error message identifies which
/// operation failed and why.
pub(crate) fn apply_schema_operations(
    schema: StructType,
    operations: Vec<SchemaOperation>,
    column_mapping_mode: ColumnMappingMode,
    current_max_column_id: Option<i64>,
    partition_columns: &[String],
    clustering_columns: Option<&[ColumnName]>,
) -> DeltaResult<SchemaEvolutionResult> {
    // Defense-in-depth: RenameColumn must be the sole operation (enforced at compile time
    // by the type-state builder, but verify at runtime for direct callers).
    let has_rename = operations
        .iter()
        .any(|op| matches!(op, SchemaOperation::RenameColumn { .. }));
    if has_rename && operations.len() > 1 {
        return Err(Error::generic(
            "RENAME COLUMN cannot be combined with other operations in the same ALTER TABLE",
        ));
    }

    // Keys are lowercased for O(1) case-insensitive lookup; StructFields retain original casing.
    let mut fields: IndexMap<String, StructField> = schema
        .into_fields()
        .map(|f| (f.name().to_lowercase(), f))
        .collect();
    let mut max_id = current_max_column_id;

    for op in operations {
        match op {
            SchemaOperation::AddColumn { field } => {
                let key = field.name().to_lowercase();
                if fields.contains_key(&key) {
                    return Err(Error::schema(format!(
                        "Cannot add column '{}': a column with that name already exists",
                        field.name()
                    )));
                }
                // Validate field is nullable (Delta protocol requires added columns to be
                // nullable so existing data files can return NULL for the new column)
                if !field.is_nullable() {
                    return Err(Error::generic(format!(
                        "Cannot add non-nullable column '{}'. Added columns must be nullable \
                         because existing data files do not contain this column.",
                        field.name()
                    )));
                }
                // If column mapping is enabled, assign column ID and physical name
                let field = if column_mapping_mode != ColumnMappingMode::None {
                    let id = max_id.as_mut().ok_or_else(|| {
                        Error::generic(
                            "Column mapping is enabled but delta.columnMapping.maxColumnId \
                             is not set in table properties",
                        )
                    })?;
                    assign_field_column_mapping(&field, id)?
                } else {
                    field
                };
                fields.insert(key, field);
            }
            SchemaOperation::SetNullable { path } => {
                let segments = path.path();
                let key = segments
                    .first()
                    .ok_or_else(|| Error::generic("Cannot set nullable: empty column path"))?
                    .to_lowercase();
                let field = fields.get_mut(&key).ok_or_else(|| {
                    Error::generic(format!(
                        "Cannot set nullable on column '{path}': column does not exist"
                    ))
                })?;
                if segments.len() == 1 {
                    field.nullable = true;
                } else {
                    // Take ownership of data_type to avoid cloning inner fields
                    let data_type = std::mem::replace(&mut field.data_type, DataType::BOOLEAN);
                    let DataType::Struct(inner) = data_type else {
                        field.data_type = data_type;
                        return Err(Error::generic(format!(
                            "Column '{}' is not a struct; cannot traverse into it while resolving '{path}'", segments[0],
                        )));
                    };
                    let modified_inner = modify_field_at_path(
                        inner.into_fields().collect(),
                        segments,
                        &segments[1..],
                        &|mut f| {
                            f.nullable = true;
                            Ok(f)
                        },
                    )?;
                    field.data_type =
                        DataType::Struct(Box::new(StructType::try_new(modified_inner)?));
                }
            }
            SchemaOperation::DropColumn { path } => {
                if column_mapping_mode == ColumnMappingMode::None {
                    return Err(Error::generic(
                        "DROP COLUMN requires column mapping to be enabled \
                         (delta.columnMapping.mode = 'name' or 'id')",
                    ));
                }
                // Partition columns are always top-level
                if path.path().len() == 1
                    && partition_columns.iter().any(|pc| pc == &path.path()[0])
                {
                    return Err(Error::generic(format!(
                        "Cannot drop column '{path}': it is a partition column"
                    )));
                }
                if let Some(clustering_cols) = clustering_columns {
                    if clustering_cols.iter().any(|cc| cc == &path) {
                        return Err(Error::generic(format!(
                            "Cannot drop column '{path}': it is a clustering column"
                        )));
                    }
                }
                // remove_field_at_path validates existence, navigates nested structs,
                // and rejects dropping the last field at any level.
                let current_fields: Vec<StructField> = fields.into_values().collect();
                let updated = remove_field_at_path(&current_fields, path.path())?;
                fields = updated
                    .into_iter()
                    .map(|f| (f.name().to_lowercase(), f))
                    .collect();
            }
            SchemaOperation::RenameColumn { path, new_name } => {
                if new_name.is_empty() {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{path}': new name must not be empty"
                    )));
                }
                if column_mapping_mode == ColumnMappingMode::None {
                    return Err(Error::generic(
                        "RENAME COLUMN requires column mapping to be enabled \
                         (delta.columnMapping.mode = 'name' or 'id')",
                    ));
                }
                // Partition columns are always top-level
                if path.path().len() == 1
                    && partition_columns.iter().any(|pc| pc == &path.path()[0])
                {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{path}': it is a partition column"
                    )));
                }
                if let Some(clustering_cols) = clustering_columns {
                    if clustering_cols.iter().any(|cc| cc == &path) {
                        return Err(Error::generic(format!(
                            "Cannot rename column '{path}': it is a clustering column"
                        )));
                    }
                }
                // Check new name doesn't conflict with siblings (case-insensitive)
                let current_fields: Vec<StructField> = fields.into_values().collect();
                let siblings = sibling_names_at_path(&current_fields, path.path())?;
                if siblings.iter().any(|s| s.eq_ignore_ascii_case(&new_name)) {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{path}' to '{new_name}': \
                         a column with that name already exists"
                    )));
                }
                let segments = path.path();
                let updated = modify_field_at_path(current_fields, segments, segments, &|field| {
                    Ok(field.with_name(&new_name))
                })?;
                fields = updated
                    .into_iter()
                    .map(|f| (f.name().to_lowercase(), f))
                    .collect();
            }
        }
    }

    let evolved_schema = StructType::try_new(fields.into_values())?;
    // If columns were added with column mapping, max_id was incremented and needs to be
    // persisted back to table properties. None means no update needed.
    let new_max_column_id = match (current_max_column_id, max_id) {
        (Some(original), Some(updated)) if updated > original => Some(updated),
        (Some(original), Some(updated)) if updated < original => {
            return Err(Error::internal_error(format!(
                "max column ID should only increase: {original} -> {updated}"
            )));
        }
        _ => None,
    };
    Ok(SchemaEvolutionResult {
        schema: evolved_schema.into(),
        new_max_column_id,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::expressions::column_name;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};

    fn simple_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap()
    }

    fn add_col(name: &str, nullable: bool) -> SchemaOperation {
        let field = if nullable {
            StructField::nullable(name, DataType::STRING)
        } else {
            StructField::not_null(name, DataType::STRING)
        };
        SchemaOperation::AddColumn { field }
    }

    fn two_field_nested_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![
                    StructField::not_null("city", DataType::STRING),
                    StructField::nullable("zip", DataType::STRING),
                ])
                .unwrap(),
            ),
        ])
        .unwrap()
    }

    // === modify_field_at_path tests ===

    #[test]
    fn modify_top_level_field() {
        let fields: Vec<StructField> = simple_schema().into_fields().collect();
        let path = vec!["id".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        let id = result.iter().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    #[test]
    fn modify_nested_field() {
        let fields: Vec<StructField> = two_field_nested_schema().into_fields().collect();
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        let addr = result.iter().find(|f| f.name() == "address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("city").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn modify_preserves_sibling_fields() {
        let fields: Vec<StructField> = two_field_nested_schema().into_fields().collect();
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        let id = result.iter().find(|f| f.name() == "id").unwrap();
        assert!(!id.is_nullable());
        let addr = result.iter().find(|f| f.name() == "address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("zip").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn modify_nonexistent_field_fails() {
        let fields: Vec<StructField> = simple_schema().into_fields().collect();
        let path = vec!["nope".to_string()];
        let err = modify_field_at_path(fields, &path, &path, &|f| Ok(f)).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn modify_through_non_struct_fails() {
        let fields: Vec<StructField> = simple_schema().into_fields().collect();
        let path = vec!["name".to_string(), "inner".to_string()];
        let err = modify_field_at_path(fields, &path, &path, &|f| Ok(f)).unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn modify_case_insensitive_lookup() {
        let fields: Vec<StructField> = simple_schema().into_fields().collect();
        let path = vec!["ID".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        let id = result.iter().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    // === apply_schema_operations: AddColumn tests ===

    // Shorthand for apply_schema_operations with no column mapping and no partition/clustering.
    fn apply_ops(
        schema: StructType,
        ops: Vec<SchemaOperation>,
    ) -> DeltaResult<SchemaEvolutionResult> {
        apply_schema_operations(schema, ops, ColumnMappingMode::None, None, &[], None)
    }

    #[rstest]
    #[case::new_column("email", true, 3)]
    #[case::duplicate_exact("name", true, 0)]
    #[case::duplicate_case_insensitive("Name", true, 0)]
    #[case::non_nullable("age", false, 0)]
    fn add_single_column(
        #[case] name: &str,
        #[case] nullable: bool,
        #[case] expected_count: usize,
    ) {
        let result = apply_ops(simple_schema(), vec![add_col(name, nullable)]);
        if expected_count > 0 {
            let schema = result.unwrap().schema;
            assert_eq!(schema.fields().count(), expected_count);
            assert!(schema.field(name).is_some());
        } else if !nullable {
            assert!(result.unwrap_err().to_string().contains("non-nullable"));
        } else {
            assert!(result.unwrap_err().to_string().contains("already exists"));
        }
    }

    #[test]
    fn add_multiple_columns_succeeds() {
        let ops = vec![add_col("email", true), add_col("age", true)];
        let result = apply_ops(simple_schema(), ops).unwrap();
        assert_eq!(result.schema.fields().count(), 4);
    }

    #[test]
    fn add_same_column_twice_in_batch_fails() {
        let ops = vec![add_col("email", true), add_col("email", true)];
        let err = apply_ops(simple_schema(), ops).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn add_column_preserves_existing_field_order() {
        let result = apply_ops(simple_schema(), vec![add_col("email", true)]).unwrap();
        let names: Vec<&String> = result.schema.fields().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "name", "email"]);
    }

    // === apply_schema_operations: SetNullable tests ===

    #[test]
    fn set_nullable_on_required_field() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("id"),
        }];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap();
        let id_field = result.schema.field("id").unwrap();
        assert!(id_field.is_nullable());
    }

    #[test]
    fn set_nullable_already_nullable_is_noop() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("name"),
        }];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap();
        let name_field = result.schema.field("name").unwrap();
        assert!(name_field.is_nullable());
    }

    #[test]
    fn set_nullable_nonexistent_column_fails() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("nonexistent"),
        }];
        let err = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn set_nullable_nested_field() {
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![StructField::not_null("city", DataType::STRING)]).unwrap(),
            ),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("address.city"),
        }];
        let result =
            apply_schema_operations(schema, ops, ColumnMappingMode::None, None, &[], None).unwrap();
        let addr = result.schema.field("address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => {
                let city = s.field("city").unwrap();
                assert!(city.is_nullable());
            }
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn set_nullable_deeply_nested_field() {
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![StructField::nullable(
                    "location",
                    StructType::try_new(vec![StructField::not_null("zipcode", DataType::STRING)])
                        .unwrap(),
                )])
                .unwrap(),
            ),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("address.location.zipcode"),
        }];
        let result =
            apply_schema_operations(schema, ops, ColumnMappingMode::None, None, &[], None).unwrap();
        let addr = result.schema.field("address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => match s.field("location").unwrap().data_type() {
                DataType::Struct(loc) => {
                    let zip = loc.field("zipcode").unwrap();
                    assert!(zip.is_nullable());
                }
                other => panic!("Expected Struct, got: {other:?}"),
            },
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn set_nullable_through_non_struct_fails() {
        // "name" is STRING, not a struct -- can't traverse into it
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("name.inner"),
        }];
        let err = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn chain_add_and_set_nullable() {
        let ops = vec![
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::SetNullable {
                path: column_name!("id"),
            },
        ];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn add_column_with_column_mapping_assigns_id_and_physical_name() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("email", DataType::STRING),
        }];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap();
        let email_field = result.schema.field("email").unwrap();

        let cm_id = email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .expect("email should have column mapping ID");
        assert_eq!(cm_id, &MetadataValue::Number(3));

        let cm_name = email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .expect("email should have column mapping physical name");
        match cm_name {
            MetadataValue::String(s) => assert!(
                s.starts_with("col-"),
                "Expected UUID physical name, got: {s}"
            ),
            other => panic!("Expected string physical name, got: {other:?}"),
        }

        assert_eq!(result.new_max_column_id, Some(3));
    }

    #[test]
    fn add_column_with_column_mapping_id_mode() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("age", DataType::INTEGER),
        }];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::Id,
            Some(5),
            &[],
            None,
        )
        .unwrap();
        let age_field = result.schema.field("age").unwrap();

        assert!(age_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .is_some());
        assert!(age_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .is_some());
        assert_eq!(result.new_max_column_id, Some(6));
    }

    #[test]
    fn add_column_without_max_column_id_fails_when_mapping_enabled() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("email", DataType::STRING),
        }];
        let err = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::Name,
            None,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("maxColumnId"));
    }

    #[test]
    fn drop_column_without_column_mapping_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("name"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("column mapping"));
    }

    #[test]
    fn drop_column_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("name"),
        }];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap();
        assert_eq!(result.schema.fields().count(), 1);
        assert!(result.schema.field("name").is_none());
        assert!(result.schema.field("id").is_some());
    }

    #[test]
    fn drop_nonexistent_column_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("nonexistent"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn drop_partition_column_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("name"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &["name".to_string()],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("partition column"));
    }

    #[test]
    fn drop_last_column_fails() {
        let single_col_schema =
            StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("only"),
        }];
        let err = apply_schema_operations(
            single_col_schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(1),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("last field"));
    }

    #[test]
    fn drop_last_nested_field_fails() {
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![StructField::nullable("street", DataType::STRING)])
                    .unwrap(),
            ),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("address.street"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(3),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("last field"));
    }

    #[test]
    fn chain_add_drop_set_nullable() {
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ])
        .unwrap();
        let ops = vec![
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::DropColumn {
                path: column_name!("age"),
            },
            SchemaOperation::SetNullable {
                path: column_name!("id"),
            },
        ];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(3),
            &[],
            None,
        )
        .unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("email").is_some());
        assert!(result.schema.field("age").is_none());
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn drop_then_readd_same_column_name() {
        let schema = simple_schema();
        let ops = vec![
            SchemaOperation::DropColumn {
                path: column_name!("name"),
            },
            SchemaOperation::AddColumn {
                field: StructField::nullable("name", DataType::INTEGER),
            },
        ];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap();
        assert_eq!(result.schema.fields().count(), 2);
        let name_field = result.schema.field("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::INTEGER);
    }

    #[test]
    fn drop_clustering_column_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("name"),
        }];
        let clustering = vec![column_name!("name")];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            Some(&clustering),
        )
        .unwrap_err();
        assert!(err.to_string().contains("clustering column"));
    }

    fn nested_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![
                    StructField::nullable("street", DataType::STRING),
                    StructField::nullable("city", DataType::STRING),
                    StructField::nullable("zip", DataType::STRING),
                ])
                .unwrap(),
            ),
        ])
        .unwrap()
    }

    #[test]
    fn drop_nested_column_succeeds() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("address.city"),
        }];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(4),
            &[],
            None,
        )
        .unwrap();
        // Top level still has 2 fields
        assert_eq!(result.schema.fields().count(), 2);
        // address struct should have 2 fields (street, zip) -- city removed
        let addr = result.schema.field("address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => {
                assert_eq!(s.fields().count(), 2);
                assert!(s.field("city").is_none());
                assert!(s.field("street").is_some());
                assert!(s.field("zip").is_some());
            }
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn drop_nested_nonexistent_column_fails() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("address.country"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(4),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn drop_nested_through_non_struct_fails() {
        let schema = simple_schema();
        // "name" is a STRING, not a struct -- can't traverse into it
        let ops = vec![SchemaOperation::DropColumn {
            path: column_name!("name.inner"),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn rename_column_without_column_mapping_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::None,
            None,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("column mapping"));
    }

    #[test]
    fn rename_column_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap();
        assert!(result.schema.field("full_name").is_some());
        assert!(result.schema.field("name").is_none());
        assert_eq!(result.schema.fields().count(), 2);
    }

    #[test]
    fn rename_nonexistent_column_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("nonexistent"),
            new_name: "new_name".to_string(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn rename_to_existing_name_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "id".to_string(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn rename_empty_name_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: String::new(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn rename_partition_column_fails() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &["name".to_string()],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("partition column"));
    }

    #[test]
    fn rename_clustering_column_fails() {
        let clustering = vec![column_name!("name")];
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            Some(&clustering),
        )
        .unwrap_err();
        assert!(err.to_string().contains("clustering column"));
    }

    #[test]
    fn rename_nested_column_succeeds() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("address.city"),
            new_name: "town".to_string(),
        }];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(4),
            &[],
            None,
        )
        .unwrap();
        let addr = result.schema.field("address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => {
                assert!(s.field("town").is_some());
                assert!(s.field("city").is_none());
                assert_eq!(s.fields().count(), 3); // street, town, zip
            }
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn rename_nested_to_existing_sibling_fails() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("address.city"),
            new_name: "street".to_string(), // "street" already exists in address
        }];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(4),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn rename_case_change_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: column_name!("name"),
            new_name: "Name".to_string(),
        }];
        let result = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap();
        assert!(result.schema.field("Name").is_some());
        assert!(result.schema.field("name").is_none());
    }

    #[test]
    fn rename_combined_with_other_ops_fails_at_runtime() {
        let schema = simple_schema();
        let ops = vec![
            SchemaOperation::RenameColumn {
                path: column_name!("name"),
                new_name: "full_name".to_string(),
            },
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
        ];
        let err = apply_schema_operations(
            schema.clone(),
            ops,
            ColumnMappingMode::Name,
            Some(2),
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be combined"));
    }
}
