//! Schema evolution operations for ALTER TABLE.
//!
//! This module defines the [`SchemaOperation`] enum and the [`apply_schema_operations`] function
//! that validates and applies schema changes to produce an evolved schema.

use indexmap::IndexMap;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
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

    /// Change a column's nullability from NOT NULL to nullable.
    SetNullable { path: ColumnName },
}

// === Schema tree manipulation helpers ===

/// Helper to modify a nested column. For each component in `remaining`, locates the matching
/// field (case-insensitive), then descends into the next nested struct. At the leaf, applies
/// `modifier` to produce the replacement field. Returns the rebuilt field list with the
/// modification applied. `full_path` is used only in error messages.
///
/// Unlike [`StructType::walk_column_fields_by`] which is iterative and read-only, this must be
/// recursive so it can rebuild parent structs bottom-up after modifying the leaf.
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

    if rest.is_empty() {
        let original = fields.remove(idx);
        fields.insert(idx, modifier(original)?);
    } else {
        // Take ownership of the field to avoid cloning inner struct fields
        let mut field = fields.remove(idx);
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

/// The result of applying schema operations.
#[derive(Debug)]
pub(crate) struct SchemaEvolutionResult {
    /// The evolved schema after all operations are applied.
    pub schema: SchemaRef,
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
    operations: &[SchemaOperation],
) -> DeltaResult<SchemaEvolutionResult> {
    // Keys are lowercased for O(1) case-insensitive lookup; StructFields retain original casing.
    let mut fields: IndexMap<String, StructField> = schema
        .into_fields()
        .map(|f| (f.name().to_lowercase(), f))
        .collect();

    for op in operations {
        match op {
            SchemaOperation::AddColumn { field } => {
                let key = field.name().to_lowercase();
                if fields.contains_key(&key) {
                    return Err(Error::generic(format!(
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
                fields.insert(key, field.clone());
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
                            "Column '{path}' is not a struct; cannot traverse into it",
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
        }
    }

    let evolved_schema = StructType::try_new(fields.into_values())?;
    Ok(SchemaEvolutionResult {
        schema: evolved_schema.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::column_name;
    use crate::schema::{DataType, StructField, StructType};

    fn base_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap()
    }

    fn nested_schema() -> StructType {
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
        let fields: Vec<StructField> = base_schema().into_fields().collect();
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
        let fields: Vec<StructField> = nested_schema().into_fields().collect();
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
        let fields: Vec<StructField> = nested_schema().into_fields().collect();
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        // "id" should be unchanged
        let id = result.iter().find(|f| f.name() == "id").unwrap();
        assert!(!id.is_nullable());
        // "zip" sibling should be unchanged
        let addr = result.iter().find(|f| f.name() == "address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("zip").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn modify_nonexistent_field_fails() {
        let fields: Vec<StructField> = base_schema().into_fields().collect();
        let path = vec!["nope".to_string()];
        let err = modify_field_at_path(fields, &path, &path, &|f| Ok(f)).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn modify_through_non_struct_fails() {
        let fields: Vec<StructField> = base_schema().into_fields().collect();
        let path = vec!["name".to_string(), "inner".to_string()];
        let err = modify_field_at_path(fields, &path, &path, &|f| Ok(f)).unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn modify_case_insensitive_lookup() {
        let fields: Vec<StructField> = base_schema().into_fields().collect();
        let path = vec!["ID".to_string()];
        let result = modify_field_at_path(fields, &path, &path, &|mut f| {
            f.nullable = true;
            Ok(f)
        })
        .unwrap();
        let id = result.iter().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    // === apply_schema_operations tests ===

    #[test]
    fn add_nullable_column_succeeds() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("email", DataType::STRING),
        }];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("email").is_some());
    }

    #[test]
    fn add_duplicate_column_fails() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("name", DataType::STRING),
        }];
        let err = apply_schema_operations(base_schema(), &ops).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn add_non_nullable_column_fails() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::not_null("age", DataType::INTEGER),
        }];
        let err = apply_schema_operations(base_schema(), &ops).unwrap_err();
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn add_duplicate_column_case_insensitive_fails() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("Name", DataType::STRING),
        }];
        let err = apply_schema_operations(base_schema(), &ops).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn add_multiple_columns_succeeds() {
        let ops = vec![
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                field: StructField::nullable("age", DataType::INTEGER),
            },
        ];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        assert_eq!(result.schema.fields().count(), 4);
    }

    #[test]
    fn add_column_preserves_existing_field_order() {
        let ops = vec![SchemaOperation::AddColumn {
            field: StructField::nullable("email", DataType::STRING),
        }];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        let names: Vec<&String> = result.schema.fields().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "name", "email"]);
    }

    // === apply_schema_operations: SetNullable tests ===

    #[test]
    fn set_nullable_on_required_field() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("id"),
        }];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        let id_field = result.schema.field("id").unwrap();
        assert!(id_field.is_nullable());
    }

    #[test]
    fn set_nullable_already_nullable_is_noop() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("name"),
        }];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        let name_field = result.schema.field("name").unwrap();
        assert!(name_field.is_nullable());
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
        let result = apply_schema_operations(schema, &ops).unwrap();
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
    fn chain_add_and_set_nullable() {
        let ops = vec![
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::SetNullable {
                path: column_name!("id"),
            },
        ];
        let result = apply_schema_operations(base_schema(), &ops).unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn set_nullable_nonexistent_column_fails() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("nonexistent"),
        }];
        let err = apply_schema_operations(base_schema(), &ops).unwrap_err();
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
        let result = apply_schema_operations(schema, &ops).unwrap();
        let addr = result.schema.field("address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("city").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn set_nullable_through_non_struct_fails() {
        let ops = vec![SchemaOperation::SetNullable {
            path: column_name!("name.inner"),
        }];
        let err = apply_schema_operations(base_schema(), &ops).unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }
}
