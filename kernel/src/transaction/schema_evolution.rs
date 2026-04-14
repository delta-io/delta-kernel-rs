//! Schema evolution operations for ALTER TABLE.
//!
//! This module defines the [`SchemaOperation`] enum and the [`apply_schema_operations`] function
//! that validates and applies schema changes to produce an evolved schema.

use indexmap::IndexMap;

use crate::error::Error;
use crate::schema::{SchemaRef, StructField, StructType};
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
    use crate::schema::{DataType, StructField, StructType};

    fn base_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap()
    }

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
}
