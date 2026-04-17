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
/// Operations are applied sequentially: each one validates against and modifies the schema
/// produced by all preceding operations, not the original input schema.
///
/// # Errors
///
/// Returns an error if any operation fails validation. The error message identifies which
/// operation failed and why.
pub(crate) fn apply_schema_operations(
    schema: StructType,
    operations: Vec<SchemaOperation>,
) -> DeltaResult<SchemaEvolutionResult> {
    // IndexMap preserves field insertion order. Keys are lowercased for case-insensitive
    // duplicate detection; StructFields retain their original casing.
    let mut fields: IndexMap<String, StructField> = schema
        .into_fields()
        .map(|f| (f.name().to_lowercase(), f))
        .collect();

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
                    return Err(Error::schema(format!(
                        "Cannot add non-nullable column '{}'. Added columns must be nullable \
                         because existing data files do not contain this column.",
                        field.name()
                    )));
                }
                fields.insert(key, field);
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
    use rstest::rstest;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};

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
        let result = apply_schema_operations(simple_schema(), vec![add_col(name, nullable)]);
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
        let result = apply_schema_operations(simple_schema(), ops).unwrap();
        assert_eq!(result.schema.fields().count(), 4);
    }

    #[test]
    fn add_same_column_twice_in_batch_fails() {
        let ops = vec![add_col("email", true), add_col("email", true)];
        let err = apply_schema_operations(simple_schema(), ops).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn add_column_preserves_existing_field_order() {
        let result =
            apply_schema_operations(simple_schema(), vec![add_col("email", true)]).unwrap();
        let names: Vec<&String> = result.schema.fields().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "name", "email"]);
    }
}
