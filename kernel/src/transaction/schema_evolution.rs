//! Schema evolution operations for ALTER TABLE.
//!
//! This module defines the [`SchemaOperation`] enum and the [`apply_schema_operations`] function
//! that validates and applies schema changes to produce an evolved schema.

use std::cmp::Ordering;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    find_max_column_id_in_schema, schema_has_column_mapping_metadata,
    strip_stray_column_mapping_metadata, try_assign_flat_column_mapping_info,
    validate_column_mapping_id, ColumnMappingMode,
};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::utils::FoldWithOption as _;
use crate::DeltaResult;

/// One segment in a path to a nested schema field.
#[internal_api]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathSegment {
    Field(String),
    ArrayElement,
    MapKey,
    MapValue,
}

/// A schema evolution operation to be applied to a table.
#[non_exhaustive]
#[derive(Debug, Clone)]
#[internal_api]
pub(crate) enum SchemaOperation {
    /// Add a column at an explicit schema path.
    ///
    /// The path identifies the struct that will contain `field`. An empty path adds `field` to the
    /// table's root schema.
    AddColumn {
        path: Vec<PathSegment>,
        field: StructField,
    },

    /// Change a column's nullability from NOT NULL to nullable.
    SetNullable { column: ColumnName },
}

fn add_field(parent: &mut StructType, field: StructField) -> DeltaResult<()> {
    let lowered = field.name().to_lowercase();
    if parent
        .fields()
        .any(|existing| existing.name().to_lowercase() == lowered)
    {
        return Err(Error::schema(format!(
            "Cannot add column '{}': a column with that name already exists",
            field.name()
        )));
    }
    parent.field_map_mut().insert(field.name().clone(), field);
    Ok(())
}

fn set_field_nullable(field: &mut StructType, name: &str) -> DeltaResult<()> {
    let field = field
        .field_map_mut()
        .values_mut()
        .find(|field| field.name().to_lowercase() == name.to_lowercase())
        .ok_or_else(|| Error::generic(format!("field '{}' does not exist", name)))?;
    field.nullable = true;
    Ok(())
}

fn to_path_segments(path: &[String]) -> Vec<PathSegment> {
    path.iter()
        .cloned()
        .map(PathSegment::Field)
        .collect::<Vec<_>>()
}

// Resolves `path` to a struct and calls `modifier` on it. Field names are matched
// case-insensitively; explicit segments select array elements and map keys or values.
fn modify_field_at_path(
    data_type: &mut DataType,
    path: &[PathSegment],
    modifier: impl FnOnce(&mut StructType) -> DeltaResult<()>,
) -> DeltaResult<()> {
    let Some((segment, rest)) = path.split_first() else {
        let DataType::Struct(parent) = data_type else {
            return Err(Error::generic("path target is not a struct"));
        };
        return modifier(parent);
    };

    match (segment, data_type) {
        (PathSegment::Field(name), DataType::Struct(parent)) => {
            let lowered = name.to_lowercase();
            let field = parent
                .field_map_mut()
                .values_mut()
                .find(|field| field.name().to_lowercase() == lowered)
                .ok_or_else(|| Error::schema(format!("field '{name}' does not exist")))?;
            modify_field_at_path(&mut field.data_type, rest, modifier)
        }
        (PathSegment::ArrayElement, DataType::Array(array)) => {
            modify_field_at_path(&mut array.element_type, rest, modifier)
        }
        (PathSegment::MapKey, DataType::Map(map)) => {
            modify_field_at_path(&mut map.key_type, rest, modifier)
        }
        (PathSegment::MapValue, DataType::Map(map)) => {
            modify_field_at_path(&mut map.value_type, rest, modifier)
        }
        (segment, data_type) => Err(Error::schema(format!(
            "path segment {segment:?} does not match {data_type}"
        ))),
    }
}

/// The result of applying schema operations.
#[derive(Debug)]
pub(crate) struct SchemaEvolutionResult {
    /// The evolved schema after all operations are applied.
    pub schema: SchemaRef,
    /// If `Some(id)`, `delta.columnMapping.maxColumnId` must be updated to this new value.
    /// `None` means the property should remain unchanged.
    pub new_max_column_id: Option<i64>,
}

/// Applies a sequence of schema operations to the given schema, returning a
/// [`SchemaEvolutionResult`] (see the struct's docs for details).
///
/// Operations are applied sequentially: each one validates against and modifies the schema
/// produced by all preceding operations, not the original input schema.
///
/// # Errors
///
/// Returns an error if any operation fails validation. The error message identifies which
/// operation failed and why.
pub(crate) fn apply_schema_operations(
    mut schema: StructType,
    operations: Vec<SchemaOperation>,
    column_mapping_mode: ColumnMappingMode,
    current_max_column_id: Option<i64>,
) -> DeltaResult<SchemaEvolutionResult> {
    let cm_enabled = column_mapping_mode != ColumnMappingMode::None;

    // Reject a persisted seed that already violates the protocol's 32-bit non-negative bound
    // before it propagates into the allocator. A negative or above-i32::MAX seed almost
    // certainly indicates a non-conforming writer; bail out rather than silently letting the
    // allocator either trip the per-field validator on every preserved sibling or produce
    // out-of-range fresh ids.
    if let Some(seed) = current_max_column_id {
        validate_column_mapping_id(seed).map_err(|e| {
            Error::invalid_protocol(format!(
                "Table property `delta.columnMapping.maxColumnId`: {e}"
            ))
        })?;
    }

    // When column mapping is enabled and the property is set, defensively take the max with
    // the schema's actual max in case a non-conforming writer left the property stale.
    let mut max_id = if cm_enabled {
        current_max_column_id.map(|cfg| cfg.max(find_max_column_id_in_schema(&schema).unwrap_or(0)))
    } else {
        current_max_column_id
    };

    for op in operations {
        let mut root = DataType::from(schema);
        let add_column = match op {
            SchemaOperation::AddColumn { path, field } => Some((path, field)),
            SchemaOperation::SetNullable { column } => {
                let (leaf, parent) = column
                    .path()
                    .split_last()
                    .ok_or_else(|| Error::generic("empty column path"))?;
                let parent = to_path_segments(parent);
                modify_field_at_path(&mut root, &parent, |parent| {
                    set_field_nullable(parent, leaf)
                })
                .map_err(|e| {
                    Error::generic(format!("Cannot set nullable on column '{column}': {e}"))
                })?;
                None
            }
        };

        if let Some((path, field)) = add_column {
            if field.is_metadata_column() {
                return Err(Error::schema(format!(
                    "Cannot add column '{}': metadata columns are not allowed in a table schema",
                    field.name()
                )));
            }
            if !matches!(field.data_type, DataType::Primitive(_)) {
                StructType::ensure_no_metadata_columns_in_field(&field)?;
            }
            if !field.is_nullable() {
                return Err(Error::schema(format!(
                    "Cannot add non-nullable column '{}'. Added columns must be nullable \
                     because existing data files do not contain this column.",
                    field.name()
                )));
            }
            let field = if cm_enabled {
                let id = max_id.as_mut().ok_or_else(|| {
                    Error::invalid_protocol(
                        "Column mapping is enabled but delta.columnMapping.maxColumnId \
                         is not set in table properties",
                    )
                })?;
                try_assign_flat_column_mapping_info(&field, id)?
            } else {
                field
            };
            modify_field_at_path(&mut root, &path, |parent| add_field(parent, field))?;
        }

        let DataType::Struct(updated_schema) = root else {
            return Err(Error::internal_error(
                "schema root changed type during schema evolution",
            ));
        };
        schema = *updated_schema;
    }

    validate_schema(&schema, column_mapping_mode)?;

    // `max_id` is only ever incremented by `try_assign_flat_column_mapping_info`. If it grew,
    // the new value must be persisted; if it went backwards, that's a bug.
    let new_max_column_id = match max_id.cmp(&current_max_column_id) {
        Ordering::Greater => max_id,
        Ordering::Equal => None,
        Ordering::Less => {
            return Err(Error::internal_error(
                "max column ID went backwards during schema evolution",
            ))
        }
    };
    Ok(SchemaEvolutionResult {
        schema: schema.into(),
        new_max_column_id,
    })
}

/// Applies schema changes and validates the resulting table configuration.
///
/// # Errors
///
/// Returns an error when an operation is invalid, the evolved schema violates Delta schema
/// rules, or the evolved metadata is incompatible with the table protocol.
pub(crate) fn evolve_table_config(
    table_config: &TableConfiguration,
    operations: Vec<SchemaOperation>,
) -> DeltaResult<TableConfiguration> {
    let schema = Arc::unwrap_or_clone(table_config.logical_schema());
    let column_mapping_mode = table_config.column_mapping_mode();
    let current_max_column_id = table_config.table_properties().column_mapping_max_column_id;
    let current_has_cm = column_mapping_mode == ColumnMappingMode::None
        && schema_has_column_mapping_metadata(&schema);
    let SchemaEvolutionResult {
        schema: evolved_schema,
        new_max_column_id,
    } = apply_schema_operations(
        schema,
        operations,
        column_mapping_mode,
        current_max_column_id,
    )?;
    let evolved_schema = if column_mapping_mode == ColumnMappingMode::None {
        strip_stray_column_mapping_metadata(current_has_cm, &evolved_schema)
            .map_or(evolved_schema, Arc::new)
    } else {
        evolved_schema
    };
    let evolved_metadata = table_config
        .metadata()
        .clone()
        .with_schema(evolved_schema.clone())?
        .fold_with(new_max_column_id, |metadata, id| {
            metadata.with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string())
        });
    TableConfiguration::try_new_with_schema(table_config, evolved_metadata, evolved_schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;
    use crate::expressions::{column_name, ColumnName};
    use crate::schema::{
        schema, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataColumnSpec, MetadataValue,
        StructField, StructType,
    };

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
        SchemaOperation::AddColumn {
            path: vec![],
            field,
        }
    }

    // Builds a struct column whose nested leaf field has the given name. Used to prove that
    // `validate_schema` (not just the top-level dup check or `StructType::try_new`) is
    // reached from `apply_schema_operations`.
    fn add_struct_with_nested_leaf(name: &str, leaf_name: &str) -> SchemaOperation {
        let inner =
            StructType::try_new(vec![StructField::nullable(leaf_name, DataType::STRING)]).unwrap();
        SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable(name, inner),
        }
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

    fn modify_field_at_path_test_helper(
        schema: StructType,
        path: &[String],
    ) -> DeltaResult<StructType> {
        let (leaf, parent) = path
            .split_last()
            .ok_or_else(|| Error::generic("empty column path"))?;
        let parent = parent
            .iter()
            .cloned()
            .map(PathSegment::Field)
            .collect::<Vec<_>>();
        let mut root = DataType::from(schema);
        modify_field_at_path(&mut root, &parent, |parent| {
            set_field_nullable(parent, leaf)
        })?;
        let DataType::Struct(schema) = root else {
            return Err(Error::internal_error(
                "schema root changed type while testing path modification",
            ));
        };
        Ok(*schema)
    }

    #[test]
    fn modify_top_level_field_sets_nullable() {
        let path = vec!["id".to_string()];
        let result = modify_field_at_path_test_helper(simple_schema(), &path).unwrap();
        let id = result.fields().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    #[test]
    fn modify_nested_field_modifies_only_leaf() {
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path_test_helper(nested_schema(), &path).unwrap();
        let addr = result.fields().find(|f| f.name() == "address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("city").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    /// Modifying one nested leaf (`address.city`) must not touch any other field.
    /// Guards against the recursive rebuild accidentally replacing siblings when it reconstructs
    /// the enclosing struct.
    #[test]
    fn modify_nested_leaf_preserves_other_fields() {
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path_test_helper(nested_schema(), &path).unwrap();
        let id = result.fields().find(|f| f.name() == "id").unwrap();
        assert!(!id.is_nullable());
        let addr = result.fields().find(|f| f.name() == "address").unwrap();
        match addr.data_type() {
            DataType::Struct(s) => assert!(s.field("zip").unwrap().is_nullable()),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn modify_nonexistent_field_fails() {
        let path = vec!["nope".to_string()];
        let err = modify_field_at_path_test_helper(simple_schema(), &path).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    /// A path that descends into a non-struct intermediate field (here: `name.inner`, where
    /// `name` is a STRING, not a struct) must error rather than silently succeed or panic.
    #[test]
    fn modify_through_non_struct_fails() {
        let path = vec!["name".to_string(), "inner".to_string()];
        let err = modify_field_at_path_test_helper(simple_schema(), &path).unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn modify_case_insensitive_lookup_finds_field() {
        let path = vec!["ID".to_string()];
        let result = modify_field_at_path_test_helper(simple_schema(), &path).unwrap();
        let id = result.fields().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    // === apply_schema_operations tests ===

    #[rstest]
    #[case::dup_exact(vec![add_col("name", true)], "already exists")]
    #[case::dup_case_insensitive(vec![add_col("Name", true)], "already exists")]
    #[case::dup_within_batch(
        vec![add_col("email", true), add_col("email", true)],
        "already exists"
    )]
    #[case::non_nullable(vec![add_col("age", false)], "non-nullable")]
    #[case::invalid_parquet_char(vec![add_col("foo,bar", true)], "invalid character")]
    #[case::nested_invalid_parquet_char(
        vec![add_struct_with_nested_leaf("addr", "bad,leaf")],
        "invalid character"
    )]
    #[case::metadata_column(
        vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::create_metadata_column("row_idx", MetadataColumnSpec::RowIndex),
        }],
        "metadata columns are not allowed"
    )]
    #[case::missing_add_parent(
        vec![SchemaOperation::AddColumn {
            path: vec![PathSegment::Field("missing".to_string())],
            field: StructField::nullable("added", DataType::STRING),
        }],
        "does not exist"
    )]
    fn apply_schema_operations_rejects(
        #[case] ops: Vec<SchemaOperation>,
        #[case] error_contains: &str,
    ) {
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::None, None)
            .unwrap_err();
        assert!(err.to_string().contains(error_contains));
    }

    #[rstest]
    #[case::single(vec![add_col("email", true)], &["id", "name", "email"])]
    #[case::at_root(
        vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("email", DataType::STRING),
        }],
        &["id", "name", "email"],
    )]
    #[case::multiple(
        vec![add_col("email", true), add_col("age", true)],
        &["id", "name", "email", "age"]
    )]
    fn apply_schema_operations_succeeds(
        #[case] ops: Vec<SchemaOperation>,
        #[case] expected_names: &[&str],
    ) {
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::None, None).unwrap();
        let actual: Vec<&str> = result.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(&actual, expected_names);
    }

    fn struct_with_existing_field() -> StructType {
        StructType::try_new([StructField::nullable("existing", DataType::STRING)]).unwrap()
    }

    fn nested_struct_at<'a>(mut data_type: &'a DataType, path: &[PathSegment]) -> &'a StructType {
        for segment in path {
            data_type = match (segment, data_type) {
                (PathSegment::ArrayElement, DataType::Array(array)) => array.element_type(),
                (PathSegment::MapKey, DataType::Map(map)) => map.key_type(),
                (PathSegment::MapValue, DataType::Map(map)) => map.value_type(),
                (segment, data_type) => {
                    panic!("path segment {segment:?} does not match {data_type}")
                }
            };
        }
        let DataType::Struct(parent) = data_type else {
            panic!("path does not resolve to a struct");
        };
        parent
    }

    #[rstest]
    #[case::struct_parent(
        DataType::from(struct_with_existing_field()),
        vec![],
    )]
    #[case::array_element(
        DataType::from(ArrayType::new(struct_with_existing_field(), true)),
        vec![PathSegment::ArrayElement],
    )]
    #[case::map_key(
        DataType::from(MapType::new(
            struct_with_existing_field(),
            DataType::INTEGER,
            true,
        )),
        vec![PathSegment::MapKey],
    )]
    #[case::map_value(
        DataType::from(MapType::new(
            DataType::INTEGER,
            struct_with_existing_field(),
            true,
        )),
        vec![PathSegment::MapValue],
    )]
    fn add_column_at_traverses_nested_types(
        #[case] parent_type: DataType,
        #[case] container_path: Vec<PathSegment>,
    ) {
        let schema =
            StructType::try_new([StructField::nullable("container", parent_type)]).unwrap();
        let mut path = vec![PathSegment::Field("container".to_string())];
        path.extend(container_path.iter().cloned());
        let operation = SchemaOperation::AddColumn {
            path,
            field: StructField::nullable("added", DataType::INTEGER),
        };

        let result =
            apply_schema_operations(schema, vec![operation], ColumnMappingMode::None, None)
                .unwrap();
        let container = result.schema.field("container").unwrap();
        let parent = nested_struct_at(container.data_type(), &container_path);

        assert!(parent.field("existing").is_some());
        assert_eq!(
            parent.field("added"),
            Some(&StructField::nullable("added", DataType::INTEGER))
        );
    }

    // === apply_schema_operations: SetNullable tests ===

    fn deeply_nested_required_schema() -> StructType {
        schema! {
            not_null "id": INTEGER,
            nullable "address": {
                nullable "location": {
                    not_null "zipcode": STRING,
                },
            },
        }
    }

    #[rstest]
    #[case::on_required_field(simple_schema(), column_name!("id"))]
    #[case::already_nullable_is_noop(simple_schema(), column_name!("name"))]
    #[case::case_insensitive(simple_schema(), column_name!("ID"))]
    #[case::nested_field(nested_schema(), column_name!("address.city"))]
    #[case::deeply_nested_field(deeply_nested_required_schema(), column_name!("address.location.zipcode"))]
    fn set_nullable_succeeds(#[case] schema: StructType, #[case] column: ColumnName) {
        let ops = vec![SchemaOperation::SetNullable {
            column: column.clone(),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        assert!(result.schema.field_at_path(column.path()).is_nullable());
    }

    #[rstest]
    #[case::nonexistent_column(column_name!("nonexistent"), "does not exist")]
    #[case::through_non_struct(column_name!("name.inner"), "not a struct")]
    #[case::empty_path(ColumnName::new(Vec::<String>::new()), "empty column path")]
    fn set_nullable_fails(#[case] column: ColumnName, #[case] error_contains: &str) {
        let ops = vec![SchemaOperation::SetNullable { column }];
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::None, None)
            .unwrap_err();
        assert!(
            err.to_string().contains(error_contains),
            "expected error to contain '{error_contains}', got: {err}"
        );
    }

    /// Setting a struct itself nullable must not mutate inner fields. Kept separate from the
    /// `set_nullable_succeeds` rstest because it asserts on inner-field preservation.
    #[test]
    fn set_nullable_on_struct_itself_preserves_inner_fields() {
        let schema = StructType::try_new(vec![StructField::not_null(
            "address",
            StructType::try_new(vec![StructField::not_null("city", DataType::STRING)]).unwrap(),
        )])
        .unwrap();
        let ops = vec![SchemaOperation::SetNullable {
            column: column_name!("address"),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        let addr = result.schema.field("address").unwrap();
        assert!(addr.is_nullable(), "struct itself must be nullable");
        match addr.data_type() {
            DataType::Struct(s) => assert!(
                !s.field("city").unwrap().is_nullable(),
                "inner field must remain NOT NULL"
            ),
            other => panic!("Expected Struct, got: {other:?}"),
        }
    }

    #[test]
    fn chain_add_and_set_nullable_applies_both() {
        let ops = vec![
            SchemaOperation::AddColumn {
                path: vec![],
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::SetNullable {
                column: column_name!("id"),
            },
        ];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::None, None).unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("email").is_some());
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn set_nullable_nested_preserves_top_level_order() {
        // SetNullable on a nested field within a middle top-level field must not reorder
        // the top-level IndexMap.
        let schema = StructType::try_new(vec![
            StructField::not_null("alpha", DataType::INTEGER),
            StructField::nullable(
                "beta",
                StructType::try_new(vec![StructField::not_null("nested", DataType::STRING)])
                    .unwrap(),
            ),
            StructField::not_null("gamma", DataType::STRING),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::SetNullable {
            column: column_name!("beta.nested"),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        let names: Vec<&String> = result.schema.fields().map(|f| f.name()).collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    // === Column mapping tests ===

    fn get_cm_id(field: &StructField) -> i64 {
        field
            .column_mapping_id()
            .expect("field should have column mapping ID")
    }

    fn get_physical_name(field: &StructField) -> String {
        match field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .expect("field should have physical name")
        {
            MetadataValue::String(s) => s.clone(),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[rstest]
    #[case::name_mode(ColumnMappingMode::Name, 2, 3)]
    #[case::id_mode(ColumnMappingMode::Id, 5, 6)]
    fn add_column_with_column_mapping_assigns_id_and_physical_name(
        #[case] mode: ColumnMappingMode,
        #[case] current_max: i64,
        #[case] expected_id: i64,
    ) {
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("email", DataType::STRING),
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, mode, Some(current_max)).unwrap();
        let email_field = result.schema.field("email").unwrap();

        assert_eq!(get_cm_id(email_field), expected_id);
        assert!(get_physical_name(email_field).starts_with("col-"));
        assert_eq!(result.new_max_column_id, Some(expected_id));
    }

    #[test]
    fn add_column_without_max_column_id_fails_when_mapping_enabled() {
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("email", DataType::STRING),
        }];
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, None)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidProtocol(_)));
        assert!(err.to_string().contains("maxColumnId"));
    }

    /// Multiple columns added in a single ALTER on a CM table: each must get a distinct ID
    /// (strictly monotone), a distinct physical name, and `new_max_column_id` must advance by
    /// exactly the number of added columns.
    #[test]
    fn add_multiple_columns_with_column_mapping_assigns_unique_ids() {
        let ops = vec![
            SchemaOperation::AddColumn {
                path: vec![],
                field: StructField::nullable("a", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                path: vec![],
                field: StructField::nullable("b", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                path: vec![],
                field: StructField::nullable("c", DataType::STRING),
            },
        ];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(10))
                .unwrap();

        let id_a = get_cm_id(result.schema.field("a").unwrap());
        let id_b = get_cm_id(result.schema.field("b").unwrap());
        let id_c = get_cm_id(result.schema.field("c").unwrap());
        assert_eq!(id_a, 11);
        assert_eq!(id_b, 12);
        assert_eq!(id_c, 13);

        let name_a = get_physical_name(result.schema.field("a").unwrap());
        let name_b = get_physical_name(result.schema.field("b").unwrap());
        let name_c = get_physical_name(result.schema.field("c").unwrap());
        assert_ne!(name_a, name_b);
        assert_ne!(name_b, name_c);
        assert_ne!(name_a, name_c);

        assert_eq!(result.new_max_column_id, Some(13));
    }

    fn struct_of_two_primitives() -> DataType {
        DataType::from(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::STRING),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
        )
    }

    /// Adding a complex column on a CM table: every inner struct field reachable through
    /// Struct/Array/Map recursion must receive a distinct ID greater than the previous max,
    /// and `new_max_column_id` must advance to the largest assigned ID.
    #[rstest]
    #[case::nested_struct(struct_of_two_primitives(), 3)]
    #[case::array_of_primitive(DataType::from(ArrayType::new(DataType::STRING, true)), 1)]
    #[case::map_of_primitives(
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        1
    )]
    #[case::array_of_struct(DataType::from(ArrayType::new(struct_of_two_primitives(), true)), 3)]
    #[case::map_value_is_struct(
        DataType::from(MapType::new(DataType::STRING, struct_of_two_primitives(), true,)),
        3
    )]
    #[case::map_key_is_struct(
        DataType::from(MapType::new(struct_of_two_primitives(), DataType::INTEGER, true,)),
        3
    )]
    fn add_complex_column_with_column_mapping_assigns_ids_to_all_inner_fields(
        #[case] data_type: DataType,
        #[case] expected_id_count: usize,
    ) {
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("col", data_type),
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(10))
                .unwrap();

        let added = result.schema.field("col").unwrap();
        let ids = added.collect_column_mapping_ids();
        let unique: HashSet<_> = ids.iter().copied().collect();

        assert_eq!(ids.len(), expected_id_count, "expected ID count mismatch");
        assert_eq!(unique.len(), ids.len(), "all assigned IDs must be distinct");
        assert!(
            ids.iter().all(|&id| id > 10),
            "all assigned IDs must exceed previous max"
        );
        assert_eq!(
            result.new_max_column_id,
            ids.iter().max().copied(),
            "new_max_column_id must equal the largest assigned ID",
        );
    }

    fn field_with_id_only(name: &str, ty: DataType, id: i64) -> StructField {
        let mut f = StructField::nullable(name, ty);
        f.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(id),
        );
        f
    }

    /// CM-enabled: a connector may pre-populate `delta.columnMapping.id` on the new column,
    /// even at nested depth. The id is preserved and the missing `physicalName` is filled in,
    /// matching delta-spark's `assignColumnIdAndPhysicalName`.
    #[rstest]
    #[case::top_level(field_with_id_only("tainted", DataType::STRING, 99))]
    #[case::nested_in_struct(StructField::nullable(
        "outer",
        StructType::try_new(vec![field_with_id_only("inner", DataType::STRING, 99)]).unwrap(),
    ))]
    fn add_column_with_preexisting_cm_metadata_is_preserved_under_cm(#[case] field: StructField) {
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field,
        }];
        let result = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::Name,
            Some(2), // current_max_column_id
        )
        .unwrap();

        // The preserved id=99 must surface as the schema's max, even though the persisted
        // maxColumnId was only 2 going in.
        assert_eq!(find_max_column_id_in_schema(&result.schema), Some(99));
        assert_eq!(result.new_max_column_id, Some(99));
    }

    /// CM-enabled, only `physicalName` provided: id is allocated as `max_id + 1` and the
    /// physical name is preserved.
    #[test]
    fn add_column_with_only_physical_name_allocates_id() {
        let mut field = StructField::nullable("named", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("user-supplied-name".to_string()),
        );
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field,
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(7))
                .unwrap();
        let added = result.schema.field("named").unwrap();
        assert_eq!(get_cm_id(added), 8);
        assert_eq!(
            added.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String("user-supplied-name".to_string()))
        );
        assert_eq!(result.new_max_column_id, Some(8));
    }

    /// A persisted `delta.columnMapping.maxColumnId` above the protocol's 32-bit cap is
    /// rejected before evolution starts, so the allocator never sees an out-of-range seed.
    /// `i64::MAX` and `i32::MAX + 1` both cross the bound; the negative case ensures the
    /// `0..=MAX_COLUMN_MAPPING_ID` range is closed on the low end too.
    #[rstest]
    #[case::above_protocol_max(i32::MAX as i64 + 1)]
    #[case::i64_max(i64::MAX)]
    #[case::negative(-1)]
    fn alter_with_out_of_range_persisted_max_column_id_is_rejected(#[case] seed: i64) {
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("anything", DataType::INTEGER),
        }];
        let err =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(seed))
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("Invalid column mapping id")
                && err.contains("Table property `delta.columnMapping.maxColumnId`")
                && err.contains(&seed.to_string()),
            "expected canonical out-of-range rejection naming the seed location and value, \
             got: {err}",
        );
    }

    /// CM-enabled, wrong-typed `delta.columnMapping.id` annotation: error.
    #[test]
    fn add_column_with_wrong_typed_id_is_rejected() {
        let mut field = StructField::nullable("bad", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::String("not-a-number".to_string()),
        );
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field,
        }];
        let err = apply_schema_operations(
            simple_schema(),
            ops,
            ColumnMappingMode::Name,
            Some(2), // current_max_column_id
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("non-numeric") && err.contains("delta.columnMapping.id"),
            "error should name the wrong-typed id annotation, got: {err}"
        );
    }

    /// If the persisted `maxColumnId` is stale (smaller than the actual max ID present in
    /// the schema), the defensive seed rebases on the schema's max so a newly added column
    /// cannot collide with an existing field's ID. Matches delta-spark's `findMaxColumnId`.
    #[test]
    fn stale_max_column_id_is_self_healed_by_schema_walk() {
        let mut existing = StructField::nullable("existing", DataType::STRING);
        existing.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(42),
        );
        let schema = StructType::try_new(vec![existing]).unwrap();
        let ops = vec![SchemaOperation::AddColumn {
            path: vec![],
            field: StructField::nullable("new", DataType::STRING),
        }];
        // Persisted maxColumnId is stale at 5, but the schema actually contains id=42.
        let result =
            apply_schema_operations(schema, ops, ColumnMappingMode::Name, Some(5)).unwrap();
        let new_id = get_cm_id(result.schema.field("new").unwrap());
        assert_eq!(
            new_id, 43,
            "new id must follow schema max (42), not stale property (5)"
        );
        assert_eq!(result.new_max_column_id, Some(43));
    }
}
