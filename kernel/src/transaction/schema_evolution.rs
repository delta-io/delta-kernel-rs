//! This module defines the [`SchemaOperation`] enum and functions that validate and
//! apply schema changes to produce an evolved schema.

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

/// A schema evolution operation to be applied to a table.
///
/// Operations are validated and applied in order during
/// [`apply_schema_operations`]. Each operation sees the schema state after all prior operations
/// have been applied.
#[non_exhaustive]
#[derive(Debug, Clone)]
#[internal_api]
pub(crate) enum SchemaOperation {
    /// Add a column or nested field to the table schema.
    ///
    /// The path identifies the struct that will contain `field`. An empty path adds `field` to the
    /// table's root schema.
    AddColumn {
        /// Path to the receiving struct; an empty path selects the root schema.
        path: ColumnName,
        /// The nullable, non-metadata field to add.
        field: StructField,
    },
    /// Change a column's nullability to nullable.
    SetNullable { column: ColumnName },
}

impl SchemaOperation {
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn add_column(field: StructField, path: ColumnName) -> SchemaOperation {
        SchemaOperation::AddColumn { path, field }
    }

    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn set_nullable(column: ColumnName) -> SchemaOperation {
        SchemaOperation::SetNullable { column }
    }
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
        .ok_or_else(|| Error::schema(format!("field '{}' does not exist", name)))?;
    field.nullable = true;
    Ok(())
}

/// Resolves `path` to a struct and applies `modifier`.
///
/// Field names are matched case-insensitively. Explicit path segments traverse arrays and maps.
fn modify_field_at_path(
    data_type: &mut DataType,
    path: &[String],
    modifier: impl FnOnce(&mut StructType) -> DeltaResult<()>,
) -> DeltaResult<()> {
    let Some((segment, rest)) = path.split_first() else {
        let DataType::Struct(parent) = data_type else {
            return Err(Error::schema("path target is not a struct"));
        };
        return modifier(parent);
    };
    match (segment.as_str(), data_type) {
        ("element", DataType::Array(array)) => {
            modify_field_at_path(&mut array.element_type, rest, modifier)
        }
        ("key", DataType::Map(map)) => modify_field_at_path(&mut map.key_type, rest, modifier),
        ("value", DataType::Map(map)) => modify_field_at_path(&mut map.value_type, rest, modifier),
        (name, DataType::Struct(parent)) => {
            let lowered = name.to_lowercase();
            let field = parent
                .field_map_mut()
                .values_mut()
                .find(|field| field.name().to_lowercase() == lowered)
                .ok_or_else(|| Error::schema(format!("field '{name}' does not exist")))?;
            modify_field_at_path(&mut field.data_type, rest, modifier)
        }
        (segment, data_type) => Err(Error::schema(format!(
            "path segment {segment:?} does not match {data_type}"
        ))),
    }
}

/// The result of applying schema operations.
#[derive(Debug)]
pub(super) struct SchemaEvolutionResult {
    /// The evolved schema after all operations are applied.
    pub(super) schema: SchemaRef,
    /// The new `delta.columnMapping.maxColumnId`, if it increased.
    pub(super) new_max_column_id: Option<i64>,
}

/// Applies schema operations sequentially, validating each operation against prior changes.
///
/// # Errors
///
/// Returns an error if an operation or the resulting schema is invalid.
pub(super) fn apply_schema_operations(
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

    // Use the schema's maximum when the persisted property is stale.
    let mut max_id = if cm_enabled {
        current_max_column_id.map(|cfg| cfg.max(find_max_column_id_in_schema(&schema).unwrap_or(0)))
    } else {
        current_max_column_id
    };

    for op in operations {
        let mut root = DataType::from(schema);
        match op {
            SchemaOperation::AddColumn { path, field } => {
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
                // TODO: Reject caller-supplied column mapping IDs and physical names recursively.
                // Preserving them can reuse a dropped column's historical identity.
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
            SchemaOperation::SetNullable { column } => {
                let (leaf, parent) = column
                    .path()
                    .split_last()
                    .ok_or_else(|| Error::generic("empty column path"))?;
                modify_field_at_path(&mut root, parent, |parent| set_field_nullable(parent, leaf))
                    .map_err(|e| {
                        Error::generic(format!("Cannot set nullable on column '{column}': {e}"))
                    })?;
            }
        }

        let DataType::Struct(updated_schema) = root else {
            return Err(Error::internal_error(
                "schema root changed type during schema evolution",
            ));
        };
        schema = *updated_schema;
    }

    validate_schema(&schema, column_mapping_mode)?;

    // `max_id` can only increase; a decrease indicates an internal error.
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

/// Applies schema operations and returns a validated table configuration.
///
/// # Errors
///
/// Returns an error if an operation cannot be applied or the resulting configuration is invalid.
pub(super) fn evolve_table_config(
    table_config: &TableConfiguration,
    operations: Vec<SchemaOperation>,
) -> DeltaResult<TableConfiguration> {
    let schema = Arc::unwrap_or_clone(table_config.logical_schema());
    let column_mapping_mode = table_config.column_mapping_mode();
    let current_max_column_id = table_config.table_properties().column_mapping_max_column_id;
    // Whether the pre-alter schema already carried column-mapping metadata -- the only fact the
    // strip below needs from it. Captured as a bool (not a clone) before
    // `apply_schema_operations` consumes `schema` by value. Short-circuits outside
    // `None` mode, where no strip fires.
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

    // Only in `None` mode: if this ALTER introduced column-mapping annotations into a table
    // that was clean before it, strip them; residual annotations already present on the
    // table are left in place (see `strip_stray_column_mapping_metadata`).
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
        .fold_with(new_max_column_id, |evolved_metadata, id| {
            evolved_metadata.with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string())
        });

    // Validates the evolved metadata against the protocol.
    let evolved_table_config =
        TableConfiguration::try_new_with_schema(table_config, evolved_metadata, evolved_schema)?;
    Ok(evolved_table_config)
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
        schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        }
    }

    fn add_col(name: &str, nullable: bool) -> SchemaOperation {
        let field = if nullable {
            StructField::nullable(name, DataType::STRING)
        } else {
            StructField::not_null(name, DataType::STRING)
        };
        SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field,
        }
    }

    fn add_struct_with_nested_leaf(name: &str, leaf_name: &str) -> SchemaOperation {
        SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field: StructField::nullable(name, schema! { nullable (leaf_name): STRING }),
        }
    }

    fn nested_schema() -> StructType {
        schema! {
            not_null "id": INTEGER,
            nullable "address": {
                not_null "city": STRING,
                nullable "zip": STRING,
            },
        }
    }

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

    fn struct_with_existing_field() -> StructType {
        StructType::try_new([StructField::nullable("existing", DataType::STRING)]).unwrap()
    }

    fn nested_struct_at<'a>(mut data_type: &'a DataType, path: &[String]) -> &'a StructType {
        for segment in path {
            data_type = match (segment.as_str(), data_type) {
                ("element", DataType::Array(array)) => array.element_type(),
                ("key", DataType::Map(map)) => map.key_type(),
                ("value", DataType::Map(map)) => map.value_type(),
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
    #[case::dup_exact(simple_schema(), vec![add_col("name", true)], "already exists")]
    #[case::dup_case_insensitive(simple_schema(), vec![add_col("Name", true)], "already exists")]
    #[case::dup_within_batch(
        simple_schema(),
        vec![add_col("email", true), add_col("email", true)],
        "already exists"
    )]
    #[case::dup_nested_sibling(
        nested_schema(),
        vec![SchemaOperation::AddColumn {
            path: column_name!("address"),
            field: StructField::nullable("city", DataType::STRING),
        }],
        "already exists"
    )]
    #[case::non_nullable(simple_schema(), vec![add_col("age", false)], "non-nullable")]
    #[case::invalid_parquet_char(
        simple_schema(),
        vec![add_col("foo,bar", true)],
        "invalid character"
    )]
    #[case::nested_invalid_parquet_char(
        simple_schema(),
        vec![add_struct_with_nested_leaf("addr", "bad,leaf")],
        "invalid character"
    )]
    #[case::metadata_column(
        simple_schema(),
        vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field: StructField::create_metadata_column("row_idx", MetadataColumnSpec::RowIndex),
        }],
        "metadata columns are not allowed"
    )]
    #[case::missing_add_parent(
        simple_schema(),
        vec![SchemaOperation::AddColumn {
            path: column_name!("missing"),
            field: StructField::nullable("added", DataType::STRING),
        }],
        "does not exist"
    )]
    #[case::mismatched_path_segment(
        nested_schema(),
        vec![SchemaOperation::AddColumn {
            path: ColumnName::new(["id", "element"]),
            field: StructField::nullable("x", DataType::STRING),
        }],
        "does not match"
    )]
    fn apply_schema_operations_rejects(
        #[case] schema: StructType,
        #[case] ops: Vec<SchemaOperation>,
        #[case] error_contains: &str,
    ) {
        let err = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap_err();
        assert!(
            err.to_string().contains(error_contains),
            "expected error to contain '{error_contains}', got: {err}"
        );
    }

    #[rstest]
    #[case::single(vec![add_col("email", true)], &["id", "name", "email"])]
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

    #[rstest]
    #[case::struct_parent(
        DataType::from(struct_with_existing_field()),
        ColumnName::new(Vec::<String>::new()),
    )]
    #[case::array_element(
        DataType::from(ArrayType::new(struct_with_existing_field(), true)),
        ColumnName::new(["element"]),
    )]
    #[case::map_key(
        DataType::from(MapType::new(
            struct_with_existing_field(),
            DataType::INTEGER,
            true,
        )),
        ColumnName::new(["key"]),
    )]
    #[case::map_value(
        DataType::from(MapType::new(
            DataType::INTEGER,
            struct_with_existing_field(),
            true,
        )),
        ColumnName::new(["value"]),
    )]
    fn add_column_at_traverses_nested_types(
        #[case] parent_type: DataType,
        #[case] container_path: ColumnName,
    ) {
        let schema =
            StructType::try_new([StructField::nullable("container", parent_type)]).unwrap();
        let path = column_name!("container").join(&container_path);
        let operation = SchemaOperation::AddColumn {
            path,
            field: StructField::nullable("added", DataType::INTEGER),
        };

        let result =
            apply_schema_operations(schema, vec![operation], ColumnMappingMode::None, None)
                .unwrap();
        let container = result.schema.field("container").unwrap();
        let parent = nested_struct_at(container.data_type(), container_path.path());

        assert!(parent.field("existing").is_some());
        assert_eq!(
            parent.field("added"),
            Some(&StructField::nullable("added", DataType::INTEGER))
        );
    }

    /// Second op may target a struct created by the first; under CM, max ID advances across both.
    #[rstest]
    #[case::without_cm(ColumnMappingMode::None, None, None)]
    #[case::with_cm(ColumnMappingMode::Name, Some(10), Some(13))]
    fn sequential_add_struct_then_nested_child(
        #[case] mode: ColumnMappingMode,
        #[case] current_max: Option<i64>,
        #[case] expected_new_max: Option<i64>,
    ) {
        let ops = vec![
            SchemaOperation::AddColumn {
                path: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("parent", struct_with_existing_field()),
            },
            SchemaOperation::AddColumn {
                path: column_name!("parent"),
                field: StructField::nullable("child", DataType::INTEGER),
            },
        ];
        // With CM: parent struct + existing leaf (op1) + child (op2) => three new IDs after max 10.
        let result = apply_schema_operations(simple_schema(), ops, mode, current_max).unwrap();
        let parent = result.schema.field("parent").unwrap();
        let DataType::Struct(s) = parent.data_type() else {
            panic!("Expected Struct, got: {:?}", parent.data_type());
        };
        assert!(s.field("existing").is_some());
        let child = s.field("child").expect("child added by second op");
        assert_eq!(result.new_max_column_id, expected_new_max);
        if let Some(expected_id) = expected_new_max {
            assert_eq!(get_cm_id(child), expected_id);
            assert!(get_physical_name(child).starts_with("col-"));
        } else {
            assert_eq!(child, &StructField::nullable("child", DataType::INTEGER));
        }
    }

    #[rstest]
    #[case::on_required_field(simple_schema(), column_name!("id"))]
    #[case::already_nullable_is_noop(simple_schema(), column_name!("name"))]
    #[case::case_insensitive(simple_schema(), column_name!("ID"))]
    #[case::deeply_nested_field(
        deeply_nested_required_schema(),
        column_name!("address.location.zipcode")
    )]
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

    #[test]
    fn set_nullable_preserves_untouched_fields_and_order() {
        let schema = StructType::try_new(vec![
            StructField::not_null("alpha", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![
                    StructField::not_null("city", DataType::STRING),
                    StructField::nullable("zip", DataType::STRING),
                ])
                .unwrap(),
            ),
            StructField::not_null("gamma", DataType::STRING),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::SetNullable {
            column: column_name!("address.city"),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();

        let names: Vec<&str> = result.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["alpha", "address", "gamma"]);
        assert!(!result.schema.field("alpha").unwrap().is_nullable());
        assert!(!result.schema.field("gamma").unwrap().is_nullable());

        let addr = result.schema.field("address").unwrap();
        assert!(addr.is_nullable());
        let DataType::Struct(s) = addr.data_type() else {
            panic!("Expected Struct, got: {:?}", addr.data_type());
        };
        assert!(s.field("city").unwrap().is_nullable());
        assert!(s.field("zip").unwrap().is_nullable());
    }

    #[test]
    fn set_nullable_on_struct_itself_preserves_inner_fields() {
        let schema = schema! {
            not_null "address": {
                not_null "city": STRING,
            },
        };
        let ops = vec![SchemaOperation::SetNullable {
            column: column_name!("address"),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        let addr = result.schema.field("address").unwrap();
        assert!(addr.is_nullable(), "struct itself must be nullable");
        let DataType::Struct(s) = addr.data_type() else {
            panic!("Expected Struct, got: {:?}", addr.data_type());
        };
        assert!(
            !s.field("city").unwrap().is_nullable(),
            "inner field must remain NOT NULL"
        );
    }

    #[test]
    fn chain_add_and_set_nullable_applies_both() {
        let ops = vec![
            add_col("email", true),
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
        let schema = schema! {
            not_null "alpha": INTEGER,
            nullable "beta": {
                not_null "nested": STRING,
            },
            not_null "gamma": STRING,
        };
        let ops = vec![SchemaOperation::SetNullable {
            column: column_name!("beta.nested"),
        }];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        let names: Vec<&String> = result.schema.fields().map(|f| f.name()).collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    // === Column mapping tests ===

    #[rstest]
    #[case::name_mode_root(
        ColumnMappingMode::Name,
        simple_schema(),
        ColumnName::new(Vec::<String>::new()),
        "email",
        2,
        3
    )]
    #[case::id_mode_root(
        ColumnMappingMode::Id,
        simple_schema(),
        ColumnName::new(Vec::<String>::new()),
        "email",
        5,
        6
    )]
    #[case::name_mode_nested(
        ColumnMappingMode::Name,
        nested_schema(),
        column_name!("address"),
        "country",
        5,
        6
    )]
    fn add_column_with_column_mapping_assigns_id_and_physical_name(
        #[case] mode: ColumnMappingMode,
        #[case] schema: StructType,
        #[case] path: ColumnName,
        #[case] name: &str,
        #[case] current_max: i64,
        #[case] expected_id: i64,
    ) {
        let ops = vec![SchemaOperation::AddColumn {
            path: path.clone(),
            field: StructField::nullable(name, DataType::STRING),
        }];
        let result = apply_schema_operations(schema, ops, mode, Some(current_max)).unwrap();

        let mut field_path = path.into_inner();
        field_path.push(name.to_string());
        let added = result.schema.field_at_path(&field_path);

        assert_eq!(get_cm_id(added), expected_id);
        assert!(get_physical_name(added).starts_with("col-"));
        assert_eq!(result.new_max_column_id, Some(expected_id));
    }

    #[test]
    fn add_column_without_max_column_id_fails_when_mapping_enabled() {
        let ops = vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field: StructField::nullable("email", DataType::STRING),
        }];
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, None)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidProtocol(_)));
        assert!(err.to_string().contains("maxColumnId"));
    }

    #[test]
    fn add_multiple_columns_with_column_mapping_assigns_unique_ids() {
        let ops = vec![
            SchemaOperation::AddColumn {
                path: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("a", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                path: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("b", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                path: ColumnName::new(Vec::<String>::new()),
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
        DataType::from(schema! {
            nullable "a": STRING,
            nullable "b": STRING,
        })
    }

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
            path: ColumnName::new(Vec::<String>::new()),
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
        let mut field = StructField::nullable(name, ty);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(id),
        );
        field
    }

    #[rstest]
    #[case::top_level(field_with_id_only("tainted", DataType::STRING, 99))]
    #[case::nested_in_struct(StructField::nullable(
        "outer",
        schema! {
            (field_with_id_only("inner", DataType::STRING, 99)),
        },
    ))]
    fn add_column_with_preexisting_cm_metadata_is_preserved_under_cm(#[case] field: StructField) {
        let ops = vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field,
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(2))
                .unwrap();

        assert_eq!(find_max_column_id_in_schema(&result.schema), Some(99));
        assert_eq!(result.new_max_column_id, Some(99));
    }

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
            path: ColumnName::new(Vec::<String>::new()),
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

    #[rstest]
    #[case::above_protocol_max(i32::MAX as i64 + 1)]
    #[case::i64_max(i64::MAX)]
    #[case::negative(-1)]
    fn alter_with_out_of_range_persisted_max_column_id_is_rejected(#[case] seed: i64) {
        let ops = vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
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

    #[test]
    fn add_column_with_wrong_typed_id_is_rejected() {
        let mut field = StructField::nullable("bad", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::String("not-a-number".to_string()),
        );
        let ops = vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field,
        }];
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(2))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("non-numeric") && err.contains("delta.columnMapping.id"),
            "error should name the wrong-typed id annotation, got: {err}"
        );
    }

    #[test]
    fn stale_max_column_id_is_self_healed_by_schema_walk() {
        let mut existing = StructField::nullable("existing", DataType::STRING);
        existing.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(42),
        );
        let schema = schema! {
            (existing),
        };
        let ops = vec![SchemaOperation::AddColumn {
            path: ColumnName::new(Vec::<String>::new()),
            field: StructField::nullable("new", DataType::STRING),
        }];
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
