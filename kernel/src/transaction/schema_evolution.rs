//! This module defines the [`SchemaOperation`] enum and functions that validate and
//! apply schema changes to produce an evolved schema.

use std::cmp::Ordering;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_features::{
    drop_column_mapping_metadata, find_max_column_id_in_schema,
    try_assign_flat_column_mapping_info, validate_column_mapping_id, ColumnMappingMode,
};
use crate::DeltaResult;

/// A schema evolution operation to be applied to a table.
///
/// Operations are validated and applied in order during
/// [`apply_schema_operations`]. Each operation sees the schema state after all prior operations
/// have been applied.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub(crate) enum SchemaOperation {
    /// Add a column or nested field to the table schema.
    ///
    /// `parent` identifies the struct that will contain `field`. An empty parent adds `field` to
    /// the table's root schema.
    AddColumn {
        /// Receiving struct; empty selects the root schema.
        parent: ColumnName,
        /// The nullable, non-metadata field to add.
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
        .ok_or_else(|| Error::schema(format!("field '{}' does not exist", name)))?;
    field.nullable = true;
    Ok(())
}

// Helper to modify a nested column. For each component in `path`, locates the matching field,
// array element, map key, or map value (case-insensitive), then descends into the next nested data
// type. At the leaf, calls `modifier` to mutate the field in place.
//
// `modifier` is expected to mutate the field's nullability, metadata, or `data_type` -- but
// not its name. Renames need additional handling (IndexMap re-keying + sibling-conflict check)
// that downstream PRs will introduce alongside the rename caller.
//
// Returns an error if a field in the path does not exist or an intermediate field is not a struct.
//
// Example:
//   fields   = [ id: int not null, address: struct { city: string not null, zip: string } ]
//   path   = ["address", "city"]
//   modifier = |f| { f.nullable = true; Ok(()) }
// yields:
//   [ id: int not null, address: struct { city: string, zip: string } ]
fn modify_field_at_path(
    data_type: &mut DataType,
    path: &[String],
    modifier: impl FnOnce(&mut StructType) -> DeltaResult<()>,
) -> DeltaResult<()> {
    let Some((segment, rest)) = path.split_first() else {
        // `path` is empty, attempt to modify the current struct.
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
        match op {
            // Protocol feature checks for the field's data type (e.g. `timestampNtz`) happen
            // later when the caller builds a new TableConfiguration from the evolved schema --
            // the alter is rejected if the table doesn't already have the required feature
            // enabled. This matches Spark, which also rejects with
            // `DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT` and requires the user to enable the
            // feature explicitly before adding such a column.
            SchemaOperation::AddColumn { parent, field } => {
                if field.is_metadata_column() {
                    return Err(Error::schema(format!(
                        "Cannot add column '{}': metadata columns are not allowed in a table schema",
                        field.name()
                    )));
                }
                if !matches!(field.data_type, DataType::Primitive(_)) {
                    StructType::ensure_no_metadata_columns_in_field(&field)?;
                }
                // Validate field is nullable (Delta protocol requires added columns to be
                // nullable so existing data files can return NULL for the new column)
                // NOTE: non-nullable columns depend on invariants feature
                if !field.is_nullable() {
                    return Err(Error::schema(format!(
                        "Cannot add non-nullable column '{}'. Added columns must be nullable \
                         because existing data files do not contain this column.",
                        field.name()
                    )));
                }
                // A newly added column must receive a fresh identity; a caller-supplied identity
                // could refer to a column that was previously dropped.
                let field_schema = StructType::new_unchecked([field]);
                let field = drop_column_mapping_metadata(&field_schema)
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        Error::internal_error(
                            "column mapping metadata removal returned an empty schema",
                        )
                    })?;
                let field = if cm_enabled {
                    let id = max_id.as_mut().ok_or_else(|| {
                        Error::invalid_protocol(
                            "Column mapping is enabled but delta.columnMapping.maxColumnId \
                             is not set in table properties",
                        )
                    })?;
                    // ALTER TABLE doesn't support icebergCompatV3 yet, so the new field never
                    // gets `delta.columnMapping.nested.ids` here. Tracking issue:
                    // <https://github.com/delta-io/delta-kernel-rs/issues/2492>
                    try_assign_flat_column_mapping_info(&field, id)?
                } else {
                    // Stray CM metadata on a mapping-disabled table is handled by the AlterTable
                    // builder, which strips annotations this ALTER newly introduces from the
                    // evolved schema.
                    field
                };
                modify_field_at_path(&mut root, parent.path(), |parent| add_field(parent, field))?;
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
            parent: ColumnName::new(Vec::<String>::new()),
            field,
        }
    }

    // Builds a struct column whose nested leaf field has the given name. Used to prove that
    // `validate_schema` (not just the top-level dup check or `StructType::try_new`) is
    // reached from `apply_schema_operations`.
    fn add_struct_with_nested_leaf(name: &str, leaf_name: &str) -> SchemaOperation {
        SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
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

    fn nested_struct_at<'a>(mut data_type: &'a DataType, parent: &[String]) -> &'a StructType {
        for segment in parent {
            data_type = match (segment.as_str(), data_type) {
                ("element", DataType::Array(array)) => array.element_type(),
                ("key", DataType::Map(map)) => map.key_type(),
                ("value", DataType::Map(map)) => map.value_type(),
                (segment, data_type) => {
                    panic!("parent segment {segment:?} does not match {data_type}")
                }
            };
        }
        let DataType::Struct(parent) = data_type else {
            panic!("parent does not resolve to a struct");
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

    // === apply_schema_operations tests ===

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
            parent: column_name!("address"),
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
            parent: ColumnName::new(Vec::<String>::new()),
            field: StructField::create_metadata_column("row_idx", MetadataColumnSpec::RowIndex),
        }],
        "metadata columns are not allowed"
    )]
    #[case::missing_add_parent(
        simple_schema(),
        vec![SchemaOperation::AddColumn {
            parent: column_name!("missing"),
            field: StructField::nullable("added", DataType::STRING),
        }],
        "does not exist"
    )]
    #[case::mismatched_path_segment(
        nested_schema(),
        vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(["id", "element"]),
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

    // === apply_schema_operations: SetNullable tests ===
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
        #[case] parent_path: ColumnName,
    ) {
        let schema =
            StructType::try_new([StructField::nullable("container", parent_type)]).unwrap();
        let parent = column_name!("container").join(&parent_path);
        let operation = SchemaOperation::AddColumn {
            parent,
            field: StructField::nullable("added", DataType::INTEGER),
        };

        let result =
            apply_schema_operations(schema, vec![operation], ColumnMappingMode::None, None)
                .unwrap();
        let container = result.schema.field("container").unwrap();
        let parent = nested_struct_at(container.data_type(), parent_path.path());

        assert!(parent.field("existing").is_some());
        assert_eq!(
            parent.field("added"),
            Some(&StructField::nullable("added", DataType::INTEGER))
        );
    }

    /// Second op may target a struct created by the first; under CM, max ID advances across both.
    #[rstest]
    #[case::without_cm(ColumnMappingMode::None, None, None)]
    #[case::with_name_cm(ColumnMappingMode::Name, Some(10), Some(13))]
    #[case::with_id_cm(ColumnMappingMode::Id, Some(10), Some(13))]
    fn sequential_add_struct_then_nested_child(
        #[case] mode: ColumnMappingMode,
        #[case] current_max: Option<i64>,
        #[case] expected_new_max: Option<i64>,
    ) {
        let ops = vec![
            SchemaOperation::AddColumn {
                parent: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("parent", struct_with_existing_field()),
            },
            SchemaOperation::AddColumn {
                parent: column_name!("parent"),
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

    /// Setting a struct itself nullable must not mutate inner fields. Kept separate from the
    /// `set_nullable_succeeds` rstest because it asserts on inner-field preservation.
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
        #[case] parent: ColumnName,
        #[case] name: &str,
        #[case] current_max: i64,
        #[case] expected_id: i64,
    ) {
        let ops = vec![SchemaOperation::AddColumn {
            parent: parent.clone(),
            field: StructField::nullable(name, DataType::STRING),
        }];
        let result = apply_schema_operations(schema, ops, mode, Some(current_max)).unwrap();

        let mut parent_path = parent.into_inner();
        parent_path.push(name.to_string());
        let added = result.schema.field_at_path(&parent_path);

        assert_eq!(get_cm_id(added), expected_id);
        assert!(get_physical_name(added).starts_with("col-"));
        assert_eq!(result.new_max_column_id, Some(expected_id));
    }

    #[test]
    fn add_column_without_max_column_id_fails_when_mapping_enabled() {
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
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
                parent: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("a", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                parent: ColumnName::new(Vec::<String>::new()),
                field: StructField::nullable("b", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                parent: ColumnName::new(Vec::<String>::new()),
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
            parent: ColumnName::new(Vec::<String>::new()),
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
        schema! {
            (field_with_id_only("inner", DataType::STRING, 99)),
        },
    ))]
    fn add_column_replaces_supplied_column_mapping_ids(#[case] field: StructField) {
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
            field,
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(2))
                .unwrap();

        let ids = result
            .schema
            .fields()
            .last()
            .expect("added field")
            .collect_column_mapping_ids();
        // The supplied id is replaced with a fresh one.
        assert!(!ids.contains(&99));
        assert!(ids.iter().all(|id| *id > 2));
        assert_eq!(result.new_max_column_id, ids.iter().max().copied());
    }

    #[test]
    fn add_column_replaces_supplied_physical_name_and_allocates_id() {
        let mut field = StructField::nullable("named", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("user-supplied-name".to_string()),
        );
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
            field,
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(7))
                .unwrap();
        let added = result.schema.field("named").unwrap();
        assert_eq!(get_cm_id(added), 8);
        assert_ne!(get_physical_name(added), "user-supplied-name");
        assert!(get_physical_name(added).starts_with("col-"));
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
            parent: ColumnName::new(Vec::<String>::new()),
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
    fn add_column_replaces_non_numeric_column_mapping_id() {
        let mut field = StructField::nullable("bad", DataType::STRING);
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::String("not-a-number".to_string()),
        );
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
            field,
        }];
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(2))
                .unwrap();
        let added = result.schema.field("bad").unwrap();
        assert_eq!(get_cm_id(added), 3);
        assert_eq!(result.new_max_column_id, Some(3));
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
        let schema = schema! {
            (existing),
        };
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::new(Vec::<String>::new()),
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
