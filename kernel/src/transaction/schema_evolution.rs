//! Schema evolution operations for ALTER TABLE.
//!
//! This module defines the [`SchemaOperation`] enum and the [`apply_schema_operations`] function
//! that validates and applies schema changes to produce an evolved schema.

use std::cmp::Ordering;

use indexmap::IndexMap;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_features::{
    find_max_column_id_in_schema, try_assign_field_column_mapping, ColumnMappingMode,
};
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
    SetNullable { column: ColumnName },
}

// Helper to modify a nested column. For each component in `path`, locates the matching field
// (case-insensitive), then descends into the next nested struct. At the leaf, calls `modifier`
// to mutate the field in place.
//
// `modifier` is expected to mutate the field's nullability, metadata, or `data_type` -- but
// not its name. Renames need additional handling (IndexMap re-keying + sibling-conflict check)
// that downstream PRs will introduce alongside the rename caller.
//
// Returns an error if a field in the path does not exist or an intermediate field is not a struct.
//
// Example:
//   fields   = [ id: int not null, address: struct { city: string not null, zip: string } ]
//   path     = ["address", "city"]
//   modifier = |f| { f.nullable = true; Ok(()) }
// yields:
//   [ id: int not null, address: struct { city: string, zip: string } ]
fn modify_field_at_path(
    fields: &mut IndexMap<String, StructField>,
    path: &[String],
    modifier: &dyn Fn(&mut StructField) -> DeltaResult<()>,
) -> DeltaResult<()> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| Error::generic("empty column path"))?;

    // Delta column names are case-insensitive.
    let lowered = first.to_lowercase();
    let idx = fields
        .iter()
        .position(|(_, f)| f.name().to_lowercase() == lowered)
        .ok_or_else(|| Error::generic(format!("field '{first}' does not exist")))?;

    if !rest.is_empty() {
        let (_, field) = fields
            .get_index_mut(idx)
            .ok_or_else(|| Error::internal_error("idx from position() invalid"))?;
        let DataType::Struct(inner) = &mut field.data_type else {
            return Err(Error::generic(format!(
                "intermediate field '{first}' is not a struct"
            )));
        };
        return modify_field_at_path(inner.field_map_mut(), rest, modifier);
    }

    // === Leaf handling ===
    let (_, field) = fields
        .get_index_mut(idx)
        .ok_or_else(|| Error::internal_error("idx from position() invalid"))?;
    modifier(field)
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

    // When column mapping is enabled and the property is set, defensively take the max with
    // the schema's actual max in case a non-conforming writer left the property stale.
    let mut max_id = if cm_enabled {
        current_max_column_id.map(|cfg| cfg.max(find_max_column_id_in_schema(&schema).unwrap_or(0)))
    } else {
        current_max_column_id
    };

    for op in operations {
        match op {
            // Protocol feature checks for the field's data type (e.g. `timestampNtz`) happen
            // later when the caller builds a new TableConfiguration from the evolved schema --
            // the alter is rejected if the table doesn't already have the required feature
            // enabled. This matches Spark, which also rejects with
            // `DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT` and requires the user to enable the
            // feature explicitly before adding such a column.
            SchemaOperation::AddColumn { field } => {
                if field.is_metadata_column() {
                    return Err(Error::schema(format!(
                        "Cannot add column '{}': metadata columns are not allowed in \
                         a table schema",
                        field.name()
                    )));
                }
                if !matches!(field.data_type, DataType::Primitive(_)) {
                    StructType::ensure_no_metadata_columns_in_field(&field)?;
                }
                // Case-insensitive sibling-conflict check (O(N) per AddColumn).
                let lowered = field.name().to_lowercase();
                if schema.fields().any(|f| f.name().to_lowercase() == lowered) {
                    return Err(Error::schema(format!(
                        "Cannot add column '{}': a column with that name already exists",
                        field.name()
                    )));
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
                let field = if cm_enabled {
                    let id = max_id.as_mut().ok_or_else(|| {
                        Error::invalid_protocol(
                            "Column mapping is enabled but delta.columnMapping.maxColumnId \
                             is not set in table properties",
                        )
                    })?;
                    // We don't support icebergCompatV3 on alter table yet, with_nested_ids is
                    // always false here Tracking issue:
                    // <https://github.com/delta-io/delta-kernel-rs/issues/2492>
                    try_assign_field_column_mapping(&field, id, /* with_nested_ids */ false)?
                } else {
                    // No upfront reject for stray CM metadata: the recursive walk in
                    // `StructType::make_physical` (called from
                    // `TableConfiguration::try_new_with_schema`
                    // by the AlterTable builder) catches any CM annotation anywhere in the tree.
                    // CREATE TABLE's non-CM path relies on the same mechanism.
                    field
                };
                schema.field_map_mut().insert(field.name().clone(), field);
            }
            SchemaOperation::SetNullable { column } => {
                modify_field_at_path(schema.field_map_mut(), column.path(), &|f| {
                    f.nullable = true;
                    Ok(())
                })
                .map_err(|e| {
                    Error::generic(format!("Cannot set nullable on column '{column}': {e}"))
                })?;
            }
        }
    }

    validate_schema(&schema, column_mapping_mode)?;

    // `max_id` is only ever incremented by `try_assign_field_column_mapping`. If it grew, the
    // new value must be persisted; if it went backwards, that's a bug.
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
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataColumnSpec, MetadataValue,
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
        SchemaOperation::AddColumn { field }
    }

    // Builds a struct column whose nested leaf field has the given name. Used to prove that
    // `validate_schema` (not just the top-level dup check or `StructType::try_new`) is
    // reached from `apply_schema_operations`.
    fn add_struct_with_nested_leaf(name: &str, leaf_name: &str) -> SchemaOperation {
        let inner =
            StructType::try_new(vec![StructField::nullable(leaf_name, DataType::STRING)]).unwrap();
        SchemaOperation::AddColumn {
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

    // Convert a StructType into the IndexMap<String, StructField> shape that
    // `modify_field_at_path` operates on.
    fn into_field_map(schema: StructType) -> IndexMap<String, StructField> {
        schema
            .into_fields()
            .map(|f| (f.name().clone(), f))
            .collect()
    }

    fn set_nullable_modifier(f: &mut StructField) -> DeltaResult<()> {
        f.nullable = true;
        Ok(())
    }

    fn modify_field_at_path_test_helper(
        schema: StructType,
        path: &[String],
    ) -> DeltaResult<IndexMap<String, StructField>> {
        let mut fields = into_field_map(schema);
        modify_field_at_path(&mut fields, path, &set_nullable_modifier)?;
        Ok(fields)
    }

    #[test]
    fn modify_top_level_field_sets_nullable() {
        let path = vec!["id".to_string()];
        let result = modify_field_at_path_test_helper(simple_schema(), &path).unwrap();
        let id = result.values().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    #[test]
    fn modify_nested_field_modifies_only_leaf() {
        let path = vec!["address".to_string(), "city".to_string()];
        let result = modify_field_at_path_test_helper(nested_schema(), &path).unwrap();
        let addr = result.values().find(|f| f.name() == "address").unwrap();
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
        let id = result.values().find(|f| f.name() == "id").unwrap();
        assert!(!id.is_nullable());
        let addr = result.values().find(|f| f.name() == "address").unwrap();
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
        let id = result.values().find(|f| f.name() == "id").unwrap();
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
            field: StructField::create_metadata_column("row_idx", MetadataColumnSpec::RowIndex),
        }],
        "metadata columns are not allowed"
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

    fn deeply_nested_required_schema() -> StructType {
        StructType::try_new(vec![
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
        .unwrap()
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
                field: StructField::nullable("a", DataType::STRING),
            },
            SchemaOperation::AddColumn {
                field: StructField::nullable("b", DataType::STRING),
            },
            SchemaOperation::AddColumn {
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
        DataType::Struct(Box::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::STRING),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
        ))
    }

    /// Adding a complex column on a CM table: every inner struct field reachable through
    /// Struct/Array/Map recursion must receive a distinct ID greater than the previous max,
    /// and `new_max_column_id` must advance to the largest assigned ID.
    #[rstest]
    #[case::nested_struct(struct_of_two_primitives(), 3)]
    #[case::array_of_primitive(
        DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
        1
    )]
    #[case::map_of_primitives(
        DataType::Map(Box::new(MapType::new(DataType::STRING, DataType::INTEGER, true))),
        1
    )]
    #[case::array_of_struct(
        DataType::Array(Box::new(ArrayType::new(struct_of_two_primitives(), true))),
        3
    )]
    #[case::map_value_is_struct(
        DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            struct_of_two_primitives(),
            true,
        ))),
        3
    )]
    #[case::map_key_is_struct(
        DataType::Map(Box::new(MapType::new(
            struct_of_two_primitives(),
            DataType::INTEGER,
            true,
        ))),
        3
    )]
    fn add_complex_column_with_column_mapping_assigns_ids_to_all_inner_fields(
        #[case] data_type: DataType,
        #[case] expected_id_count: usize,
    ) {
        let ops = vec![SchemaOperation::AddColumn {
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

    fn field_with_stray_cm_id(name: &str, ty: DataType) -> StructField {
        let mut f = StructField::nullable(name, ty);
        f.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(99),
        );
        f
    }

    /// CM-enabled: stray CM annotations on a new column are rejected at any nesting depth.
    #[rstest]
    #[case::top_level(field_with_stray_cm_id("tainted", DataType::STRING))]
    #[case::nested_in_struct(StructField::nullable(
        "outer",
        DataType::Struct(Box::new(
            StructType::try_new(vec![field_with_stray_cm_id("inner", DataType::STRING)]).unwrap(),
        )),
    ))]
    fn add_column_with_preexisting_cm_metadata_is_rejected_under_cm(#[case] field: StructField) {
        let ops = vec![SchemaOperation::AddColumn { field }];
        let err = apply_schema_operations(simple_schema(), ops, ColumnMappingMode::Name, Some(2))
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("pre-populated"));
        assert!(
            msg.contains("delta.columnMapping.id"),
            "error should name the offending annotation, got: {msg}"
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
