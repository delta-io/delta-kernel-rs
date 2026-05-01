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

    /// Drop a column. Supports nested columns (e.g. `column_name!("address.city")`).
    /// Requires column mapping.
    DropColumn { column: ColumnName },

    /// Rename a column by path. Supports nested columns. Requires column mapping.
    RenameColumn {
        column: ColumnName,
        new_name: String,
    },

    /// Change a column's nullability from NOT NULL to nullable.
    SetNullable { column: ColumnName },
}

/// Signal returned by [`modify_field_at_path`] modifiers describing what to do with the leaf
/// field after the modifier runs.
#[must_use]
pub(crate) enum LeafAction {
    /// Keep the leaf in its parent IndexMap. Leaf was mutated in place (or unchanged).
    Keep,
    /// Remove the leaf field from its parent IndexMap. `modify_field_at_path` rejects this
    /// action if it would leave the parent struct with zero fields.
    Remove,
}

// Helper to modify a nested column. For each component in `path`, locates the matching field
// (case-insensitive), then descends into the next nested struct. At the leaf, calls `modifier`
// and applies its returned [`LeafAction`] in place.
//
// `Keep` mutations include rename: if `modifier` changes `field.name`, the IndexMap key is
// updated to match (preserving insertion order) and a case-insensitive sibling-conflict check
// is applied.
// `Remove` deletes the leaf entry, preserving the order of remaining siblings.
//
// Returns an error if a field in the path does not exist, an intermediate field is not a struct,
// a `Keep` rename would collide with a sibling, or a `Remove` action would leave the parent
// struct empty.
//
// Example (mutate):
//   fields  = [ id: int not null, address: struct { city: string not null, zip: string } ]
//   path    = ["address", "city"]
//   modifier = |f| { f.nullable = true; Ok(LeafAction::Keep) }
// yields:
//   [ id: int not null, address: struct { city: string, zip: string } ]
//
// Example (remove):
//   path    = ["address", "city"]
//   modifier = |_| Ok(LeafAction::Remove)
// yields:
//   [ id: int not null, address: struct { zip: string } ]
fn modify_field_at_path(
    fields: &mut IndexMap<String, StructField>,
    path: &[String],
    modifier: &dyn Fn(&mut StructField) -> DeltaResult<LeafAction>,
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
    let parent_len = fields.len();
    debug_assert!(
        parent_len > 0,
        "idx from position() implies non-empty fields"
    );
    let (_, field) = fields
        .get_index_mut(idx)
        .ok_or_else(|| Error::internal_error("idx from position() invalid"))?;
    let old_name = field.name().clone();
    match modifier(field)? {
        LeafAction::Keep => {
            let new_name = field.name().clone();
            if old_name == new_name {
                return Ok(());
            }
            // Rename: validate against siblings (case-insensitive), then re-key the entry while
            // preserving insertion order via swap_remove + insert + swap_indices (all O(1)).
            let lowered_new = new_name.to_lowercase();
            if fields
                .iter()
                .enumerate()
                .any(|(i, (_, f))| i != idx && f.name().to_lowercase() == lowered_new)
            {
                return Err(Error::generic(format!(
                    "Cannot rename '{old_name}' to '{new_name}': \
                     a column with that name already exists"
                )));
            }
            let (_, old_value) = fields
                .swap_remove_index(idx)
                .ok_or_else(|| Error::internal_error("entry vanished between lookup and remove"))?;
            fields.insert(new_name, old_value);
            fields.swap_indices(idx, fields.len() - 1);
            Ok(())
        }
        LeafAction::Remove => {
            // Reject drops that would leave the parent struct empty
            // Note: Matches Spark behaviour.
            if parent_len == 1 {
                return Err(Error::generic(format!(
                    "field '{}' is the last field at its level",
                    field.name()
                )));
            }
            // Preserve insertion order of remaining siblings.
            fields.shift_remove_index(idx);
            Ok(())
        }
    }
}

/// Rejects an operation when column mapping is not enabled. `op_name_uppercase` is used in the
/// error message so it reads naturally (e.g. "DROP COLUMN requires...").
fn require_column_mapping(cm_enabled: bool, op_name_uppercase: &str) -> DeltaResult<()> {
    if !cm_enabled {
        return Err(Error::generic(format!(
            "{op_name_uppercase} requires column mapping to be enabled \
             (delta.columnMapping.mode = 'name' or 'id')"
        )));
    }
    Ok(())
}

/// Rejects empty paths and columns the table layout locks (partition columns, clustering
/// columns, or struct ancestors of clustering columns).
fn reject_layout_locked_column(
    column: &ColumnName,
    partition_columns: &[String],
    clustering_columns: &[ColumnName],
    op_name: &str,
) -> DeltaResult<()> {
    let path = column.path();
    if path.is_empty() {
        return Err(Error::generic(format!(
            "Cannot {op_name} column: empty column path"
        )));
    }
    // Partition columns are always top-level. The slice pattern matches iff path.len() == 1
    // and binds the sole element to `field_name`.
    if let [field_name] = path {
        let field_name_lower = field_name.to_lowercase();
        if partition_columns
            .iter()
            .any(|pc| pc.to_lowercase() == field_name_lower)
        {
            return Err(Error::generic(format!(
                "Cannot {op_name} column '{column}': it is a partition column"
            )));
        }
    }
    // Clustering columns may be nested; modifying an ancestor struct of one is also rejected.
    // Matches Spark.
    if clustering_columns.iter().any(|cc| column.is_prefix_of(cc)) {
        return Err(Error::generic(format!(
            "Cannot {op_name} column '{column}': it is a clustering column or an ancestor of one"
        )));
    }
    Ok(())
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
    partition_columns: &[String],
    clustering_columns: &[ColumnName],
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
                    try_assign_field_column_mapping(&field, id)?
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
                    Ok(LeafAction::Keep)
                })
                .map_err(|e| {
                    Error::generic(format!("Cannot set nullable on column '{column}': {e}"))
                })?;
            }
            SchemaOperation::DropColumn { column } => {
                require_column_mapping(cm_enabled, "DROP COLUMN")?;
                reject_layout_locked_column(
                    &column,
                    partition_columns,
                    clustering_columns,
                    "drop",
                )?;
                modify_field_at_path(schema.field_map_mut(), column.path(), &|_| {
                    Ok(LeafAction::Remove)
                })
                .map_err(|e| Error::generic(format!("Cannot drop column '{column}': {e}")))?;
            }
            SchemaOperation::RenameColumn { column, new_name } => {
                require_column_mapping(cm_enabled, "RENAME COLUMN")?;
                // `new_name` validity (non-empty, valid chars) is enforced by `validate_schema`
                // at the end of this function via `SchemaValidator::transform_struct_field`.
                // TODO(#2446): delta-spark supports renaming partition and clustering columns
                // and rewrites `Metadata.partitionColumns` / clustering domain metadata
                // accordingly. Until then, we reject via the layout-lock helper.
                reject_layout_locked_column(
                    &column,
                    partition_columns,
                    clustering_columns,
                    "rename",
                )?;
                modify_field_at_path(schema.field_map_mut(), column.path(), &|f| {
                    f.name = new_name.clone();
                    Ok(LeafAction::Keep)
                })
                .map_err(|e| Error::generic(format!("Cannot rename column '{column}': {e}")))?;
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

    fn nested_schema() -> StructType {
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![
                    StructField::nullable("street", DataType::STRING),
                    StructField::not_null("city", DataType::STRING),
                    StructField::nullable("zip", DataType::STRING),
                ])
                .unwrap(),
            ),
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

    // === modify_field_at_path tests ===

    // Convert a StructType into the IndexMap<String, StructField> shape that
    // `modify_field_at_path` operates on.
    fn into_field_map(schema: StructType) -> IndexMap<String, StructField> {
        schema
            .into_fields()
            .map(|f| (f.name().clone(), f))
            .collect()
    }

    fn set_nullable_modifier(f: &mut StructField) -> DeltaResult<LeafAction> {
        f.nullable = true;
        Ok(LeafAction::Keep)
    }

    fn modify_field_at_path_test_helper(
        schema: StructType,
        path: &[String],
    ) -> DeltaResult<IndexMap<String, StructField>> {
        let mut fields = into_field_map(schema);
        modify_field_at_path(&mut fields, path, &set_nullable_modifier)?;
        Ok(fields)
    }

    fn remove_field_at_path_test_helper(
        schema: StructType,
        path: &[String],
    ) -> DeltaResult<IndexMap<String, StructField>> {
        let mut fields = into_field_map(schema);
        modify_field_at_path(&mut fields, path, &|_| Ok(LeafAction::Remove))?;
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

    #[rstest]
    #[case::modify(true)]
    #[case::remove(false)]
    fn nonexistent_field_fails(#[case] is_modify: bool) {
        let path = vec!["nope".to_string()];
        let err = if is_modify {
            modify_field_at_path_test_helper(simple_schema(), &path)
        } else {
            remove_field_at_path_test_helper(simple_schema(), &path)
        }
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    /// A path that descends into a non-struct intermediate field (here: `name.inner`, where
    /// `name` is a STRING, not a struct) must error rather than silently succeed or panic.
    #[rstest]
    #[case::modify(true)]
    #[case::remove(false)]
    fn through_non_struct_fails(#[case] is_modify: bool) {
        let path = vec!["name".to_string(), "inner".to_string()];
        let err = if is_modify {
            modify_field_at_path_test_helper(simple_schema(), &path)
        } else {
            remove_field_at_path_test_helper(simple_schema(), &path)
        }
        .unwrap_err();
        assert!(err.to_string().contains("not a struct"));
    }

    #[test]
    fn modify_case_insensitive_lookup_finds_field() {
        let path = vec!["ID".to_string()];
        let result = modify_field_at_path_test_helper(simple_schema(), &path).unwrap();
        let id = result.values().find(|f| f.name() == "id").unwrap();
        assert!(id.is_nullable());
    }

    // === modify_field_at_path: remove via LeafAction::Remove ===

    #[test]
    fn remove_top_level_field() {
        let path = vec!["name".to_string()];
        let result = remove_field_at_path_test_helper(simple_schema(), &path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.values().next().unwrap().name(), "id");
    }

    #[test]
    fn remove_nested_field() {
        let path = vec!["address".to_string(), "city".to_string()];
        let result = remove_field_at_path_test_helper(nested_schema(), &path).unwrap();
        let addr = result.values().find(|f| f.name() == "address").unwrap();
        let DataType::Struct(s) = addr.data_type() else {
            panic!("expected struct");
        };
        assert!(s.field("city").is_none());
        assert!(s.field("zip").is_some());
    }

    /// After removing a field, the surviving fields must retain their original order.
    /// `modify_field_at_path` uses `shift_remove_index` for `LeafAction::Remove`, which
    /// preserves order. This test verifies that across drop positions (first / middle / last).
    #[rstest]
    #[case::first(0, &["b", "c", "d", "e"])]
    #[case::middle(2, &["a", "b", "d", "e"])]
    #[case::last(4, &["a", "b", "c", "d"])]
    fn remove_preserves_order_at_position(
        #[case] drop_idx: usize,
        #[case] expected_order: &[&str],
    ) {
        let names = ["a", "b", "c", "d", "e"];
        let schema = StructType::try_new(
            names
                .iter()
                .map(|n| StructField::nullable(*n, DataType::STRING))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let drop_name = names[drop_idx].to_string();
        let result = remove_field_at_path_test_helper(schema, &[drop_name]).unwrap();
        let actual_order: Vec<&str> = result.values().map(|f| f.name().as_str()).collect();
        assert_eq!(&actual_order, expected_order);
    }

    /// No-empty-struct invariant: removing the last remaining field at any level must error.
    #[rstest]
    #[case::top_level(
        StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap(),
        vec!["only".to_string()]
    )]
    #[case::nested(
        StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "address",
                StructType::try_new(vec![StructField::nullable("street", DataType::STRING)])
                    .unwrap(),
            ),
        ])
        .unwrap(),
        vec!["address".to_string(), "street".to_string()]
    )]
    fn remove_last_field_fails(#[case] schema: StructType, #[case] path: Vec<String>) {
        let err = remove_field_at_path_test_helper(schema, &path).unwrap_err();
        assert!(err.to_string().contains("last field at its level"));
    }

    #[test]
    fn remove_case_insensitive_lookup() {
        let path = vec!["NAME".to_string()];
        let result = remove_field_at_path_test_helper(simple_schema(), &path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.values().next().unwrap().name(), "id");
    }

    /// Removing one nested leaf (`address.city`) must not touch any other field. Guards against
    /// the recursion accidentally replacing siblings when it descends and returns.
    #[test]
    fn remove_nested_leaf_preserves_other_fields() {
        let path = vec!["address".to_string(), "city".to_string()];
        let result = remove_field_at_path_test_helper(nested_schema(), &path).unwrap();
        // Top-level "id" unchanged
        let id = result.values().find(|f| f.name() == "id").unwrap();
        assert!(!id.is_nullable());
        // "address" still present, inner "zip" survived, inner "city" gone
        let addr = result.values().find(|f| f.name() == "address").unwrap();
        let DataType::Struct(s) = addr.data_type() else {
            panic!("expected struct");
        };
        assert!(s.field("zip").is_some());
        assert!(s.field("city").is_none());
    }

    /// Covers 4-level recursion: `a.b.c.d`. Exercises the recursion for both modify and remove.
    fn four_level_nested_schema() -> StructType {
        StructType::try_new(vec![StructField::nullable(
            "a",
            StructType::try_new(vec![StructField::nullable(
                "b",
                StructType::try_new(vec![StructField::nullable(
                    "c",
                    StructType::try_new(vec![
                        StructField::not_null("d", DataType::INTEGER),
                        StructField::nullable("sibling", DataType::STRING),
                    ])
                    .unwrap(),
                )])
                .unwrap(),
            )])
            .unwrap(),
        )])
        .unwrap()
    }

    /// Walk to `result`'s `a.b.c` leaf struct. The 4-level helpers above produce schemas where
    /// the modified/removed leaf lives there, so tests just assert on the resulting `d_struct`.
    fn walk_to_d_struct(result: &IndexMap<String, StructField>) -> &StructType {
        let a = result.values().next().unwrap();
        let DataType::Struct(b_struct) = a.data_type() else {
            panic!("a should be struct");
        };
        let DataType::Struct(c_struct) = b_struct.field("b").unwrap().data_type() else {
            panic!("b should be struct");
        };
        let DataType::Struct(d_struct) = c_struct.field("c").unwrap().data_type() else {
            panic!("c should be struct");
        };
        d_struct
    }

    /// 4-level recursion (`a.b.c.d`) for both modify and remove. The helper must descend
    /// through each level, apply the leaf action, and rebuild on the way back up without
    /// disturbing the sibling.
    #[rstest]
    #[case::modify(true)]
    #[case::remove(false)]
    fn deeply_nested_4_levels(#[case] is_modify: bool) {
        let path = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let result = if is_modify {
            modify_field_at_path_test_helper(four_level_nested_schema(), &path).unwrap()
        } else {
            remove_field_at_path_test_helper(four_level_nested_schema(), &path).unwrap()
        };
        let d_struct = walk_to_d_struct(&result);
        if is_modify {
            assert!(d_struct.field("d").unwrap().is_nullable());
            assert!(d_struct.field("sibling").unwrap().is_nullable());
        } else {
            assert!(d_struct.field("d").is_none());
            assert!(d_struct.field("sibling").is_some());
        }
    }

    // Shorthand for apply_schema_operations with no column mapping and no partition/clustering.
    // Used by AddColumn, SetNullable, and DropColumn tests below.
    fn apply_ops(
        schema: StructType,
        ops: Vec<SchemaOperation>,
    ) -> DeltaResult<SchemaEvolutionResult> {
        apply_schema_operations(schema, ops, ColumnMappingMode::None, None, &[], &[])
    }

    // Shorthand for apply_schema_operations with column mapping enabled and a given max id, but
    // no partition/clustering columns.
    fn apply_ops_with_cm(
        schema: StructType,
        ops: Vec<SchemaOperation>,
        mode: ColumnMappingMode,
        current_max_column_id: Option<i64>,
    ) -> DeltaResult<SchemaEvolutionResult> {
        apply_schema_operations(schema, ops, mode, current_max_column_id, &[], &[])
    }

    // === apply_schema_operations: AddColumn tests ===

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
        let err = apply_ops(simple_schema(), ops).unwrap_err();
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
        let result = apply_ops(simple_schema(), ops).unwrap();
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
        let result = apply_ops(schema, ops).unwrap();
        assert!(result.schema.field_at_path(column.path()).is_nullable());
    }

    #[rstest]
    #[case::nonexistent_column(column_name!("nonexistent"), "does not exist")]
    #[case::through_non_struct(column_name!("name.inner"), "not a struct")]
    #[case::empty_path(ColumnName::new(Vec::<String>::new()), "empty column path")]
    fn set_nullable_fails(#[case] column: ColumnName, #[case] error_contains: &str) {
        let ops = vec![SchemaOperation::SetNullable { column }];
        let err = apply_ops(simple_schema(), ops).unwrap_err();
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
        let result = apply_ops(schema, ops).unwrap();
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
        let result = apply_ops(simple_schema(), ops).unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("email").is_some());
        assert!(result.schema.field("id").unwrap().is_nullable());
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
        let result = apply_ops_with_cm(simple_schema(), ops, mode, Some(current_max)).unwrap();
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
        let err =
            apply_ops_with_cm(simple_schema(), ops, ColumnMappingMode::Name, None).unwrap_err();
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
            apply_ops_with_cm(simple_schema(), ops, ColumnMappingMode::Name, Some(10)).unwrap();

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
            apply_ops_with_cm(simple_schema(), ops, ColumnMappingMode::Name, Some(10)).unwrap();

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
        let err =
            apply_ops_with_cm(simple_schema(), ops, ColumnMappingMode::Name, Some(2)).unwrap_err();
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
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(5)).unwrap();
        let new_id = get_cm_id(result.schema.field("new").unwrap());
        assert_eq!(
            new_id, 43,
            "new id must follow schema max (42), not stale property (5)"
        );
        assert_eq!(result.new_max_column_id, Some(43));
    }

    // === apply_schema_operations: DropColumn tests ===

    /// Schema flavor selector for `drop_column_failures`. Each variant builds a distinct
    /// schema so one rstest can exercise drops across simple, single-column, and nested shapes.
    enum DropSchema {
        /// `simple_schema()` = { id: int not null, name: string }
        Simple,
        /// { only: string }
        SingleColumn,
        /// { id: int not null, address: struct { street: string } }
        AddressStreetOnly,
        /// `nested_schema()` = { id: int not null, address: struct { street, city, zip } }
        NestedThreeFields,
    }

    fn build_drop_schema(flavor: DropSchema) -> StructType {
        match flavor {
            DropSchema::Simple => simple_schema(),
            DropSchema::SingleColumn => {
                StructType::try_new(vec![StructField::nullable("only", DataType::STRING)]).unwrap()
            }
            DropSchema::AddressStreetOnly => StructType::try_new(vec![
                StructField::not_null("id", DataType::INTEGER),
                StructField::nullable(
                    "address",
                    StructType::try_new(vec![StructField::nullable("street", DataType::STRING)])
                        .unwrap(),
                ),
            ])
            .unwrap(),
            DropSchema::NestedThreeFields => nested_schema(),
        }
    }

    #[rstest]
    #[case::without_cm(
        DropSchema::Simple, ColumnMappingMode::None, None,
        column_name!("name"), vec![], vec![], "column mapping"
    )]
    #[case::nonexistent(
        DropSchema::Simple, ColumnMappingMode::Name, Some(2),
        column_name!("nonexistent"), vec![], vec![], "does not exist"
    )]
    #[case::partition_same_case(
        DropSchema::Simple, ColumnMappingMode::Name, Some(2),
        column_name!("name"), vec!["name".to_string()], vec![], "partition column"
    )]
    #[case::partition_diff_case(
        DropSchema::Simple, ColumnMappingMode::Name, Some(2),
        column_name!("NAME"), vec!["name".to_string()], vec![], "partition column"
    )]
    #[case::last_column(
        DropSchema::SingleColumn, ColumnMappingMode::Name, Some(1),
        column_name!("only"), vec![], vec![], "last field"
    )]
    #[case::last_nested_field(
        DropSchema::AddressStreetOnly, ColumnMappingMode::Name, Some(3),
        column_name!("address.street"), vec![], vec![], "last field"
    )]
    #[case::clustering_same_case(
        DropSchema::Simple, ColumnMappingMode::Name, Some(4),
        column_name!("name"), vec![], vec![column_name!("name")], "clustering column"
    )]
    #[case::clustering_diff_case(
        DropSchema::Simple, ColumnMappingMode::Name, Some(4),
        column_name!("NAME"), vec![], vec![column_name!("name")], "clustering column"
    )]
    #[case::clustering_ancestor(
        DropSchema::NestedThreeFields, ColumnMappingMode::Name, Some(4),
        column_name!("address"), vec![], vec![column_name!("address.city")], "clustering column"
    )]
    #[case::nested_nonexistent(
        DropSchema::NestedThreeFields, ColumnMappingMode::Name, Some(4),
        column_name!("address.country"), vec![], vec![], "does not exist"
    )]
    #[case::nested_through_non_struct(
        DropSchema::Simple, ColumnMappingMode::Name, Some(2),
        column_name!("name.inner"), vec![], vec![], "not a struct"
    )]
    #[case::empty_path(
        DropSchema::Simple, ColumnMappingMode::Name, Some(2),
        ColumnName::new(Vec::<String>::new()), vec![], vec![], "empty column path"
    )]
    fn drop_column_failures(
        #[case] schema_flavor: DropSchema,
        #[case] cm: ColumnMappingMode,
        #[case] max_id: Option<i64>,
        #[case] drop_column: ColumnName,
        #[case] partition: Vec<String>,
        #[case] clustering: Vec<ColumnName>,
        #[case] error_contains: &str,
    ) {
        let ops = vec![SchemaOperation::DropColumn {
            column: drop_column,
        }];
        let err = apply_schema_operations(
            build_drop_schema(schema_flavor),
            ops,
            cm,
            max_id,
            &partition,
            &clustering,
        )
        .unwrap_err();
        assert!(err.to_string().contains(error_contains));
    }

    #[test]
    fn drop_column_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::DropColumn {
            column: column_name!("name"),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        assert_eq!(result.schema.fields().count(), 1);
        assert!(result.schema.field("name").is_none());
        assert!(result.schema.field("id").is_some());
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
                column: column_name!("age"),
            },
            SchemaOperation::SetNullable {
                column: column_name!("id"),
            },
        ];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(3)).unwrap();
        assert_eq!(result.schema.fields().count(), 3);
        assert!(result.schema.field("email").is_some());
        assert!(result.schema.field("age").is_none());
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn drop_then_re_add_same_column_name() {
        // Pre-assign a column-mapping id to "name" so we can verify that after drop + re-add
        // the new "name" gets a fresh id strictly greater than the dropped column's id.
        let dropped_name_id: i64 = 2;
        let mut name_field = StructField::nullable("name", DataType::STRING);
        name_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(dropped_name_id),
        );
        name_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("col-existing".to_string()),
        );
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            name_field,
        ])
        .unwrap();
        let ops = vec![
            SchemaOperation::DropColumn {
                column: column_name!("name"),
            },
            SchemaOperation::AddColumn {
                field: StructField::nullable("name", DataType::INTEGER),
            },
        ];
        let result =
            apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(dropped_name_id)).unwrap();
        assert_eq!(result.schema.fields().count(), 2);
        let re_added = result.schema.field("name").unwrap();
        assert_eq!(re_added.data_type(), &DataType::INTEGER);
        let expected_new_id = dropped_name_id + 1;
        assert_eq!(get_cm_id(re_added), expected_new_id);
        assert_eq!(result.new_max_column_id, Some(expected_new_id));
        let new_physical_name = get_physical_name(re_added);
        assert_ne!(
            new_physical_name, "col-existing",
            "re-added column must get a fresh physical name distinct from the dropped column's"
        );
    }

    #[test]
    fn drop_nested_column_succeeds() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::DropColumn {
            column: column_name!("address.city"),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(4)).unwrap();
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

    // === apply_schema_operations: RenameColumn tests ===

    #[rstest]
    #[case::without_cm(
        simple_schema(), column_name!("name"), "full_name",
        ColumnMappingMode::None, vec![], vec![], "column mapping"
    )]
    #[case::nonexistent(
        simple_schema(), column_name!("nonexistent"), "new_name",
        ColumnMappingMode::Name, vec![], vec![], "does not exist"
    )]
    #[case::to_existing_top_level(
        simple_schema(), column_name!("name"), "id",
        ColumnMappingMode::Name, vec![], vec![], "already exists"
    )]
    #[case::empty_name(
        simple_schema(), column_name!("name"), "",
        ColumnMappingMode::Name, vec![], vec![], "cannot be empty"
    )]
    #[case::newline_in_name(
        simple_schema(), column_name!("name"), "full\nname",
        ColumnMappingMode::Name, vec![], vec![], "newline"
    )]
    #[case::partition_column(
        simple_schema(), column_name!("name"), "full_name",
        ColumnMappingMode::Name, vec!["name".to_string()], vec![], "partition column"
    )]
    #[case::partition_column_target_uppercase(
        simple_schema(), column_name!("NAME"), "full_name",
        ColumnMappingMode::Name, vec!["name".to_string()], vec![], "partition column"
    )]
    #[case::partition_column_stored_uppercase(
        simple_schema(), column_name!("name"), "full_name",
        ColumnMappingMode::Name, vec!["NAME".to_string()], vec![], "partition column"
    )]
    #[case::clustering_column(
        simple_schema(), column_name!("name"), "full_name",
        ColumnMappingMode::Name, vec![], vec![column_name!("name")], "clustering column"
    )]
    #[case::clustering_ancestor_target_uppercase(
        nested_schema(), column_name!("ADDRESS"), "location",
        ColumnMappingMode::Name, vec![], vec![column_name!("address.city")],
        "clustering column"
    )]
    #[case::clustering_ancestor(
        nested_schema(), column_name!("address"), "location",
        ColumnMappingMode::Name, vec![], vec![column_name!("address.city")],
        "clustering column"
    )]
    #[case::nested_sibling_conflict(
        nested_schema(), column_name!("address.city"), "street",
        ColumnMappingMode::Name, vec![], vec![], "already exists"
    )]
    #[case::case_insensitive_sibling_conflict(
        simple_schema(), column_name!("name"), "ID",
        ColumnMappingMode::Name, vec![], vec![], "already exists"
    )]
    #[case::empty_path(
        simple_schema(), ColumnName::new(Vec::<String>::new()), "new",
        ColumnMappingMode::Name, vec![], vec![], "empty column path"
    )]
    #[case::through_non_struct(
        simple_schema(), column_name!("name.inner"), "new",
        ColumnMappingMode::Name, vec![], vec![], "not a struct"
    )]
    fn rename_fails(
        #[case] schema: StructType,
        #[case] column: ColumnName,
        #[case] new_name: &str,
        #[case] mode: ColumnMappingMode,
        #[case] partition: Vec<String>,
        #[case] clustering: Vec<ColumnName>,
        #[case] expected_err: &str,
    ) {
        let ops = vec![SchemaOperation::RenameColumn {
            column,
            new_name: new_name.to_string(),
        }];
        let err = apply_schema_operations(schema, ops, mode, Some(10), &partition, &clustering)
            .unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing '{expected_err}', got: {err}"
        );
    }

    #[test]
    fn rename_column_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        assert!(result.schema.field("full_name").is_some());
        assert!(result.schema.field("name").is_none());
        assert_eq!(result.schema.fields().count(), 2);
    }

    #[test]
    fn rename_nested_column_succeeds() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("address.city"),
            new_name: "town".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(4)).unwrap();
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
    fn rename_case_change_succeeds() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("name"),
            new_name: "Name".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        // Asserting on the literal stored name proves the case actually changed -- the
        // IndexMap key and the StructField.name both round-trip the requested case.
        let renamed = result.schema.field("Name").expect("renamed column present");
        assert_eq!(renamed.name(), "Name");
        assert!(result.schema.field("name").is_none());
    }

    #[test]
    fn rename_to_same_name_is_noop() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("name"),
            new_name: "name".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        assert!(result.schema.field("name").is_some());
        assert_eq!(result.schema.fields().count(), 2);
    }

    /// Rename must preserve the column's `delta.columnMapping.id` and
    /// `delta.columnMapping.physicalName`. This is the whole point of rename under CM --
    /// without this invariant, existing Parquet data would be orphaned.
    #[test]
    fn rename_preserves_column_mapping_metadata() {
        let mut name_field = StructField::nullable("name", DataType::STRING);
        name_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(2),
        );
        name_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("col-abc".to_string()),
        );
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            name_field,
        ])
        .unwrap();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        let renamed = result.schema.field("full_name").unwrap();
        assert_eq!(
            renamed.get_config_value(&ColumnMetadataKey::ColumnMappingId),
            Some(&MetadataValue::Number(2)),
            "CM id must be preserved"
        );
        assert_eq!(
            renamed.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String("col-abc".to_string())),
            "CM physical name must be preserved"
        );
    }

    /// Nested rename must preserve CM metadata too (same invariant as top-level).
    #[test]
    fn rename_nested_preserves_column_mapping_metadata() {
        let mut city_field = StructField::nullable("city", DataType::STRING);
        city_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(3),
        );
        city_field.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("col-xyz".to_string()),
        );
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("address", StructType::try_new(vec![city_field]).unwrap()),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("address.city"),
            new_name: "town".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(3)).unwrap();
        let addr = result.schema.field("address").unwrap();
        let DataType::Struct(inner) = addr.data_type() else {
            panic!("expected struct");
        };
        let town = inner.field("town").expect("renamed column present");
        assert_eq!(
            town.get_config_value(&ColumnMetadataKey::ColumnMappingId),
            Some(&MetadataValue::Number(3)),
        );
        assert_eq!(
            town.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String("col-xyz".to_string())),
        );
    }

    /// Rename must NOT bump `maxColumnId` -- it doesn't introduce a column.
    #[test]
    fn rename_does_not_bump_max_column_id() {
        let schema = simple_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("name"),
            new_name: "full_name".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(5)).unwrap();
        assert_eq!(
            result.new_max_column_id, None,
            "rename must not bump maxColumnId"
        );
    }

    /// Renaming a nested leaf must not clobber siblings (parallels
    /// `modify_nested_leaf_preserves_other_fields`).
    #[test]
    fn rename_nested_leaf_preserves_other_fields() {
        let schema = nested_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("address.city"),
            new_name: "town".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(4)).unwrap();
        // Top-level "id" untouched.
        assert!(result.schema.field("id").is_some());
        // Siblings of "city" in address struct must still be present.
        let addr = result.schema.field("address").unwrap();
        let DataType::Struct(inner) = addr.data_type() else {
            panic!("expected struct");
        };
        assert!(inner.field("street").is_some());
        assert!(inner.field("zip").is_some());
    }

    /// Renaming an ancestor struct must preserve every nested field's CM id and physical name.
    /// This is the recursive case of the "rename preserves CM metadata" invariant.
    #[test]
    fn rename_ancestor_struct_preserves_nested_cm_metadata() {
        let mut city = StructField::nullable("city", DataType::STRING);
        city.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(3),
        );
        city.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("col-city".to_string()),
        );
        let mut zip = StructField::nullable("zip", DataType::STRING);
        zip.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(4),
        );
        zip.metadata.insert(
            ColumnMetadataKey::ColumnMappingPhysicalName
                .as_ref()
                .to_string(),
            MetadataValue::String("col-zip".to_string()),
        );
        let schema = StructType::try_new(vec![
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("address", StructType::try_new(vec![city, zip]).unwrap()),
        ])
        .unwrap();
        let ops = vec![SchemaOperation::RenameColumn {
            column: column_name!("address"),
            new_name: "location".to_string(),
        }];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(4)).unwrap();
        let location = result.schema.field("location").expect("renamed struct");
        let DataType::Struct(inner) = location.data_type() else {
            panic!("expected struct");
        };
        let inner_city = inner.field("city").expect("nested city preserved");
        assert_eq!(
            inner_city.get_config_value(&ColumnMetadataKey::ColumnMappingId),
            Some(&MetadataValue::Number(3)),
        );
        assert_eq!(
            inner_city.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String("col-city".to_string())),
        );
        let inner_zip = inner.field("zip").expect("nested zip preserved");
        assert_eq!(
            inner_zip.get_config_value(&ColumnMetadataKey::ColumnMappingId),
            Some(&MetadataValue::Number(4)),
        );
        assert_eq!(
            inner_zip.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            Some(&MetadataValue::String("col-zip".to_string())),
        );
    }

    /// Chain `add_column` then `rename_column` in one ALTER. The newly-added column gets a
    /// fresh CM id and is then renamed to a different logical name in the same pass. This is
    /// the only chain the type-state allows that involves rename (rename is terminal).
    #[test]
    fn chain_add_then_rename_added_column() {
        let schema = simple_schema();
        let ops = vec![
            SchemaOperation::AddColumn {
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::RenameColumn {
                column: column_name!("email"),
                new_name: "contact_email".to_string(),
            },
        ];
        let result = apply_ops_with_cm(schema, ops, ColumnMappingMode::Name, Some(2)).unwrap();
        assert!(result.schema.field("contact_email").is_some());
        assert!(result.schema.field("email").is_none());
        let added_renamed = result.schema.field("contact_email").unwrap();
        // The fresh CM id must come from the AddColumn step, not have been wiped by rename.
        let cm_id = get_cm_id(added_renamed);
        assert_eq!(cm_id, 3, "added column should get fresh id (max+1)");
        // Physical name must follow the standard "col-..." pattern from the AddColumn step.
        assert!(get_physical_name(added_renamed).starts_with("col-"));
        assert_eq!(
            result.new_max_column_id,
            Some(3),
            "max id advances to cover the added column even though it was renamed"
        );
    }
}
