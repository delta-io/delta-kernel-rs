//! Schema evolution operations for table and write transactions.
//!
//! This module defines operations for validating and applying schema changes.

use std::cmp::Ordering;
use std::sync::Arc;

use indexmap::IndexMap;

use crate::error::Error;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    find_max_column_id_in_schema, schema_has_column_mapping_metadata,
    strip_stray_column_mapping_metadata, try_assign_flat_column_mapping_info,
    validate_column_mapping_id, ColumnMappingMode, TableFeature,
};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::utils::{require, FoldWithOption as _};
use crate::DeltaResult;

/// Context shared by schema changes while they are applied.
struct SchemaEvolutionContext {
    column_mapping_enabled: bool,
    max_column_id: Option<i64>,
}

/// A schema change that can be applied to a transaction.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// Adds a field at a column path.
    ///
    /// Traversal through arrays uses their element type and traversal through maps uses their
    /// value type. Map keys cannot be evolved.
    AddField {
        /// Full path to the new field, including its name as the final component.
        column: ColumnName,
        /// The nullable field to add.
        field: StructField,
    },
    /// Makes an existing field nullable.
    ///
    /// Every intermediate path component must be a struct field.
    SetNullable {
        /// Path to the field whose nullability should be relaxed.
        column: ColumnName,
    },
}

impl SchemaChange {
    /// Creates a change that adds `field` at `column`.
    pub fn add_field(column: ColumnName, field: StructField) -> Self {
        Self::AddField { column, field }
    }

    /// Creates a change that makes `column` nullable.
    pub fn set_nullable(column: ColumnName) -> Self {
        Self::SetNullable { column }
    }

    fn apply(
        &self,
        fields: &mut IndexMap<String, StructField>,
        context: &mut SchemaEvolutionContext,
    ) -> DeltaResult<()> {
        match self {
            Self::AddField { column, field } => {
                apply_traversal(fields, &AddFieldTraversal { column, field }, context)
            }
            Self::SetNullable { column } => {
                apply_traversal(fields, &SetNullableTraversal { column }, context)
            }
        }
    }
}

trait TraversalPolicy {
    fn path(&self) -> &[String];

    fn apply_at_leaf(
        &self,
        fields: &mut IndexMap<String, StructField>,
        field_index: Option<usize>,
        context: &mut SchemaEvolutionContext,
    ) -> DeltaResult<()>;

    fn descend<'a>(
        &self,
        data_type: &'a mut DataType,
        path_component: &str,
    ) -> DeltaResult<&'a mut StructType>;

    fn wrap_error(&self, error: Error) -> Error;
}

struct AddFieldTraversal<'a> {
    column: &'a ColumnName,
    field: &'a StructField,
}

struct SetNullableTraversal<'a> {
    column: &'a ColumnName,
}

fn apply_traversal(
    fields: &mut IndexMap<String, StructField>,
    traversal: &impl TraversalPolicy,
    context: &mut SchemaEvolutionContext,
) -> DeltaResult<()> {
    modify_field_at_path(fields, traversal.path(), traversal, context)
        .map_err(|error| traversal.wrap_error(error))
}

fn modify_field_at_path(
    fields: &mut IndexMap<String, StructField>,
    path: &[String],
    traversal: &impl TraversalPolicy,
    context: &mut SchemaEvolutionContext,
) -> DeltaResult<()> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| Error::generic("empty column path"))?;

    // Delta column names are case-insensitive.
    let lowered = first.to_lowercase();
    let field_index = fields
        .iter()
        .position(|(_, field)| field.name().to_lowercase() == lowered);

    if rest.is_empty() {
        return traversal.apply_at_leaf(fields, field_index, context);
    }

    let field_index =
        field_index.ok_or_else(|| Error::generic(format!("field '{first}' does not exist")))?;
    let (_, field) = fields
        .get_index_mut(field_index)
        .ok_or_else(|| Error::internal_error("idx from position() invalid"))?;
    let inner = traversal.descend(&mut field.data_type, first)?;
    modify_field_at_path(inner.field_map_mut(), rest, traversal, context)
}

impl TraversalPolicy for AddFieldTraversal<'_> {
    fn path(&self) -> &[String] {
        self.column.path()
    }

    fn apply_at_leaf(
        &self,
        fields: &mut IndexMap<String, StructField>,
        field_index: Option<usize>,
        context: &mut SchemaEvolutionContext,
    ) -> DeltaResult<()> {
        let leaf = self
            .column
            .path()
            .last()
            .ok_or_else(|| Error::generic("empty column path"))?;
        if !leaf.eq_ignore_ascii_case(self.field.name()) {
            return Err(Error::schema(format!(
                "path leaf '{leaf}' does not match field name '{}'",
                self.field.name()
            )));
        }
        if field_index.is_some() {
            return Err(Error::schema("a column with that name already exists"));
        }
        if self.field.is_metadata_column() {
            return Err(Error::schema(
                "metadata columns are not allowed in a table schema",
            ));
        }
        if !matches!(self.field.data_type, DataType::Primitive(_)) {
            StructType::ensure_no_metadata_columns_in_field(self.field)?;
        }
        if !self.field.is_nullable() {
            return Err(Error::schema(
                "Added columns cannot be non-nullable because existing data files do not contain \
                 this column",
            ));
        }

        let field = if context.column_mapping_enabled {
            let id = context.max_column_id.as_mut().ok_or_else(|| {
                Error::invalid_protocol(
                    "Column mapping is enabled but delta.columnMapping.maxColumnId is not set in \
                     table properties",
                )
            })?;
            try_assign_flat_column_mapping_info(self.field, id)?
        } else {
            self.field.clone()
        };
        fields.insert(field.name().clone(), field);
        Ok(())
    }

    fn descend<'a>(
        &self,
        data_type: &'a mut DataType,
        path_component: &str,
    ) -> DeltaResult<&'a mut StructType> {
        match data_type {
            DataType::Struct(inner) => Ok(inner),
            DataType::Array(array) => self.descend(&mut array.element_type, path_component),
            DataType::Map(map) => self.descend(&mut map.value_type, path_component),
            _ => Err(Error::generic(format!(
                "intermediate field '{path_component}' is not a struct and does not contain one"
            ))),
        }
    }

    fn wrap_error(&self, error: Error) -> Error {
        if matches!(error, Error::InvalidProtocol(_)) {
            error
        } else {
            Error::schema(format!(
                "Cannot add column '{}': {error}",
                self.field.name()
            ))
        }
    }
}

impl TraversalPolicy for SetNullableTraversal<'_> {
    fn path(&self) -> &[String] {
        self.column.path()
    }

    fn apply_at_leaf(
        &self,
        fields: &mut IndexMap<String, StructField>,
        field_index: Option<usize>,
        _context: &mut SchemaEvolutionContext,
    ) -> DeltaResult<()> {
        let field_index = field_index.ok_or_else(|| {
            let leaf = self.column.path().last().map_or("", String::as_str);
            Error::generic(format!("field '{leaf}' does not exist"))
        })?;
        let (_, field) = fields
            .get_index_mut(field_index)
            .ok_or_else(|| Error::internal_error("idx from position() invalid"))?;
        field.nullable = true;
        Ok(())
    }

    fn descend<'a>(
        &self,
        data_type: &'a mut DataType,
        path_component: &str,
    ) -> DeltaResult<&'a mut StructType> {
        let DataType::Struct(inner) = data_type else {
            return Err(Error::generic(format!(
                "intermediate field '{path_component}' is not a struct"
            )));
        };
        Ok(inner)
    }

    fn wrap_error(&self, error: Error) -> Error {
        Error::generic(format!(
            "Cannot set nullable on column '{}': {error}",
            self.column
        ))
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
    operations: Vec<SchemaChange>,
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
    let max_id = if cm_enabled {
        current_max_column_id.map(|cfg| cfg.max(find_max_column_id_in_schema(&schema).unwrap_or(0)))
    } else {
        current_max_column_id
    };

    let mut context = SchemaEvolutionContext {
        column_mapping_enabled: cm_enabled,
        max_column_id: max_id,
    };
    for operation in operations {
        operation.apply(schema.field_map_mut(), &mut context)?;
    }

    validate_schema(&schema, column_mapping_mode)?;

    // `max_id` is only ever incremented by `try_assign_flat_column_mapping_info`. If it grew,
    // the new value must be persisted; if it went backwards, that's a bug.
    let new_max_column_id = match context.max_column_id.cmp(&current_max_column_id) {
        Ordering::Greater => context.max_column_id,
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

/// Applies schema operations and rebuilds the table configuration around the evolved schema.
///
/// # Errors
///
/// Returns an error if the table enables a feature that schema evolution does not support yet, an
/// operation is invalid, the evolved schema violates Delta schema rules, or the evolved metadata is
/// incompatible with the table protocol.
pub(crate) fn evolve_table_config(
    table_config: &TableConfiguration,
    operations: Vec<SchemaChange>,
) -> DeltaResult<TableConfiguration> {
    ensure_schema_evolution_supported(table_config)?;
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

// Rejects tables whose enabled features kernel cannot evolve the schema of yet. Lives on the one
// path every caller funnels through so the ALTER TABLE builder and
// `Transaction::with_schema_changes` cannot drift apart on which tables they accept.
fn ensure_schema_evolution_supported(table_config: &TableConfiguration) -> DeltaResult<()> {
    // Added columns would not receive the required column-mapping nested ids. See
    // [`crate::table_features::ICEBERG_COMPAT_V3_INFO`] for the tracking issue.
    require!(
        !table_config.is_feature_enabled(&TableFeature::IcebergCompatV3),
        Error::unsupported(
            "Schema changes are not yet supported on tables with icebergCompatV3 enabled"
        )
    );
    // TODO(#2630): Support schema evolution on tables with column defaults.
    require!(
        !table_config.is_feature_enabled(&TableFeature::AllowColumnDefaults),
        Error::unsupported(
            "Schema changes are not yet supported on tables with allowColumnDefaults enabled"
        )
    );
    Ok(())
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

    type Operation = SchemaChange;

    fn add_field_at(column: ColumnName, field: StructField) -> Operation {
        SchemaChange::add_field(column, field)
    }

    fn add_field(field: StructField) -> Operation {
        let column = ColumnName::new([field.name().clone()]);
        add_field_at(column, field)
    }

    fn set_nullable(column: ColumnName) -> Operation {
        SchemaChange::set_nullable(column)
    }

    fn add_col(name: &str, nullable: bool) -> Operation {
        let field = if nullable {
            StructField::nullable(name, DataType::STRING)
        } else {
            StructField::not_null(name, DataType::STRING)
        };
        add_field(field)
    }

    // Builds a struct column whose nested leaf field has the given name. Used to prove that
    // `validate_schema` (not just the top-level dup check or `StructType::try_new`) is
    // reached from `apply_schema_operations`.
    fn add_struct_with_nested_leaf(name: &str, leaf_name: &str) -> Operation {
        let inner =
            StructType::try_new(vec![StructField::nullable(leaf_name, DataType::STRING)]).unwrap();
        add_field(StructField::nullable(name, inner))
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

    fn modify_field_at_path_test_helper(
        schema: StructType,
        path: &[String],
    ) -> DeltaResult<IndexMap<String, StructField>> {
        let mut fields = into_field_map(schema);
        let mut context = SchemaEvolutionContext {
            column_mapping_enabled: false,
            max_column_id: None,
        };
        SchemaChange::set_nullable(ColumnName::new(path.to_vec()))
            .apply(&mut fields, &mut context)?;
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
        vec![add_field(StructField::create_metadata_column(
            "row_idx",
            MetadataColumnSpec::RowIndex,
        ))],
        "metadata columns are not allowed"
    )]
    fn apply_schema_operations_rejects(#[case] ops: Vec<Operation>, #[case] error_contains: &str) {
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
        #[case] ops: Vec<Operation>,
        #[case] expected_names: &[&str],
    ) {
        let result =
            apply_schema_operations(simple_schema(), ops, ColumnMappingMode::None, None).unwrap();
        let actual: Vec<&str> = result.schema.fields().map(|f| f.name().as_str()).collect();
        assert_eq!(&actual, expected_names);
    }

    #[derive(Clone, Copy)]
    enum NestedContainer {
        Struct,
        Array,
        Map,
    }

    fn schema_with_nested_container(container: NestedContainer) -> StructType {
        let value = schema! { nullable "existing": STRING };
        let data_type = match container {
            NestedContainer::Struct => DataType::from(value),
            NestedContainer::Array => DataType::from(ArrayType::new(value, true)),
            NestedContainer::Map => DataType::from(MapType::new(
                schema! { nullable "key_field": STRING },
                value,
                true,
            )),
        };
        StructType::try_new([StructField::nullable("container", data_type)]).unwrap()
    }

    fn nested_value_struct(field: &StructField, container: NestedContainer) -> &StructType {
        match (field.data_type(), container) {
            (DataType::Struct(inner), NestedContainer::Struct) => inner,
            (DataType::Array(array), NestedContainer::Array) => match &array.element_type {
                DataType::Struct(inner) => inner,
                other => panic!("Expected struct array element, got: {other:?}"),
            },
            (DataType::Map(map), NestedContainer::Map) => match &map.value_type {
                DataType::Struct(inner) => inner,
                other => panic!("Expected struct map value, got: {other:?}"),
            },
            (other, _) => panic!("Unexpected nested container: {other:?}"),
        }
    }

    #[rstest]
    #[case::nested_struct(NestedContainer::Struct)]
    #[case::array_element_struct(NestedContainer::Array)]
    #[case::map_value_struct(NestedContainer::Map)]
    fn add_field_recurses_through_additive_containers(#[case] container: NestedContainer) {
        let schema = schema_with_nested_container(container);
        let ops = vec![add_field_at(
            column_name!("CONTAINER.added"),
            StructField::nullable("added", DataType::INTEGER),
        )];

        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        let nested = nested_value_struct(result.schema.field("container").unwrap(), container);
        assert!(nested.field("existing").is_some());
        assert!(nested.field("added").is_some());

        if let NestedContainer::Map = container {
            let DataType::Map(map) = result.schema.field("container").unwrap().data_type() else {
                panic!("Expected map");
            };
            let DataType::Struct(key) = &map.key_type else {
                panic!("Expected struct map key");
            };
            assert!(key.field("key_field").is_some());
            assert!(key.field("added").is_none());
        }
    }

    #[rstest]
    #[case::duplicate(
        column_name!("container.existing"),
        StructField::nullable("existing", DataType::INTEGER),
        "already exists"
    )]
    #[case::path_leaf_mismatch(
        column_name!("container.other"),
        StructField::nullable("added", DataType::INTEGER),
        "does not match field name"
    )]
    fn add_nested_field_rejects_invalid_leaf(
        #[case] column: ColumnName,
        #[case] field: StructField,
        #[case] error_contains: &str,
    ) {
        let ops = vec![add_field_at(column, field)];
        let err = apply_schema_operations(
            schema_with_nested_container(NestedContainer::Struct),
            ops,
            ColumnMappingMode::None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains(error_contains));
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
    #[case::deeply_nested_field(
        deeply_nested_required_schema(),
        column_name!("address.location.zipcode")
    )]
    fn set_nullable_succeeds(#[case] schema: StructType, #[case] column: ColumnName) {
        let ops = vec![set_nullable(column.clone())];
        let result = apply_schema_operations(schema, ops, ColumnMappingMode::None, None).unwrap();
        assert!(result.schema.field_at_path(column.path()).is_nullable());
    }

    #[rstest]
    #[case::nonexistent_column(column_name!("nonexistent"), "does not exist")]
    #[case::through_non_struct(column_name!("name.inner"), "not a struct")]
    #[case::empty_path(ColumnName::new(Vec::<String>::new()), "empty column path")]
    fn set_nullable_fails(#[case] column: ColumnName, #[case] error_contains: &str) {
        let ops = vec![set_nullable(column)];
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
        let ops = vec![set_nullable(column_name!("address"))];
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
            add_field(StructField::nullable("email", DataType::STRING)),
            set_nullable(column_name!("id")),
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
        let ops = vec![set_nullable(column_name!("beta.nested"))];
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
        let ops = vec![add_field(StructField::nullable("email", DataType::STRING))];
        let result =
            apply_schema_operations(simple_schema(), ops, mode, Some(current_max)).unwrap();
        let email_field = result.schema.field("email").unwrap();

        assert_eq!(get_cm_id(email_field), expected_id);
        assert!(get_physical_name(email_field).starts_with("col-"));
        assert_eq!(result.new_max_column_id, Some(expected_id));
    }

    #[test]
    fn add_column_without_max_column_id_fails_when_mapping_enabled() {
        let ops = vec![add_field(StructField::nullable("email", DataType::STRING))];
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
            add_field(StructField::nullable("a", DataType::STRING)),
            add_field(StructField::nullable("b", DataType::STRING)),
            add_field(StructField::nullable("c", DataType::STRING)),
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
        let ops = vec![add_field(StructField::nullable("col", data_type))];
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
        let ops = vec![add_field(field)];
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
        let ops = vec![add_field(field)];
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
        let ops = vec![add_field(StructField::nullable(
            "anything",
            DataType::INTEGER,
        ))];
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
        let ops = vec![add_field(field)];
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
        let ops = vec![add_field(StructField::nullable("new", DataType::STRING))];
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
