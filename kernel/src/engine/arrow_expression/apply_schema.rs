use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use super::super::arrow_conversion::{kernel_metadata_to_arrow_metadata, lookup_nested_field_id};
use super::super::arrow_utils::make_arrow_error;
use crate::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MapArray, RecordBatch, StructArray,
};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use crate::engine::ensure_data_types::{ensure_data_types, ValidationMode};
use crate::error::{DeltaResult, Error};
use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use crate::schema::{ArrayType, DataType, MapType, Schema, StructField};

#[derive(Clone)]
pub(crate) struct NestedIdCtx<'a> {
    ancestor: &'a StructField,
    relative_path: String,
}

impl<'a> NestedIdCtx<'a> {
    fn new(ancestor: &'a StructField) -> Self {
        Self {
            ancestor,
            relative_path: ancestor.name().to_string(),
        }
    }

    /// Produce a child context with `.role` appended to the path, reusing the same ancestor.
    fn extend(&self, role: &str) -> Self {
        Self {
            ancestor: self.ancestor,
            relative_path: format!("{}.{}", self.relative_path, role),
        }
    }
}

/// Looks up the nested id for the given role (extending `ctx.relative_path`) and returns the
/// child context alongside the id (if any) to recurse with.
fn enter_nested_role<'a>(
    ctx: Option<&NestedIdCtx<'a>>,
    role: &str,
) -> DeltaResult<(Option<NestedIdCtx<'a>>, Option<i64>)> {
    let Some(ctx) = ctx else {
        return Ok((None, None));
    };
    let child = ctx.extend(role);
    let id = lookup_nested_field_id(ctx.ancestor, &child.relative_path).map_err(Error::from)?;
    Ok((Some(child), id))
}

/// Build arrow field metadata containing just `PARQUET:field_id`, or empty if `None`.
fn parquet_field_id_only_metadata(id: Option<i64>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    if let Some(id) = id {
        meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    }
    meta
}

// Apply a schema to an array. The array _must_ be a `StructArray`. Returns a `RecordBatch` where
// the names of fields, nullable, and metadata in the struct have been transformed to match those
// in the schema specified by `schema`.
//
// Note: If the struct array has top-level nulls, the child columns are expected to already have
// those nulls propagated. Arrow's JSON reader does this automatically, and parquet data goes
// through `fix_nested_null_masks` which handles it. We decompose the struct and discard its null
// buffer since RecordBatch cannot have top-level nulls.
pub(crate) fn apply_schema(array: &dyn Array, schema: &DataType) -> DeltaResult<RecordBatch> {
    let DataType::Struct(struct_schema) = schema else {
        return Err(Error::generic(
            "apply_schema at top-level must be passed a struct schema",
        ));
    };
    let applied = apply_schema_to_struct(array, struct_schema)?;
    let (fields, columns, _nulls) = applied.into_parts();

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

// helper to transform an arrow field+col into the specified target type. If `rename` is specified
// the field will be renamed to the contained `str`.
fn new_field_with_metadata(
    field_name: &str,
    data_type: &ArrowDataType,
    nullable: bool,
    metadata: Option<HashMap<String, String>>,
) -> ArrowField {
    let mut field = ArrowField::new(field_name, data_type.clone(), nullable);
    if let Some(metadata) = metadata {
        field.set_metadata(metadata);
    };
    field
}

// A helper that is a wrapper over `transform_field_and_col`. This will take apart the passed struct
// and use that method to transform each column and then put the struct back together. Target types
// and names for each column should be passed in `target_types_and_names`. The number of elements in
// the `target_types_and_names` iterator _must_ be the same as the number of columns in
// `struct_array`. The transformation is ordinal. That is, the order of fields in `target_fields`
// _must_ match the order of the columns in `struct_array`.
fn transform_struct(
    struct_array: &StructArray,
    target_fields: impl Iterator<Item = impl Borrow<StructField>>,
) -> DeltaResult<StructArray> {
    let (input_fields, arrow_cols, nulls) = struct_array.clone().into_parts();
    let input_col_count = arrow_cols.len();
    let result_iter = arrow_cols
        .into_iter()
        .zip(input_fields.iter())
        .zip(target_fields)
        .map(|((sa_col, input_field), target_field)| -> DeltaResult<_> {
            let target_field = target_field.borrow();
            // When the target field's type is Array/Map, anchor a fresh nested-id context at
            // this StructField. Struct reset happens implicitly: inside a nested struct each
            // child restarts this same branch. Primitives ignore the context entirely.
            let child_ctx = match target_field.data_type() {
                DataType::Array(_) | DataType::Map(_) => Some(NestedIdCtx::new(target_field)),
                _ => None,
            };
            let transformed_col =
                apply_schema_to(&sa_col, target_field.data_type(), child_ctx.as_ref())?;
            let arrow_metadata = kernel_metadata_to_arrow_metadata(target_field)?;
            // If both the input field and the target field carry a field ID they must agree,
            // otherwise we would silently overwrite one field ID with another.
            if let (Some(input_id), Some(target_id)) = (
                input_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
                arrow_metadata.get(PARQUET_FIELD_ID_META_KEY),
            ) {
                if input_id != target_id {
                    return Err(Error::generic(format!(
                        "Field '{}': input field ID {} conflicts with target field ID {}",
                        target_field.name, input_id, target_id
                    )));
                }
            }
            let transformed_field = new_field_with_metadata(
                &target_field.name,
                transformed_col.data_type(),
                target_field.nullable,
                Some(arrow_metadata),
            );
            Ok((transformed_field, transformed_col))
        });
    let (transformed_fields, transformed_cols): (Vec<ArrowField>, Vec<ArrayRef>) =
        result_iter.process_results(|iter| iter.unzip())?;
    if transformed_cols.len() != input_col_count {
        return Err(Error::InternalError(format!(
            "Passed struct had {input_col_count} columns, but transformed column has {}",
            transformed_cols.len()
        )));
    }
    Ok(StructArray::try_new(
        transformed_fields.into(),
        transformed_cols,
        nulls,
    )?)
}

// Transform a struct array. The data is in `array`, and the target fields are in `kernel_fields`.
fn apply_schema_to_struct(array: &dyn Array, kernel_fields: &Schema) -> DeltaResult<StructArray> {
    let Some(sa) = array.as_struct_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a struct but isn't a StructArray",
        ));
    };
    transform_struct(sa, kernel_fields.fields())
}

// deconstruct the array, then rebuild the mapped version
fn apply_schema_to_list(
    array: &dyn Array,
    target_inner_type: &ArrayType,
    ctx: Option<&NestedIdCtx<'_>>,
) -> DeltaResult<ListArray> {
    let Some(la) = array.as_list_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a list but isn't a ListArray",
        ));
    };
    let (field, offset_buffer, values, nulls) = la.clone().into_parts();

    let (element_ctx, element_id) =
        enter_nested_role(ctx, super::super::arrow_conversion::LIST_ARRAY_ROOT)?;

    // Mirror the top-level conflict check in `transform_struct`: if the engine's input array's
    // inner field already carries a PARQUET:field_id, it must agree with the nested-ids-derived
    // target id.
    if let (Some(input_id), Some(target_id)) =
        (field.metadata().get(PARQUET_FIELD_ID_META_KEY), element_id)
    {
        if input_id != &target_id.to_string() {
            return Err(Error::generic(format!(
                "Array element: input field ID {input_id} conflicts with target ID {target_id}"
            )));
        }
    }

    let transformed_values = apply_schema_to(
        &values,
        &target_inner_type.element_type,
        element_ctx.as_ref(),
    )?;
    let mut transformed_field = ArrowField::new(
        field.name(),
        transformed_values.data_type().clone(),
        target_inner_type.contains_null,
    );
    transformed_field.set_metadata(parquet_field_id_only_metadata(element_id));

    Ok(ListArray::try_new(
        Arc::new(transformed_field),
        offset_buffer,
        transformed_values,
        nulls,
    )?)
}

// deconstruct a map, and rebuild it with the specified target kernel type
fn apply_schema_to_map(
    array: &dyn Array,
    kernel_map_type: &MapType,
    ctx: Option<&NestedIdCtx<'_>>,
) -> DeltaResult<MapArray> {
    let Some(ma) = array.as_map_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a map but isn't a MapArray",
        ));
    };
    let (map_field, offset_buffer, map_struct_array, nulls, ordered) = ma.clone().into_parts();
    let (inner_fields, inner_cols, inner_nulls) = map_struct_array.into_parts();
    if inner_cols.len() != 2 || inner_fields.len() != 2 {
        return Err(make_arrow_error(
            "MapArray inner struct must have exactly 2 columns (key, value)",
        ));
    }

    let (key_ctx, key_id) =
        enter_nested_role(ctx, super::super::arrow_conversion::MAP_KEY_DEFAULT)?;
    let (value_ctx, value_id) =
        enter_nested_role(ctx, super::super::arrow_conversion::MAP_VALUE_DEFAULT)?;

    let conflict_check = |role: &str, input: &ArrowField, target_id: Option<i64>| {
        if let (Some(input_id), Some(t_id)) =
            (input.metadata().get(PARQUET_FIELD_ID_META_KEY), target_id)
        {
            if input_id != &t_id.to_string() {
                return Err(Error::generic(format!(
                    "Map {role}: input field ID {input_id} conflicts with target ID {t_id}"
                )));
            }
        }
        Ok(())
    };
    conflict_check("key", &inner_fields[0], key_id)?;
    conflict_check("value", &inner_fields[1], value_id)?;

    let transformed_key =
        apply_schema_to(&inner_cols[0], &kernel_map_type.key_type, key_ctx.as_ref())?;
    let transformed_value = apply_schema_to(
        &inner_cols[1],
        &kernel_map_type.value_type,
        value_ctx.as_ref(),
    )?;

    let mut key_field = ArrowField::new(
        inner_fields[0].name(),
        transformed_key.data_type().clone(),
        false,
    );
    key_field.set_metadata(parquet_field_id_only_metadata(key_id));

    let mut value_field = ArrowField::new(
        inner_fields[1].name(),
        transformed_value.data_type().clone(),
        kernel_map_type.value_contains_null,
    );
    value_field.set_metadata(parquet_field_id_only_metadata(value_id));

    let transformed_struct = StructArray::try_new(
        vec![key_field, value_field].into(),
        vec![transformed_key, transformed_value],
        inner_nulls,
    )?;

    let transformed_map_field = ArrowField::new(
        map_field.name().clone(),
        transformed_struct.data_type().clone(),
        map_field.is_nullable(),
    );
    Ok(MapArray::try_new(
        Arc::new(transformed_map_field),
        offset_buffer,
        transformed_struct,
        nulls,
        ordered,
    )?)
}

// apply `schema` to `array`. This handles renaming, and adjusting nullability and metadata. if the
// actual data types don't match, this will return an error.
//
// `ctx` carries the nearest-ancestor StructField + current relative path for propagating nested
// field IDs from `parquet.field.nested.ids` / `delta.columnMapping.nested.ids` onto synthesized
// list / map inner arrow fields. Top-level callers pass `None`; the per-field loop in
// `transform_struct` anchors a fresh context when it encounters an Array/Map target field.
pub(crate) fn apply_schema_to(
    array: &ArrayRef,
    schema: &DataType,
    ctx: Option<&NestedIdCtx<'_>>,
) -> DeltaResult<ArrayRef> {
    use DataType::*;
    let array: ArrayRef = match schema {
        Struct(stype) => Arc::new(apply_schema_to_struct(array, stype)?),
        Array(atype) => Arc::new(apply_schema_to_list(array, atype, ctx)?),
        Map(mtype) => Arc::new(apply_schema_to_map(array, mtype, ctx)?),
        _ => {
            ensure_data_types(schema, array.data_type(), ValidationMode::Full)?;
            array.clone()
        }
    };
    Ok(array)
}

#[cfg(test)]
mod apply_schema_validation_tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrow::array::{Int32Array, StructArray};
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
    use crate::utils::test_utils::assert_result_error_with_message;

    #[test]
    fn test_apply_schema_basic_functionality() {
        // Test that apply_schema works for basic field transformation
        let input_array = create_test_struct_array_2_fields();
        let target_schema = create_target_schema_2_fields();

        // This should succeed - basic schema application
        let result = apply_schema_to_struct(&input_array, &target_schema);
        assert!(result.is_ok(), "Basic schema application should succeed");

        let result_array = result.unwrap();
        assert_eq!(
            result_array.len(),
            input_array.len(),
            "Row count should be preserved"
        );
        assert_eq!(result_array.num_columns(), 2, "Should have 2 columns");
    }

    // Helper functions to create test data
    fn create_test_struct_array_2_fields() -> StructArray {
        let field1 = ArrowField::new("a", ArrowDataType::Int32, false);
        let field2 = ArrowField::new("b", ArrowDataType::Int32, false);
        let schema = ArrowSchema::new(vec![field1, field2]);

        let a_data = Int32Array::from(vec![1, 2, 3]);
        let b_data = Int32Array::from(vec![4, 5, 6]);

        StructArray::try_new(
            schema.fields.clone(),
            vec![Arc::new(a_data), Arc::new(b_data)],
            None,
        )
        .unwrap()
    }

    fn create_target_schema_2_fields() -> StructType {
        StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::INTEGER, false),
        ])
    }

    /// Test that apply_schema handles structs with top-level nulls correctly.
    ///
    /// This simulates a Delta log scenario where each row is one action type (add, remove, etc.).
    /// When extracting `add.stats_parsed`, rows where `add` is null (e.g., remove actions) should
    /// have null child columns. The child columns are expected to already have nulls propagated
    /// (Arrow's JSON reader does this, and parquet data goes through `fix_nested_null_masks`).
    #[test]
    fn test_apply_schema_handles_top_level_nulls() {
        // Create a struct array with 4 rows where rows 1 and 3 have top-level nulls.
        // This simulates: [add_action, remove_action, add_action, remove_action]
        // where remove_action rows have null for the entire struct.
        let field_a = ArrowField::new("a", ArrowDataType::Int32, true);
        let field_b = ArrowField::new("b", ArrowDataType::Int32, true);
        let schema = ArrowSchema::new(vec![field_a, field_b]);

        // Child columns with nulls already propagated (simulating what Arrow readers do).
        // Rows 1 and 3 are null because the parent struct is null at those positions.
        let a_data = Int32Array::from(vec![Some(1), None, Some(3), None]);
        let b_data = Int32Array::from(vec![Some(10), None, Some(30), None]);

        // Top-level struct nulls: rows 0 and 2 are valid, rows 1 and 3 are null
        let null_buffer = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false]));

        let struct_array = StructArray::try_new(
            schema.fields.clone(),
            vec![Arc::new(a_data), Arc::new(b_data)],
            Some(null_buffer),
        )
        .unwrap();

        // Target schema with nullable fields
        let target_schema = DataType::Struct(Box::new(StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, true),
            StructField::new("b", DataType::INTEGER, true),
        ])));

        // Apply schema - should successfully convert to RecordBatch
        let result = apply_schema(&struct_array, &target_schema).unwrap();

        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.num_columns(), 2);

        // Verify columns preserve nulls from child arrays
        let col_a = result.column(0);
        assert!(col_a.is_valid(0), "Row 0 should be valid");
        assert!(col_a.is_null(1), "Row 1 should be null");
        assert!(col_a.is_valid(2), "Row 2 should be valid");
        assert!(col_a.is_null(3), "Row 3 should be null");

        let col_b = result.column(1);
        assert!(col_b.is_valid(0), "Row 0 should be valid");
        assert!(col_b.is_null(1), "Row 1 should be null");
        assert!(col_b.is_valid(2), "Row 2 should be valid");
        assert!(col_b.is_null(3), "Row 3 should be null");

        // Verify the actual values for valid rows
        let col_a = col_a
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column a should be Int32Array");
        let col_b = col_b
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column b should be Int32Array");

        assert_eq!(col_a.value(0), 1);
        assert_eq!(col_a.value(2), 3);
        assert_eq!(col_b.value(0), 10);
        assert_eq!(col_b.value(2), 30);
    }

    /// Test that apply_schema translates "parquet.field.id" kernel metadata to the Arrow-specific
    /// "PARQUET:field_id" key. This ensures the same key translation applied during schema
    /// conversion (`TryFromKernel<&StructField> for ArrowField`) is also applied when
    /// `apply_schema` is used to map data onto an existing schema (e.g. in the arrow expression
    /// evaluator).
    #[test]
    fn test_apply_schema_transforms_parquet_field_id_metadata() {
        let field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();
        let target_schema =
            StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)
                .with_metadata([(field_id_key.to_string(), MetadataValue::Number(42))])]);

        let arrow_field = ArrowField::new("a", ArrowDataType::Int32, false);
        let input_array = StructArray::try_new(
            vec![arrow_field].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            None,
        )
        .unwrap();

        let result = apply_schema_to_struct(&input_array, &target_schema).unwrap();

        let (_, output_field) = result.fields().find("a").unwrap();
        // "parquet.field.id" must be translated to the Arrow/Parquet native key
        assert_eq!(
            output_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("42"),
            "parquet.field.id should be translated to PARQUET:field_id"
        );
        // The original key must not be present
        assert!(
            !output_field.metadata().contains_key(field_id_key),
            "original parquet.field.id key should not be present after translation"
        );
    }

    /// Test that apply_schema succeeds when the input Arrow field already carries the same field
    /// ID as the target kernel schema field (no conflict).
    #[test]
    fn test_apply_schema_matching_field_ids_succeed() {
        let field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();
        let target_schema =
            StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)
                .with_metadata([(field_id_key.to_string(), MetadataValue::Number(42))])]);

        let arrow_field = ArrowField::new("a", ArrowDataType::Int32, false)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "42".to_string())].into());
        let input_array = StructArray::try_new(
            vec![arrow_field].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            None,
        )
        .unwrap();

        let result = apply_schema_to_struct(&input_array, &target_schema);
        assert!(result.is_ok(), "Matching field IDs should succeed");
    }

    /// Test that apply_schema fails when the input Arrow field already carries a *different* field
    /// ID than the target kernel schema field.
    #[test]
    fn test_apply_schema_conflicting_field_ids_fail() {
        let field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();
        let target_schema =
            StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)
                .with_metadata([(field_id_key.to_string(), MetadataValue::Number(42))])]);

        let arrow_field = ArrowField::new("a", ArrowDataType::Int32, false)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "99".to_string())].into());
        let input_array = StructArray::try_new(
            vec![arrow_field].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            None,
        )
        .unwrap();

        assert_result_error_with_message(
            apply_schema_to_struct(&input_array, &target_schema),
            "conflicts with",
        );
    }

    // ===== IcebergCompatV3 nested field ids =====

    /// Build a JSON nested-ids map for use on StructField metadata.
    fn nested_ids_json<const N: usize>(entries: [(&str, i64); N]) -> MetadataValue {
        let obj: serde_json::Map<String, serde_json::Value> = entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::from(v)))
            .collect();
        MetadataValue::Other(serde_json::Value::Object(obj))
    }

    /// Walk an arrow DataType, collecting PARQUET:field_id from every field encountered
    /// (in-order: list element, map key, map value, struct fields). Returns pairs of
    /// (field_name, field_id) mirroring the traversal. Useful to assert that nested ids
    /// flow onto element/key/value.
    fn collect_dt_field_ids(dt: &ArrowDataType) -> Vec<(String, String)> {
        fn collect(dt: &ArrowDataType, acc: &mut Vec<(String, String)>) {
            match dt {
                ArrowDataType::List(entry)
                | ArrowDataType::LargeList(entry)
                | ArrowDataType::FixedSizeList(entry, _) => {
                    if let Some(id) = entry.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                        acc.push((entry.name().clone(), id.clone()));
                    }
                    collect(entry.data_type(), acc);
                }
                ArrowDataType::Map(entry, _) => {
                    if let Some(id) = entry.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                        acc.push((entry.name().clone(), id.clone()));
                    }
                    collect(entry.data_type(), acc);
                }
                ArrowDataType::Struct(fields) => {
                    for f in fields {
                        if let Some(id) = f.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                            acc.push((f.name().clone(), id.clone()));
                        }
                        collect(f.data_type(), acc);
                    }
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        collect(dt, &mut out);
        out
    }

    // ----- Input builders -----

    /// Build a `List<List<Int32>>` array with trivial values so we only care about type metadata.
    fn list_of_list_of_int() -> Arc<dyn Array> {
        use crate::arrow::array::builder::{Int32Builder, ListBuilder};
        let mut b = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        // one outer element containing one inner element [1]
        b.values().values().append_value(1);
        b.values().append(true);
        b.append(true);
        Arc::new(b.finish())
    }

    /// Build a `Map<Int32, List<Int32>>` array.
    fn map_int_to_list_of_int() -> Arc<dyn Array> {
        use crate::arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder};
        let mut b = MapBuilder::new(
            None,
            Int32Builder::new(),
            ListBuilder::new(Int32Builder::new()),
        );
        b.keys().append_value(1);
        b.values().values().append_value(10);
        b.values().append(true);
        b.append(true).unwrap();
        Arc::new(b.finish())
    }

    /// Protocol example case 1: col1: array[array[int]], nested ids on col1.
    #[test]
    fn test_apply_schema_nested_ids_array_of_array() {
        let col = list_of_list_of_int();
        let input = StructArray::try_new(
            vec![ArrowField::new("col1", col.data_type().clone(), true)].into(),
            vec![col],
            None,
        )
        .unwrap();

        let inner_array = crate::schema::ArrayType::new(DataType::INTEGER, true);
        let outer_array =
            crate::schema::ArrayType::new(DataType::Array(Box::new(inner_array)), true);
        let target = StructType::new_unchecked([StructField::new(
            "col1",
            DataType::Array(Box::new(outer_array)),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            nested_ids_json([("col1.element", 100), ("col1.element.element", 101)]),
        )])]);

        let out = apply_schema_to_struct(&input, &target).unwrap();
        let ids = collect_dt_field_ids(out.data_type());
        // arrow-rs's ListBuilder names the inner field "item"; apply_schema preserves that.
        // What matters is that both nested levels carry the right PARQUET:field_id.
        assert_eq!(
            ids,
            vec![
                ("item".to_string(), "100".to_string()),
                ("item".to_string(), "101".to_string()),
            ]
        );
    }

    /// Protocol example case 2: col2: map[int, array[int]], nested ids on col2.
    #[test]
    fn test_apply_schema_nested_ids_map_to_array() {
        let col = map_int_to_list_of_int();
        let input = StructArray::try_new(
            vec![ArrowField::new("col2", col.data_type().clone(), true)].into(),
            vec![col],
            None,
        )
        .unwrap();

        let value_array = crate::schema::ArrayType::new(DataType::INTEGER, true);
        let map = crate::schema::MapType::new(
            DataType::INTEGER,
            DataType::Array(Box::new(value_array)),
            true,
        );
        let target = StructType::new_unchecked([StructField::new(
            "col2",
            DataType::Map(Box::new(map)),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            nested_ids_json([
                ("col2.key", 102),
                ("col2.value", 103),
                ("col2.value.element", 104),
            ]),
        )])]);

        let out = apply_schema_to_struct(&input, &target).unwrap();
        let ids = collect_dt_field_ids(out.data_type());
        // arrow-rs's MapBuilder uses "keys"/"values" for its inner struct fields, and
        // ListBuilder inside uses "item". apply_schema preserves input field names.
        assert_eq!(
            ids,
            vec![
                ("keys".to_string(), "102".to_string()),
                ("values".to_string(), "103".to_string()),
                ("item".to_string(), "104".to_string()),
            ]
        );
    }

    /// `delta.columnMapping.nested.ids` takes precedence over `parquet.field.nested.ids`.
    #[test]
    fn test_apply_schema_nested_ids_internal_key_wins() {
        use crate::arrow::array::builder::{Int32Builder, ListBuilder};
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(1);
        b.append(true);
        let list = Arc::new(b.finish());
        let input = StructArray::try_new(
            vec![ArrowField::new("col1", list.data_type().clone(), true)].into(),
            vec![list],
            None,
        )
        .unwrap();

        let arr = crate::schema::ArrayType::new(DataType::INTEGER, true);
        let target = StructType::new_unchecked([StructField::new(
            "col1",
            DataType::Array(Box::new(arr)),
            true,
        )
        .with_metadata([
            (
                ColumnMetadataKey::ColumnMappingNestedIds.as_ref(),
                nested_ids_json([("col1.element", 42)]),
            ),
            (
                ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
                nested_ids_json([("col1.element", 999)]),
            ),
        ])]);

        let out = apply_schema_to_struct(&input, &target).unwrap();
        let ids = collect_dt_field_ids(out.data_type());
        assert_eq!(ids, vec![("item".to_string(), "42".to_string())]);
    }

    /// Conflict check: the input array's inner element already carries a PARQUET:field_id that
    /// disagrees with the target nested id.
    #[test]
    fn test_apply_schema_nested_ids_conflict_errors() {
        use crate::arrow::array::builder::{Int32Builder, ListBuilder};
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(1);
        b.append(true);
        let list_arr = b.finish();
        // Re-wrap with an `element` field carrying the conflicting PARQUET:field_id.
        let (_, offsets, values, nulls) = list_arr.into_parts();
        let element_field = ArrowField::new("element", ArrowDataType::Int32, true)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "999".to_string())].into());
        let conflicting_list = Arc::new(
            crate::arrow::array::ListArray::try_new(
                Arc::new(element_field),
                offsets,
                values,
                nulls,
            )
            .unwrap(),
        );
        let input = StructArray::try_new(
            vec![ArrowField::new(
                "col1",
                conflicting_list.data_type().clone(),
                true,
            )]
            .into(),
            vec![conflicting_list],
            None,
        )
        .unwrap();

        let arr = crate::schema::ArrayType::new(DataType::INTEGER, true);
        let target = StructType::new_unchecked([StructField::new(
            "col1",
            DataType::Array(Box::new(arr)),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            nested_ids_json([("col1.element", 42)]),
        )])]);

        assert_result_error_with_message(
            apply_schema_to_struct(&input, &target),
            "conflicts with target ID",
        );
    }
}
