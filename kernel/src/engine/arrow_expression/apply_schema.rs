use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use super::super::arrow_conversion::{
    kernel_flat_parquet_id_to_arrow, lookup_nested_field_id, parquet_field_id_metadata,
    LIST_ARRAY_ROOT, MAP_KEY_DEFAULT, MAP_VALUE_DEFAULT,
};
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
use crate::schema::{ArrayType, ColumnMetadataKey, DataType, MapType, Schema, StructField};

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
            let transformed_col = apply_schema_to_inner(
                &sa_col,
                target_field.data_type(),
                Some(target_field),
                &target_field.name,
            )?;
            let mut arrow_metadata = kernel_flat_parquet_id_to_arrow(target_field)?;
            // `ColumnMetadataKey::ColumnMappingNestedIds` is a kernel-side metadata key, not
            // retained in Arrow; its content is processed by `apply_schema_to_list` /
            // `apply_schema_to_map`.
            arrow_metadata.remove(ColumnMetadataKey::ColumnMappingNestedIds.as_ref());
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
        return Err(Error::internal_error(format!(
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

// Rebuild a [`ListArray`] under the contract of [`apply_schema_to_inner`] (see that fn's doc
// for an example). The inner value field's `PARQUET:field_id` (if any) is looked up at
// `<relative_path>.element` on `nearest_ancestor_struct_field`'s nested-ids JSON.
fn apply_schema_to_list(
    array: &dyn Array,
    target_inner_type: &ArrayType,
    nearest_ancestor_struct_field: Option<&StructField>,
    relative_path: &str,
) -> DeltaResult<ListArray> {
    let Some(la) = array.as_list_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a list but isn't a ListArray",
        ));
    };
    let (arrow_element_field, offset_buffer, arrow_values, nulls) = la.clone().into_parts();

    let element_path = format!("{relative_path}.{LIST_ARRAY_ROOT}");
    let element_id = nearest_ancestor_struct_field
        .map(|f| lookup_nested_field_id(f, &element_path))
        .transpose()?
        .flatten();
    let transformed_arrow_values = apply_schema_to_inner(
        &arrow_values,
        &target_inner_type.element_type,
        nearest_ancestor_struct_field,
        &element_path,
    )?;

    // Bare element type carries no kernel-side metadata; `PARQUET:field_id` is the only
    // metadata to set, looked up from the ancestor's nested-ids JSON.
    let transformed_arrow_element_field = ArrowField::new(
        arrow_element_field.name(),
        transformed_arrow_values.data_type().clone(),
        target_inner_type.contains_null,
    )
    .with_metadata(parquet_field_id_metadata(element_id));
    Ok(ListArray::try_new(
        Arc::new(transformed_arrow_element_field),
        offset_buffer,
        transformed_arrow_values,
        nulls,
    )?)
}

// Rebuild a [`MapArray`] under the contract of [`apply_schema_to_inner`] (see that fn's doc
// for an example). The inner key and value fields' `PARQUET:field_id` (if any) are looked
// up at `<relative_path>.key` and `<relative_path>.value` on `ancestor`'s nested-ids JSON.
fn apply_schema_to_map(
    array: &dyn Array,
    kernel_map_type: &MapType,
    ancestor: Option<&StructField>,
    relative_path: &str,
) -> DeltaResult<MapArray> {
    let Some(ma) = array.as_map_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a map but isn't a MapArray",
        ));
    };
    // Deconstruct the input MapArray and its inner entries struct.
    let (arrow_map_field, offset_buffer, arrow_map_struct_array, nulls, ordered) =
        ma.clone().into_parts();
    let (arrow_input_fields, mut arrow_cols, arrow_struct_nulls) =
        arrow_map_struct_array.into_parts();
    if arrow_cols.len() != 2 || arrow_input_fields.len() != 2 {
        return Err(Error::internal_error(format!(
            "Map entries struct must have exactly 2 columns (key, value), got {}",
            arrow_cols.len()
        )));
    }
    let arrow_value_col = arrow_cols.remove(1);
    let arrow_key_col = arrow_cols.remove(0);
    let arrow_input_key_name = arrow_input_fields[0].name().clone();
    let arrow_input_value_name = arrow_input_fields[1].name().clone();

    // Look up nested field ids for the synthesized key/value fields from the ancestor, and
    //    recurse on each column.
    let key_path = format!("{relative_path}.{MAP_KEY_DEFAULT}");
    let value_path = format!("{relative_path}.{MAP_VALUE_DEFAULT}");
    let key_id = ancestor
        .map(|a| lookup_nested_field_id(a, &key_path))
        .transpose()?
        .flatten();
    let value_id = ancestor
        .map(|a| lookup_nested_field_id(a, &value_path))
        .transpose()?
        .flatten();
    let transformed_arrow_key = apply_schema_to_inner(
        &arrow_key_col,
        &kernel_map_type.key_type,
        ancestor,
        &key_path,
    )?;
    let transformed_arrow_value = apply_schema_to_inner(
        &arrow_value_col,
        &kernel_map_type.value_type,
        ancestor,
        &value_path,
    )?;

    // Bare key/value types carry no kernel-side metadata; `PARQUET:field_id` is the only
    // metadata to set, looked up from the ancestor's nested-ids JSON.
    let arrow_key_field = ArrowField::new(
        arrow_input_key_name,
        transformed_arrow_key.data_type().clone(),
        false,
    )
    .with_metadata(parquet_field_id_metadata(key_id));
    let arrow_value_field = ArrowField::new(
        arrow_input_value_name,
        transformed_arrow_value.data_type().clone(),
        kernel_map_type.value_contains_null,
    )
    .with_metadata(parquet_field_id_metadata(value_id));
    let arrow_entries_struct = StructArray::try_new(
        vec![arrow_key_field.clone(), arrow_value_field.clone()].into(),
        vec![transformed_arrow_key, transformed_arrow_value],
        arrow_struct_nulls,
    )?;
    let arrow_entries_field = ArrowField::new(
        arrow_map_field.name(),
        ArrowDataType::Struct(vec![arrow_key_field, arrow_value_field].into()),
        arrow_map_field.is_nullable(),
    );
    Ok(MapArray::try_new(
        Arc::new(arrow_entries_field),
        offset_buffer,
        arrow_entries_struct,
        nulls,
        ordered,
    )?)
}

// apply `schema` to `array`. This handles renaming, and adjusting nullability and metadata. if the
// actual data types don't match, this will return an error.
pub(crate) fn apply_schema_to(array: &ArrayRef, schema: &DataType) -> DeltaResult<ArrayRef> {
    apply_schema_to_inner(array, schema, None, "")
}

/// Recursive worker for [`apply_schema_to`]. Rebuilds `array` recursively so that its schema is
/// aligned with the kernel-side schema. This will try to change the name, nullability, and
/// metadata of the input `array` to match the kernel-side schema. Data values, offsets, and null
/// buffers pass through unchanged. Specifically:
///
/// - Name: use the kernel-side name when kernel has one (e.g. on a [`StructField`]); retain the
///   input Arrow name when kernel has none (e.g. on a list `element` or map `key`/`value`; kernel's
///   [`ArrayType::element_type`] / [`MapType::key_type`] / [`MapType::value_type`] are bare
///   [`DataType`]s with no name).
/// - Nullability: use the kernel-side nullability.
/// - Metadata: take the kernel side for most keys; translate the few that have an Arrow-native form
///   (e.g. `parquet.field.id` -> `PARQUET:field_id`). Input Arrow metadata is dropped.
///
/// `ancestor` is the nearest ancestor kernel [`StructField`]; its metadata holds the nested-ids
/// JSON map (`None` at the top level). `relative_path` is the dot-chained path rooted at
/// `ancestor`'s name (empty at the top level).
///
/// # Example
///
/// `arr_in_map: map<int, array<int>>`. Kernel side dictates the top-level field's name,
/// nullability, and metadata. Inner field names
/// are producer-defined and pass through unchanged; their nullability comes from the kernel
/// side.
///
/// Kernel [`StructField`]:
///
/// ```json
/// {
///   "name": "arr_in_map",
///   "type": {
///     "type": "map",
///     "keyType": "integer",
///     "valueType": {"type": "array", "elementType": "integer", "containsNull": true},
///     "valueContainsNull": true
///   },
///   "nullable": true,
///   "metadata": {
///     "parquet.field.id": 1,
///     "delta.columnMapping.nested.ids": {
///       "arr_in_map.key": 100,
///       "arr_in_map.value": 101,
///       "arr_in_map.value.element": 102
///     }
///   }
/// }
/// ```
///
/// Input Arrow schema (custom names, mismatched nullability, stale field id):
///
/// ```json
/// {
///   "name": "stale_name", "type": "map", "nullable": false,
///   "metadata": {"PARQUET:field_id": "999"},
///   "entries": {
///     "name": "custom_entries", "type": "struct", "nullable": false,
///     "fields": [
///       {"name": "custom_key", "type": "int32", "nullable": false},
///       {"name": "custom_value", "type": "list", "nullable": false,
///        "element": {"name": "custom_item", "type": "int32", "nullable": false}}
///     ]
///   }
/// }
/// ```
///
/// Rebuilt Arrow schema:
///
/// ```json
/// {
///   "name": "arr_in_map", "type": "map", "nullable": true,
///   "metadata": {"PARQUET:field_id": "1"},
///   "entries": {
///     "name": "custom_entries", "type": "struct", "nullable": false,
///     "fields": [
///       {"name": "custom_key", "type": "int32", "nullable": false,
///        "metadata": {"PARQUET:field_id": "100"}},
///       {"name": "custom_value", "type": "list", "nullable": true,
///        "metadata": {"PARQUET:field_id": "101"},
///        "element": {"name": "custom_item", "type": "int32", "nullable": true,
///                    "metadata": {"PARQUET:field_id": "102"}}}
///     ]
///   }
/// }
/// ```
fn apply_schema_to_inner(
    array: &ArrayRef,
    schema: &DataType,
    ancestor: Option<&StructField>,
    relative_path: &str,
) -> DeltaResult<ArrayRef> {
    use DataType::*;
    let array: ArrayRef = match schema {
        Struct(stype) => Arc::new(apply_schema_to_struct(array, stype)?),
        Array(atype) => Arc::new(apply_schema_to_list(array, atype, ancestor, relative_path)?),
        Map(mtype) => Arc::new(apply_schema_to_map(array, mtype, ancestor, relative_path)?),
        _ => {
            ensure_data_types(schema, array.data_type(), ValidationMode::Full)?;
            array.clone()
        }
    };
    Ok(array)
}

#[cfg(test)]
mod apply_schema_validation_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{Int32Array, RecordBatch, StructArray};
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
    use crate::utils::test_utils::{
        array_in_map_arrow_data_without_field_ids, array_in_map_kernel_schema,
        array_in_map_with_field_ids, assert_result_error_with_message,
        collect_arrow_field_metadata, complex_nested_with_field_ids,
    };

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

    // === Nested-id propagation tests ===

    #[test]
    fn test_apply_schema_threads_nested_ids_onto_arrow_schema() {
        let meta_key = ColumnMetadataKey::ColumnMappingNestedIds.as_ref();
        let fixture = complex_nested_with_field_ids(meta_key);
        let kernel_type = DataType::Struct(Box::new(fixture.kernel_schema));
        let result = apply_schema(&fixture.input_arrow_data, &kernel_type).unwrap();

        assert_eq!(
            result.schema().as_ref(),
            &fixture.expected_arrow_schema,
            "apply_schema should attach nested field ids to synthesized map/array fields",
        );
    }

    /// Custom field names on the input arrow schema should survive `apply_schema`. The
    /// metadata from the input arrow schema is replaced by kernel-derived metadata.
    #[test]
    fn test_apply_schema_retains_field_names() {
        let one = |k: &str, v: &str| HashMap::from([(k.to_string(), v.to_string())]);
        let input = array_in_map_arrow_data_with_custom_names_and_meta(
            one("custom.key", "key_val"),
            one("custom.value", "value_val"),
            one("custom.element", "elem_val"),
        );
        let kernel_schema =
            array_in_map_with_field_ids(ColumnMetadataKey::ColumnMappingNestedIds.as_ref());
        let result = apply_schema(&input, &DataType::Struct(Box::new(kernel_schema))).unwrap();

        let result_schema = result.schema();
        let ArrowDataType::Map(entries_field, _) = result_schema.field(0).data_type() else {
            panic!("array_in_map should remain a map");
        };
        assert_eq!(entries_field.name(), "custom_entries");
        let ArrowDataType::Struct(fields) = entries_field.data_type() else {
            panic!("map entries should remain a struct");
        };
        assert_eq!(fields[0].name(), "custom_key");
        assert_eq!(fields[1].name(), "custom_value");
        let ArrowDataType::List(element_field) = fields[1].data_type() else {
            panic!("map value should remain a list");
        };
        assert_eq!(element_field.name(), "custom_item");

        // Kernel-derived `PARQUET:field_id` is the *only* metadata on each synthesized field
        // (input metadata is replaced wholesale).
        let expect_only_field_id = |f: &ArrowField, id: &str| {
            assert_eq!(
                f.metadata(),
                &HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]),
            );
        };
        expect_only_field_id(&fields[0], "100");
        expect_only_field_id(&fields[1], "101");
        expect_only_field_id(element_field, "102");
    }

    #[rstest]
    /// The nested ids JSON map is missing (i.e. `delta.columnMapping.nested.ids` is not present).
    #[case::without_nested_id_metadata(array_in_map_kernel_schema(std::iter::empty::<(
        String,
        MetadataValue,
    )>()), /* expected_field_ids */ &[])]
    /// Only some of the nested ids are present.
    #[case::only_partial_nested_ids_match(array_in_map_kernel_schema([(
        ColumnMetadataKey::ColumnMappingNestedIds.as_ref().to_string(),
        MetadataValue::Other(test_utils::nested_ids_json(&[
            ("array_in_map.key", 100),
            ("array_in_map.value.element", 102),
            ("array_in_map.notTheKey", 999),
        ])),
    )]), &[("element", "102"), ("key", "100")])]
    fn test_apply_schema_sets_field_ids_for_matched_nested_ids_only(
        #[case] schema: StructType,
        #[case] expected_field_ids: &[(&str, &str)],
    ) {
        let kernel_type = DataType::Struct(Box::new(schema));
        let result =
            apply_schema(&array_in_map_arrow_data_without_field_ids(), &kernel_type).unwrap();
        let field_ids: HashMap<String, String> =
            collect_arrow_field_metadata(result.schema().as_ref(), PARQUET_FIELD_ID_META_KEY)
                .into_iter()
                .collect();

        assert_eq!(field_ids.len(), expected_field_ids.len());
        for (field_name, expected_id) in expected_field_ids {
            assert_eq!(
                field_ids.get(*field_name).map(String::as_str),
                Some(*expected_id),
            );
        }
    }

    #[rstest]
    #[case::not_json_object(
        MetadataValue::String("not a json object".to_string()),
        "must be a JSON object",
    )]
    #[case::entry_not_an_integer(
        MetadataValue::Other(serde_json::json!({ "array_in_map.key": "oops" })),
        "must be an integer",
    )]
    fn test_apply_schema_invalid_nested_ids_metadata_errors(
        #[case] value: MetadataValue,
        #[case] expected_error_substring: &str,
    ) {
        let schema = array_in_map_kernel_schema([(
            ColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_string(),
            value,
        )]);
        let kernel_type = DataType::Struct(Box::new(schema));

        assert_result_error_with_message(
            apply_schema(&array_in_map_arrow_data_without_field_ids(), &kernel_type),
            expected_error_substring,
        );
    }

    /// Build a StructArray for `array_in_map: map<int, list<int>>` with custom Arrow names and
    /// caller-provided metadata.
    fn array_in_map_arrow_data_with_custom_names_and_meta(
        key_metadata: HashMap<String, String>,
        value_metadata: HashMap<String, String>,
        element_metadata: HashMap<String, String>,
    ) -> StructArray {
        let element_field = ArrowField::new("custom_item", ArrowDataType::Int32, true)
            .with_metadata(element_metadata);
        let key_field =
            ArrowField::new("custom_key", ArrowDataType::Int32, false).with_metadata(key_metadata);
        let value_field = ArrowField::new(
            "custom_value",
            ArrowDataType::List(Arc::new(element_field)),
            true,
        )
        .with_metadata(value_metadata);
        let entries_field = ArrowField::new(
            "custom_entries",
            ArrowDataType::Struct(vec![key_field, value_field].into()),
            false,
        );
        let array_in_map_field = ArrowField::new(
            "array_in_map",
            ArrowDataType::Map(Arc::new(entries_field), false),
            true,
        );
        let input_batch =
            RecordBatch::new_empty(Arc::new(ArrowSchema::new(vec![array_in_map_field])));
        StructArray::try_new(
            input_batch.schema().fields.clone(),
            input_batch.columns().to_vec(),
            None,
        )
        .unwrap()
    }
}
