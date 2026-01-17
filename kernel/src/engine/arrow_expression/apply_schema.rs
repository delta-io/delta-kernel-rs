use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::arrow::array::{
    make_array, Array, ArrayRef, AsArray, ListArray, MapArray, RecordBatch, StructArray,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

use super::super::arrow_utils::make_arrow_error;
use crate::engine::ensure_data_types::ensure_data_types;
use crate::error::{DeltaResult, Error};
use crate::schema::{ArrayType, DataType, MapType, Schema, StructField};

// Apply a schema to an array. The array _must_ be a `StructArray`. Returns a `RecordBatch where the
// names of fields, nullable, and metadata in the struct have been transformed to match those in
// schema specified by `schema`
//
// If the struct array has top-level nulls (rows where the entire struct is null), those nulls are
// propagated to all child columns. This allows expressions like `add.stats_parsed` to return null
// for rows where `add` is null, rather than erroring.
pub(crate) fn apply_schema(array: &dyn Array, schema: &DataType) -> DeltaResult<RecordBatch> {
    let DataType::Struct(struct_schema) = schema else {
        return Err(Error::generic(
            "apply_schema at top-level must be passed a struct schema",
        ));
    };
    let applied = apply_schema_to_struct(array, struct_schema)?;
    let (fields, columns, nulls) = applied.into_parts();

    // If there are top-level nulls, propagate them to each child column.
    // This handles cases where we're extracting a struct from a nullable parent
    // (e.g., `add.stats_parsed` where `add` can be null for non-add rows).
    let columns = if let Some(ref struct_nulls) = nulls {
        if struct_nulls.null_count() > 0 {
            columns
                .into_iter()
                .map(|column| propagate_nulls_to_column(&column, struct_nulls))
                .collect()
        } else {
            columns
        }
    } else {
        columns
    };

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

/// Propagate a parent null buffer to a column, combining it with the column's existing nulls.
fn propagate_nulls_to_column(column: &ArrayRef, parent_nulls: &NullBuffer) -> ArrayRef {
    let data = column.to_data();
    let combined_nulls = NullBuffer::union(Some(parent_nulls), data.nulls());
    let builder = data.into_builder().nulls(combined_nulls);
    // SAFETY: We're only adding more nulls to an existing valid array.
    // The union can only grow the set of NULL rows, preserving data validity.
    let data = unsafe { builder.build_unchecked() };
    make_array(data)
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
    let (_, arrow_cols, nulls) = struct_array.clone().into_parts();
    let input_col_count = arrow_cols.len();
    let result_iter =
        arrow_cols
            .into_iter()
            .zip(target_fields)
            .map(|(sa_col, target_field)| -> DeltaResult<_> {
                let target_field = target_field.borrow();
                let transformed_col = apply_schema_to(&sa_col, target_field.data_type())?;
                let transformed_field = new_field_with_metadata(
                    &target_field.name,
                    transformed_col.data_type(),
                    target_field.nullable,
                    Some(target_field.metadata_with_string_values()),
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
) -> DeltaResult<ListArray> {
    let Some(la) = array.as_list_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a list but isn't a ListArray",
        ));
    };
    let (field, offset_buffer, values, nulls) = la.clone().into_parts();

    let transformed_values = apply_schema_to(&values, &target_inner_type.element_type)?;
    let transformed_field = ArrowField::new(
        field.name(),
        transformed_values.data_type().clone(),
        target_inner_type.contains_null,
    );
    Ok(ListArray::try_new(
        Arc::new(transformed_field),
        offset_buffer,
        transformed_values,
        nulls,
    )?)
}

// deconstruct a map, and rebuild it with the specified target kernel type
fn apply_schema_to_map(array: &dyn Array, kernel_map_type: &MapType) -> DeltaResult<MapArray> {
    let Some(ma) = array.as_map_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a map but isn't a MapArray",
        ));
    };
    let (map_field, offset_buffer, map_struct_array, nulls, ordered) = ma.clone().into_parts();
    let target_fields = map_struct_array
        .fields()
        .iter()
        .zip([&kernel_map_type.key_type, &kernel_map_type.value_type])
        .zip([false, kernel_map_type.value_contains_null])
        .map(|((arrow_field, target_type), nullable)| {
            StructField::new(arrow_field.name(), target_type.clone(), nullable)
        });

    // Arrow puts the key type/val as the first field/col and the value type/val as the second. So
    // we just transform like a 'normal' struct, but we know there are two fields/cols and we
    // specify the key/value types as the target type iterator.
    let transformed_map_struct_array = transform_struct(&map_struct_array, target_fields)?;

    let transformed_map_field = ArrowField::new(
        map_field.name().clone(),
        transformed_map_struct_array.data_type().clone(),
        map_field.is_nullable(),
    );
    Ok(MapArray::try_new(
        Arc::new(transformed_map_field),
        offset_buffer,
        transformed_map_struct_array,
        nulls,
        ordered,
    )?)
}

// apply `schema` to `array`. This handles renaming, and adjusting nullability and metadata. if the
// actual data types don't match, this will return an error
pub(crate) fn apply_schema_to(array: &ArrayRef, schema: &DataType) -> DeltaResult<ArrayRef> {
    use DataType::*;
    let array: ArrayRef = match schema {
        Struct(stype) => Arc::new(apply_schema_to_struct(array, stype)?),
        Array(atype) => Arc::new(apply_schema_to_list(array, atype)?),
        Map(mtype) => Arc::new(apply_schema_to_map(array, mtype)?),
        _ => {
            ensure_data_types(schema, array.data_type(), true)?;
            array.clone()
        }
    };
    Ok(array)
}

#[cfg(test)]
mod apply_schema_validation_tests {
    use super::*;

    use std::sync::Arc;

    use crate::arrow::array::{Int32Array, StructArray};
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::schema::{DataType, StructField, StructType};

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

    /// Test that top-level struct nulls are propagated to child columns.
    ///
    /// This simulates a Delta log scenario where each row is one action type (add, remove, etc.).
    /// When extracting `add.stats_parsed`, rows where `add` is null (e.g., remove actions) should
    /// return null for all child columns rather than erroring.
    #[test]
    fn test_apply_schema_propagates_top_level_nulls() {
        // Create a struct array with 4 rows where rows 1 and 3 have top-level nulls.
        // This simulates: [add_action, remove_action, add_action, remove_action]
        // where remove_action rows have null for the entire struct.
        let field_a = ArrowField::new("a", ArrowDataType::Int32, true);
        let field_b = ArrowField::new("b", ArrowDataType::Int32, true);
        let schema = ArrowSchema::new(vec![field_a, field_b]);

        // Child column data - note these have values even for rows that will be struct-null
        // because Arrow stores child data independently of the struct's null buffer.
        let a_data = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let b_data = Int32Array::from(vec![Some(10), Some(20), Some(30), Some(40)]);

        // Top-level nulls: rows 0 and 2 are valid, rows 1 and 3 are null
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

        // Apply schema - this should propagate top-level nulls to child columns
        let result = apply_schema(&struct_array, &target_schema).unwrap();

        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.num_columns(), 2);

        // Verify column "a" has nulls propagated from the struct's null buffer
        let col_a = result.column(0);
        assert!(col_a.is_valid(0), "Row 0 should be valid");
        assert!(col_a.is_null(1), "Row 1 should be null (struct was null)");
        assert!(col_a.is_valid(2), "Row 2 should be valid");
        assert!(col_a.is_null(3), "Row 3 should be null (struct was null)");

        // Verify column "b" has nulls propagated from the struct's null buffer
        let col_b = result.column(1);
        assert!(col_b.is_valid(0), "Row 0 should be valid");
        assert!(col_b.is_null(1), "Row 1 should be null (struct was null)");
        assert!(col_b.is_valid(2), "Row 2 should be valid");
        assert!(col_b.is_null(3), "Row 3 should be null (struct was null)");

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
}
