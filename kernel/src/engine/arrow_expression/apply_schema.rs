use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MapArray, RecordBatch, StructArray,
};
use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

use super::super::arrow_utils::make_arrow_error;
use crate::engine::ensure_data_types::ensure_data_types;
use crate::error::{DeltaResult, Error};
use crate::schema::{ArrayType, DataType, MapType, Schema, StructField};

// Apply a schema to an array. The array _must_ be a `StructArray`. Returns a `RecordBatch where the
// names of fields, nullable, and metadata in the struct have been transformed to match those in
// schema specified by `schema`
pub(crate) fn apply_schema(array: &dyn Array, schema: &DataType) -> DeltaResult<RecordBatch> {
    let DataType::Struct(struct_schema) = schema else {
        return Err(Error::generic(
            "apply_schema at top-level must be passed a struct schema",
        ));
    };
    let applied = apply_schema_to_struct(array, struct_schema)?;
    let (fields, columns, nulls) = applied.into_parts();
    if let Some(nulls) = nulls {
        if nulls.null_count() != 0 {
            return Err(Error::invalid_struct_data(
                "Top-level nulls in struct are not supported",
            ));
        }
    }
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
    target_fields: impl ExactSizeIterator<Item = impl Borrow<StructField>>,
) -> DeltaResult<StructArray> {
    let (_, arrow_cols, nulls) = struct_array.clone().into_parts();
    let input_col_count = arrow_cols.len();

    // Validate field counts before processing
    if input_col_count != target_fields.len() {
        return Err(Error::InternalError(format!(
            "Field count mismatch: input struct has {} columns, but target schema has {} fields",
            input_col_count,
            target_fields.len()
        )));
    }

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
    use crate::arrow::array::{Int32Array, StructArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::schema::{DataType, StructField, StructType};
    use std::sync::Arc;

    #[test]
    fn test_apply_schema_input_more_fields_than_target() {
        // Input struct with 3 fields, target schema with 2 fields
        let input_array = create_test_struct_array_3_fields();
        let target_schema = create_target_schema_2_fields();

        // This should fail - input has more fields than target
        let result = apply_schema_to_struct(&input_array, &target_schema);
        assert!(
            result.is_err(),
            "Should fail when input has more fields than target"
        );

        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Field count mismatch"),
            "Error should mention field count mismatch"
        );
    }

    #[test]
    fn test_apply_schema_target_more_fields_than_input() {
        // Input struct with 2 fields, target schema with 3 fields
        let input_array = create_test_struct_array_2_fields();
        let target_schema = create_target_schema_3_fields();

        // This should fail - target has more fields than input
        let result = apply_schema_to_struct(&input_array, &target_schema);
        assert!(
            result.is_err(),
            "Should fail when target has more fields than input"
        );

        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Field count mismatch"),
            "Error should mention field count mismatch"
        );
    }

    #[test]
    fn test_apply_schema_matching_field_counts() {
        // Input struct with 2 fields, target schema with 2 fields
        let input_array = create_test_struct_array_2_fields();
        let target_schema = create_target_schema_2_fields();

        // This should succeed - matching field counts
        let result = apply_schema_to_struct(&input_array, &target_schema);
        assert!(result.is_ok(), "Should succeed when field counts match");
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

    fn create_test_struct_array_3_fields() -> StructArray {
        let field1 = ArrowField::new("a", ArrowDataType::Int32, false);
        let field2 = ArrowField::new("b", ArrowDataType::Int32, false);
        let field3 = ArrowField::new("c", ArrowDataType::Int32, false);
        let schema = ArrowSchema::new(vec![field1, field2, field3]);

        let a_data = Int32Array::from(vec![1, 2, 3]);
        let b_data = Int32Array::from(vec![4, 5, 6]);
        let c_data = Int32Array::from(vec![7, 8, 9]);

        StructArray::try_new(
            schema.fields.clone(),
            vec![Arc::new(a_data), Arc::new(b_data), Arc::new(c_data)],
            None,
        )
        .unwrap()
    }

    fn create_target_schema_2_fields() -> StructType {
        StructType::new([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::INTEGER, false),
        ])
    }

    fn create_target_schema_3_fields() -> StructType {
        StructType::new([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::INTEGER, false),
            StructField::new("c", DataType::INTEGER, false),
        ])
    }
}
