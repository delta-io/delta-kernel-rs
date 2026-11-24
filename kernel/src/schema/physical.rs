use super::{ArrayType, DataType, MapType, StructField, StructType};
use crate::KernelResult;
use crate::memory::Owned;
use crate::table_features::ColumnMappingMode;

// Validation belongs to MakePhysical. This walk preserves its materialization semantics without
// using infallible schema transforms or revalidating physical names as logical names.
pub(super) fn materialize(
    schema: &StructType,
    mode: ColumnMappingMode,
) -> KernelResult<StructType> {
    StructType::try_from_unchecked_results(
        schema.fields().map(|field| materialize_field(field, mode)),
    )
}

pub(super) fn materialize_field(
    field: &StructField,
    mode: ColumnMappingMode,
) -> KernelResult<StructField> {
    if field.is_metadata_column() {
        return field.try_clone();
    }
    Ok(StructField {
        name: field.physical_name(mode).to_owned(),
        data_type: materialize_type(field.data_type(), mode)?,
        nullable: field.nullable,
        metadata: field.logical_to_physical_metadata(mode),
    })
}

fn materialize_type(data_type: &DataType, mode: ColumnMappingMode) -> KernelResult<DataType> {
    stacker::maybe_grow(64 * 1024, 2 * 1024 * 1024, || {
        Ok(match data_type {
            DataType::Struct(value) => DataType::Struct(Owned::try_new(materialize(value, mode)?)?),
            DataType::Array(value) => DataType::Array(Owned::try_new(ArrayType {
                type_name: value.type_name.clone(),
                element_type: materialize_type(&value.element_type, mode)?,
                contains_null: value.contains_null,
            })?),
            DataType::Map(value) => DataType::Map(Owned::try_new(MapType {
                type_name: value.type_name.clone(),
                key_type: materialize_type(&value.key_type, mode)?,
                value_type: materialize_type(&value.value_type, mode)?,
                value_contains_null: value.value_contains_null,
            })?),
            // Variant/File layouts and UDT annotations preserve their logical type identity.
            value => value.try_clone()?,
        })
    })
}
