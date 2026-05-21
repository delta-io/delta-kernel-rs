//! Schema fixtures for column mappings.
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};

pub fn same_leaf_phy_name_under_different_parents() -> StructType {
    let parent1_children = StructType::new_unchecked([cm_field("a", 2, "x", DataType::INTEGER)]);
    let parent2_children = StructType::new_unchecked([cm_field("a", 4, "x", DataType::INTEGER)]);
    StructType::new_unchecked([
        cm_field(
            "outer1",
            1,
            "outer1",
            DataType::Struct(Box::new(parent1_children)),
        ),
        cm_field(
            "outer2",
            3,
            "outer2",
            DataType::Struct(Box::new(parent2_children)),
        ),
    ])
}

pub fn nested_field_with_same_phy_path() -> StructType {
    let innermost = StructType::new_unchecked([
        cm_field("a", 3, "x", DataType::INTEGER),
        cm_field("b", 4, "x", DataType::INTEGER),
    ]);
    let arr_of_struct = ArrayType::new(DataType::Struct(Box::new(innermost)), true);
    let map_to_arr = MapType::new(DataType::STRING, arr_of_struct, true);
    let inner_struct = StructType::new_unchecked([cm_field("inner", 2, "inner", map_to_arr)]);
    StructType::new_unchecked([cm_field(
        "outer",
        1,
        "outer",
        DataType::Struct(Box::new(inner_struct)),
    )])
}

fn cm_field(name: &str, id: i64, phys: &str, ty: impl Into<DataType>) -> StructField {
    StructField::new(name, ty, true).with_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(id),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String(phys.to_string()),
        ),
    ])
}
