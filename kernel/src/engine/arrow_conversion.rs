//! Conversions from kernel schema types to arrow schema types.

use std::collections::HashMap;
use std::sync::Arc;

use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use itertools::Itertools;

use crate::error::Error;
use crate::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, PrimitiveType, StructField,
    StructType,
};

pub(crate) const LIST_ARRAY_ROOT: &str = "element";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

/// Converts kernel [`StructField`] metadata to Arrow field metadata format.
///
/// Specifically, this transforms the `"parquet.field.id"` key (used by kernel/delta-spark) to
/// `"PARQUET:field_id"` (the native Parquet/Arrow metadata key), enabling correct field ID
/// handling by the Arrow/Parquet writer.
pub(crate) fn kernel_metadata_to_arrow_metadata(
    field: &StructField,
) -> Result<HashMap<String, String>, ArrowError> {
    field
        .metadata()
        .iter()
        .map(|(key, val)| {
            let transformed_key = if key == ColumnMetadataKey::ParquetFieldId.as_ref() {
                PARQUET_FIELD_ID_META_KEY.to_string()
            } else {
                key.clone()
            };
            match val {
                MetadataValue::String(s) => Ok((transformed_key, s.clone())),
                _ => Ok((
                    transformed_key,
                    serde_json::to_string(val).map_err(|e| ArrowError::JsonError(e.to_string()))?,
                )),
            }
        })
        .collect()
}

/// Convert a kernel type into an arrow type (automatically implemented for all types that
/// implement [`TryFromKernel`])
pub trait TryIntoArrow<ArrowType> {
    fn try_into_arrow(self) -> Result<ArrowType, ArrowError>;
}

/// Convert an arrow type into a kernel type (a similar [`TryIntoKernel`] trait is automatically
/// implemented for all types that implement [`TryFromArrow`])
pub trait TryFromArrow<ArrowType>: Sized {
    fn try_from_arrow(t: ArrowType) -> Result<Self, ArrowError>;
}

/// Convert an arrow type into a kernel type (automatically implemented for all types that
/// implement [`TryFromArrow`])
pub trait TryIntoKernel<KernelType> {
    fn try_into_kernel(self) -> Result<KernelType, ArrowError>;
}

/// Convert a kernel type into an arrow type (a similar [`TryIntoArrow`] trait is automatically
/// implemented for all types that implement [`TryFromKernel`])
pub trait TryFromKernel<KernelType>: Sized {
    fn try_from_kernel(t: KernelType) -> Result<Self, ArrowError>;
}

impl<KernelType, ArrowType> TryIntoArrow<ArrowType> for KernelType
where
    ArrowType: TryFromKernel<KernelType>,
{
    fn try_into_arrow(self) -> Result<ArrowType, ArrowError> {
        ArrowType::try_from_kernel(self)
    }
}

impl<KernelType, ArrowType> TryIntoKernel<KernelType> for ArrowType
where
    KernelType: TryFromArrow<ArrowType>,
{
    fn try_into_kernel(self) -> Result<KernelType, ArrowError> {
        KernelType::try_from_arrow(self)
    }
}

impl TryFromKernel<&StructType> for ArrowSchema {
    fn try_from_kernel(s: &StructType) -> Result<Self, ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(|f| f.try_into_arrow()).try_collect()?;
        Ok(ArrowSchema::new(fields))
    }
}

impl TryFromKernel<&StructField> for ArrowField {
    fn try_from_kernel(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = kernel_metadata_to_arrow_metadata(f)?;
        let field = ArrowField::new(f.name(), f.data_type().try_into_arrow()?, f.is_nullable())
            .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFromKernel<&ArrayType> for ArrowField {
    fn try_from_kernel(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            LIST_ARRAY_ROOT,
            a.element_type().try_into_arrow()?,
            a.contains_null(),
        ))
    }
}

impl TryFromKernel<&MapType> for ArrowField {
    fn try_from_kernel(a: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(MAP_KEY_DEFAULT, a.key_type().try_into_arrow()?, false),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        a.value_type().try_into_arrow()?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false, // always non-null
        ))
    }
}

impl TryFromKernel<&DataType> for ArrowDataType {
    fn try_from_kernel(t: &DataType) -> Result<Self, ArrowError> {
        match t {
            DataType::Primitive(p) => {
                match p {
                    PrimitiveType::String => Ok(ArrowDataType::Utf8),
                    PrimitiveType::Long => Ok(ArrowDataType::Int64), // undocumented type
                    PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                    PrimitiveType::Short => Ok(ArrowDataType::Int16),
                    PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                    PrimitiveType::Float => Ok(ArrowDataType::Float32),
                    PrimitiveType::Double => Ok(ArrowDataType::Float64),
                    PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                    PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                    PrimitiveType::Decimal(dtype) => Ok(ArrowDataType::Decimal128(
                        dtype.precision(),
                        dtype.scale() as i8, // 0..=38
                    )),
                    PrimitiveType::Date => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days since 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    // TODO: https://github.com/delta-io/delta/issues/643
                    PrimitiveType::Timestamp => Ok(ArrowDataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    )),
                    PrimitiveType::TimestampNtz => {
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                }
            }
            DataType::Struct(s) => Ok(ArrowDataType::Struct(
                s.fields()
                    .map(TryIntoArrow::try_into_arrow)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into_arrow()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(
                Arc::new(m.as_ref().try_into_arrow()?),
                false,
            )),
            DataType::Variant(s) => {
                if *t == DataType::unshredded_variant() {
                    Ok(ArrowDataType::Struct(
                        s.fields()
                            .map(TryIntoArrow::try_into_arrow)
                            .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                            .into(),
                    ))
                } else {
                    Err(ArrowError::SchemaError(format!(
                        "Incorrect Variant Schema: {t}. Only the unshredded variant schema is supported right now."
                    )))
                }
            }
        }
    }
}

impl TryFromArrow<&ArrowSchema> for StructType {
    fn try_from_arrow(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        StructType::try_from_results(
            arrow_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().try_into_kernel()),
        )
        .map_err(|e| ArrowError::from_external_error(e.into()))
    }
}

impl TryFromArrow<ArrowSchemaRef> for StructType {
    fn try_from_arrow(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        arrow_schema.as_ref().try_into_kernel()
    }
}

impl TryFromArrow<&ArrowField> for StructField {
    fn try_from_arrow(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_arrow(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| {
            // Transform "PARQUET:field_id" to "parquet.field.id" when reading from Parquet
            let transformed_key = if k == PARQUET_FIELD_ID_META_KEY {
                ColumnMetadataKey::ParquetFieldId.as_ref().to_string()
            } else {
                k.clone()
            };
            (transformed_key, v)
        })))
    }
}

impl TryFromArrow<&ArrowDataType> for DataType {
    fn try_from_arrow(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(DataType::STRING),
            ArrowDataType::LargeUtf8 => Ok(DataType::STRING),
            ArrowDataType::Utf8View => Ok(DataType::STRING),
            ArrowDataType::Int64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::Int32 => Ok(DataType::INTEGER),
            ArrowDataType::Int16 => Ok(DataType::SHORT),
            ArrowDataType::Int8 => Ok(DataType::BYTE),
            ArrowDataType::UInt64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::UInt32 => Ok(DataType::INTEGER),
            ArrowDataType::UInt16 => Ok(DataType::SHORT),
            ArrowDataType::UInt8 => Ok(DataType::BYTE),
            ArrowDataType::Float32 => Ok(DataType::FLOAT),
            ArrowDataType::Float64 => Ok(DataType::DOUBLE),
            ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
            ArrowDataType::Binary => Ok(DataType::BINARY),
            ArrowDataType::FixedSizeBinary(_) => Ok(DataType::BINARY),
            ArrowDataType::LargeBinary => Ok(DataType::BINARY),
            ArrowDataType::BinaryView => Ok(DataType::BINARY),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(ArrowError::from_external_error(
                        Error::invalid_decimal("Negative scales are not supported in Delta").into(),
                    ));
                };
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| ArrowError::from_external_error(e.into()))
            }
            ArrowDataType::Date32 => Ok(DataType::DATE),
            ArrowDataType::Date64 => Ok(DataType::DATE),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::TIMESTAMP)
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::TIMESTAMP)
            }
            ArrowDataType::Struct(fields) => DataType::try_struct_type_from_results(
                fields.iter().map(|field| field.as_ref().try_into_kernel()),
            )
            .map_err(|e| ArrowError::from_external_error(e.into())),
            ArrowDataType::List(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_kernel()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::ListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_kernel()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeList(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_kernel()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_kernel()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                (*field).data_type().try_into_kernel()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from_arrow(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from_arrow(struct_fields[1].data_type())?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(MapType::new(key_type, value_type, value_type_nullable).into())
                } else {
                    unreachable!("DataType::Map should contain a struct field child");
                }
            }
            // Dictionary types are just an optimized in-memory representation of an array.
            // Schema-wise, they are the same as the value type.
            ArrowDataType::Dictionary(_, value_type) => {
                Ok(value_type.as_ref().try_into_kernel()?)
            }
            s => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::arrow_conversion::ArrowField;
    use crate::engine::arrow_data::unshredded_variant_arrow_type;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, SchemaTransform,
        StructField, StructType,
    };
    use crate::DeltaResult;
    use std::collections::HashMap;

    #[test]
    fn test_metadata_string_conversion() -> DeltaResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("description", "hello world".to_owned());
        let struct_field = StructField::not_null("name", DataType::STRING).with_metadata(metadata);

        let arrow_field = ArrowField::try_from_kernel(&struct_field)?;
        let new_metadata = arrow_field.metadata();

        assert_eq!(
            new_metadata.get("description").unwrap(),
            &"hello world".to_owned()
        );
        Ok(())
    }

    #[test]
    fn test_variant_shredded_type_fail() -> DeltaResult<()> {
        let unshredded_variant = DataType::unshredded_variant();
        let unshredded_variant_arrow = ArrowDataType::try_from_kernel(&unshredded_variant)?;
        assert!(unshredded_variant_arrow == unshredded_variant_arrow_type());
        let shredded_variant = DataType::variant_type([
            StructField::nullable("metadata", DataType::BINARY),
            StructField::nullable("value", DataType::BINARY),
            StructField::nullable("typed_value", DataType::INTEGER),
        ])?;
        let shredded_variant_arrow = ArrowDataType::try_from_kernel(&shredded_variant);
        assert!(shredded_variant_arrow
            .unwrap_err()
            .to_string()
            .contains("Incorrect Variant Schema"));
        Ok(())
    }

    /// Helper visitor to collect all field IDs from a kernel StructType
    struct FieldIdCollector {
        field_ids: Vec<(String, String)>, // (field_name, field_id)
    }

    impl FieldIdCollector {
        fn new() -> Self {
            Self {
                field_ids: Vec::new(),
            }
        }
    }

    impl<'a> crate::schema::SchemaTransform<'a> for FieldIdCollector {
        fn transform_struct_field(
            &mut self,
            field: &'a crate::schema::StructField,
        ) -> Option<std::borrow::Cow<'a, crate::schema::StructField>> {
            // Collect field ID if present
            if let Some(field_id) = field
                .metadata()
                .get(crate::schema::ColumnMetadataKey::ParquetFieldId.as_ref())
            {
                self.field_ids
                    .push((field.name().to_string(), field_id.to_string()));
            }
            // Recurse into nested types
            self.recurse_into_struct_field(field)
        }
    }

    /// Helper function to recursively collect field IDs from an Arrow schema
    fn collect_arrow_field_ids(schema: &ArrowSchema, metadata_key: &str) -> Vec<(String, String)> {
        let mut field_ids = Vec::new();

        fn collect_from_fields(
            fields: &[std::sync::Arc<ArrowField>],
            metadata_key: &str,
            field_ids: &mut Vec<(String, String)>,
        ) {
            for field in fields {
                collect_from_field(field, metadata_key, field_ids);
            }
        }

        fn collect_from_field(
            field: &ArrowField,
            metadata_key: &str,
            field_ids: &mut Vec<(String, String)>,
        ) {
            // Collect field ID from this field
            if let Some(id) = field.metadata().get(metadata_key) {
                field_ids.push((field.name().clone(), id.clone()));
            }

            // Recurse into nested types
            match field.data_type() {
                ArrowDataType::Struct(fields) => {
                    collect_from_fields(fields, metadata_key, field_ids);
                }
                ArrowDataType::List(element)
                | ArrowDataType::LargeList(element)
                | ArrowDataType::FixedSizeList(element, _) => {
                    collect_from_field(element, metadata_key, field_ids);
                }
                ArrowDataType::Map(entry_field, _) => {
                    collect_from_field(entry_field, metadata_key, field_ids);
                }
                _ => {}
            }
        }

        collect_from_fields(schema.fields(), metadata_key, &mut field_ids);
        field_ids
    }

    #[test]
    fn test_recursive_field_id_transformation() -> DeltaResult<()> {
        // Create a complex nested structure with field IDs at multiple levels:
        // top_struct {
        //   simple_field: int (field_id=1)
        //   nested_struct: struct { (field_id=2)
        //     inner_field: string (field_id=3)
        //   }
        //   array_field: array<struct { (field_id=4)
        //     array_item: int (field_id=5)
        //   }>
        //   map_field: map<struct { (field_id=6)
        //     map_key_field: string (field_id=7)
        //   }, struct {
        //     map_value_field: int (field_id=8)
        //   }>
        // }

        // Build nested struct
        let inner_struct_type = StructType::try_new(vec![StructField::new(
            "inner_field",
            DataType::STRING,
            false,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(3),
        )])])?;

        // Build array element struct
        let array_item_struct = StructType::try_new(vec![StructField::new(
            "array_item",
            DataType::INTEGER,
            false,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(5),
        )])])?;
        let array_type = ArrayType::new(DataType::Struct(Box::new(array_item_struct)), false);

        // Build map with struct key and struct value (both with field IDs)
        let map_key_struct = StructType::try_new(vec![StructField::new(
            "map_key_field",
            DataType::STRING,
            false,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(7),
        )])])?;
        let map_value_struct = StructType::try_new(vec![StructField::new(
            "map_value_field",
            DataType::INTEGER,
            false,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(8),
        )])])?;
        let map_type = MapType::new(
            DataType::Struct(Box::new(map_key_struct)),
            DataType::Struct(Box::new(map_value_struct)),
            false,
        );

        // Build top-level struct
        let top_struct = StructType::try_new(vec![
            StructField::new("simple_field", DataType::INTEGER, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(1),
            )]),
            StructField::new(
                "nested_struct",
                DataType::Struct(Box::new(inner_struct_type)),
                false,
            )
            .with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(2),
            )]),
            StructField::new("array_field", DataType::Array(Box::new(array_type)), false)
                .with_metadata([(
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(4),
                )]),
            StructField::new("map_field", DataType::Map(Box::new(map_type)), false).with_metadata(
                [(
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(6),
                )],
            ),
        ])?;

        // Convert to Arrow schema
        let arrow_schema = ArrowSchema::try_from_kernel(&top_struct)?;

        // Verify field IDs are transformed to PARQUET:field_id at all levels using helper function
        let arrow_field_ids: HashMap<String, String> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY)
                .into_iter()
                .collect();

        // Expected field IDs in Arrow format (PARQUET:field_id)
        let expected_arrow_ids: HashMap<String, String> = [
            ("simple_field", "1"),
            ("nested_struct", "2"),
            ("inner_field", "3"),
            ("array_field", "4"),
            ("array_item", "5"),
            ("map_field", "6"),
            ("map_key_field", "7"),
            ("map_value_field", "8"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        assert_eq!(
            arrow_field_ids, expected_arrow_ids,
            "All field IDs should be transformed to PARQUET:field_id"
        );

        // Test reverse transformation: Arrow -> Kernel using visitor
        let kernel_struct = StructType::try_from_arrow(&arrow_schema)?;

        // Use visitor to collect all field IDs from the kernel struct
        let mut collector = FieldIdCollector::new();
        let _ = collector.transform_struct(&kernel_struct);

        // Verify all 8 field IDs were transformed back to parquet.field.id
        let kernel_field_ids: HashMap<String, String> = collector.field_ids.into_iter().collect();

        let expected_kernel_ids: HashMap<String, String> = [
            ("simple_field", "1"),
            ("nested_struct", "2"),
            ("inner_field", "3"),
            ("array_field", "4"),
            ("array_item", "5"),
            ("map_field", "6"),
            ("map_key_field", "7"),
            ("map_value_field", "8"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        assert_eq!(
            kernel_field_ids, expected_kernel_ids,
            "All field IDs should be transformed back to parquet.field.id"
        );

        Ok(())
    }
}
