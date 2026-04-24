//! Conversions between kernel schema types and arrow schema types.

pub mod scalar;

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use crate::arrow::error::ArrowError;
use crate::error::Error;
use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
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

/// Looks up a nested field id for the given dot-path in the StructField's nested-ids metadata.
pub(crate) fn lookup_nested_field_id(
    field: &StructField,
    path: &str,
) -> Result<Option<i64>, ArrowError> {
    // Order matters: `delta.columnMapping.nested.ids` takes precedence over
    // `parquet.field.nested.ids` when both are present on the same StructField.
    let keys = [
        ColumnMetadataKey::ColumnMappingNestedIds.as_ref(),
        ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
    ];
    for key in keys {
        let Some(value) = field.metadata().get(key) else {
            continue;
        };
        let MetadataValue::Other(json) = value else {
            return Err(ArrowError::SchemaError(format!(
                "'{key}' on field '{}' must be a JSON object",
                field.name()
            )));
        };
        let Some(obj) = json.as_object() else {
            return Err(ArrowError::SchemaError(format!(
                "'{key}' on field '{}' must be a JSON object",
                field.name()
            )));
        };
        let Some(entry) = obj.get(path) else {
            return Ok(None);
        };
        return entry.as_i64().map(Some).ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "'{key}['{path}']' on field '{}' must be an integer, got {entry}",
                field.name()
            ))
        });
    }
    Ok(None)
}

/// Helper: arrow field metadata containing just the `PARQUET:field_id` entry, or empty if `None`.
fn parquet_field_id_metadata(id: Option<i64>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    if let Some(id) = id {
        meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    }
    meta
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

/// Converts a kernel [`StructType`] to a `Vec<ArrowField>`.
fn try_kernel_struct_to_arrow_fields(s: &StructType) -> Result<Vec<ArrowField>, ArrowError> {
    s.fields().map(|f| f.try_into_arrow()).try_collect()
}

impl TryFromKernel<&StructType> for ArrowSchema {
    fn try_from_kernel(s: &StructType) -> Result<Self, ArrowError> {
        Ok(ArrowSchema::new(try_kernel_struct_to_arrow_fields(s)?))
    }
}

impl TryFromKernel<&StructField> for ArrowField {
    fn try_from_kernel(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = kernel_metadata_to_arrow_metadata(f)?;
        let arrow_type = match f.data_type() {
            // For Array/Map fields, build the inner arrow structure with nested field IDs
            // looked up from this StructField's `parquet.field.nested.ids` metadata. The rest
            // of the type graph below recurses via `nested_type_to_arrow` so further nesting
            // keeps using `f` as the nearest-ancestor until a `Struct` resets the root.
            DataType::Array(_) | DataType::Map(_) => {
                map_array_type_to_arrow(f, f.name(), f.data_type())?
            }
            _ => f.data_type().try_into_arrow()?,
        };
        let field = ArrowField::new(f.name(), arrow_type, f.is_nullable()).with_metadata(metadata);

        Ok(field)
    }
}

/// Recursive arrow-type builder that propagates nested field IDs from `ancestor`'s
/// `parquet.field.nested.ids` / `delta.columnMapping.nested.ids` metadata onto the synthesized
/// `element` / `key` / `value` arrow fields. `relative_path` is the dot-chained path rooted at
/// `ancestor.name()` for the current position in the type graph.
fn map_array_type_to_arrow(
    ancestor: &StructField,
    relative_path: &str,
    dt: &DataType,
) -> Result<ArrowDataType, ArrowError> {
    match dt {
        DataType::Array(a) => {
            let element_path = format!("{relative_path}.{LIST_ARRAY_ROOT}");
            let element_id = lookup_nested_field_id(ancestor, &element_path)?;
            let element_type = map_array_type_to_arrow(ancestor, &element_path, a.element_type())?;
            let element_field = ArrowField::new(LIST_ARRAY_ROOT, element_type, a.contains_null())
                .with_metadata(parquet_field_id_metadata(element_id));
            Ok(ArrowDataType::List(Arc::new(element_field)))
        }
        DataType::Map(m) => {
            let key_path = format!("{relative_path}.{MAP_KEY_DEFAULT}");
            let value_path = format!("{relative_path}.{MAP_VALUE_DEFAULT}");
            let key_id = lookup_nested_field_id(ancestor, &key_path)?;
            let value_id = lookup_nested_field_id(ancestor, &value_path)?;
            let key_type = map_array_type_to_arrow(ancestor, &key_path, m.key_type())?;
            let value_type = map_array_type_to_arrow(ancestor, &value_path, m.value_type())?;
            let key_field = ArrowField::new(MAP_KEY_DEFAULT, key_type, false)
                .with_metadata(parquet_field_id_metadata(key_id));
            let value_field =
                ArrowField::new(MAP_VALUE_DEFAULT, value_type, m.value_contains_null())
                    .with_metadata(parquet_field_id_metadata(value_id));
            let entries = ArrowField::new(
                MAP_ROOT_DEFAULT,
                ArrowDataType::Struct(vec![key_field, value_field].into()),
                false, // always non-null
            );
            Ok(ArrowDataType::Map(Arc::new(entries), false))
        }
        // Struct resets the root; each of its own StructField children handles its own
        // nested-ids metadata when recursing through TryFromKernel<&StructField>.
        DataType::Struct(_) | DataType::Primitive(_) | DataType::Variant(_) => dt.try_into_arrow(),
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
                try_kernel_struct_to_arrow_fields(s)?.into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into_arrow()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(
                Arc::new(m.as_ref().try_into_arrow()?),
                false,
            )),
            DataType::Variant(s) => {
                if *t == DataType::unshredded_variant() {
                    Ok(ArrowDataType::Struct(
                        try_kernel_struct_to_arrow_fields(s)?.into(),
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
        let metadata = arrow_field.metadata();
        // If both the native Arrow key (PARQUET:field_id) and the kernel key (parquet.field.id)
        // are present with different values, the translation below would silently overwrite one
        // with the other. Detect and reject this up front.
        if let (Some(arrow_id), Some(kernel_id)) = (
            metadata.get(PARQUET_FIELD_ID_META_KEY),
            metadata.get(ColumnMetadataKey::ParquetFieldId.as_ref()),
        ) {
            if arrow_id != kernel_id {
                return Err(ArrowError::SchemaError(format!(
                    "Field '{}': conflicting parquet field IDs: '{}' ({}) vs '{}' ({})",
                    arrow_field.name(),
                    arrow_id,
                    PARQUET_FIELD_ID_META_KEY,
                    kernel_id,
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                )));
            }
        }
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_arrow(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(metadata.iter().map(|(k, v)| {
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
    use std::collections::HashMap;

    use super::*;
    use crate::engine::arrow_conversion::ArrowField;
    use crate::engine::arrow_data::unshredded_variant_arrow_type;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };
    use crate::transforms::SchemaTransform;
    use crate::DeltaResult;

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
    #[derive(Default)]
    struct FieldIdCollector {
        field_ids: Vec<(String, String)>, // (field_name, field_id)
    }

    impl<'a> SchemaTransform<'a> for FieldIdCollector {
        fn transform_struct_field(
            &mut self,
            field: &'a StructField,
        ) -> Option<std::borrow::Cow<'a, StructField>> {
            // Collect field ID if present
            if let Some(field_id) = field
                .metadata()
                .get(ColumnMetadataKey::ParquetFieldId.as_ref())
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
                ArrowDataType::List(entry)
                | ArrowDataType::LargeList(entry)
                | ArrowDataType::FixedSizeList(entry, _)
                | ArrowDataType::Map(entry, _) => {
                    collect_from_field(entry, metadata_key, field_ids);
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

        let expected_ids: HashMap<String, String> = [
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

        // Verify field IDs are transformed to PARQUET:field_id at all levels
        let arrow_field_ids: HashMap<String, String> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY)
                .into_iter()
                .collect();
        assert_eq!(
            arrow_field_ids, expected_ids,
            "All field IDs should be transformed to PARQUET:field_id"
        );

        // Test round-trip: Arrow -> Kernel, field IDs should be preserved unchanged
        let kernel_struct = StructType::try_from_arrow(&arrow_schema)?;
        let mut collector = FieldIdCollector::default();
        collector.transform_struct(&kernel_struct);
        let kernel_field_ids: HashMap<String, String> = collector.field_ids.into_iter().collect();
        assert_eq!(
            kernel_field_ids, arrow_field_ids,
            "Kernel field IDs should match Arrow field IDs after round-trip"
        );

        Ok(())
    }

    /// When an Arrow field carries both `PARQUET:field_id` and `parquet.field.id` with the same
    /// value, the round-trip to kernel should succeed (one key is kept after translation).
    #[test]
    fn test_arrow_to_kernel_matching_field_ids_succeed() {
        let arrow_field = ArrowField::new("a", ArrowDataType::Int32, false).with_metadata(
            [
                (PARQUET_FIELD_ID_META_KEY.to_string(), "42".to_string()),
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                    "42".to_string(),
                ),
            ]
            .into(),
        );
        let result = StructField::try_from_arrow(&arrow_field);
        assert!(result.is_ok(), "Matching field IDs should succeed");
    }

    /// When an Arrow field carries both `PARQUET:field_id` and `parquet.field.id` with *different*
    /// values, converting to kernel must fail rather than silently overwriting one ID with the
    /// other.
    #[test]
    fn test_arrow_to_kernel_conflicting_field_ids_fail() {
        let arrow_field = ArrowField::new("a", ArrowDataType::Int32, false).with_metadata(
            [
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                    "2".to_string(),
                ),
            ]
            .into(),
        );
        crate::utils::test_utils::assert_result_error_with_message(
            StructField::try_from_arrow(&arrow_field),
            "conflicting parquet field IDs",
        );
    }

    // ===== IcebergCompatV3 nested field ids =====

    /// Build a JSON metadata value mapping nested paths to ids, suitable for use as the value of
    /// `parquet.field.nested.ids` or `delta.columnMapping.nested.ids` on a StructField.
    fn nested_ids_json<const N: usize>(entries: [(&str, i64); N]) -> MetadataValue {
        let obj: serde_json::Map<String, serde_json::Value> = entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::from(v)))
            .collect();
        MetadataValue::Other(serde_json::Value::Object(obj))
    }

    /// Recursively pull `PARQUET:field_id` entries from the arrow schema, indexed by field name.
    fn arrow_parquet_field_ids(schema: &ArrowSchema) -> HashMap<String, String> {
        collect_arrow_field_ids(schema, PARQUET_FIELD_ID_META_KEY)
            .into_iter()
            .collect()
    }

    /// Protocol example: `col1: array[array[int]]` stores all nested ids on `col1` itself.
    #[test]
    fn test_nested_ids_array_of_array() -> DeltaResult<()> {
        let inner_array = ArrayType::new(DataType::INTEGER, true);
        let outer_array = ArrayType::new(DataType::Array(Box::new(inner_array)), true);
        let schema = StructType::try_new(vec![StructField::new(
            "col1",
            DataType::Array(Box::new(outer_array)),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            nested_ids_json([("col1.element", 100), ("col1.element.element", 101)]),
        )])])?;

        let arrow_schema = ArrowSchema::try_from_kernel(&schema)?;
        let ids = arrow_parquet_field_ids(&arrow_schema);
        // Both nested `element` arrow fields carry the same name; collect_arrow_field_ids preserves
        // duplicates (Vec semantics), so converting through a HashMap collapses them. Assert the
        // raw ordered list instead for full coverage.
        let ordered: Vec<(String, String)> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY);
        assert_eq!(
            ordered,
            vec![
                ("element".to_string(), "100".to_string()),
                ("element".to_string(), "101".to_string()),
            ],
            "Outer array element => 100; inner array element => 101"
        );
        assert!(!ids.contains_key("col1"), "col1 has no PARQUET:field_id");
        Ok(())
    }

    /// Protocol example: `col2: map[int, array[int]]` — `col2.key`, `col2.value`, and
    /// `col2.value.element` all stored on `col2`.
    #[test]
    fn test_nested_ids_map_of_int_to_array() -> DeltaResult<()> {
        let value_array = ArrayType::new(DataType::INTEGER, true);
        let map = MapType::new(
            DataType::INTEGER,
            DataType::Array(Box::new(value_array)),
            true,
        );
        let schema = StructType::try_new(vec![StructField::new(
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
        )])])?;

        let arrow_schema = ArrowSchema::try_from_kernel(&schema)?;
        let ordered: Vec<(String, String)> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY);
        assert_eq!(
            ordered,
            vec![
                ("key".to_string(), "102".to_string()),
                ("value".to_string(), "103".to_string()),
                ("element".to_string(), "104".to_string()),
            ]
        );
        Ok(())
    }

    /// Protocol example: `col3: map[int, struct<subcol1: array[int]>]`. Nested ids for the map
    /// itself live on `col3`; nested ids for the array inside `subcol1` live on `subcol1` because
    /// a Struct resets the nearest-ancestor root.
    #[test]
    fn test_nested_ids_map_of_int_to_struct_with_array() -> DeltaResult<()> {
        let subcol1_array = ArrayType::new(DataType::INTEGER, true);
        let subcol1 = StructField::new("subcol1", DataType::Array(Box::new(subcol1_array)), true)
            .with_metadata([(
                ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
                nested_ids_json([("subcol1.element", 107)]),
            )]);
        let value_struct = StructType::try_new(vec![subcol1])?;
        let map = MapType::new(
            DataType::INTEGER,
            DataType::Struct(Box::new(value_struct)),
            true,
        );
        let schema = StructType::try_new(vec![StructField::new(
            "col3",
            DataType::Map(Box::new(map)),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            nested_ids_json([("col3.key", 105), ("col3.value", 106)]),
        )])])?;

        let arrow_schema = ArrowSchema::try_from_kernel(&schema)?;
        let ordered: Vec<(String, String)> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY);
        assert_eq!(
            ordered,
            vec![
                ("key".to_string(), "105".to_string()),
                ("value".to_string(), "106".to_string()),
                ("element".to_string(), "107".to_string()),
            ]
        );
        Ok(())
    }

    /// When both `delta.columnMapping.nested.ids` and `parquet.field.nested.ids` are present, the
    /// `delta.columnMapping.nested.ids` key takes precedence.
    #[test]
    fn test_nested_ids_internal_key_wins() -> DeltaResult<()> {
        let array = ArrayType::new(DataType::INTEGER, true);
        let schema = StructType::try_new(vec![StructField::new(
            "col1",
            DataType::Array(Box::new(array)),
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
        ])])?;

        let arrow_schema = ArrowSchema::try_from_kernel(&schema)?;
        let ordered: Vec<(String, String)> =
            collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY);
        assert_eq!(
            ordered,
            vec![("element".to_string(), "42".to_string())],
            "delta.columnMapping.nested.ids should win over parquet.field.nested.ids"
        );
        Ok(())
    }

    /// Missing nested-id entry produces no PARQUET:field_id on the nested arrow field.
    #[test]
    fn test_nested_ids_missing_entry_omits_id() -> DeltaResult<()> {
        let array = ArrayType::new(DataType::INTEGER, true);
        let schema = StructType::try_new(vec![StructField::new(
            "col1",
            DataType::Array(Box::new(array)),
            true,
        )])?;
        let arrow_schema = ArrowSchema::try_from_kernel(&schema)?;
        let ordered = collect_arrow_field_ids(&arrow_schema, PARQUET_FIELD_ID_META_KEY);
        assert!(ordered.is_empty());
        Ok(())
    }
}
