//! Conversions between kernel schema types and arrow schema types.
//!
//! Two directions, used at the engine <-> kernel boundary:
//!
//! - **arrow -> kernel** ([`TryFromArrow`] / [`TryIntoKernel`]): an example usage is parsing a
//!   parquet footer's Arrow schema into a kernel `StructType` for checkpoint introspection.
//!
//! - **kernel -> arrow** ([`TryFromKernel`] / [`TryIntoArrow`]): an example usage is materializing
//!   a kernel `StructType` as an `ArrowSchema` to build a `RecordBatch` on the write path.

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
/// Specifically:
/// - Transforms the `"parquet.field.id"` key (used by kernel/delta-spark) to `"PARQUET:field_id"`
///   (the native Parquet/Arrow metadata key), enabling correct field ID handling by the
///   Arrow/Parquet writer.
/// - Strips the nested-ids JSON key (`"delta.columnMapping.nested.ids"`). Its content is already
///   projected onto the synthesized list/map fields' `"PARQUET:field_id"` by
///   [`kernel_map_array_into_arrow`].
pub(crate) fn kernel_metadata_to_arrow_metadata(
    field: &StructField,
) -> Result<HashMap<String, String>, ArrowError> {
    field
        .metadata()
        .iter()
        .filter(|(key, _)| *key != ColumnMetadataKey::ColumnMappingNestedIds.as_ref())
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

/// Looks up a nested-field id for the given dot-path on a StructField's nested-ids metadata.
///
/// Parquet field IDs for synthesized list/map element/key/value fields are stored under
/// `delta.columnMapping.nested.ids`. The path is dot-chained from the nearest ancestor
/// StructField's name.
///
/// # Example
///
/// Given `col2: map[int, array[int]]` whose StructField's metadata has
/// `{ "col2.key": 102, "col2.value": 103, "col2.value.element": 104 }`:
///
/// - `lookup_nested_field_id(col2, "col2.key")` -> `Some(102)`
/// - `lookup_nested_field_id(col2, "col2.value")` -> `Some(103)`
/// - `lookup_nested_field_id(col2, "col2.value.element")` -> `Some(104)`
///
/// Returns `None` when the metadata key is not present, or when the path is not in the JSON
/// map. The latter is expected when the caller passes a logical schema, because nested.ids
/// JSON keys are rooted at the physical field name.
///
/// Returns an error when the metadata value is not a JSON object, or the id found is not an
/// integer.
pub(crate) fn lookup_nested_field_id(
    field: &StructField,
    path: &str,
) -> Result<Option<i64>, ArrowError> {
    let key = ColumnMetadataKey::ColumnMappingNestedIds.as_ref();
    let Some(value) = field.metadata().get(key) else {
        return Ok(None);
    };
    let obj = match value {
        MetadataValue::Other(serde_json::Value::Object(obj)) => Some(obj),
        _ => None,
    }
    .ok_or_else(|| {
        ArrowError::SchemaError(format!(
            "'{key}' on field '{}' must be a JSON object",
            field.name()
        ))
    })?;
    // Missing entry returns Ok(None) rather than Err. The JSON map's keys are rooted at the
    // *physical* field name, so when this function is invoked on a logical schema, it's
    // expected that the lookup misses.
    obj.get(path).map_or(Ok(None), |entry| {
        entry.as_i64().map(Some).ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "'{key}['{path}']' on field '{}' must be an integer, got {entry}",
                field.name()
            ))
        })
    })
}

/// Builds Arrow field metadata containing the `PARQUET:field_id` entry, or empty if `id` is
/// `None`.
pub(crate) fn parquet_field_id_metadata(id: Option<i64>) -> HashMap<String, String> {
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
        // For Array/Map fields, recurse via `kernel_map_array_into_arrow` so that any
        // `delta.columnMapping.nested.ids` metadata on this StructField gets threaded onto the
        // synthesized element/key/value Arrow fields. Other types (Struct, Primitive, Variant)
        // keep using the default conversion.
        let arrow_type = match f.data_type() {
            DataType::Array(_) | DataType::Map(_) => {
                kernel_map_array_into_arrow(f, f.name(), f.data_type())?
            }
            _ => f.data_type().try_into_arrow()?,
        };
        let field = ArrowField::new(f.name(), arrow_type, f.is_nullable()).with_metadata(metadata);

        Ok(field)
    }
}

/// Recursive Arrow-type builder that propagates nested field IDs from `ancestor`'s
/// `delta.columnMapping.nested.ids` metadata onto the synthesized `element` / `key` / `value`
/// Arrow fields.
///
/// # Parameters
/// - `ancestor`: the nearest ancestor kernel [`StructField`]; its metadata holds the nested-ids
///   JSON map.
/// - `relative_path`: dot-chained path rooted at `ancestor.name()`.
/// - `datatype`: the kernel data type at the current position.
///
/// # Example
///
/// Kernel field: [`StructField`]:
///
/// ```json
/// {
///   "name": "array_in_map",
///   "type": {
///     "type": "map",
///     "keyType":   "integer",
///     "valueType": { "type": "array", "elementType": "integer", "containsNull": true },
///     "valueContainsNull": true
///   },
///   "nullable": true,
///   "metadata": {
///     "delta.columnMapping.nested.ids": {
///       "array_in_map.key":           100,
///       "array_in_map.value":         101,
///       "array_in_map.value.element": 102
///     }
///   }
/// }
/// ```
///
/// Calling `kernel_map_array_into_arrow(field, "array_in_map", &field.data_type())`
/// produces this Arrow `DataType` (each synthesized `key`/`value`/`element` Arrow Field
/// carries `PARQUET:field_id` in its `metadata`, looked up from the JSON above):
///
/// ```json
/// {
///   "type": "map",
///   "keys_sorted": false,
///   "entries": {
///     "name": "key_value",
///     "type": "struct",
///     "nullable": false,
///     "fields": [
///       {
///         "name": "key",
///         "type": "int32",
///         "nullable": false,
///         "metadata": { "PARQUET:field_id": "100" }
///       },
///       {
///         "name": "value",
///         "type": "list",
///         "nullable": true,
///         "metadata": { "PARQUET:field_id": "101" },
///         "element": {
///           "name": "element",
///           "type": "int32",
///           "nullable": true,
///           "metadata": { "PARQUET:field_id": "102" }
///         }
///       }
///     ]
///   }
/// }
/// ```
fn kernel_map_array_into_arrow(
    ancestor: &StructField,
    relative_path: &str,
    datatype: &DataType,
) -> Result<ArrowDataType, ArrowError> {
    match datatype {
        DataType::Array(a) => {
            let element_path = format!("{relative_path}.{LIST_ARRAY_ROOT}");
            let element_id = lookup_nested_field_id(ancestor, &element_path)?;
            let arrow_element_type =
                kernel_map_array_into_arrow(ancestor, &element_path, a.element_type())?;
            // Kernel's array element field is anonymous; we use `LIST_ARRAY_ROOT` as the
            // synthesized field name by convention. Same below for map's `key`/`value` fields.
            let arrow_element_field =
                ArrowField::new(LIST_ARRAY_ROOT, arrow_element_type, a.contains_null())
                    .with_metadata(parquet_field_id_metadata(element_id));
            Ok(ArrowDataType::List(Arc::new(arrow_element_field)))
        }
        DataType::Map(m) => {
            let key_path = format!("{relative_path}.{MAP_KEY_DEFAULT}");
            let value_path = format!("{relative_path}.{MAP_VALUE_DEFAULT}");
            let key_id = lookup_nested_field_id(ancestor, &key_path)?;
            let value_id = lookup_nested_field_id(ancestor, &value_path)?;
            let arrow_key_type = kernel_map_array_into_arrow(ancestor, &key_path, m.key_type())?;
            let arrow_value_type =
                kernel_map_array_into_arrow(ancestor, &value_path, m.value_type())?;
            // Map keys are never nullable.
            let arrow_key_field = ArrowField::new(MAP_KEY_DEFAULT, arrow_key_type, false)
                .with_metadata(parquet_field_id_metadata(key_id));
            let arrow_value_field =
                ArrowField::new(MAP_VALUE_DEFAULT, arrow_value_type, m.value_contains_null())
                    .with_metadata(parquet_field_id_metadata(value_id));
            // `MapArray::try_new` rejects a nullable entries field, so hardcode nullable to `false`
            // here. See <https://docs.rs/arrow/latest/arrow/array/struct.MapArray.html#method.try_new>.
            let arrow_entries_field = ArrowField::new(
                MAP_ROOT_DEFAULT,
                ArrowDataType::Struct(vec![arrow_key_field, arrow_value_field].into()),
                false, /* always non-null */
            );
            Ok(ArrowDataType::Map(
                Arc::new(arrow_entries_field),
                false, /* keys_sorted */
            ))
        }
        DataType::Struct(_) | DataType::Primitive(_) | DataType::Variant(_) => {
            datatype.try_into_arrow()
        }
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
        let arrow_metadata = arrow_field.metadata();
        // If both the native Arrow key (PARQUET:field_id) and the kernel key (parquet.field.id)
        // are present with different values, the translation below would silently overwrite one
        // with the other. Detect and reject this up front.
        if let (Some(arrow_id), Some(kernel_id)) = (
            arrow_metadata.get(PARQUET_FIELD_ID_META_KEY),
            arrow_metadata.get(ColumnMetadataKey::ParquetFieldId.as_ref()),
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
        let mut kernel_metadata: HashMap<String, MetadataValue> = arrow_metadata
            .iter()
            .map(|(k, v)| -> Result<_, ArrowError> {
                // Transform "PARQUET:field_id" to "parquet.field.id" when reading from Parquet.
                // `parquet.field.id` in kernel is `MetadataValue::Number(i64)`.
                if k == PARQUET_FIELD_ID_META_KEY || k == ColumnMetadataKey::ParquetFieldId.as_ref()
                {
                    let id: i64 = v.parse().map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "'{k}' on field '{}' must be an integer, got '{v}'",
                            arrow_field.name()
                        ))
                    })?;
                    return Ok((
                        ColumnMetadataKey::ParquetFieldId.as_ref().to_string(),
                        MetadataValue::Number(id),
                    ));
                }
                Ok((k.clone(), MetadataValue::from(v)))
            })
            .collect::<Result<_, _>>()?;
        let mut nested_ids = serde_json::Map::new();
        collect_nested_field_ids_from_arrow(
            arrow_field.data_type(),
            arrow_field.name(),
            &mut nested_ids,
        )?;
        if !nested_ids.is_empty() {
            kernel_metadata.insert(
                ColumnMetadataKey::ColumnMappingNestedIds
                    .as_ref()
                    .to_string(),
                MetadataValue::Other(serde_json::Value::Object(nested_ids)),
            );
        }

        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_arrow(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(kernel_metadata))
    }
}

/// Inverse of [`kernel_map_array_into_arrow`]: walks `arrow_type` and aggregates
/// `PARQUET:field_id` from the element/key/value fields of any list/map, keyed by dot-path
/// rooted at `relative_path`.
///
/// # Example
///
/// Given an Arrow [`MapArray`] field `m: map<int, list<int>>` whose inner `key`, `value`, and
/// `value.element` fields carry `PARQUET:field_id` of `100`, `101`, `102`, calling
/// `collect_nested_field_ids_from_arrow(m.data_type(), "m", &mut out)` populates `out` with:
///
/// ```text
/// { "m.key": 100, "m.value": 101, "m.value.element": 102 }
/// ```
///
/// [`MapArray`]: arrow::array::MapArray
fn collect_nested_field_ids_from_arrow(
    arrow_type: &ArrowDataType,
    relative_path: &str,
    out: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), ArrowError> {
    // `delta.columnMapping.nested.ids` only stores the nested field ids element/key/values
    // of list/map fields. So we only recurse into list/map here.
    let inner_fields: Vec<(&ArrowField, &str)> = match arrow_type {
        ArrowDataType::List(elem)
        | ArrowDataType::ListView(elem)
        | ArrowDataType::LargeList(elem)
        | ArrowDataType::LargeListView(elem) => vec![(elem.as_ref(), LIST_ARRAY_ROOT)],
        ArrowDataType::FixedSizeList(elem, _) => vec![(elem.as_ref(), LIST_ARRAY_ROOT)],
        ArrowDataType::Map(entries, _) => match entries.data_type() {
            ArrowDataType::Struct(fields) if fields.len() == 2 => vec![
                (fields[0].as_ref(), MAP_KEY_DEFAULT),
                (fields[1].as_ref(), MAP_VALUE_DEFAULT),
            ],
            other => {
                return Err(ArrowError::SchemaError(format!(
                    "Map entries must be a struct with exactly 2 fields (key, value), got {other}"
                )))
            }
        },
        // Dictionary is a transparent wrapper (kernel side collapses it to its value type), so
        // recurse without consuming a path segment.
        ArrowDataType::Dictionary(_, value_type) => {
            return collect_nested_field_ids_from_arrow(value_type, relative_path, out);
        }
        _ => return Ok(()),
    };

    for (inner, segment) in inner_fields {
        let path = format!("{relative_path}.{segment}");
        if let Some(id_str) = inner.metadata().get(PARQUET_FIELD_ID_META_KEY) {
            let id: i64 = id_str.parse().map_err(|_| {
                ArrowError::SchemaError(format!(
                    "'{PARQUET_FIELD_ID_META_KEY}' on '{}' must be an integer, got '{id_str}'",
                    inner.name()
                ))
            })?;
            out.insert(path.clone(), serde_json::Value::from(id));
        }
        collect_nested_field_ids_from_arrow(inner.data_type(), &path, out)?;
    }
    Ok(())
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

    use rstest::rstest;

    use super::*;
    use crate::engine::arrow_conversion::ArrowField;
    use crate::engine::arrow_data::unshredded_variant_arrow_type;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
    };
    use crate::transforms::{transform_output_type, SchemaTransform};
    use crate::utils::test_utils::{
        array_in_map_kernel_schema, assert_result_error_with_message, collect_arrow_field_metadata,
        complex_nested_with_field_ids,
    };
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
        transform_output_type!(|'a, T| ());

        fn transform_struct_field(&mut self, field: &'a StructField) {
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

    #[test]
    fn test_try_into_arrow_threads_nested_ids_onto_arrow_schema() -> DeltaResult<()> {
        let meta_key = ColumnMetadataKey::ColumnMappingNestedIds.as_ref();
        let fixture = complex_nested_with_field_ids(meta_key);
        let arrow_schema: ArrowSchema = (&fixture.kernel_schema).try_into_arrow()?;

        assert_eq!(
            arrow_schema, fixture.expected_arrow_schema,
            "try_into_arrow should attach nested field ids for metadata key {meta_key}",
        );
        Ok(())
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
    fn test_try_into_arrow_sets_field_ids_for_matched_nested_ids_only(
        #[case] schema: StructType,
        #[case] expected_field_ids: &[(&str, &str)],
    ) -> DeltaResult<()> {
        let arrow_schema: ArrowSchema = (&schema).try_into_arrow()?;
        let field_ids: HashMap<String, String> =
            collect_arrow_field_metadata(&arrow_schema, PARQUET_FIELD_ID_META_KEY)
                .into_iter()
                .collect();

        assert_eq!(field_ids.len(), expected_field_ids.len());
        for (field_name, expected_id) in expected_field_ids {
            assert_eq!(
                field_ids.get(*field_name).map(String::as_str),
                Some(*expected_id),
            );
        }
        Ok(())
    }

    #[test]
    fn test_try_into_arrow_invalid_nested_ids_metadata_errors() {
        let schema = array_in_map_kernel_schema([(
            ColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_string(),
            MetadataValue::String("not a json object".to_string()),
        )]);
        assert_result_error_with_message(
            ArrowSchema::try_from_kernel(&schema),
            "must be a JSON object",
        );

        let schema = array_in_map_kernel_schema([(
            ColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_string(),
            MetadataValue::Other(serde_json::json!({ "array_in_map.key": "oops" })),
        )]);
        assert_result_error_with_message(
            ArrowSchema::try_from_kernel(&schema),
            "must be an integer",
        );
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
            collect_arrow_field_metadata(&arrow_schema, PARQUET_FIELD_ID_META_KEY)
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
        let kernel = StructField::try_from_arrow(&arrow_field).unwrap();
        // The two arrow keys collapse to a single kernel `parquet.field.id` entry whose value is
        // a `Number`.
        assert_eq!(
            kernel
                .metadata()
                .get(ColumnMetadataKey::ParquetFieldId.as_ref()),
            Some(&MetadataValue::Number(42)),
        );
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
        assert_result_error_with_message(
            StructField::try_from_arrow(&arrow_field),
            "conflicting parquet field IDs",
        );
    }

    /// Inverse of `test_try_into_arrow_threads_nested_ids_onto_arrow_schema`: feed the same
    /// fixture's expected Arrow schema back through `try_from_arrow` and verify it round-trips
    /// to the original kernel schema.
    #[test]
    fn test_try_from_arrow_aggregates_synthesized_nested_ids() {
        let fixture =
            complex_nested_with_field_ids(ColumnMetadataKey::ColumnMappingNestedIds.as_ref());
        let kernel_schema_from_arrow: StructType =
            (&fixture.expected_arrow_schema).try_into_kernel().unwrap();
        assert_eq!(kernel_schema_from_arrow, fixture.kernel_schema);
    }

    /// When the input Arrow schema has no `PARQUET:field_id` on any inner list/map field, the
    /// resulting kernel [`StructField`] must not carry a `delta.columnMapping.nested.ids` entry.
    #[test]
    fn test_try_from_arrow_no_nested_ids_when_input_lacks_field_ids() {
        let kernel_schema_without_metadata =
            array_in_map_kernel_schema(std::iter::empty::<(String, MetadataValue)>());
        let arrow_schema_without_parquet_id: ArrowSchema =
            (&kernel_schema_without_metadata).try_into_arrow().unwrap();

        let kernel_schema: StructType = (&arrow_schema_without_parquet_id)
            .try_into_kernel()
            .unwrap();
        let array_in_map = kernel_schema.fields().next().unwrap();
        assert!(!array_in_map
            .metadata()
            .contains_key(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()));
    }

    /// A non-integer `PARQUET:field_id` on an inner list/map field must produce a clear error
    /// rather than silently being dropped.
    #[test]
    fn test_try_from_arrow_invalid_inner_field_id_errors() {
        let element_field = ArrowField::new("element", ArrowDataType::Int32, true)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "oops".to_string())].into());
        let list_field = ArrowField::new("arr", ArrowDataType::List(Arc::new(element_field)), true);
        assert_result_error_with_message(
            StructField::try_from_arrow(&list_field),
            "must be an integer",
        );
    }

    /// Every list-flavored Arrow datatype that the converter accepts must also be walked by the
    /// nested-ids aggregator.
    #[rstest]
    #[case::list(ArrowDataType::List(arc_elem_with_id(42)))]
    #[case::list_view(ArrowDataType::ListView(arc_elem_with_id(42)))]
    #[case::large_list(ArrowDataType::LargeList(arc_elem_with_id(42)))]
    #[case::large_list_view(ArrowDataType::LargeListView(arc_elem_with_id(42)))]
    #[case::fixed_size_list(ArrowDataType::FixedSizeList(arc_elem_with_id(42), 3))]
    fn test_try_from_arrow_aggregates_nested_id_for_all_list_kinds(
        #[case] dt: ArrowDataType,
    ) -> DeltaResult<()> {
        let arrow_field = ArrowField::new("arr", dt, true);
        let kernel = StructField::try_from_arrow(&arrow_field)?;
        assert_eq!(
            kernel
                .metadata()
                .get(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()),
            Some(&MetadataValue::Other(
                serde_json::json!({"arr.element": 42})
            )),
        );
        Ok(())
    }

    /// `Dictionary` is a transparent wrapper on the Arrow side (kernel collapses it to its value
    /// type), so a nested-ids walker must still descend through it.
    #[test]
    fn test_try_from_arrow_aggregates_nested_id_through_dictionary() -> DeltaResult<()> {
        let arrow_dict = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::List(arc_elem_with_id(7))),
        );
        let arrow_field = ArrowField::new("arr", arrow_dict, true);
        let kernel = StructField::try_from_arrow(&arrow_field)?;
        assert_eq!(
            kernel
                .metadata()
                .get(ColumnMetadataKey::ColumnMappingNestedIds.as_ref()),
            Some(&MetadataValue::Other(serde_json::json!({"arr.element": 7}))),
        );
        Ok(())
    }

    fn arc_elem_with_id(id: i32) -> Arc<ArrowField> {
        Arc::new(
            ArrowField::new("element", ArrowDataType::Int32, true)
                .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into()),
        )
    }
}
