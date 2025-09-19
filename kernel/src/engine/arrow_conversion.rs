//! Conversions from kernel schema types to arrow schema types.

use std::sync::Arc;

use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use crate::arrow::error::ArrowError;
use itertools::Itertools;

use crate::error::Error;
use crate::schema::{
    ArrayType, DataType, MapType, MetadataValue, PrimitiveType, StructField, StructType,
};

pub(crate) const LIST_ARRAY_ROOT: &str = "element";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

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
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match &val {
                &MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<Result<_, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

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
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
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
    use crate::{
        schema::{DataType, StructField},
        DeltaResult,
    };
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
}
