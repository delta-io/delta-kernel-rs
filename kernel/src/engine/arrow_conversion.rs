//! Conversions from kernel types to arrow types

use std::sync::Arc;

use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use itertools::Itertools;

use crate::error::Error;
use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};

pub(crate) const LIST_ARRAY_ROOT: &str = "item";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

impl TryFrom<&StructType> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &StructType) -> Result<Self, ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(TryInto::try_into).try_collect()?;
        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| Ok((key.clone(), serde_json::to_string(val)?)))
            .collect::<Result<_, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        let field = ArrowField::new(
            f.name(),
            ArrowDataType::try_from(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFrom<&ArrayType> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            LIST_ARRAY_ROOT,
            ArrowDataType::try_from(a.element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFrom<&MapType> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(
                        MAP_KEY_DEFAULT,
                        ArrowDataType::try_from(a.key_type())?,
                        false,
                    ),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        ArrowDataType::try_from(a.value_type())?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false, // always non-null
        ))
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(t: &DataType) -> Result<Self, ArrowError> {
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
                    PrimitiveType::Decimal(precision, scale) => {
                        PrimitiveType::check_decimal(*precision, *scale)
                            .map_err(|e| ArrowError::from_external_error(e.into()))?;
                        Ok(ArrowDataType::Decimal128(*precision, *scale as i8))
                    }
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
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(Arc::new(m.as_ref().try_into()?), false)),
        }
    }
}

impl TryFrom<&ArrowSchema> for StructType {
    type Error = ArrowError;

    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let new_fields: Result<Vec<StructField>, _> = arrow_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().try_into())
            .collect();
        Ok(StructType::new(new_fields?))
    }
}

impl TryFrom<ArrowSchemaRef> for StructType {
    type Error = ArrowError;

    fn try_from(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        arrow_schema.as_ref().try_into()
    }
}

impl TryFrom<&ArrowField> for StructField {
    type Error = ArrowError;

    fn try_from(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ArrowError;

    fn try_from(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(DataType::Primitive(PrimitiveType::String)),
            ArrowDataType::LargeUtf8 => Ok(DataType::Primitive(PrimitiveType::String)),
            ArrowDataType::Int64 => Ok(DataType::Primitive(PrimitiveType::Long)), // undocumented type
            ArrowDataType::Int32 => Ok(DataType::Primitive(PrimitiveType::Integer)),
            ArrowDataType::Int16 => Ok(DataType::Primitive(PrimitiveType::Short)),
            ArrowDataType::Int8 => Ok(DataType::Primitive(PrimitiveType::Byte)),
            ArrowDataType::UInt64 => Ok(DataType::Primitive(PrimitiveType::Long)), // undocumented type
            ArrowDataType::UInt32 => Ok(DataType::Primitive(PrimitiveType::Integer)),
            ArrowDataType::UInt16 => Ok(DataType::Primitive(PrimitiveType::Short)),
            ArrowDataType::UInt8 => Ok(DataType::Primitive(PrimitiveType::Byte)),
            ArrowDataType::Float32 => Ok(DataType::Primitive(PrimitiveType::Float)),
            ArrowDataType::Float64 => Ok(DataType::Primitive(PrimitiveType::Double)),
            ArrowDataType::Boolean => Ok(DataType::Primitive(PrimitiveType::Boolean)),
            ArrowDataType::Binary => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::FixedSizeBinary(_) => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::LargeBinary => Ok(DataType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(ArrowError::from_external_error(
                        Error::invalid_decimal("Negative scales are not supported in Delta").into(),
                    ));
                };
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| ArrowError::from_external_error(e.into()))
            }
            ArrowDataType::Date32 => Ok(DataType::Primitive(PrimitiveType::Date)),
            ArrowDataType::Date64 => Ok(DataType::Primitive(PrimitiveType::Date)),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(DataType::Primitive(PrimitiveType::TimestampNtz))
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::Primitive(PrimitiveType::Timestamp))
            }
            ArrowDataType::Struct(fields) => {
                let converted_fields: Result<Vec<StructField>, _> = fields
                    .iter()
                    .map(|field| field.as_ref().try_into())
                    .collect();
                Ok(DataType::Struct(Box::new(StructType::new(
                    converted_fields?,
                ))))
            }
            ArrowDataType::List(field) => Ok(DataType::Array(Box::new(ArrayType::new(
                (*field).data_type().try_into()?,
                (*field).is_nullable(),
            )))),
            ArrowDataType::LargeList(field) => Ok(DataType::Array(Box::new(ArrayType::new(
                (*field).data_type().try_into()?,
                (*field).is_nullable(),
            )))),
            ArrowDataType::FixedSizeList(field, _) => Ok(DataType::Array(Box::new(
                ArrayType::new((*field).data_type().try_into()?, (*field).is_nullable()),
            ))),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = struct_fields[0].data_type().try_into()?;
                    let value_type = struct_fields[1].data_type().try_into()?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(DataType::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        value_type_nullable,
                    ))))
                } else {
                    panic!("DataType::Map should contain a struct field child");
                }
            }
            // Dictionary types are just an optimized in-memory representation of an array.
            // Schema-wise, they are the same as the value type.
            ArrowDataType::Dictionary(_, value_type) => Ok(value_type.as_ref().try_into()?),
            s => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
        }
    }
}
