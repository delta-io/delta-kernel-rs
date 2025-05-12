//! Conversions from kernel types to arrow types

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use crate::arrow::error::ArrowError;
use itertools::Itertools;

use delta_kernel::error::Error;
use delta_kernel::schema::{
    ArrayType, DataType, MapType, MetadataValue, PrimitiveType, StructField, StructType,
};

pub(crate) const LIST_ARRAY_ROOT: &str = "element";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

// Newtype wrappers for Arrow types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineArrowSchema(pub ArrowSchema);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineArrowSchemaRef(pub ArrowSchemaRef);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineArrowField(pub ArrowField);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineArrowFieldRef<'a>(pub &'a ArrowField);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineArrowDataType(pub ArrowDataType);

// Helper functions to implement conversions without relying on orphan rule violations

// Convert StructType to ArrowSchema
pub fn arrow_schema_from_struct_type(schema: &StructType) -> Result<ArrowSchema, ArrowError> {
    let fields: Vec<ArrowField> = schema.fields().map(arrow_field_from_struct_field).try_collect()?;
    Ok(ArrowSchema::new(fields))
}

// Convert ArrowSchemaRef to StructType
pub fn struct_type_from_arrow_schema_ref(schema: ArrowSchemaRef) -> Result<StructType, ArrowError> {
    struct_type_from_arrow_schema(schema.as_ref())
}

// Convert &ArrowSchema to StructType
pub fn struct_type_from_arrow_schema(arrow_schema: &ArrowSchema) -> Result<StructType, ArrowError> {
    StructType::try_new(
        arrow_schema
            .fields()
            .iter()
            .map(struct_field_from_arrow_field_ref),
    )
}

// Convert StructField to ArrowField
pub fn arrow_field_from_struct_field(f: &StructField) -> Result<ArrowField, ArrowError> {
    let metadata = f
        .metadata()
        .iter()
        .map(|(key, val)| match &val {
            &MetadataValue::String(val) => Ok((key.clone(), val.clone())),
            _ => Ok((key.clone(), serde_json::to_string(val)?)),
        })
        .collect::<Result<_, serde_json::Error>>()
        .map_err(|err| ArrowError::JsonError(err.to_string()))?;

    let field = ArrowField::new(
        f.name(),
        arrow_data_type_from_data_type(f.data_type())?,
        f.is_nullable(),
    )
    .with_metadata(metadata);

    Ok(field)
}

// Convert DataType to ArrowDataType
pub fn arrow_data_type_from_data_type(t: &DataType) -> Result<ArrowDataType, ArrowError> {
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
                .map(arrow_field_from_struct_field)
                .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                .into(),
        )),
        DataType::Array(a) => {
            let field = arrow_field_from_array_type(a)?;
            Ok(ArrowDataType::List(Arc::new(field)))
        },
        DataType::Map(m) => {
            let field = arrow_field_from_map_type(m)?;
            Ok(ArrowDataType::Map(Arc::new(field), false))
        }
    }
}

// Convert ArrayType to ArrowField
pub fn arrow_field_from_array_type(a: &ArrayType) -> Result<ArrowField, ArrowError> {
    Ok(ArrowField::new(
        LIST_ARRAY_ROOT,
        arrow_data_type_from_data_type(a.element_type())?,
        a.contains_null(),
    ))
}

// Convert MapType to ArrowField
pub fn arrow_field_from_map_type(a: &MapType) -> Result<ArrowField, ArrowError> {
    Ok(ArrowField::new(
        MAP_ROOT_DEFAULT,
        ArrowDataType::Struct(
            vec![
                ArrowField::new(
                    MAP_KEY_DEFAULT,
                    arrow_data_type_from_data_type(a.key_type())?,
                    false,
                ),
                ArrowField::new(
                    MAP_VALUE_DEFAULT,
                    arrow_data_type_from_data_type(a.value_type())?,
                    a.value_contains_null(),
                ),
            ]
            .into(),
        ),
        false, // always non-null
    ))
}

// Convert ArrowField to StructField
pub fn struct_field_from_arrow_field(arrow_field: &ArrowField) -> Result<StructField, ArrowError> {
    Ok(StructField::new(
        arrow_field.name().clone(),
        data_type_from_arrow_data_type(arrow_field.data_type())?,
        arrow_field.is_nullable(),
    )
    .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
}

// Convert Arc<ArrowField> to StructField
pub fn struct_field_from_arrow_field_ref(arrow_field: &Arc<ArrowField>) -> Result<StructField, ArrowError> {
    struct_field_from_arrow_field(arrow_field.as_ref())
}

// Convert ArrowDataType to DataType
pub fn data_type_from_arrow_data_type(arrow_datatype: &ArrowDataType) -> Result<DataType, ArrowError> {
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
        ArrowDataType::Struct(fields) => {
            DataType::try_struct_type(fields.iter().map(struct_field_from_arrow_field_ref))
        }
        ArrowDataType::List(field) => {
            Ok(ArrayType::new(data_type_from_arrow_data_type((*field).data_type())?, (*field).is_nullable()).into())
        }
        ArrowDataType::ListView(field) => {
            Ok(ArrayType::new(data_type_from_arrow_data_type((*field).data_type())?, (*field).is_nullable()).into())
        }
        ArrowDataType::LargeList(field) => {
            Ok(ArrayType::new(data_type_from_arrow_data_type((*field).data_type())?, (*field).is_nullable()).into())
        }
        ArrowDataType::LargeListView(field) => {
            Ok(ArrayType::new(data_type_from_arrow_data_type((*field).data_type())?, (*field).is_nullable()).into())
        }
        ArrowDataType::FixedSizeList(field, _) => {
            Ok(ArrayType::new(data_type_from_arrow_data_type((*field).data_type())?, (*field).is_nullable()).into())
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                let key_type = data_type_from_arrow_data_type(struct_fields[0].data_type())?;
                let value_type = data_type_from_arrow_data_type(struct_fields[1].data_type())?;
                let value_type_nullable = struct_fields[1].is_nullable();
                Ok(MapType::new(key_type, value_type, value_type_nullable).into())
            } else {
                panic!("DataType::Map should contain a struct field child");
            }
        }
        // Dictionary types are just an optimized in-memory representation of an array.
        // Schema-wise, they are the same as the value type.
        ArrowDataType::Dictionary(_, value_type) => Ok(data_type_from_arrow_data_type(value_type.as_ref())?),
        s => Err(ArrowError::SchemaError(format!(
            "Invalid data type for Delta Lake: {s}"
        ))),
    }
}

// Deref implementations to allow transparent access to the inner types
impl Deref for EngineArrowSchema {
    type Target = ArrowSchema;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EngineArrowSchema {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for EngineArrowSchemaRef {
    type Target = ArrowSchemaRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for EngineArrowField {
    type Target = ArrowField;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EngineArrowField {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> Deref for EngineArrowFieldRef<'a> {
    type Target = &'a ArrowField;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for EngineArrowDataType {
    type Target = ArrowDataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EngineArrowDataType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// From/Into implementations for easy conversions
impl From<ArrowSchema> for EngineArrowSchema {
    fn from(schema: ArrowSchema) -> Self {
        EngineArrowSchema(schema)
    }
}

impl From<EngineArrowSchema> for ArrowSchema {
    fn from(schema: EngineArrowSchema) -> Self {
        schema.0
    }
}

impl From<ArrowSchemaRef> for EngineArrowSchemaRef {
    fn from(schema_ref: ArrowSchemaRef) -> Self {
        EngineArrowSchemaRef(schema_ref)
    }
}

impl From<EngineArrowSchemaRef> for ArrowSchemaRef {
    fn from(schema_ref: EngineArrowSchemaRef) -> Self {
        schema_ref.0
    }
}

impl From<ArrowField> for EngineArrowField {
    fn from(field: ArrowField) -> Self {
        EngineArrowField(field)
    }
}

impl From<Arc<ArrowField>> for EngineArrowField {
    fn from(field: Arc<ArrowField>) -> Self {
        EngineArrowField((*field).clone())
    }
}

impl From<EngineArrowField> for ArrowField {
    fn from(field: EngineArrowField) -> Self {
        field.0
    }
}

impl<'a> From<&'a ArrowField> for EngineArrowFieldRef<'a> {
    fn from(field_ref: &'a ArrowField) -> Self {
        EngineArrowFieldRef(field_ref)
    }
}

impl From<ArrowDataType> for EngineArrowDataType {
    fn from(data_type: ArrowDataType) -> Self {
        EngineArrowDataType(data_type)
    }
}

impl From<EngineArrowDataType> for ArrowDataType {
    fn from(data_type: EngineArrowDataType) -> Self {
        data_type.0
    }
}

// The original TryFrom implementations have been replaced with helper functions
// to avoid orphan rule violations. The implementations have been moved to
// standalone functions above.

#[cfg(test)]
mod tests {
    use crate::arrow_conversion::{ArrowField, ArrowSchema, EngineArrowField, EngineArrowSchema, EngineArrowDataType, ArrowDataType};
    use crate::{
        schema::{DataType, StructField, StructType},
        DeltaResult,
    };
    use std::collections::HashMap;

    #[test]
    fn test_metadata_string_conversion() -> DeltaResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("description", "hello world".to_owned());
        let struct_field = StructField::not_null("name", DataType::STRING).with_metadata(metadata);

        let arrow_field = ArrowField::try_from(&struct_field)?;
        let new_metadata = arrow_field.metadata();

        assert_eq!(
            new_metadata.get("description").unwrap(),
            &"hello world".to_owned()
        );
        Ok(())
    }

    #[test]
    fn test_newtype_wrappers() -> DeltaResult<()> {
        // Test that we can create a newtype and access the inner value through deref
        let mut metadata = HashMap::new();
        metadata.insert("description", "hello world".to_owned());
        let struct_field = StructField::not_null("name", DataType::STRING).with_metadata(metadata);

        // Convert to ArrowField
        let arrow_field = ArrowField::try_from(&struct_field)?;

        // Wrap in EngineArrowField
        let engine_field = EngineArrowField::from(arrow_field);

        // Access through deref
        let name = engine_field.name();
        assert_eq!(name, "name");

        // Test metadata access works through deref
        let new_metadata = engine_field.metadata();
        assert_eq!(
            new_metadata.get("description").unwrap(),
            &"hello world".to_owned()
        );

        Ok(())
    }

    #[test]
    fn test_newtypes_with_helper_functions() -> DeltaResult<()> {
        // Test that our newtypes work with our helper functions through Deref

        // Create an Arrow schema with some fields
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]);

        // Wrap in EngineArrowSchema
        let engine_schema = EngineArrowSchema(arrow_schema);

        // Convert to StructType using helper function through Deref
        let struct_type = struct_type_from_arrow_schema_ref(Arc::new(engine_schema.clone().into()))?;

        // Verify the conversion worked correctly
        assert_eq!(struct_type.fields().count(), 2);
        assert_eq!(struct_type.field_with_name("id").unwrap().data_type(), &DataType::INTEGER);
        assert_eq!(struct_type.field_with_name("name").unwrap().data_type(), &DataType::STRING);

        // Now test DataType conversion
        let arrow_data_type = ArrowDataType::Int32;
        let engine_data_type = EngineArrowDataType(arrow_data_type);

        // Convert to DataType using helper function through Deref
        let data_type = data_type_from_arrow_data_type(&engine_data_type)?;

        // Verify the conversion worked correctly
        assert_eq!(data_type, DataType::INTEGER);

        Ok(())
    }
}