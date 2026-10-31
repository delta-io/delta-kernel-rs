//! Measures each retained schema near its construction. Nested nodes carry no counts.

use std::mem::size_of;

use super::{ArrayType, DataType, MapType, MetadataColumnSpec, StructField, StructType};
use crate::snapshot::memory::{
    hash_table_bytes, index_map_bytes, HeapSize, Memory, SchemaMemory, SizeContext, Unavailable,
};
use crate::KernelResult;

/// Measures one schema root, optionally combining the required logical-default validation.
/// Physical and partition-excluded roots are measured independently of the logical shape.
pub(crate) fn measure_schema(
    schema: &StructType,
    validate_defaults: bool,
) -> KernelResult<(bool, Memory)> {
    let mut measurement = Measurement {
        total: SchemaMemory::default(),
        has_default: false,
        context: SizeContext::default(),
    };
    measurement.visit_struct(schema, validate_defaults)?;
    Ok((measurement.has_default, measurement.total.finish()))
}

/// Measures each owned field while its construction is fresh, then the completed root's storage.
/// Fields must satisfy the same validity requirements as [`StructType::new_unchecked`].
pub(crate) fn build_and_measure_schema(
    fields: impl Iterator<Item = StructField>,
) -> KernelResult<(StructType, Memory)> {
    let mut measurement = Measurement {
        total: SchemaMemory::default(),
        has_default: false,
        context: SizeContext::default(),
    };
    let mut result = Ok(());
    let mut measured_fields = 0;
    let schema = StructType::new_unchecked(fields.inspect(|field| {
        measured_fields += 1;
        if result.is_ok() {
            result = measurement.visit_field(field, false);
        }
    }));
    result?;
    if measured_fields != schema.fields.len() {
        return Ok((schema, Memory::unavailable(Unavailable::SchemaShape)));
    }
    measurement.add_root_storage(&schema);
    for key in schema.fields.keys() {
        measurement.total += key.heap_size(&mut measurement.context);
    }
    Ok((schema, measurement.total.finish()))
}

struct Measurement {
    total: SchemaMemory,
    has_default: bool,
    context: SizeContext,
}

impl Measurement {
    fn add_root_storage(&mut self, schema: &StructType) {
        self.total += schema.type_name.heap_size(&mut self.context)
            + index_map_bytes::<String, StructField>(schema.fields.capacity())
            + hash_table_bytes::<(MetadataColumnSpec, usize)>(schema.metadata_columns.capacity());
    }

    fn visit_struct(&mut self, schema: &StructType, validate: bool) -> KernelResult<()> {
        self.add_root_storage(schema);
        for (key, field) in &schema.fields {
            self.total += key.heap_size(&mut self.context);
            self.visit_field(field, validate)?;
        }
        Ok(())
    }

    fn visit_field(&mut self, field: &StructField, validate: bool) -> KernelResult<()> {
        self.total += field.name.heap_size(&mut self.context);
        self.total.add_field_metadata(&field.metadata);
        if validate && field.column_default()?.is_some() {
            self.has_default = true;
        }
        self.visit_type(&field.data_type, validate)
    }

    fn visit_type(&mut self, data_type: &DataType, validate: bool) -> KernelResult<()> {
        match data_type {
            DataType::Struct(schema) | DataType::Variant(schema) | DataType::File(schema) => {
                self.total += Memory::precise(size_of::<StructType>());
                // Variant/File inner fields are physical storage, not user column defaults.
                self.visit_struct(schema, validate && matches!(data_type, DataType::Struct(_)))?;
            }
            DataType::Array(array) => {
                self.total += Memory::precise(size_of::<ArrayType>())
                    + array.type_name.heap_size(&mut self.context);
                self.visit_type(&array.element_type, validate)?;
            }
            DataType::Map(map) => {
                self.total += Memory::precise(size_of::<MapType>())
                    + map.type_name.heap_size(&mut self.context);
                self.visit_type(&map.key_type, validate)?;
                self.visit_type(&map.value_type, validate)?;
            }
            DataType::Primitive(primitive) => self.total += primitive.heap_size(&mut self.context),
            #[cfg(feature = "udt-in-dev")]
            DataType::UserDefined(udt) => {
                self.total += Memory::precise(size_of::<DataType>())
                    + udt.annotation().heap_size(&mut self.context);
                // UDT physical fields are not logical column-default annotations.
                self.visit_type(udt.sql_type(), false)?;
            }
        }
        Ok(())
    }
}

impl HeapSize for StructType {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        // A retained checkpoint schema is not one of the four configuration roots.
        // Measure it once when assembling the Snapshot, without retaining node counters.
        measure_schema(self, false).map_or_else(
            |_| Memory::unavailable(Unavailable::SchemaShape),
            |(_, size)| size,
        )
    }
}

impl HeapSize for super::PrimitiveType {
    fn heap_size(&self, _context: &mut SizeContext) -> Memory {
        match self {
            #[cfg(feature = "geo-type-in-dev")]
            Self::Geometry(value) => {
                Memory::precise(size_of::<super::GeometryType>()) + value.crs.heap_size(_context)
            }
            #[cfg(feature = "geo-type-in-dev")]
            Self::Geography(value) => {
                Memory::precise(size_of::<super::GeographyType>()) + value.crs.heap_size(_context)
            }
            _ => Memory::ZERO,
        }
    }
}
