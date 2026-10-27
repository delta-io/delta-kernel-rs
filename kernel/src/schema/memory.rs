//! Measures retained schema roots during column-default validation. Nested nodes carry no counts.

use std::collections::HashSet;
use std::mem::size_of;

use super::{ArrayType, DataType, MapType, MetadataColumnSpec, StructField, StructType};
use crate::snapshot::memory::{
    hash_table_bytes, index_map_bytes, HeapSize, Memory, SizeContext, Unavailable,
};
use crate::KernelResult;

/// Validates logical column defaults while measuring corresponding retained schema representations.
/// Roots are logical, physical, partition-excluded logical, and partition-excluded physical.
/// The representations must preserve nested field order and container shape; only the last two
/// roots omit partition columns. Unexpected shapes make accounting unavailable, not validation.
pub(crate) fn validate_defaults_and_measure_schemas(
    schemas: [&StructType; 4],
    partition_columns: &HashSet<&str>,
) -> KernelResult<(bool, [Memory; 4])> {
    let mut measurement = Measurement {
        totals: [Memory::ZERO; 4],
        has_default: false,
        context: SizeContext::default(),
    };
    measurement.visit_struct(schemas.map(Some), true, Some(partition_columns))?;
    Ok((measurement.has_default, measurement.totals))
}

struct Measurement {
    totals: [Memory; 4],
    has_default: bool,
    context: SizeContext,
}

impl Measurement {
    fn visit_struct(
        &mut self,
        schemas: [Option<&StructType>; 4],
        validate: bool,
        partition_columns: Option<&HashSet<&str>>,
    ) -> KernelResult<()> {
        let Some(logical) = schemas[0] else {
            return Ok(());
        };
        for (i, schema) in schemas.iter().enumerate() {
            if let Some(schema) = schema {
                self.totals[i] += schema.type_name.heap_size(&mut self.context)
                    + index_map_bytes::<String, StructField>(schema.fields.capacity())
                    + hash_table_bytes::<(MetadataColumnSpec, usize)>(
                        schema.metadata_columns.capacity(),
                    );
            }
        }
        let mut fields = schemas.map(|schema| schema.map(|s| s.fields.iter()));
        for logical_field in logical.fields() {
            let is_partition = partition_columns
                .is_some_and(|columns| columns.contains(logical_field.name().as_str()));
            let current = std::array::from_fn(|i| {
                if i >= 2 && is_partition {
                    return None;
                }
                let next = fields[i].as_mut().and_then(Iterator::next);
                if next.is_none() && schemas[i].is_some() {
                    self.totals[i] = Memory::unavailable(Unavailable::SchemaShape);
                }
                next.map(|(key, field)| {
                    self.totals[i] += key.heap_size(&mut self.context)
                        + field.name.heap_size(&mut self.context)
                        + field.metadata.heap_size(&mut self.context);
                    field
                })
            });
            if validate && logical_field.column_default()?.is_some() {
                self.has_default = true;
            }
            self.visit_type(
                current.map(|field| field.map(StructField::data_type)),
                validate,
            )?;
        }
        for (i, fields) in fields.iter_mut().enumerate() {
            if fields.as_mut().and_then(Iterator::next).is_some() {
                self.totals[i] = Memory::unavailable(Unavailable::SchemaShape);
            }
        }
        Ok(())
    }

    fn visit_type(&mut self, types: [Option<&DataType>; 4], validate: bool) -> KernelResult<()> {
        let Some(logical) = types[0] else {
            return Ok(());
        };
        match logical {
            DataType::Struct(_) | DataType::Variant(_) | DataType::File(_) => {
                let schemas = std::array::from_fn(|i| match (logical, types[i]) {
                    (DataType::Struct(_), Some(DataType::Struct(schema)))
                    | (DataType::Variant(_), Some(DataType::Variant(schema)))
                    | (DataType::File(_), Some(DataType::File(schema))) => {
                        self.totals[i] += Memory::precise(size_of::<StructType>());
                        Some(schema.as_ref())
                    }
                    (_, Some(_)) => {
                        self.totals[i] = Memory::unavailable(Unavailable::SchemaShape);
                        None
                    }
                    (_, None) => None,
                });
                // Variant/File inner fields are physical storage, not user column defaults.
                self.visit_struct(
                    schemas,
                    validate && matches!(logical, DataType::Struct(_)),
                    None,
                )?;
            }
            DataType::Array(_) => {
                let elements = std::array::from_fn(|i| match types[i] {
                    Some(DataType::Array(array)) => {
                        self.totals[i] += Memory::precise(size_of::<ArrayType>())
                            + array.type_name.heap_size(&mut self.context);
                        Some(&array.element_type)
                    }
                    Some(_) => {
                        self.totals[i] = Memory::unavailable(Unavailable::SchemaShape);
                        None
                    }
                    None => None,
                });
                self.visit_type(elements, validate)?;
            }
            DataType::Map(_) => {
                let maps = std::array::from_fn::<_, 4, _>(|i| match types[i] {
                    Some(DataType::Map(map)) => {
                        self.totals[i] += Memory::precise(size_of::<MapType>())
                            + map.type_name.heap_size(&mut self.context);
                        Some(map.as_ref())
                    }
                    Some(_) => {
                        self.totals[i] = Memory::unavailable(Unavailable::SchemaShape);
                        None
                    }
                    None => None,
                });
                self.visit_type(maps.map(|map| map.map(|m| &m.key_type)), validate)?;
                self.visit_type(maps.map(|map| map.map(|m| &m.value_type)), validate)?;
            }
            DataType::Primitive(_) => {
                for (i, data_type) in types.iter().enumerate() {
                    match data_type {
                        Some(DataType::Primitive(primitive)) => {
                            self.totals[i] += primitive.heap_size(&mut self.context);
                        }
                        Some(_) => self.totals[i] = Memory::unavailable(Unavailable::SchemaShape),
                        None => {}
                    }
                }
            }
            #[cfg(feature = "udt-in-dev")]
            DataType::UserDefined(_) => {
                for (i, data_type) in types.iter().enumerate() {
                    if data_type.is_some() {
                        self.totals[i] = Memory::unavailable(Unavailable::UnsupportedType);
                    }
                }
            }
        }
        Ok(())
    }
}

impl HeapSize for StructType {
    fn heap_size(&self, _: &mut SizeContext) -> Memory {
        // Standalone schemas have no construction summary. Do not introduce a fallback walk.
        Memory::unavailable(Unavailable::UnmeasuredSchema)
    }
}
