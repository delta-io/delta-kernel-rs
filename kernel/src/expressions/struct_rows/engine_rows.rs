//! Decodes engine batches one struct row at a time through existing leaf getters.

use std::sync::Arc;

use super::TryFromStructRows;
use crate::engine_data::{EngineData, GetData, RowVisitor};
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::schema::{
    ColumnName, DataType, PrimitiveType, SchemaRef, StructField, StructType, ToSchema,
};
use crate::{KernelError, KernelResult};

pub(super) struct StructVisitor<'a, T: TryFromStructRows> {
    pub(super) batch: &'a dyn EngineData,
    pub(super) state: &'a mut T::State,
}

impl<T: TryFromStructRows> RowVisitor for StructVisitor<'_, T> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        T::Row::leaf_columns().as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if getters.len() != T::Row::leaf_columns().as_ref().0.len() {
            return Err(KernelError::internal_error(
                "Result getter count disagrees with the validated leaf schema",
            ));
        }
        let schema = T::Row::schema_ref();
        let fields = read_fields(
            self.batch,
            schema,
            &mut ColumnName::default(),
            &mut getters.iter().copied(),
        )?;
        (0..row_count)
            .try_for_each(|row| T::push(self.state, read_struct_row(schema, &fields, row)?))
    }
}

enum FieldReader<'a> {
    Leaf(&'a dyn GetData<'a>),
    Struct {
        schema: SchemaRef,
        fields: Vec<FieldReader<'a>>,
        nulls: Vec<bool>,
    },
}

fn read_fields<'a>(
    batch: &dyn EngineData,
    schema: &'a StructType,
    path: &mut ColumnName,
    getters: &mut impl Iterator<Item = &'a dyn GetData<'a>>,
) -> KernelResult<Vec<FieldReader<'a>>> {
    schema
        .fields()
        .map(|field| {
            if let DataType::Struct(children) = field.data_type() {
                path.path_mut().push(field.name().clone());
                let nulls = batch.is_null(path)?;
                let fields = read_fields(batch, children, path, getters)?;
                path.path_mut().pop();
                Ok(FieldReader::Struct {
                    schema: Arc::new(children.as_ref().clone()),
                    fields,
                    nulls,
                })
            } else {
                Ok(FieldReader::Leaf(getters.next().ok_or_else(|| {
                    KernelError::missing_column("Missing result column getter")
                })?))
            }
        })
        .collect()
}

fn read_struct_row(
    schema: &SchemaRef,
    fields: &[FieldReader<'_>],
    row: usize,
) -> KernelResult<StructData> {
    let mut values = Vec::with_capacity(schema.num_fields());
    schema
        .fields()
        .zip(fields)
        .map(|(field, reader)| match reader {
            FieldReader::Leaf(getter) => read_scalar(*getter, row, field),
            FieldReader::Struct {
                schema,
                fields,
                nulls,
            } => {
                if nulls[row] {
                    Ok(Scalar::Null(field.data_type().clone()))
                } else {
                    read_struct_row(schema, fields, row).map(Scalar::Struct)
                }
            }
        })
        .try_for_each(|value| value.map(|value| values.push(value)))?;
    StructData::try_new(schema.clone(), values)
}

pub(crate) fn ensure_supported(field: &StructField) -> KernelResult<()> {
    let supported = match field.data_type() {
        &DataType::BOOLEAN
        | &DataType::BYTE
        | &DataType::SHORT
        | &DataType::INTEGER
        | &DataType::LONG
        | &DataType::FLOAT
        | &DataType::DOUBLE
        | &DataType::STRING
        | &DataType::BINARY
        | &DataType::DATE => true,
        DataType::Array(array) => {
            array.element_type() == &DataType::STRING && !array.contains_null()
        }
        DataType::Map(map) => {
            map.key_type() == &DataType::STRING
                && map.value_type() == &DataType::STRING
                && !map.value_contains_null()
        }
        DataType::Struct(fields) => {
            for field in fields.fields() {
                ensure_supported(field)?;
            }
            true
        }
        DataType::Primitive(
            PrimitiveType::Timestamp
            | PrimitiveType::TimestampNtz
            | PrimitiveType::Void
            | PrimitiveType::Decimal(_)
            | PrimitiveType::IntervalDayTime(_)
            | PrimitiveType::IntervalYearMonth(_),
        ) => false,
        #[cfg(feature = "geo-type-in-dev")]
        DataType::Primitive(PrimitiveType::Geometry(_) | PrimitiveType::Geography(_)) => false,
        #[cfg(feature = "udt-in-dev")]
        DataType::UserDefined(_) => false,
        DataType::Variant(_) | DataType::File(_) => false,
    };
    if supported {
        Ok(())
    } else {
        Err(KernelError::unsupported(format!(
            "Plan result field {} has unsupported type {}",
            field.name(),
            field.data_type()
        )))
    }
}

fn read_scalar<'a>(
    getter: &'a dyn GetData<'a>,
    row: usize,
    field: &StructField,
) -> KernelResult<Scalar> {
    let name = field.name();
    let value = match field.data_type() {
        &DataType::BOOLEAN => getter.get_bool(row, name)?.map(Scalar::Boolean),
        &DataType::BYTE => getter.get_byte(row, name)?.map(Scalar::Byte),
        &DataType::SHORT => getter.get_short(row, name)?.map(Scalar::Short),
        &DataType::INTEGER => getter.get_int(row, name)?.map(Scalar::Integer),
        &DataType::LONG => getter.get_long(row, name)?.map(Scalar::Long),
        &DataType::FLOAT => getter.get_float(row, name)?.map(Scalar::Float),
        &DataType::DOUBLE => getter.get_double(row, name)?.map(Scalar::Double),
        &DataType::DATE => getter.get_date(row, name)?.map(Scalar::Date),
        &DataType::STRING => getter
            .get_str(row, name)?
            .map(|value| Scalar::String(value.to_owned())),
        &DataType::BINARY => getter
            .get_binary(row, name)?
            .map(|value| Scalar::Binary(value.to_vec())),
        DataType::Array(_) => getter
            .get_list(row, name)?
            .map(|list| {
                list.iter()
                    .enumerate()
                    .map(|(index, value)| {
                        value.map(str::to_owned).ok_or_else(|| {
                            KernelError::invalid_struct_data(format!(
                                "String list element at index {index} cannot be null"
                            ))
                        })
                    })
                    .collect::<KernelResult<Vec<_>>>()
                    .map(|values| Scalar::Array(ArrayData::from(values)))
            })
            .transpose()?,
        DataType::Map(map_type) => getter.get_map(row, name)?.map(|map| {
            Scalar::Map(MapData::from_pairs::<String, String>(
                map.entries(),
                map_type.value_contains_null(),
            ))
        }),
        _ => {
            return Err(KernelError::internal_error(format!(
                "Unexpected type {} for result field {name} after schema validation",
                field.data_type()
            )));
        }
    };
    Ok(value.unwrap_or_else(|| Scalar::Null(field.data_type().clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Date32Array;

    #[test]
    fn dates_preserve_values_and_nulls() -> KernelResult<()> {
        let values = Date32Array::from(vec![Some(-1), Some(0), None]);
        let field = StructField::nullable("value", DataType::DATE);
        ensure_supported(&field)?;
        assert_eq!(
            (0..values.len())
                .map(|row| read_scalar(&values, row, &field))
                .collect::<KernelResult<Vec<_>>>()?,
            vec![
                Scalar::Date(-1),
                Scalar::Date(0),
                Scalar::Null(DataType::DATE)
            ]
        );
        Ok(())
    }
}
