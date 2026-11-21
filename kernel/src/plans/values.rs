//! Materializes engine batches as struct rows through existing leaf getters.

use crate::engine_data::{EngineData, GetData, RowVisitor};
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::schema::{
    ColumnName, ColumnNamesAndTypes, DataType, PrimitiveType, SchemaRef, StructField,
};
use crate::{KernelError, KernelResult};

pub(super) fn read_struct(
    batch: &dyn EngineData,
    schema: &'static SchemaRef,
    columns: &'static ColumnNamesAndTypes,
) -> KernelResult<Vec<StructData>> {
    let mut visitor = StructVisitor {
        schema,
        columns,
        rows: Vec::new(),
    };
    visitor.visit_rows_of(batch)?;
    Ok(visitor.rows)
}

struct StructVisitor {
    schema: &'static SchemaRef,
    columns: &'static ColumnNamesAndTypes,
    rows: Vec<StructData>,
}

impl RowVisitor for StructVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if getters.len() != self.columns.as_ref().0.len() {
            return Err(KernelError::missing_column(
                "Expected one getter for each result leaf",
            ));
        }
        self.rows = (0..row_count)
            .map(|row| {
                let values = self
                    .schema
                    .fields()
                    .zip(getters)
                    .map(|(field, getter)| read_scalar(*getter, row, field))
                    .collect::<KernelResult<_>>()?;
                StructData::try_new(self.schema.clone(), values)
            })
            .collect::<KernelResult<_>>()?;
        Ok(())
    }
}

pub(super) fn ensure_supported(field: &StructField) -> KernelResult<()> {
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
        DataType::Struct(_) => false,
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
            return Err(KernelError::unsupported(format!(
                "Plan result field {name}"
            )));
        }
    };
    Ok(value.unwrap_or_else(|| Scalar::Null(field.data_type().clone())))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use test_utils::generate_batch;

    use super::*;
    use crate::arrow::array::{ArrayRef, Date32Array};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::schema::lazy_schema_ref;

    #[test]
    fn dates_preserve_values_and_nulls() -> KernelResult<()> {
        let values = Arc::new(Date32Array::from(vec![Some(-1), Some(0), None])) as ArrayRef;
        static SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! { nullable "value": DATE };
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| SCHEMA.leaves(None));
        ensure_supported(SCHEMA.field("value").unwrap())?;
        let batch = generate_batch([("value", values)])?;
        let rows = read_struct(&ArrowEngineData::new(batch), &SCHEMA, &COLUMNS)?;
        assert_eq!(
            rows.into_iter()
                .flat_map(|row| row.into_parts().1)
                .collect::<Vec<_>>(),
            vec![
                Scalar::Date(-1),
                Scalar::Date(0),
                Scalar::Null(DataType::DATE)
            ]
        );
        Ok(())
    }
}
