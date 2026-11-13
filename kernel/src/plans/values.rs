//! Materializes executor batches using statically cached visitor metadata.

use crate::engine_data::{EngineData, GetData, RowVisitor};
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::plans::PlanResultRow;
use crate::plans::ir::nodes::Values;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, SchemaRef, StructField};
use crate::{KernelError, KernelResult, KernelResultIterator};

pub(super) fn from_batches<T: PlanResultRow>(
    batches: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
) -> KernelResult<Values> {
    let (schema, columns) = T::schema_and_columns();
    for field in schema.fields() {
        ensure_supported(field)?;
    }
    let mut visitor = ValuesVisitor {
        schema,
        columns,
        rows: Vec::new(),
    };
    for batch in batches {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    Ok(Values::new(schema.clone(), visitor.rows))
}

struct ValuesVisitor {
    schema: &'static SchemaRef,
    columns: &'static ColumnNamesAndTypes,
    rows: Vec<Vec<Scalar>>,
}

impl RowVisitor for ValuesVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if getters.len() != self.columns.as_ref().0.len() {
            return Err(KernelError::missing_column(
                "Incorrect number of result column getters",
            ));
        }
        self.rows.reserve(row_count);
        for row in 0..row_count {
            let mut getters = getters.iter().copied();
            let values = self
                .schema
                .fields()
                .map(|field| read_field(&mut getters, row, field))
                .collect::<KernelResult<Vec<_>>>()?;
            if getters.next().is_some() {
                return Err(KernelError::missing_column(
                    "Unused result column getters after reconstructing row",
                ));
            }
            self.rows.push(values);
        }
        Ok(())
    }
}

fn ensure_supported(field: &StructField) -> KernelResult<()> {
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
            if field.is_nullable() {
                return Err(KernelError::unsupported(format!(
                    "Nullable plan result struct field {} is unsupported",
                    field.name()
                )));
            }
            if fields.num_fields() == 0 {
                return Err(KernelError::unsupported(format!(
                    "Empty plan result struct field {} is unsupported",
                    field.name()
                )));
            }
            for child in fields.fields() {
                ensure_supported(child)?;
            }
            true
        }
        _ => false,
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

fn read_field<'a>(
    getters: &mut impl Iterator<Item = &'a dyn GetData<'a>>,
    row: usize,
    field: &StructField,
) -> KernelResult<Scalar> {
    if let DataType::Struct(fields) = field.data_type() {
        let values = fields
            .fields()
            .map(|child| read_field(getters, row, child))
            .collect::<KernelResult<Vec<_>>>()?;
        // Schema-order recursion and typed leaf reads preserve the constructor's invariants.
        return Ok(Scalar::Struct(StructData::from_values_unchecked(
            fields.as_ref().clone(),
            values,
        )));
    }
    let getter = getters.next().ok_or_else(|| {
        KernelError::missing_column(format!(
            "Missing result column getter for field {}",
            field.name()
        ))
    })?;
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
            .map(|v| Scalar::String(v.to_owned())),
        &DataType::BINARY => getter
            .get_binary(row, name)?
            .map(|v| Scalar::Binary(v.to_vec())),
        DataType::Array(_) => getter
            .get_list(row, name)?
            .map(|list| Scalar::Array(ArrayData::from(list.materialize()))),
        DataType::Map(_) => getter
            .get_map(row, name)?
            .map(|map| {
                let values = map.materialize();
                if values.len() != map.keys().count() {
                    return Err(KernelError::invalid_struct_data(format!(
                        "Null value or duplicate key in plan result map field {name}"
                    )));
                }
                Ok(Scalar::Map(MapData::from(values)))
            })
            .transpose()?,
        _ => {
            return Err(KernelError::unsupported(format!(
                "Plan result field {name}"
            )));
        }
    };
    if value.is_none() && !field.is_nullable() {
        return Err(KernelError::invalid_struct_data(format!(
            "Value for non-nullable field {name} cannot be null"
        )));
    }
    Ok(value.unwrap_or_else(|| Scalar::Null(field.data_type().clone())))
}
