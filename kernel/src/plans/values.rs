//! Materializes engine batches as struct rows through existing leaf getters.

use std::sync::{Arc, LazyLock};

use crate::engine_data::{EngineData, GetData, RowVisitor};
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::schema::{ArrayType, ColumnName, DataType, MapType, SchemaRef, StructField};
use crate::{KernelError, KernelResult};

pub(super) fn read_struct(
    batch: &dyn EngineData,
    schema: &SchemaRef,
    path: &mut ColumnName,
    nulls: Option<&[bool]>,
) -> KernelResult<Vec<Option<StructData>>> {
    let mut rows: Vec<Vec<Scalar>> = (0..batch.len())
        .map(|_| Vec::with_capacity(schema.num_fields()))
        .collect();
    for field in schema.fields() {
        path.path_mut().push(field.name().clone());
        let values = if let DataType::Struct(children) = field.data_type() {
            let child_nulls = batch.is_null(path)?;
            read_struct(
                batch,
                &Arc::new(children.as_ref().clone()),
                path,
                Some(&child_nulls),
            )?
            .into_iter()
            .map(|row| match row {
                Some(row) => Scalar::Struct(row),
                None => Scalar::Null(field.data_type().clone()),
            })
            .collect()
        } else {
            let mut visitor = ColumnVisitor {
                field,
                nulls,
                values: Vec::new(),
            };
            batch.visit_rows(std::slice::from_ref(path), &mut visitor)?;
            visitor.values
        };
        path.path_mut().pop();
        for (row, value) in rows.iter_mut().zip(values) {
            row.push(value);
        }
    }
    rows.into_iter()
        .enumerate()
        .map(|(index, values)| {
            nulls
                .is_none_or(|nulls| !nulls[index])
                .then(|| StructData::try_new(schema.clone(), values))
                .transpose()
        })
        .collect()
}

struct ColumnVisitor<'a> {
    field: &'a StructField,
    nulls: Option<&'a [bool]>,
    values: Vec<Scalar>,
}

impl RowVisitor for ColumnVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAME: LazyLock<[ColumnName; 1]> = LazyLock::new(|| [ColumnName::new(["value"])]);
        static COLLECTIONS: LazyLock<[DataType; 2]> = LazyLock::new(|| {
            [
                ArrayType::new(DataType::STRING, false).into(),
                MapType::new(DataType::STRING, DataType::STRING, true).into(),
            ]
        });
        let types: &'static [DataType] = match self.field.data_type() {
            &DataType::BOOLEAN => &[DataType::BOOLEAN],
            &DataType::BYTE => &[DataType::BYTE],
            &DataType::SHORT => &[DataType::SHORT],
            &DataType::INTEGER => &[DataType::INTEGER],
            &DataType::LONG => &[DataType::LONG],
            &DataType::FLOAT => &[DataType::FLOAT],
            &DataType::DOUBLE => &[DataType::DOUBLE],
            &DataType::STRING => &[DataType::STRING],
            &DataType::BINARY => &[DataType::BINARY],
            &DataType::DATE => &[DataType::DATE],
            DataType::Array(_) => &COLLECTIONS[..1],
            DataType::Map(_) => &COLLECTIONS[1..],
            _ => &[],
        };
        (NAME.as_slice(), types)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [getter] = getters else {
            return Err(KernelError::missing_column(
                "Expected one result column getter",
            ));
        };
        self.values = (0..row_count)
            .map(|row| {
                if self.nulls.is_none_or(|nulls| !nulls[row]) {
                    read_scalar(*getter, row, self.field)
                } else {
                    Ok(Scalar::Null(self.field.data_type().clone()))
                }
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
            map.key_type() == &DataType::STRING && map.value_type() == &DataType::STRING
        }
        DataType::Struct(fields) => {
            for field in fields.fields() {
                ensure_supported(field)?;
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
