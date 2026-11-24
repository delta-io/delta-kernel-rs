//! Streams log-action JSON without Arrow's full-batch intermediate output buffer.

use std::io::Write;

use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use super::filter_to_record_batch;
use crate::arrow::array::{
    Array, BooleanArray, FixedSizeListArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    PrimitiveArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use crate::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use crate::engine_data::FilteredEngineData;
use crate::memory::JsonOutput;
use crate::{KernelError, KernelResult, KernelResultIterator};

pub(super) fn write(
    data: impl KernelResultIterator<FilteredEngineData>,
) -> KernelResult<bytes::Bytes> {
    let mut output = JsonOutput::new();
    let result: KernelResult<()> = (|| {
        for chunk in data {
            let batch = filter_to_record_batch(chunk?)?;
            for row in 0..batch.num_rows() {
                serde_json::to_writer(&mut output, &Row { batch: &batch, row })
                    .map_err(KernelError::from)?;
                output.write_all(b"\n")?;
            }
        }
        Ok(())
    })();
    if let Some(error) = output.failure() {
        return Err(error.into());
    }
    result?;
    Ok(output.finish())
}

struct Row<'a> {
    batch: &'a RecordBatch,
    row: usize,
}
impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (field, array) in self
            .batch
            .schema()
            .fields()
            .iter()
            .zip(self.batch.columns())
        {
            if !array.is_null(self.row) {
                map.serialize_entry(
                    field.name(),
                    &Cell {
                        array: array.as_ref(),
                        row: self.row,
                    },
                )?;
            }
        }
        map.end()
    }
}

struct Cell<'a> {
    array: &'a dyn Array,
    row: usize,
}

macro_rules! scalar {
    ($value:expr, $serializer:expr, $ty:ty) => {{
        let array = $value
            .array
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| S::Error::custom("Arrow array does not match its declared type"))?;
        array.value($value.row).serialize($serializer)
    }};
}
impl Serialize for Cell<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.array.is_null(self.row) {
            return serializer.serialize_none();
        }
        match self.array.data_type() {
            DataType::Null => serializer.serialize_none(),
            DataType::Boolean => scalar!(self, serializer, BooleanArray),
            DataType::Int8 => scalar!(self, serializer, PrimitiveArray<Int8Type>),
            DataType::Int16 => scalar!(self, serializer, PrimitiveArray<Int16Type>),
            DataType::Int32 => scalar!(self, serializer, PrimitiveArray<Int32Type>),
            DataType::Int64 => scalar!(self, serializer, PrimitiveArray<Int64Type>),
            DataType::UInt8 => scalar!(self, serializer, PrimitiveArray<UInt8Type>),
            DataType::UInt16 => scalar!(self, serializer, PrimitiveArray<UInt16Type>),
            DataType::UInt32 => scalar!(self, serializer, PrimitiveArray<UInt32Type>),
            DataType::UInt64 => scalar!(self, serializer, PrimitiveArray<UInt64Type>),
            DataType::Float32 => scalar!(self, serializer, PrimitiveArray<Float32Type>),
            DataType::Float64 => scalar!(self, serializer, PrimitiveArray<Float64Type>),
            DataType::Utf8 => scalar!(self, serializer, StringArray),
            DataType::LargeUtf8 => scalar!(self, serializer, LargeStringArray),
            DataType::Utf8View => scalar!(self, serializer, StringViewArray),
            DataType::Struct(_) => {
                let array = self
                    .array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| S::Error::custom("expected a struct array"))?;
                let mut map = serializer.serialize_map(None)?;
                for (field, child) in array.fields().iter().zip(array.columns()) {
                    if !child.is_null(self.row) {
                        map.serialize_entry(
                            field.name(),
                            &Cell {
                                array: child.as_ref(),
                                row: self.row,
                            },
                        )?;
                    }
                }
                map.end()
            }
            DataType::List(_) => self.list::<S, ListArray>(serializer),
            DataType::LargeList(_) => self.list::<S, LargeListArray>(serializer),
            DataType::FixedSizeList(_, _) => self.list::<S, FixedSizeListArray>(serializer),
            DataType::Map(_, _) => {
                let array = self
                    .array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| S::Error::custom("expected a map array"))?;
                let offsets = array.value_offsets();
                let range = offsets[self.row] as usize..offsets[self.row + 1] as usize;
                let mut map = serializer.serialize_map(Some(range.len()))?;
                for row in range {
                    if array.keys().is_null(row) {
                        return Err(S::Error::custom("null map key"));
                    }
                    map.serialize_entry(
                        &Cell {
                            array: array.keys().as_ref(),
                            row,
                        },
                        &Cell {
                            array: array.values().as_ref(),
                            row,
                        },
                    )?;
                }
                map.end()
            }
            _ => Err(S::Error::custom(
                "limited JSON output supports log-action primitive and container types",
            )),
        }
    }
}

trait ListValues: Array {
    fn row_values(&self, row: usize) -> crate::arrow::array::ArrayRef;
}
impl ListValues for ListArray {
    fn row_values(&self, row: usize) -> crate::arrow::array::ArrayRef {
        self.value(row)
    }
}
impl ListValues for LargeListArray {
    fn row_values(&self, row: usize) -> crate::arrow::array::ArrayRef {
        self.value(row)
    }
}
impl ListValues for FixedSizeListArray {
    fn row_values(&self, row: usize) -> crate::arrow::array::ArrayRef {
        self.value(row)
    }
}
impl Cell<'_> {
    fn list<S: Serializer, A: ListValues + 'static>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let array = self
            .array
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| S::Error::custom("expected a list array"))?;
        let values = array.row_values(self.row);
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for row in 0..values.len() {
            sequence.serialize_element(&Cell {
                array: values.as_ref(),
                row,
            })?;
        }
        sequence.end()
    }
}
