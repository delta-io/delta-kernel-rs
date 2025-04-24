//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use crate::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use crate::arrow::buffer::OffsetBuffer;
use crate::arrow::compute::concat;
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
};

use super::arrow_conversion::LIST_ARRAY_ROOT;
use crate::engine::arrow_data::ArrowEngineData;
use crate::error::{DeltaResult, Error};
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator};

use itertools::Itertools;
use tracing::debug;

use apply_schema::{apply_schema, apply_schema_to};
use evaluate_expression::evaluate_expression;

mod apply_schema;
mod evaluate_expression;
mod in_list;

#[cfg(test)]
mod tests;

// TODO leverage scalars / Datum

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> DeltaResult<ArrayRef> {
        use Scalar::*;
        let arr: ArrayRef = match self {
            Integer(val) => Arc::new(Int32Array::from_value(*val, num_rows)),
            Long(val) => Arc::new(Int64Array::from_value(*val, num_rows)),
            Short(val) => Arc::new(Int16Array::from_value(*val, num_rows)),
            Byte(val) => Arc::new(Int8Array::from_value(*val, num_rows)),
            Float(val) => Arc::new(Float32Array::from_value(*val, num_rows)),
            Double(val) => Arc::new(Float64Array::from_value(*val, num_rows)),
            String(val) => Arc::new(StringArray::from(vec![val.clone(); num_rows])),
            Boolean(val) => Arc::new(BooleanArray::from(vec![*val; num_rows])),
            Timestamp(val) => {
                Arc::new(TimestampMicrosecondArray::from_value(*val, num_rows).with_timezone("UTC"))
            }
            TimestampNtz(val) => Arc::new(TimestampMicrosecondArray::from_value(*val, num_rows)),
            Date(val) => Arc::new(Date32Array::from_value(*val, num_rows)),
            Binary(val) => Arc::new(BinaryArray::from(vec![val.as_slice(); num_rows])),
            Decimal(val) => Arc::new(
                Decimal128Array::from_value(val.bits(), num_rows)
                    .with_precision_and_scale(val.precision(), val.scale() as i8)?, // 0..=38
            ),
            Struct(data) => {
                let arrays = data
                    .values()
                    .iter()
                    .map(|val| val.to_array(num_rows))
                    .try_collect()?;
                let fields: Fields = data
                    .fields()
                    .iter()
                    .map(ArrowField::try_from)
                    .try_collect()?;
                Arc::new(StructArray::try_new(fields, arrays, None)?)
            }
            Array(data) => {
                #[allow(deprecated)]
                let values = data.array_elements();
                let vecs: Vec<_> = values.iter().map(|v| v.to_array(num_rows)).try_collect()?;
                let values: Vec<_> = vecs.iter().map(|x| x.as_ref()).collect();
                let offsets: Vec<_> = vecs.iter().map(|v| v.len()).collect();
                let offset_buffer = OffsetBuffer::from_lengths(offsets);
                let field = ArrowField::try_from(data.array_type())?;
                Arc::new(ListArray::new(
                    Arc::new(field),
                    offset_buffer,
                    concat(values.as_slice())?,
                    None,
                ))
            }
            Null(DataType::BYTE) => Arc::new(Int8Array::new_null(num_rows)),
            Null(DataType::SHORT) => Arc::new(Int16Array::new_null(num_rows)),
            Null(DataType::INTEGER) => Arc::new(Int32Array::new_null(num_rows)),
            Null(DataType::LONG) => Arc::new(Int64Array::new_null(num_rows)),
            Null(DataType::FLOAT) => Arc::new(Float32Array::new_null(num_rows)),
            Null(DataType::DOUBLE) => Arc::new(Float64Array::new_null(num_rows)),
            Null(DataType::STRING) => Arc::new(StringArray::new_null(num_rows)),
            Null(DataType::BOOLEAN) => Arc::new(BooleanArray::new_null(num_rows)),
            Null(DataType::TIMESTAMP) => {
                Arc::new(TimestampMicrosecondArray::new_null(num_rows).with_timezone("UTC"))
            }
            Null(DataType::TIMESTAMP_NTZ) => {
                Arc::new(TimestampMicrosecondArray::new_null(num_rows))
            }
            Null(DataType::DATE) => Arc::new(Date32Array::new_null(num_rows)),
            Null(DataType::BINARY) => Arc::new(BinaryArray::new_null(num_rows)),
            Null(DataType::Primitive(PrimitiveType::Decimal(dtype))) => Arc::new(
                Decimal128Array::new_null(num_rows)
                    .with_precision_and_scale(dtype.precision(), dtype.scale() as i8)?, // 0..=38
            ),
            Null(DataType::Struct(t)) => {
                let fields: Fields = t.fields().map(ArrowField::try_from).try_collect()?;
                Arc::new(StructArray::new_null(fields, num_rows))
            }
            Null(DataType::Array(t)) => {
                let field = ArrowField::new(LIST_ARRAY_ROOT, t.element_type().try_into()?, true);
                Arc::new(ListArray::new_null(Arc::new(field), num_rows))
            }
            Null(DataType::Map { .. }) => {
                return Err(Error::unsupported(
                    "Scalar::to_array does not yet support Map types",
                ));
            }
        };
        Ok(arr)
    }
}

#[derive(Debug)]
pub struct ArrowEvaluationHandler;

impl EvaluationHandler for ArrowEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression,
            output_type,
        })
    }

    /// Create a single-row array with all-null leaf values. Note that if a nested struct is
    /// included in the `output_type`, the entire struct will be NULL (instead of a not-null struct
    /// with NULL fields).
    fn null_row(&self, output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
        let fields = output_schema.fields();
        let arrays = fields
            .map(|field| Scalar::Null(field.data_type().clone()).to_array(1))
            .try_collect()?;
        let record_batch =
            RecordBatch::try_new(Arc::new(output_schema.as_ref().try_into()?), arrays)?;
        Ok(Box::new(ArrowEngineData::new(record_batch)))
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Expression,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        debug!("Arrow evaluator evaluating: {:#?}", self.expression);
        let batch = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::engine_data_type("ArrowEngineData"))?
            .record_batch();
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };
        let array_ref = evaluate_expression(&self.expression, batch, Some(&self.output_type))?;
        let batch: RecordBatch = if let DataType::Struct(_) = self.output_type {
            apply_schema(&array_ref, &self.output_type)?
        } else {
            let array_ref = apply_schema_to(&array_ref, &self.output_type)?;
            let arrow_type: ArrowDataType = ArrowDataType::try_from(&self.output_type)?;
            let schema = ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)]);
            RecordBatch::try_new(Arc::new(schema), vec![array_ref])?
        };
        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}
