//! Expression handling based on arrow-rs compute kernels.
mod apply_schema;
mod evaluate_expression;
#[cfg(test)]
mod tests;

use super::arrow_conversion::LIST_ARRAY_ROOT;
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
use crate::engine::arrow_data::ArrowEngineData;
use crate::error::{DeltaResult, Error};
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::{EngineData, ExpressionEvaluator, ExpressionHandler};
use apply_schema::{apply_schema, apply_schema_to};
use evaluate_expression::evaluate_expression;
use itertools::Itertools;
use std::sync::Arc;
use tracing::debug;

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
            Decimal(val, precision, scale) => Arc::new(
                Decimal128Array::from_value(*val, num_rows)
                    .with_precision_and_scale(*precision, *scale as i8)?,
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
            Null(data_type) => match data_type {
                DataType::Primitive(primitive) => match primitive {
                    PrimitiveType::Byte => Arc::new(Int8Array::new_null(num_rows)),
                    PrimitiveType::Short => Arc::new(Int16Array::new_null(num_rows)),
                    PrimitiveType::Integer => Arc::new(Int32Array::new_null(num_rows)),
                    PrimitiveType::Long => Arc::new(Int64Array::new_null(num_rows)),
                    PrimitiveType::Float => Arc::new(Float32Array::new_null(num_rows)),
                    PrimitiveType::Double => Arc::new(Float64Array::new_null(num_rows)),
                    PrimitiveType::String => Arc::new(StringArray::new_null(num_rows)),
                    PrimitiveType::Boolean => Arc::new(BooleanArray::new_null(num_rows)),
                    PrimitiveType::Timestamp => {
                        Arc::new(TimestampMicrosecondArray::new_null(num_rows).with_timezone("UTC"))
                    }
                    PrimitiveType::TimestampNtz => {
                        Arc::new(TimestampMicrosecondArray::new_null(num_rows))
                    }
                    PrimitiveType::Date => Arc::new(Date32Array::new_null(num_rows)),
                    PrimitiveType::Binary => Arc::new(BinaryArray::new_null(num_rows)),
                    PrimitiveType::Decimal(precision, scale) => Arc::new(
                        Decimal128Array::new_null(num_rows)
                            .with_precision_and_scale(*precision, *scale as i8)?,
                    ),
                },
                DataType::Struct(t) => {
                    let fields: Fields = t.fields().map(ArrowField::try_from).try_collect()?;
                    Arc::new(StructArray::new_null(fields, num_rows))
                }
                DataType::Array(t) => {
                    let field =
                        ArrowField::new(LIST_ARRAY_ROOT, t.element_type().try_into()?, true);
                    Arc::new(ListArray::new_null(Arc::new(field), num_rows))
                }
                DataType::Map { .. } => unimplemented!(),
            },
        };
        Ok(arr)
    }
}

#[derive(Debug)]
pub struct ArrowExpressionHandler;

impl ExpressionHandler for ArrowExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression: Box::new(expression),
            output_type,
        })
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Box<Expression>,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        debug!(
            "Arrow evaluator evaluating: {:#?}",
            self.expression.as_ref()
        );
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
