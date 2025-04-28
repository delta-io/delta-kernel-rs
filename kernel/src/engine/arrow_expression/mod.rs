//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use crate::arrow::array::{self, ArrayBuilder, ArrayRef, RecordBatch};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

use crate::engine::arrow_data::ArrowEngineData;
use crate::error::{DeltaResult, Error};
use crate::expressions::{Expression, Scalar};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::utils::require;
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator};

use itertools::Itertools;
use tracing::debug;

use apply_schema::{apply_schema, apply_schema_to};
use evaluate_expression::evaluate_expression;

mod apply_schema;
mod evaluate_expression;

#[cfg(test)]
mod tests;

// TODO leverage scalars / Datum

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> DeltaResult<ArrayRef> {
        let data_type = ArrowDataType::try_from(&self.data_type())?;
        let mut builder = array::make_builder(&data_type, num_rows);
        self.append(&mut builder, num_rows)?;
        Ok(builder.finish())
    }

    fn append(&self, builder: &mut dyn ArrayBuilder, num_rows: usize) -> DeltaResult<()> {
        use Scalar::*;
        macro_rules! builder_as {
            ($t:ty) => {{
                builder.as_any_mut().downcast_mut::<$t>().ok_or_else(|| {
                    Error::invalid_expression(format!("Invalid builder for {}", self.data_type()))
                })?
            }};
        }

        macro_rules! append_val_as {
            ($t:ty, $val:expr) => {{
                let builder = builder_as!($t);
                for _ in 0..num_rows {
                    builder.append_value($val)
                }
            }};
        }

        macro_rules! append_null_as {
            ($t:ty) => {{
                let builder = builder_as!($t);
                for _ in 0..num_rows {
                    builder.append_null()
                }
            }};
        }

        type GenericBuilder = Box<dyn ArrayBuilder>;

        match self {
            Integer(val) => append_val_as!(array::Int32Builder, *val),
            Long(val) => append_val_as!(array::Int64Builder, *val),
            Short(val) => append_val_as!(array::Int16Builder, *val),
            Byte(val) => append_val_as!(array::Int8Builder, *val),
            Float(val) => append_val_as!(array::Float32Builder, *val),
            Double(val) => append_val_as!(array::Float64Builder, *val),
            String(val) => append_val_as!(array::StringBuilder, val),
            Boolean(val) => append_val_as!(array::BooleanBuilder, *val),
            Timestamp(val) | TimestampNtz(val) => {
                // timezone was already set at builder construction time
                append_val_as!(array::TimestampMicrosecondBuilder, *val)
            }
            Date(val) => append_val_as!(array::Date32Builder, *val),
            Binary(val) => append_val_as!(array::BinaryBuilder, val),
            // precision and scale were already set at builder construction time
            Decimal(val) => append_val_as!(array::Decimal128Builder, val.bits()),
            Struct(data) => {
                let builder = builder_as!(array::StructBuilder);
                require!(
                    builder.num_fields() == data.fields().len(),
                    Error::generic("Struct builder has wrong number of fields")
                );
                for _ in 0..num_rows {
                    let field_builders = builder.field_builders_mut().iter_mut();
                    for (builder, value) in field_builders.zip(data.values()) {
                        value.append(builder, 1)?;
                    }
                    builder.append(true);
                }
            }
            Array(data) => {
                let builder = builder_as!(array::ListBuilder<GenericBuilder>);
                for _ in 0..num_rows {
                    #[allow(deprecated)]
                    for value in data.array_elements() {
                        value.append(builder.values(), 1)?;
                    }
                    builder.append(true);
                }
            }
            Null(DataType::INTEGER) => append_null_as!(array::Int32Builder),
            Null(DataType::LONG) => append_null_as!(array::Int64Builder),
            Null(DataType::SHORT) => append_null_as!(array::Int16Builder),
            Null(DataType::BYTE) => append_null_as!(array::Int8Builder),
            Null(DataType::FLOAT) => append_null_as!(array::Float32Builder),
            Null(DataType::DOUBLE) => append_null_as!(array::Float64Builder),
            Null(DataType::STRING) => append_null_as!(array::StringBuilder),
            Null(DataType::BOOLEAN) => append_null_as!(array::BooleanBuilder),
            Null(DataType::TIMESTAMP | DataType::TIMESTAMP_NTZ) => {
                append_null_as!(array::TimestampMicrosecondBuilder)
            }
            Null(DataType::DATE) => append_null_as!(array::Date32Builder),
            Null(DataType::BINARY) => append_null_as!(array::BinaryBuilder),
            Null(DataType::Primitive(PrimitiveType::Decimal(_))) => {
                append_null_as!(array::Decimal128Builder)
            }
            Null(DataType::Struct(_)) => append_null_as!(array::StructBuilder),
            Null(DataType::Array(_)) => append_null_as!(array::ListBuilder<GenericBuilder>),
            Null(DataType::Map(_)) => {
                // For some reason, there is no `MapBuilder::append_null` method -- even tho
                // StructBuilder and ListBuilder both provide it.
                let builder = builder_as!(array::MapBuilder<GenericBuilder, GenericBuilder>);
                for _ in 0..num_rows {
                    builder.append(false)?;
                }
            }
        }
        Ok(())
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
