//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use crate::arrow::array::{self, ArrayBuilder, ArrayRef, RecordBatch, StructArray};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

use super::arrow_conversion::{TryFromKernel as _, TryIntoArrow as _};
use crate::engine::arrow_data::{extract_record_batch, ArrowEngineData};
use crate::error::{DeltaResult, Error};
use crate::expressions::{ArrayData, Expression, ExpressionRef, PredicateRef, Scalar};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::utils::require;
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator, PredicateEvaluator};

use itertools::Itertools;
use tracing::debug;

use apply_schema::{apply_schema, apply_schema_to};
use evaluate_expression::{evaluate_expression, evaluate_predicate, extract_column};

mod apply_schema;
pub mod evaluate_expression;
pub mod opaque;

#[cfg(test)]
mod tests;

// TODO leverage scalars / Datum

impl Scalar {
    /// Convert scalar to arrow array.
    pub fn to_array(&self, num_rows: usize) -> DeltaResult<ArrayRef> {
        let data_type = ArrowDataType::try_from_kernel(&self.data_type())?;
        let mut builder = array::make_builder(&data_type, num_rows);
        self.append_to(&mut builder, num_rows)?;
        Ok(builder.finish())
    }

    // Arrow uses composable "builders" to assemble arrays one row at a time. Each concrete `Array`
    // type has a corresponding concrete `ArrayBuilder` type. For primitive types, the builder just
    // needs to `append` one value per row. For complex types, the builder needs to recursively
    // append values to each of its children as needed, and then its own `append` only defines the
    // validity for the row. Unfortunately, there is no generic way to append values to builders;
    // the `ArrayBuilder` trait only knows how to `finalize` itself to produce an `ArrayRef`. So we
    // have to cast each builder to the appropriate type, based on the scalar's data type. For
    // details, refer to the arrow documentation:
    //
    // https://docs.rs/arrow/latest/arrow/array/struct.PrimitiveBuilder.html
    // https://docs.rs/arrow/latest/arrow/array/struct.GenericListBuilder.html
    // https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
    //
    // NOTE: `ListBuilder` and `MapBuilder` are take generic element/key/value builders in order to
    // work with specific builder types directly. However, `array::make_builder` instantiates them
    // with `Box<dyn Builder>` instead, which greatly simplifies our job in working with them. We
    // can just extract the builder trait, and let recursive calls cast it to the desired type.
    //
    // WARNING: List and map builders do _NOT_ require appending any child entries to NULL list/map
    // rows, because empty list/map is a valid state. But struct builders _DO_ require appending
    // (possibly NULL) entries in order to preserve consistent row counts between the struct and its
    // fields.
    fn append_to(&self, builder: &mut dyn ArrayBuilder, num_rows: usize) -> DeltaResult<()> {
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
                    builder.append_value($val);
                }
            }};
        }

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
                        value.append_to(builder, 1)?;
                    }
                    builder.append(true);
                }
            }
            Array(data) => {
                let builder = builder_as!(array::ListBuilder<Box<dyn ArrayBuilder>>);
                for _ in 0..num_rows {
                    #[allow(deprecated)]
                    for value in data.array_elements() {
                        value.append_to(builder.values(), 1)?;
                    }
                    builder.append(true);
                }
            }
            Map(data) => {
                let builder =
                    builder_as!(array::MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>);
                for _ in 0..num_rows {
                    for (key, val) in data.pairs() {
                        key.append_to(builder.keys(), 1)?;
                        val.append_to(builder.values(), 1)?;
                    }
                    builder.append(true)?;
                }
            }
            Null(data_type) => Self::append_null(builder, data_type, num_rows)?,
        }

        Ok(())
    }

    fn append_null(
        builder: &mut dyn ArrayBuilder,
        data_type: &DataType,
        num_rows: usize,
    ) -> DeltaResult<()> {
        // Almost the same as above -- differs only in the data type parameter
        macro_rules! builder_as {
            ($t:ty) => {{
                builder.as_any_mut().downcast_mut::<$t>().ok_or_else(|| {
                    Error::invalid_expression(format!("Invalid builder for {data_type}"))
                })?
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

        match *data_type {
            DataType::INTEGER => append_null_as!(array::Int32Builder),
            DataType::LONG => append_null_as!(array::Int64Builder),
            DataType::SHORT => append_null_as!(array::Int16Builder),
            DataType::BYTE => append_null_as!(array::Int8Builder),
            DataType::FLOAT => append_null_as!(array::Float32Builder),
            DataType::DOUBLE => append_null_as!(array::Float64Builder),
            DataType::STRING => append_null_as!(array::StringBuilder),
            DataType::BOOLEAN => append_null_as!(array::BooleanBuilder),
            DataType::TIMESTAMP | DataType::TIMESTAMP_NTZ => {
                append_null_as!(array::TimestampMicrosecondBuilder)
            }
            DataType::DATE => append_null_as!(array::Date32Builder),
            DataType::BINARY => append_null_as!(array::BinaryBuilder),
            DataType::Primitive(PrimitiveType::Decimal(_)) => {
                append_null_as!(array::Decimal128Builder)
            }
            DataType::Struct(ref stype) => {
                // WARNING: Unlike ArrayBuilder and MapBuilder, StructBuilder always requires us to
                // insert an entry for each child builder, even when we're inserting NULL.
                let builder = builder_as!(array::StructBuilder);
                require!(
                    builder.num_fields() == stype.num_fields(),
                    Error::generic("Struct builder has wrong number of fields")
                );
                for _ in 0..num_rows {
                    let field_builders = builder.field_builders_mut().iter_mut();
                    for (builder, field) in field_builders.zip(stype.fields()) {
                        Self::append_null(builder, &field.data_type, 1)?;
                    }
                    builder.append(false);
                }
            }
            DataType::Array(_) => append_null_as!(array::ListBuilder<Box<dyn ArrayBuilder>>),
            DataType::Map(_) => {
                // For some reason, there is no `MapBuilder::append_null` method -- even tho
                // StructBuilder and ListBuilder both provide it.
                let builder =
                    builder_as!(array::MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>);
                for _ in 0..num_rows {
                    builder.append(false)?;
                }
            }
            DataType::Variant(_) => {
                return Err(Error::unsupported(
                    "Variant is not supported as scalar yet.",
                ));
            }
        }
        Ok(())
    }
}

impl ArrayData {
    /// Convert kernel [`ArrayData`] to an Arrow [`ArrayRef`] of the equivalent type.
    pub fn to_arrow(&self) -> DeltaResult<ArrayRef> {
        let arrow_data_type = ArrowDataType::try_from_kernel(self.array_type().element_type())?;

        #[allow(deprecated)]
        let elements = self.array_elements();
        let mut builder = array::make_builder(&arrow_data_type, elements.len());
        for element in elements {
            element.append_to(&mut builder, 1)?;
        }

        Ok(builder.finish())
    }
}

#[derive(Debug)]
pub struct ArrowEvaluationHandler;

impl EvaluationHandler for ArrowEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        schema: SchemaRef,
        expression: ExpressionRef,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression,
            output_type,
        })
    }

    fn new_predicate_evaluator(
        &self,
        schema: SchemaRef,
        predicate: PredicateRef,
    ) -> Arc<dyn PredicateEvaluator> {
        Arc::new(DefaultPredicateEvaluator {
            input_schema: schema,
            predicate,
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
            RecordBatch::try_new(Arc::new(output_schema.as_ref().try_into_arrow()?), arrays)?;
        Ok(Box::new(ArrowEngineData::new(record_batch)))
    }
}

#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: ExpressionRef,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        debug!("Arrow evaluator evaluating: {:#?}", self.expression);
        let batch = extract_record_batch(batch)?;
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into_arrow()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };

        let batch = match (self.expression.as_ref(), &self.output_type) {
            (Expression::Transform(transform), DataType::Struct(_)) if transform.is_identity() => {
                // Empty transform optimization: Skip expression evaluation and directly apply the
                // output schema to the input RecordBatch. This is used to cheaply apply a new
                // output schema to existing data without changing it, e.g. for column mapping.
                let array = match transform.input_path() {
                    None => Arc::new(StructArray::from(batch.clone())),
                    Some(path) => extract_column(batch, path)?,
                };
                apply_schema(&array, &self.output_type)?
            }
            (expr, output_type @ DataType::Struct(_)) => {
                let array_ref = evaluate_expression(expr, batch, Some(output_type))?;
                apply_schema(&array_ref, output_type)?
            }
            (expr, output_type) => {
                let array_ref = evaluate_expression(expr, batch, Some(output_type))?;
                let array_ref = apply_schema_to(&array_ref, output_type)?;
                let arrow_type = ArrowDataType::try_from_kernel(output_type)?;
                let schema = ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)]);
                RecordBatch::try_new(Arc::new(schema), vec![array_ref])?
            }
        };

        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}

#[derive(Debug)]
pub struct DefaultPredicateEvaluator {
    input_schema: SchemaRef,
    predicate: PredicateRef,
}

impl PredicateEvaluator for DefaultPredicateEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        debug!("Arrow evaluator evaluating: {:#?}", self.predicate);
        let batch = extract_record_batch(batch)?;
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into_arrow()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };
        let array = evaluate_predicate(&self.predicate, batch, false)?;
        let schema = ArrowSchema::new(vec![ArrowField::new(
            "output",
            ArrowDataType::Boolean,
            true,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;
        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}
