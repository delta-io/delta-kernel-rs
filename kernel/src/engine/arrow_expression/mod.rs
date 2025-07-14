//! Expression handling based on arrow-rs compute kernels.
use std::sync::Arc;

use crate::arrow::array::{self, ArrayBuilder, ArrayRef, RecordBatch};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

use super::arrow_conversion::{TryFromKernel as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::error::{DeltaResult, Error};
use crate::expressions::{Expression, Predicate, Scalar};
use crate::schema::{DataType, PrimitiveType, SchemaRef};
use crate::utils::require;
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator, PredicateEvaluator};

use itertools::Itertools;
use tracing::debug;

use apply_schema::{apply_schema, apply_schema_to};
use evaluate_expression::{evaluate_expression, evaluate_predicate};

mod apply_schema;
pub mod evaluate_expression;
pub mod opaque;

#[cfg(test)]
mod tests;

// TODO leverage scalars / Datum

// This trait is a hack, needed because [`array::StructBuilder::field_builders`] was not added until
// arrow-55, and `StructBuilder::field_builder` (available in all versions) only works for concrete
// builders. We can't ask for `dyn ArrayBuilder` directly (the method requires `Sized` types), nor
// can we ask for `Box<dyn ArrayBuilder>` (which is what the builder actually stores internally),
// because the `Box` derefs `&dyn ArrayBuilder` _before_ being downcast to `Any`. Instead, we must
// wrap the builder in a trait that can call `StructBuilder::field_builder` once the type is known,
// with a trivial implementation of the same trait for `Box<dyn ArrayBuilder>` that
// `array::make_builder` returns (and populates list and map builders with).
//
// Once we drop support for older arrow versions, we can change [`Scalar::append`] below to take
// `&mut dyn ArrayBuilder` directly and cast it as needed.
trait ArrayBuilderAs {
    fn array_builder_as<T: ArrayBuilder>(&mut self) -> Option<&mut T>;
}

impl ArrayBuilderAs for Box<dyn ArrayBuilder> {
    fn array_builder_as<T: ArrayBuilder>(&mut self) -> Option<&mut T> {
        self.as_any_mut().downcast_mut()
    }
}

#[cfg(not(feature = "arrow-55"))]
struct StructFieldBuilder<'a> {
    builder: &'a mut array::StructBuilder,
    field_index: usize,
}

#[cfg(not(feature = "arrow-55"))]
impl ArrayBuilderAs for StructFieldBuilder<'_> {
    fn array_builder_as<T: ArrayBuilder>(&mut self) -> Option<&mut T> {
        self.builder.field_builder::<T>(self.field_index)
    }
}

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
    // can just extract the builder trait,and let recursive calls cast it to the desired type.
    //
    // WARNING: List and map builders do _NOT_ require appending any child entries to NULL list/map
    // rows, because empty list/map is a valid state. But struct builders _DO_ require appending
    // (possibly NULL) entries in order to preserve consistent row counts between the struct and its
    // fields.
    fn append_to(&self, builder: &mut impl ArrayBuilderAs, num_rows: usize) -> DeltaResult<()> {
        use Scalar::*;
        macro_rules! builder_as {
            ($t:ty) => {{
                builder.array_builder_as::<$t>().ok_or_else(|| {
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
                    // TODO: Get rid of this alternate code path when we drop arrow-54 support.
                    #[cfg(not(feature = "arrow-55"))]
                    for (field_index, value) in data.values().iter().enumerate() {
                        let builder = &mut StructFieldBuilder {
                            builder,
                            field_index,
                        };
                        value.append_to(builder, 1)?;
                    }

                    #[cfg(feature = "arrow-55")]
                    let field_builders = builder.field_builders_mut().iter_mut();
                    #[cfg(feature = "arrow-55")]
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
        builder: &mut impl ArrayBuilderAs,
        data_type: &DataType,
        num_rows: usize,
    ) -> DeltaResult<()> {
        // Almost the same as above -- differs only in the data type parameter
        macro_rules! builder_as {
            ($t:ty) => {{
                builder.array_builder_as::<$t>().ok_or_else(|| {
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
                    builder.num_fields() == stype.fields_len(),
                    Error::generic("Struct builder has wrong number of fields")
                );
                for _ in 0..num_rows {
                    // TODO: Get rid of this alternate code path when we drop arrow-54 support.
                    #[cfg(not(feature = "arrow-55"))]
                    for (field_index, field) in stype.fields().enumerate() {
                        let builder = &mut StructFieldBuilder {
                            builder,
                            field_index,
                        };
                        Self::append_null(builder, &field.data_type, 1)?;
                    }

                    #[cfg(feature = "arrow-55")]
                    let field_builders = builder.field_builders_mut().iter_mut();
                    #[cfg(feature = "arrow-55")]
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

    fn new_predicate_evaluator(
        &self,
        schema: SchemaRef,
        predicate: Predicate,
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
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into_arrow()?;
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
            let arrow_type = ArrowDataType::try_from_kernel(&self.output_type)?;
            let schema = ArrowSchema::new(vec![ArrowField::new("output", arrow_type, true)]);
            RecordBatch::try_new(Arc::new(schema), vec![array_ref])?
        };
        Ok(Box::new(ArrowEngineData::new(batch)))
    }
}

#[derive(Debug)]
pub struct DefaultPredicateEvaluator {
    input_schema: SchemaRef,
    predicate: Predicate,
}

impl PredicateEvaluator for DefaultPredicateEvaluator {
    fn evaluate(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        debug!("Arrow evaluator evaluating: {:#?}", self.predicate);
        let batch = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| Error::engine_data_type("ArrowEngineData"))?
            .record_batch();
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
