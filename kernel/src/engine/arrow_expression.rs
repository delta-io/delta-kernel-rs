//! Expression handling based on arrow-rs compute kernels.
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, is_null, not, or_kleene};
use arrow_arith::numeric::{add, div, mul, sub};
use arrow_array::cast::AsArray;
use arrow_array::{types::*, MapArray};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_ord::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_ord::comparison::in_list_utf8;
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Fields, IntervalUnit,
    Schema as ArrowSchema, TimeUnit,
};
use arrow_select::concat::concat;
use itertools::Itertools;

use super::arrow_conversion::LIST_ARRAY_ROOT;
use super::arrow_utils::make_arrow_error;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::prim_array_cmp;
use crate::engine::ensure_data_types::ensure_data_types;
use crate::error::{DeltaResult, Error};
use crate::expressions::{
    BinaryExpression, BinaryOperator, Expression, Scalar, UnaryExpression, UnaryOperator,
    VariadicExpression, VariadicOperator,
};
use crate::schema::{ArrayType, DataType, MapType, PrimitiveType, Schema, SchemaRef, StructField};
use crate::{EngineData, ExpressionEvaluator, ExpressionHandler};

// TODO leverage scalars / Datum

fn downcast_to_bool(arr: &dyn Array) -> DeltaResult<&BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::generic("expected boolean array"))
}

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

fn wrap_comparison_result(arr: BooleanArray) -> ArrayRef {
    Arc::new(arr) as _
}

trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.column_by_name(name)
    }
}

// Given a RecordBatch or StructArray, recursively probe for a nested column path and return the
// corresponding column, or Err if the path is invalid. For example, given the following schema:
// ```text
// root: {
//   a: int32,
//   b: struct {
//     c: int32,
//     d: struct {
//       e: int32,
//       f: int64,
//     },
//   },
// }
// ```
// The path ["b", "d", "f"] would retrieve the int64 column while ["a", "b"] would produce an error.
fn extract_column(mut parent: &dyn ProvidesColumnByName, col: &[String]) -> DeltaResult<ArrayRef> {
    let mut field_names = col.iter();
    let Some(mut field_name) = field_names.next() else {
        return Err(ArrowError::SchemaError("Empty column path".to_string()))?;
    };
    loop {
        let child = parent
            .column_by_name(field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("No such field: {field_name}")))?;
        field_name = match field_names.next() {
            Some(name) => name,
            None => return Ok(child.clone()),
        };
        parent = child
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ArrowError::SchemaError(format!("Not a struct: {field_name}")))?;
    }
}

fn evaluate_expression(
    expression: &Expression,
    batch: &RecordBatch,
    result_type: Option<&DataType>,
) -> DeltaResult<ArrayRef> {
    use BinaryOperator::*;
    use Expression::*;
    match (expression, result_type) {
        (Literal(scalar), _) => Ok(scalar.to_array(batch.num_rows())?),
        (Column(name), _) => extract_column(batch, name),
        (Struct(fields), Some(DataType::Struct(output_schema))) => {
            let columns = fields
                .iter()
                .zip(output_schema.fields())
                .map(|(expr, field)| evaluate_expression(expr, batch, Some(field.data_type())));
            let output_cols: Vec<ArrayRef> = columns.try_collect()?;
            let output_fields: Vec<ArrowField> = output_cols
                .iter()
                .zip(output_schema.fields())
                .map(|(output_col, output_field)| -> DeltaResult<_> {
                    Ok(ArrowField::new(
                        output_field.name(),
                        output_col.data_type().clone(),
                        output_col.is_nullable(),
                    ))
                })
                .try_collect()?;
            let result = StructArray::try_new(output_fields.into(), output_cols, None)?;
            Ok(Arc::new(result))
        }
        (Struct(_), _) => Err(Error::generic(
            "Data type is required to evaluate struct expressions",
        )),
        (Unary(UnaryExpression { op, expr }), _) => {
            let arr = evaluate_expression(expr.as_ref(), batch, None)?;
            Ok(match op {
                UnaryOperator::Not => Arc::new(not(downcast_to_bool(&arr)?)?),
                UnaryOperator::IsNull => Arc::new(is_null(&arr)?),
            })
        }
        (
            Binary(BinaryExpression {
                op: In,
                left,
                right,
            }),
            _,
        ) => match (left.as_ref(), right.as_ref()) {
            (Literal(_), Column(_)) => {
                let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
                let right_arr = evaluate_expression(right.as_ref(), batch, None)?;
                if let Some(string_arr) = left_arr.as_string_opt::<i32>() {
                    if let Some(right_arr) = right_arr.as_list_opt::<i32>() {
                        return in_list_utf8(string_arr, right_arr)
                            .map(wrap_comparison_result)
                            .map_err(Error::generic_err);
                    }
                }
                prim_array_cmp! {
                    left_arr, right_arr,
                    (ArrowDataType::Int8, Int8Type),
                    (ArrowDataType::Int16, Int16Type),
                    (ArrowDataType::Int32, Int32Type),
                    (ArrowDataType::Int64, Int64Type),
                    (ArrowDataType::UInt8, UInt8Type),
                    (ArrowDataType::UInt16, UInt16Type),
                    (ArrowDataType::UInt32, UInt32Type),
                    (ArrowDataType::UInt64, UInt64Type),
                    (ArrowDataType::Float16, Float16Type),
                    (ArrowDataType::Float32, Float32Type),
                    (ArrowDataType::Float64, Float64Type),
                    (ArrowDataType::Timestamp(TimeUnit::Second, _), TimestampSecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Millisecond, _), TimestampMillisecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Microsecond, _), TimestampMicrosecondType),
                    (ArrowDataType::Timestamp(TimeUnit::Nanosecond, _), TimestampNanosecondType),
                    (ArrowDataType::Date32, Date32Type),
                    (ArrowDataType::Date64, Date64Type),
                    (ArrowDataType::Time32(TimeUnit::Second), Time32SecondType),
                    (ArrowDataType::Time32(TimeUnit::Millisecond), Time32MillisecondType),
                    (ArrowDataType::Time64(TimeUnit::Microsecond), Time64MicrosecondType),
                    (ArrowDataType::Time64(TimeUnit::Nanosecond), Time64NanosecondType),
                    (ArrowDataType::Duration(TimeUnit::Second), DurationSecondType),
                    (ArrowDataType::Duration(TimeUnit::Millisecond), DurationMillisecondType),
                    (ArrowDataType::Duration(TimeUnit::Microsecond), DurationMicrosecondType),
                    (ArrowDataType::Duration(TimeUnit::Nanosecond), DurationNanosecondType),
                    (ArrowDataType::Interval(IntervalUnit::DayTime), IntervalDayTimeType),
                    (ArrowDataType::Interval(IntervalUnit::YearMonth), IntervalYearMonthType),
                    (ArrowDataType::Interval(IntervalUnit::MonthDayNano), IntervalMonthDayNanoType),
                    (ArrowDataType::Decimal128(_, _), Decimal128Type),
                    (ArrowDataType::Decimal256(_, _), Decimal256Type)
                }
            }
            (Column(name), Literal(Scalar::Array(ad))) => {
                use crate::expressions::ArrayData;

                let column = extract_column(batch, name)?;
                let data_type = ad
                    .array_type()
                    .element_type()
                    .as_primitive_opt()
                    .ok_or_else(|| {
                        Error::invalid_expression(format!(
                            "IN only supports array literals with primitive elements, got: '{:?}'",
                            ad.array_type().element_type()
                        ))
                    })?;

                fn op(
                    col: impl Iterator<Item = Option<impl Into<Scalar>>>,
                    ad: &ArrayData,
                ) -> BooleanArray {
                    #[allow(deprecated)]
                    let res = col.map(|val| val.map(|v| ad.array_elements().contains(&v.into())));
                    BooleanArray::from_iter(res)
                }

                // safety: as_* methods on arrow arrays can panic, but we checked the data type before applying.
                let arr: BooleanArray = match (column.data_type(), data_type) {
                    (ArrowDataType::Utf8, PrimitiveType::String) => op(column.as_string::<i32>().iter(), ad),
                    (ArrowDataType::LargeUtf8, PrimitiveType::String) => op(column.as_string::<i64>().iter(), ad),
                    (ArrowDataType::Utf8View, PrimitiveType::String) => op(column.as_string_view().iter(), ad),
                    (ArrowDataType::Int8, PrimitiveType::Byte) => op(column.as_primitive::<Int8Type>().iter(), ad),
                    (ArrowDataType::Int16, PrimitiveType::Short) => op(column.as_primitive::<Int16Type>().iter(), ad),
                    (ArrowDataType::Int32, PrimitiveType::Integer) => op(column.as_primitive::<Int32Type>().iter(), ad),
                    (ArrowDataType::Int64, PrimitiveType::Long) => op(column.as_primitive::<Int64Type>().iter(), ad),
                    (ArrowDataType::Float32, PrimitiveType::Float) => op(column.as_primitive::<Float32Type>().iter(), ad),
                    (ArrowDataType::Float64, PrimitiveType::Double) => op(column.as_primitive::<Float64Type>().iter(), ad),
                    (ArrowDataType::Date32, PrimitiveType::Date) => {
                        #[allow(deprecated)]
                        let res = column
                            .as_primitive::<Date32Type>()
                            .iter()
                            .map(|val| val.map(|v| ad.array_elements().contains(&Scalar::Date(v))));
                        BooleanArray::from_iter(res)
                    }
                    (
                        ArrowDataType::Timestamp(TimeUnit::Microsecond, unit),
                        kt @ PrimitiveType::Timestamp | kt @ PrimitiveType::TimestampNtz,
                    ) => {
                        let res = column.as_primitive::<TimestampMicrosecondType>().iter();
                        match (unit, kt) {
                            // regardless of the time zone stored in the timestamp, the underlying value is always in UTC
                            (Some(_), PrimitiveType::Timestamp) => {
                                BooleanArray::from_iter(res.map(|val| {
                                    #[allow(deprecated)]
                                    val.map(|v| ad.array_elements().contains(&Scalar::Timestamp(v)))
                                }))
                            }
                            (None, PrimitiveType::TimestampNtz) => {
                                BooleanArray::from_iter(res.map(|val| {
                                    val.map(|v| {
                                        #[allow(deprecated)]
                                        ad.array_elements().contains(&Scalar::TimestampNtz(v))
                                    })
                                }))
                            }
                            _ => unreachable!(),
                        }
                    }
                    (l, r) => {
                        return Err(Error::invalid_expression(format!(
                        "Cannot check if value of type '{l}' is contained in array with values of type '{r}'"
                    )))
                    }
                };
                Ok(Arc::new(arr))
            }
            (Literal(lit), Literal(Scalar::Array(ad))) => {
                #[allow(deprecated)]
                let exists = ad.array_elements().contains(lit);
                Ok(Arc::new(BooleanArray::from(vec![exists])))
            }
            (l, r) => Err(Error::invalid_expression(format!(
                "Invalid right value for (NOT) IN comparison, left is: {l} right is: {r}"
            ))),
        },
        (
            Binary(BinaryExpression {
                op: NotIn,
                left,
                right,
            }),
            _,
        ) => {
            let reverse_op = Expression::binary(In, *left.clone(), *right.clone());
            let reverse_expr = evaluate_expression(&reverse_op, batch, None)?;
            not(reverse_expr.as_boolean())
                .map(wrap_comparison_result)
                .map_err(Error::generic_err)
        }
        (Binary(BinaryExpression { op, left, right }), _) => {
            let left_arr = evaluate_expression(left.as_ref(), batch, None)?;
            let right_arr = evaluate_expression(right.as_ref(), batch, None)?;

            type Operation = fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>;
            let eval: Operation = match op {
                Plus => add,
                Minus => sub,
                Multiply => mul,
                Divide => div,
                LessThan => |l, r| lt(l, r).map(wrap_comparison_result),
                LessThanOrEqual => |l, r| lt_eq(l, r).map(wrap_comparison_result),
                GreaterThan => |l, r| gt(l, r).map(wrap_comparison_result),
                GreaterThanOrEqual => |l, r| gt_eq(l, r).map(wrap_comparison_result),
                Equal => |l, r| eq(l, r).map(wrap_comparison_result),
                NotEqual => |l, r| neq(l, r).map(wrap_comparison_result),
                Distinct => |l, r| distinct(l, r).map(wrap_comparison_result),
                // NOTE: [Not]In was already covered above
                In | NotIn => return Err(Error::generic("Invalid expression given")),
            };

            eval(&left_arr, &right_arr).map_err(Error::generic_err)
        }
        (Variadic(VariadicExpression { op, exprs }), None | Some(&DataType::BOOLEAN)) => {
            type Operation = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>;
            let (reducer, default): (Operation, _) = match op {
                VariadicOperator::And => (and_kleene, true),
                VariadicOperator::Or => (or_kleene, false),
            };
            exprs
                .iter()
                .map(|expr| evaluate_expression(expr, batch, result_type))
                .reduce(|l, r| {
                    Ok(reducer(downcast_to_bool(&l?)?, downcast_to_bool(&r?)?)
                        .map(wrap_comparison_result)?)
                })
                .unwrap_or_else(|| {
                    evaluate_expression(&Expression::literal(default), batch, result_type)
                })
        }
        (Variadic(_), _) => {
            // NOTE: Update this error message if we add support for variadic operations on other types
            Err(Error::Generic(format!(
                "Variadic {expression:?} is expected to return boolean results, got {result_type:?}"
            )))
        }
    }
}

// Apply a schema to an array. The array _must_ be a `StructArray`. Returns a `RecordBatch where the
// names of fields, nullable, and metadata in the struct have been transformed to match those in
// schema specified by `schema`
fn apply_schema(array: &dyn Array, schema: &DataType) -> DeltaResult<RecordBatch> {
    let DataType::Struct(struct_schema) = schema else {
        return Err(Error::generic(
            "apply_schema at top-level must be passed a struct schema",
        ));
    };
    let applied = apply_schema_to_struct(array, struct_schema)?;
    Ok(applied.into())
}

// helper to transform an arrow field+col into the specified target type. If `rename` is specified
// the field will be renamed to the contained `str`.
fn new_field_with_metadata(
    field_name: &str,
    data_type: &ArrowDataType,
    nullable: bool,
    metadata: Option<HashMap<String, String>>,
) -> ArrowField {
    let mut field = ArrowField::new(field_name, data_type.clone(), nullable);
    if let Some(metadata) = metadata {
        field.set_metadata(metadata);
    };
    field
}

// A helper that is a wrapper over `transform_field_and_col`. This will take apart the passed struct
// and use that method to transform each column and then put the struct back together. Target types
// and names for each column should be passed in `target_fields`. The number of elements in
// the `target_fields` iterator _must_ be the same as the number of columns in
// `struct_array`. The transformation is ordinal. That is, the order of fields in `target_fields`
// _must_ match the order of the columns in `struct_array`.
fn transform_struct(
    struct_array: &StructArray,
    target_fields: impl Iterator<Item = impl Borrow<StructField>>,
) -> DeltaResult<StructArray> {
    let (_, arrow_cols, nulls) = struct_array.clone().into_parts();
    let input_col_count = arrow_cols.len();
    let result_iter =
        arrow_cols
            .into_iter()
            .zip(target_fields)
            .map(|(sa_col, target_field)| -> DeltaResult<_> {
                let target_field = target_field.borrow();
                let transformed_col = apply_schema_to(&sa_col, target_field.data_type())?;
                let transformed_field = new_field_with_metadata(
                    &target_field.name,
                    transformed_col.data_type(),
                    target_field.nullable,
                    Some(target_field.metadata_with_string_values()),
                );
                Ok((transformed_field, transformed_col))
            });
    let (transformed_fields, transformed_cols): (Vec<ArrowField>, Vec<ArrayRef>) =
        result_iter.process_results(|iter| iter.unzip())?;
    if transformed_cols.len() != input_col_count {
        return Err(Error::InternalError(format!(
            "Passed struct had {input_col_count} columns, but transformed column has {}",
            transformed_cols.len()
        )));
    }
    Ok(StructArray::try_new(
        transformed_fields.into(),
        transformed_cols,
        nulls,
    )?)
}

// Transform a struct array. The data is in `array`, and the target fields are in `kernel_fields`.
fn apply_schema_to_struct(array: &dyn Array, kernel_fields: &Schema) -> DeltaResult<StructArray> {
    let Some(sa) = array.as_struct_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a struct but isn't a StructArray",
        ));
    };
    transform_struct(sa, kernel_fields.fields())
}

// deconstruct the array, then rebuild the mapped version
fn apply_schema_to_list(
    array: &dyn Array,
    target_inner_type: &ArrayType,
) -> DeltaResult<ListArray> {
    let Some(la) = array.as_list_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a list but isn't a ListArray",
        ));
    };
    let (field, offset_buffer, values, nulls) = la.clone().into_parts();

    let transformed_values = apply_schema_to(&values, &target_inner_type.element_type)?;
    let transformed_field = ArrowField::new(
        field.name(),
        transformed_values.data_type().clone(),
        target_inner_type.contains_null,
    );
    Ok(ListArray::try_new(
        Arc::new(transformed_field),
        offset_buffer,
        transformed_values,
        nulls,
    )?)
}

// deconstruct a map, and rebuild it with the specified target kernel type
fn apply_schema_to_map(array: &dyn Array, kernel_map_type: &MapType) -> DeltaResult<MapArray> {
    let Some(ma) = array.as_map_opt() else {
        return Err(make_arrow_error(
            "Arrow claimed to be a map but isn't a MapArray",
        ));
    };
    let (map_field, offset_buffer, map_struct_array, nulls, ordered) = ma.clone().into_parts();
    let target_fields = map_struct_array
        .fields()
        .iter()
        .zip([&kernel_map_type.key_type, &kernel_map_type.value_type])
        .zip([false, kernel_map_type.value_contains_null])
        .map(|((arrow_field, target_type), nullable)| {
            StructField::new(arrow_field.name(), target_type.clone(), nullable)
        });

    // Arrow puts the key type/val as the first field/col and the value type/val as the second. So
    // we just transform like a 'normal' struct, but we know there are two fields/cols and we
    // specify the key/value types as the target type iterator.
    let transformed_map_struct_array = transform_struct(&map_struct_array, target_fields)?;

    let transformed_map_field = ArrowField::new(
        map_field.name().clone(),
        transformed_map_struct_array.data_type().clone(),
        map_field.is_nullable(),
    );
    Ok(MapArray::try_new(
        Arc::new(transformed_map_field),
        offset_buffer,
        transformed_map_struct_array,
        nulls,
        ordered,
    )?)
}

// apply `schema` to `array`. This handles renaming, and adjusting nullability and metadata. if the
// actual data types don't match, this will return an error
fn apply_schema_to(array: &ArrayRef, schema: &DataType) -> DeltaResult<ArrayRef> {
    use DataType::*;
    let array: ArrayRef = match schema {
        Struct(stype) => Arc::new(apply_schema_to_struct(array, stype)?),
        Array(atype) => Arc::new(apply_schema_to_list(array, atype)?),
        Map(mtype) => Arc::new(apply_schema_to_map(array, mtype)?),
        _ => {
            ensure_data_types(schema, array.data_type(), true)?;
            array.clone()
        }
    };
    Ok(array)
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

#[cfg(test)]
mod tests {
    use std::ops::{Add, Div, Mul, Sub};

    use arrow_array::{GenericStringArray, Int32Array};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::*;
    use crate::expressions::*;
    use crate::schema::ArrayType;
    use crate::DataType as DeltaDataTypes;

    #[test]
    fn test_array_column() {
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

        let schema = Schema::new([arr_field.clone()]);

        let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let not_op = Expression::binary(BinaryOperator::NotIn, 5, column_expr!("item"));

        let in_op = Expression::binary(BinaryOperator::In, 5, column_expr!("item"));

        let result = evaluate_expression(&not_op, &batch, None).unwrap();
        let expected = BooleanArray::from(vec![true, false, true]);
        assert_eq!(result.as_ref(), &expected);

        let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![false, true, false]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_bad_right_type_array() {
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

        let in_op = Expression::binary(BinaryOperator::NotIn, 5, column_expr!("item"));

        let in_result = evaluate_expression(&in_op, &batch, None);

        assert!(in_result.is_err());
        assert_eq!(
            in_result.unwrap_err().to_string(),
            "Invalid expression evaluation: Cannot cast to list array: Int32"
        );
    }

    #[test]
    fn test_literal_type_array() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let in_op = Expression::binary(
            BinaryOperator::NotIn,
            5,
            Scalar::Array(ArrayData::new(
                ArrayType::new(DeltaDataTypes::INTEGER, false),
                vec![Scalar::Integer(1), Scalar::Integer(2)],
            )),
        );

        let in_result = evaluate_expression(&in_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![true]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_invalid_array_sides() {
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));

        let schema = Schema::new([arr_field.clone()]);

        let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let in_op = Expression::binary(
            BinaryOperator::NotIn,
            column_expr!("item"),
            column_expr!("item"),
        );

        let in_result = evaluate_expression(&in_op, &batch, None);

        assert!(in_result.is_err());
        assert_eq!(
            in_result.unwrap_err().to_string(),
            "Invalid expression evaluation: Invalid right value for (NOT) IN comparison, left is: Column(item) right is: Column(item)".to_string()
        )
    }

    #[test]
    fn test_str_arrays() {
        let values = GenericStringArray::<i32>::from(vec![
            "hi", "bye", "hi", "hi", "bye", "bye", "hi", "bye", "hi",
        ]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let arr_field = Arc::new(Field::new("item", DataType::List(field.clone()), true));
        let schema = Schema::new([arr_field.clone()]);
        let array = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())]).unwrap();

        let str_not_op = Expression::binary(BinaryOperator::NotIn, "bye", column_expr!("item"));

        let str_in_op = Expression::binary(BinaryOperator::In, "hi", column_expr!("item"));

        let result = evaluate_expression(&str_in_op, &batch, None).unwrap();
        let expected = BooleanArray::from(vec![true, true, true]);
        assert_eq!(result.as_ref(), &expected);

        let in_result = evaluate_expression(&str_not_op, &batch, None).unwrap();
        let in_expected = BooleanArray::from(vec![false, false, false]);
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_column_in_array() {
        let values = Int32Array::from(vec![0, 1, 2, 3]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let rhs = Expression::literal(Scalar::Array(ArrayData::new(
            ArrayType::new(PrimitiveType::Integer.into(), false),
            [Scalar::Integer(1), Scalar::Integer(3)],
        )));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();

        let in_op = Expression::binary(BinaryOperator::In, column_expr!("item"), rhs.clone());
        let in_result =
            evaluate_expression(&in_op, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
        let in_expected = BooleanArray::from(vec![false, true, false, true]);
        assert_eq!(in_result.as_ref(), &in_expected);

        let not_in_op = Expression::binary(BinaryOperator::NotIn, column_expr!("item"), rhs);
        let not_in_result =
            evaluate_expression(&not_in_op, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let not_in_expected = BooleanArray::from(vec![true, false, true, false]);
        assert_eq!(not_in_result.as_ref(), &not_in_expected);

        let in_expected = BooleanArray::from(vec![false, true, false, true]);

        // Date arrays
        let values = Date32Array::from(vec![0, 1, 2, 3]);
        let field = Arc::new(Field::new("item", DataType::Date32, true));
        let rhs = Expression::literal(Scalar::Array(ArrayData::new(
            ArrayType::new(PrimitiveType::Date.into(), false),
            [Scalar::Date(1), Scalar::Date(3)],
        )));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();
        let in_op = Expression::binary(BinaryOperator::In, column_expr!("item"), rhs.clone());
        let in_result =
            evaluate_expression(&in_op, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
        assert_eq!(in_result.as_ref(), &in_expected);

        // Timestamp arrays
        let values = TimestampMicrosecondArray::from(vec![0, 1, 2, 3]).with_timezone("UTC");
        let field = Arc::new(Field::new(
            "item",
            (&crate::schema::DataType::TIMESTAMP).try_into().unwrap(),
            true,
        ));
        let rhs = Expression::literal(Scalar::Array(ArrayData::new(
            ArrayType::new(PrimitiveType::Timestamp.into(), false),
            [Scalar::Timestamp(1), Scalar::Timestamp(3)],
        )));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();
        let in_op = Expression::binary(BinaryOperator::In, column_expr!("item"), rhs.clone());
        let in_result =
            evaluate_expression(&in_op, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
        assert_eq!(in_result.as_ref(), &in_expected);

        // Timestamp NTZ arrays
        let values = TimestampMicrosecondArray::from(vec![0, 1, 2, 3]);
        let field = Arc::new(Field::new(
            "item",
            (&crate::schema::DataType::TIMESTAMP_NTZ)
                .try_into()
                .unwrap(),
            true,
        ));
        let rhs = Expression::literal(Scalar::Array(ArrayData::new(
            ArrayType::new(PrimitiveType::TimestampNtz.into(), false),
            [Scalar::TimestampNtz(1), Scalar::TimestampNtz(3)],
        )));
        let schema = Schema::new([field.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values.clone())]).unwrap();
        let in_op = Expression::binary(BinaryOperator::In, column_expr!("item"), rhs.clone());
        let in_result =
            evaluate_expression(&in_op, &batch, Some(&crate::schema::DataType::BOOLEAN)).unwrap();
        assert_eq!(in_result.as_ref(), &in_expected);
    }

    #[test]
    fn test_extract_column() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values.clone())]).unwrap();
        let column = column_expr!("a");

        let results = evaluate_expression(&column, &batch, None).unwrap();
        assert_eq!(results.as_ref(), &values);

        let schema = Schema::new(vec![Field::new(
            "b",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)])),
            false,
        )]);

        let struct_values: ArrayRef = Arc::new(values.clone());
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, false)),
            struct_values,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(struct_array.clone())],
        )
        .unwrap();
        let column = column_expr!("b.a");
        let results = evaluate_expression(&column, &batch, None).unwrap();
        assert_eq!(results.as_ref(), &values);
    }

    #[test]
    fn test_binary_op_scalar() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = column_expr!("a");

        let expression = column.clone().add(1);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 3, 4]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().sub(1);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 1, 2]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().mul(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        // TODO handle type casting
        let expression = column.div(1);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert_eq!(results.as_ref(), expected.as_ref())
    }

    #[test]
    fn test_binary_op() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(values.clone()), Arc::new(values)],
        )
        .unwrap();
        let column_a = column_expr!("a");
        let column_b = column_expr!("b");

        let expression = column_a.clone().add(column_b.clone());
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![2, 4, 6]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column_a.clone().sub(column_b.clone());
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![0, 0, 0]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column_a.clone().mul(column_b);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 4, 9]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_binary_cmp() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        let column = column_expr!("a");

        let expression = column.clone().lt(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().lt_eq(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().gt(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().gt_eq(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().eq(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column.clone().ne(2);
        let results = evaluate_expression(&expression, &batch, None).unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false, true]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_logical() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        )
        .unwrap();
        let column_a = column_expr!("a");
        let column_b = column_expr!("b");

        let expression = column_a.clone().and(column_b.clone());
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![false, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column_a.clone().and(true);
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column_a.clone().or(column_b);
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, true]));
        assert_eq!(results.as_ref(), expected.as_ref());

        let expression = column_a.clone().or(false);
        let results =
            evaluate_expression(&expression, &batch, Some(&crate::schema::DataType::BOOLEAN))
                .unwrap();
        let expected = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(results.as_ref(), expected.as_ref());
    }
}
