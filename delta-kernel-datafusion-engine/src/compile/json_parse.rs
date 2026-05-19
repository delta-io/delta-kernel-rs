//! JSON parsing expression generation for `Expression::ParseJson`.
//!
//! Converts a kernel target schema into DataFusion expressions that extract typed values from
//! JSON strings using `datafusion-functions-json`.

use datafusion_common::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion_common::error::DataFusionError;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{lit, Expr};
use datafusion_functions_json::udfs::{
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_str_udf,
};
use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

/// Generate extraction expression for a single field in `target_schema`.
pub(crate) fn generate_json_extract_expr(
    json_col: &Expr,
    field: &StructField,
    path: &[String],
) -> Result<Expr, DataFusionError> {
    let mut field_path = path.to_vec();
    field_path.push(field.name().to_string());

    match field.data_type() {
        DataType::Primitive(prim) => generate_primitive_extract(json_col, prim, &field_path),
        DataType::Struct(inner) => generate_struct_extract(json_col, inner, &field_path),
        DataType::Array(_) => Err(crate::error::unsupported(format!(
            "ParseJson array extraction is not yet supported for field '{}'",
            field.name()
        ))),
        DataType::Map(_) => Err(crate::error::unsupported(format!(
            "ParseJson map extraction is not yet supported for field '{}'",
            field.name()
        ))),
        DataType::Variant(_) => Err(crate::error::unsupported(format!(
            "ParseJson variant extraction is not yet supported for field '{}'",
            field.name()
        ))),
    }
}

fn generate_primitive_extract(
    json_col: &Expr,
    prim: &PrimitiveType,
    path: &[String],
) -> Result<Expr, DataFusionError> {
    let mut args = vec![json_col.clone()];
    args.extend(path.iter().map(|p| lit(p.clone())));

    let udf = match prim {
        PrimitiveType::Long
        | PrimitiveType::Integer
        | PrimitiveType::Short
        | PrimitiveType::Byte => json_get_int_udf(),
        PrimitiveType::String => json_get_str_udf(),
        PrimitiveType::Float | PrimitiveType::Double => json_get_float_udf(),
        PrimitiveType::Boolean => json_get_bool_udf(),
        PrimitiveType::Date
        | PrimitiveType::Timestamp
        | PrimitiveType::TimestampNtz
        | PrimitiveType::Binary
        | PrimitiveType::Decimal(_) => json_get_str_udf(),
    };

    let extracted = Expr::ScalarFunction(ScalarFunction::new_udf(udf, args));
    let target_type = match prim {
        PrimitiveType::Integer => Some(ArrowDataType::Int32),
        PrimitiveType::Short => Some(ArrowDataType::Int16),
        PrimitiveType::Byte => Some(ArrowDataType::Int8),
        PrimitiveType::Float => Some(ArrowDataType::Float32),
        PrimitiveType::Date => Some(ArrowDataType::Date32),
        PrimitiveType::Timestamp => Some(ArrowDataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        PrimitiveType::TimestampNtz => Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
        PrimitiveType::Decimal(dec) => Some(ArrowDataType::Decimal128(
            dec.precision(),
            dec.scale() as i8,
        )),
        PrimitiveType::Long
        | PrimitiveType::Double
        | PrimitiveType::String
        | PrimitiveType::Boolean
        | PrimitiveType::Binary => None,
    };

    match target_type {
        Some(data_type) => Ok(Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(extracted),
            data_type,
        })),
        None => Ok(extracted),
    }
}

fn generate_struct_extract(
    json_col: &Expr,
    struct_type: &StructType,
    path: &[String],
) -> Result<Expr, DataFusionError> {
    let mut args = Vec::with_capacity(struct_type.fields().count() * 2);
    for field in struct_type.fields() {
        args.push(lit(field.name().to_string()));
        args.push(generate_json_extract_expr(json_col, field, path)?);
    }
    Ok(datafusion_functions::core::expr_fn::named_struct(args))
}

/// Generate top-level extraction expressions for each output field.
pub(crate) fn generate_schema_extractions(
    json_col: &Expr,
    target_schema: &StructType,
) -> Result<Vec<(Expr, String)>, DataFusionError> {
    let mut out = Vec::new();
    for field in target_schema.fields() {
        out.push((
            generate_json_extract_expr(json_col, field, &[])?,
            field.name().to_string(),
        ));
    }
    Ok(out)
}
