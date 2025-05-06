use std::sync::Arc;

use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{col, lit, utils::conjunction, BinaryExpr, Expr, Operator};
use delta_kernel::expressions::{
    BinaryExpression, BinaryOperator, DecimalData, Expression, JunctionExpression,
    JunctionOperator, Scalar, UnaryExpression, UnaryOperator,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType};

use crate::error::to_df_err;

pub(crate) fn to_datafusion_expr(expr: &Expression) -> DFResult<Expr> {
    match expr {
        Expression::Column(name) => Ok(col(name.to_string())),
        Expression::Literal(scalar) => scalar_to_df_scalar(scalar),
        Expression::Binary(BinaryExpression { left, op, right }) => {
            let left_expr = to_datafusion_expr(left)?;
            let right_expr = to_datafusion_expr(right)?;
            Ok(match op {
                BinaryOperator::Equal => left_expr.eq(right_expr),
                BinaryOperator::NotEqual => left_expr.not_eq(right_expr),
                BinaryOperator::LessThan => left_expr.lt(right_expr),
                BinaryOperator::LessThanOrEqual => left_expr.lt_eq(right_expr),
                BinaryOperator::GreaterThan => left_expr.gt(right_expr),
                BinaryOperator::GreaterThanOrEqual => left_expr.gt_eq(right_expr),
                BinaryOperator::Plus => left_expr + right_expr,
                BinaryOperator::Minus => left_expr - right_expr,
                BinaryOperator::Multiply => left_expr * right_expr,
                BinaryOperator::Divide => left_expr / right_expr,
                BinaryOperator::Distinct => Err(DataFusionError::NotImplemented(
                    "DISTINCT operator not supported".into(),
                ))?,
                BinaryOperator::In => Err(DataFusionError::NotImplemented(
                    "IN operator not supported".into(),
                ))?,
                BinaryOperator::NotIn => Err(DataFusionError::NotImplemented(
                    "NOT IN operator not supported".into(),
                ))?,
            })
        }
        Expression::Unary(UnaryExpression { op, expr }) => {
            let inner_expr = to_datafusion_expr(expr)?;
            Ok(match op {
                UnaryOperator::IsNull => inner_expr.is_null(),
                UnaryOperator::Not => !inner_expr,
            })
        }
        Expression::Junction(JunctionExpression { op, exprs }) => {
            let df_exprs: DFResult<Vec<_>> = exprs.iter().map(to_datafusion_expr).collect();
            let df_exprs = df_exprs?;

            match op {
                JunctionOperator::And => Ok(df_exprs
                    .into_iter()
                    .reduce(|a, b| a.and(b))
                    .unwrap_or(lit(true))),
                JunctionOperator::Or => Ok(df_exprs
                    .into_iter()
                    .reduce(|a, b| a.or(b))
                    .unwrap_or(lit(false))),
            }
        }
        Expression::Struct(fields) => {
            let df_exprs: DFResult<Vec<_>> = fields.iter().map(to_datafusion_expr).collect();
            let df_exprs = df_exprs?;
            Err(DataFusionError::NotImplemented(format!(
                "Struct expressions not supported: {:?}",
                df_exprs
            )))
        }
    }
}

fn scalar_to_df_scalar(scalar: &Scalar) -> DFResult<Expr> {
    Ok(lit(match scalar {
        Scalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Scalar::String(value) => ScalarValue::Utf8(Some(value.clone())),
        Scalar::Byte(value) => ScalarValue::Int8(Some(*value)),
        Scalar::Short(value) => ScalarValue::Int16(Some(*value)),
        Scalar::Integer(value) => ScalarValue::Int32(Some(*value)),
        Scalar::Long(value) => ScalarValue::Int64(Some(*value)),
        Scalar::Float(value) => ScalarValue::Float32(Some(*value)),
        Scalar::Double(value) => ScalarValue::Float64(Some(*value)),
        Scalar::Timestamp(value) => {
            ScalarValue::TimestampMicrosecond(Some(*value), Some("UTC".into()))
        }
        Scalar::TimestampNtz(value) => ScalarValue::TimestampMicrosecond(Some(*value), None),
        Scalar::Date(value) => ScalarValue::Date32(Some(*value)),
        Scalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        Scalar::Decimal(data) => {
            let value = data.bits();
            let precision = data.precision();
            let scale = data.scale();
            ScalarValue::Decimal128(Some(value), precision, scale as i8)
        }
        Scalar::Null(data_type) => match data_type {
            &DataType::BOOLEAN => ScalarValue::Boolean(None),
            &DataType::STRING => ScalarValue::Utf8(None),
            &DataType::BYTE => ScalarValue::Int8(None),
            &DataType::SHORT => ScalarValue::Int16(None),
            &DataType::INTEGER => ScalarValue::Int32(None),
            &DataType::LONG => ScalarValue::Int64(None),
            &DataType::FLOAT => ScalarValue::Float32(None),
            &DataType::DOUBLE => ScalarValue::Float64(None),
            &DataType::TIMESTAMP => ScalarValue::TimestampMicrosecond(None, Some("UTC".into())),
            &DataType::TIMESTAMP_NTZ => ScalarValue::TimestampMicrosecond(None, None),
            &DataType::DATE => ScalarValue::Date32(None),
            &DataType::BINARY => ScalarValue::Binary(None),
            DataType::Primitive(PrimitiveType::Decimal(decimal_type)) => {
                let precision = decimal_type.precision();
                let scale = decimal_type.scale();
                ScalarValue::Decimal128(None, precision, scale as i8)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Null value with type {:?} not supported",
                    data_type
                )))
            }
        },
        Scalar::Struct(_) => {
            return Err(DataFusionError::NotImplemented(
                "Struct scalar values not supported".into(),
            ))
        }
        Scalar::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Array scalar values not supported".into(),
            ))
        }
        Scalar::Map(_) => {
            return Err(DataFusionError::NotImplemented(
                "Map scalar values not supported".into(),
            ))
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};
    use delta_kernel::expressions::{BinaryExpression, BinaryOperator, JunctionOperator};
}
