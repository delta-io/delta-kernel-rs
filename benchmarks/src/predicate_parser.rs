//! Parses SQL WHERE clause expressions into kernel [`KPred`] types.
//!
//! Supports a subset of SQL sufficient for benchmark predicates:
//! - Binary comparisons: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
//! - Null-safe equals: `<=>`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - Arithmetic operators: `+`, `-`, `*`, `/`, `%`
//! - `IS NULL`, `IS NOT NULL`
//! - `IN (...)`, `NOT IN (...)`
//! - `BETWEEN ... AND ...`
//! - Column references and literal values (integers, floats, strings, booleans)
//! - Typed literals: `DATE'...'`, `TIMESTAMP'...'`, `TIMESTAMP_NTZ'...'`
//!
//! Unsupported (returns error): `LIKE`, function calls (`HEX`, `size`, `length`),
//! `TIME '...'` typed literals.
//!
//! # Type resolution
//!
//! This module uses bidirectional type checking to infer the correct types for literals and to
//! verify that the resulting predicate is well-typed. Bidirectional type checking has two modes:
//!
//! 1) **Type synthesis**: Given an expression, try to infer its type. For example, given a schema
//!    where `int_col` has type `Integer`, the column reference `int_col` synthesizes to `Integer`.
//!    Synthesis returns `Some(type)` for unambiguous types, or `None` when the type is ambiguous.
//!
//! 2) **Type checking**: Given an expression and a target type, check that the expression can
//!    resolve to that type. For example, the literal `5` will successfully type check against
//!    `Integer`, `Short`, or `Long`, but will fail against `String`.
//!
//! The predicate parser uses synthesis and checking to correctly resolve the types of literals so
//! that predicates are well-formed. Consider the predicate `double_col > 3.14`:
//!
//! 1) Synthesize the LHS `double_col`. The schema lookup determines the type is `Double`.
//! 2) Synthesize the RHS `3.14`. The type is ambiguous (`Float` or `Double`), so synthesis
//!    returns `None`.
//! 3) Since LHS has a concrete type (`Double`) and RHS is ambiguous, use `Double` as the target.
//! 4) Type check both sides against `Double`, producing `Column("double_col")` on the LHS and
//!    `Scalar::Double(3.14)` on the RHS.
//! 5) Return the resolved predicate: `Predicate::gt(Column("double_col"), Scalar::Double(3.14))`

use std::borrow::Cow;

use delta_kernel::expressions::{
    ArrayData, BinaryExpressionOp as KBinOp, BinaryPredicateOp as KPredOp, ColumnName,
    Expression as KExpr, Predicate as KPred, Scalar,
};
use delta_kernel::schema::{ArrayType, DataType, PrimitiveType, Schema};
use itertools::Itertools as _;
use sqlparser::ast::{
    BinaryOperator as PBinOp, Expr as PExpr, UnaryOperator as PUnaryOp, Value as PVal,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Preprocesses SQL to replace `TIMESTAMP_NTZ` with `TIMESTAMP` since sqlparser doesn't
/// recognize `TIMESTAMP_NTZ`.
///
/// The actual type (Timestamp vs TimestampNtz) is determined by schema-based type inference
/// (the column's type determines parsing).
///
/// Note: This may incorrectly match `TIMESTAMP_NTZ` inside string literals
/// (e.g., `str_col = 'TIMESTAMP_NTZ is cool'`), but this is acceptable for benchmark
/// predicates which don't contain such edge cases.
fn preprocess_timestamp_ntz(input: &str) -> Cow<'_, str> {
    if input.contains("TIMESTAMP_NTZ") {
        Cow::Owned(input.replace("TIMESTAMP_NTZ", "TIMESTAMP"))
    } else {
        Cow::Borrowed(input)
    }
}

/// Parses a SQL WHERE clause expression string into a kernel [`KPred`] with type checking.
///
/// Performs bidirectional type checking using the provided schema:
/// - Column types are looked up in the schema
/// - Literals are checked against expected types from columns
/// - Type mismatches produce detailed error messages
///
/// # Note
///
/// `TIMESTAMP_NTZ` literals are preprocessed by stripping the prefix, since sqlparser doesn't
/// recognize this keyword. This preprocessing uses simple string matching and may incorrectly
/// match `TIMESTAMP_NTZ` inside string literals (e.g., `str_col = 'TIMESTAMP_NTZ is cool'`).
/// This is acceptable for benchmark predicates which don't contain such edge cases.
///
/// # Example
/// ```ignore
/// let schema = Schema::try_new(vec![
///     StructField::new("id", DataType::LONG, false),
///     StructField::new("name", DataType::STRING, false),
/// ])?;
/// let pred = parse_predicate("id < 500 AND name = 'alice'", &schema).unwrap();
/// ```
pub fn parse_predicate(sql: &str, schema: &Schema) -> Result<KPred, Box<dyn std::error::Error>> {
    let sql = preprocess_timestamp_ntz(sql);
    let dialect = GenericDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(&sql)?
        .parse_expr()
        .map_err(|e| format!("Failed to parse predicate: {e}"))?;
    expr_to_predicate(schema, &expr)
}

////////////////////////////////////////////////////////////////////////
// Typed (bidirectional) conversion functions
////////////////////////////////////////////////////////////////////////

/// Tries to infer an expression's type, returning both the KExpr and its DataType.
///
/// Returns `Some((KExpr, DataType))` for:
/// - Column references: look up type in schema, return column expression
/// - Boolean literals: return literal expression with BOOLEAN type
///
/// Returns `None` if the expression needs type context (numeric literals, strings, NULL).
fn synthesize_expr(schema: &Schema, expr: &PExpr) -> Option<(KExpr, DataType)> {
    match expr {
        PExpr::Identifier(ident) => {
            let ty = schema.field(ident.value.as_str())?.data_type().clone();
            let expr = KExpr::column([ident.value.clone()]);
            Some((expr, ty))
        }
        PExpr::CompoundIdentifier(parts) => {
            let col_name = ColumnName::new(parts.iter().map(|p| p.value.clone()));
            let ty = schema
                .walk_column_fields(&col_name)
                .ok()?
                .last()?
                .data_type()
                .clone();
            let expr = KExpr::column(parts.iter().map(|p| p.value.clone()));
            Some((expr, ty))
        }
        PExpr::Value(v) => match &v.value {
            PVal::Boolean(b) => Some((Scalar::Boolean(*b).into(), DataType::BOOLEAN)),
            _ => None, // Numeric, string, null need type context
        },
        // Typed literals need type context from the other side of comparison
        // because TIMESTAMP and TIMESTAMP_NTZ use the same syntax after preprocessing
        PExpr::TypedString { .. } => None,
        PExpr::Nested(inner) => synthesize_expr(schema, inner),
        PExpr::BinaryOp { left, op, right } => {
            let (l, r, ty) = synthesize_binary_exprs(schema, left, right).ok()?;
            let expr = match op {
                PBinOp::Plus => KExpr::binary(KBinOp::Plus, l, r),
                PBinOp::Minus => KExpr::binary(KBinOp::Minus, l, r),
                PBinOp::Multiply => KExpr::binary(KBinOp::Multiply, l, r),
                PBinOp::Divide => KExpr::binary(KBinOp::Divide, l, r),
                // Modulo: a % b = a - (b * (a / b))
                // This relies on kernel's Divide producing integer division for integer types,
                // which truncates toward zero (like Rust/C, matching Spark behavior).
                // Examples:
                //   7 % 3  =  7 - (3 * (7 / 3))  =  7 - (3 * 2)  =  1
                //  -7 % 3  = -7 - (3 * (-7 / 3)) = -7 - (3 * -2) = -1
                //   7 % -3 =  7 - (-3 * (7 / -3)) = 7 - (-3 * -2) = 1
                PBinOp::Modulo => {
                    let a_div_b = KExpr::binary(KBinOp::Divide, l.clone(), r.clone());
                    let b_times_quotient = KExpr::binary(KBinOp::Multiply, r, a_div_b);
                    KExpr::binary(KBinOp::Minus, l, b_times_quotient)
                }
                _ => return None,
            };
            Some((expr, ty))
        }
        _ => None,
    }
}

/// Checks that an expression has the expected type, returning the typed KExpr.
fn check_expr(
    schema: &Schema,
    expected_ty: &DataType,
    expr: &PExpr,
) -> Result<KExpr, Box<dyn std::error::Error>> {
    match expr {
        PExpr::Value(v) => check_literal(expected_ty, &v.value, false),
        PExpr::UnaryOp {
            op: PUnaryOp::Minus,
            expr: inner,
        } => match inner.as_ref() {
            PExpr::Value(v) => check_literal(expected_ty, &v.value, true),
            _ => Err(format!("Unsupported unary minus on: {expr}").into()),
        },
        // Typed literals like DATE'2024-01-01' and TIMESTAMP '2024-01-01 00:00:00'
        // Use expected_ty from column to parse with correct type (Timestamp vs TimestampNtz)
        PExpr::TypedString { value, .. } => {
            let (PVal::SingleQuotedString(s) | PVal::DoubleQuotedString(s)) = value else {
                return Err(format!("Invalid typed string value: {value}").into());
            };
            let DataType::Primitive(prim) = expected_ty else {
                return Err(
                    format!("Typed literal cannot be used for type: {expected_ty:?}").into(),
                );
            };
            Ok(prim.parse_scalar(s).map_err(|e| e.to_string())?.into())
        }
        PExpr::Nested(inner) => check_expr(schema, expected_ty, inner),
        _ => {
            // For non-literals, synthesize and verify compatibility
            let (e, actual_ty) = synthesize_expr(schema, expr)
                .ok_or_else(|| format!("Cannot determine type for: {expr}"))?;
            match can_coerce(&actual_ty, expected_ty) {
                true => Ok(e),
                false => Err(
                    format!("Type mismatch: expected {expected_ty:?}, got {actual_ty:?}").into(),
                ),
            }
        }
    }
}

/// Checks a literal value against an expected type and converts it to that type.
/// Uses kernel's `PrimitiveType::parse_scalar` for parsing.
fn check_literal(
    expected_ty: &DataType,
    value: &PVal,
    is_negative: bool,
) -> Result<KExpr, Box<dyn std::error::Error>> {
    use PrimitiveType::*;
    match (expected_ty, value) {
        // Numeric literals - only for numeric primitive types
        (
            DataType::Primitive(
                prim @ (Byte | Short | Integer | Long | Float | Double | Decimal(_)),
            ),
            PVal::Number(n, _),
        ) => {
            let raw = format!("{}{n}", if is_negative { "-" } else { "" });
            let scalar = prim.parse_scalar(&raw).map_err(|e| e.to_string())?;
            Ok(scalar.into())
        }

        // String literals - use parse_scalar (handles String, Date, Timestamp, etc.)
        (DataType::Primitive(prim), PVal::SingleQuotedString(s) | PVal::DoubleQuotedString(s)) => {
            let scalar = prim.parse_scalar(s).map_err(|e| e.to_string())?;
            Ok(scalar.into())
        }

        // Boolean literals
        (DataType::Primitive(Boolean), PVal::Boolean(b)) => Ok(Scalar::Boolean(*b).into()),

        // NULL can be any type
        (ty, PVal::Null) => Ok(Scalar::Null(ty.clone()).into()),

        // Type mismatches
        (expected, actual) => Err(format!(
            "Type mismatch: cannot use {:?} for column of type {:?}",
            actual, expected
        )
        .into()),
    }
}

/// Checks if one type can be coerced to another using kernel's type widening rules.
fn can_coerce(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Primitive(f), DataType::Primitive(t)) => f == t || f.can_widen_to(t),
        _ => from == to,
    }
}

/// Finds the widest common type between two types, if one exists.
fn find_common_type(l_ty: &DataType, r_ty: &DataType) -> Option<DataType> {
    if l_ty == r_ty {
        Some(l_ty.clone())
    } else if can_coerce(l_ty, r_ty) {
        Some(r_ty.clone())
    } else if can_coerce(r_ty, l_ty) {
        Some(l_ty.clone())
    } else {
        None
    }
}

/// Synthesizes both operands of a binary operation and checks them against the widest common type.
/// Returns `Ok((left_expr, right_expr, common_type))` on success.
fn synthesize_binary_exprs(
    schema: &Schema,
    left: &PExpr,
    right: &PExpr,
) -> Result<(KExpr, KExpr, DataType), Box<dyn std::error::Error>> {
    let l_syn = synthesize_expr(schema, left);
    let r_syn = synthesize_expr(schema, right);

    let common_ty = match (&l_syn, &r_syn) {
        (Some((_, l_ty)), Some((_, r_ty))) => find_common_type(l_ty, r_ty)
            .ok_or_else(|| format!("Type mismatch: cannot compare {l_ty:?} with {r_ty:?}"))?,
        (Some((_, ty)), None) | (None, Some((_, ty))) => ty.clone(),
        (None, None) => {
            return Err(format!("Cannot determine types for: {left:?} and {right:?}").into())
        }
    };

    // Type check each side to the common type, ensuring that the expression is coerced to the
    // common type.
    let l = check_expr(schema, &common_ty, left)?;
    let r = check_expr(schema, &common_ty, right)?;

    Ok((l, r, common_ty))
}

/// Converts a sqlparser AST expression into a kernel [`KPred`] with type checking.
fn expr_to_predicate(schema: &Schema, expr: &PExpr) -> Result<KPred, Box<dyn std::error::Error>> {
    match expr {
        PExpr::BinaryOp { left, op, right } => binary_op_to_predicate(schema, left, op, right),
        PExpr::UnaryOp {
            op: PUnaryOp::Not,
            expr,
        } => Ok(KPred::not(expr_to_predicate(schema, expr)?)),
        PExpr::IsNull(e) | PExpr::IsNotNull(e) => {
            let (col_expr, _) = synthesize_expr(schema, e)
                .ok_or_else(|| format!("Cannot synthesize type for IS NULL operand: {e}"))?;
            match expr {
                PExpr::IsNull(_) => Ok(KPred::is_null(col_expr)),
                _ => Ok(KPred::is_not_null(col_expr)),
            }
        }
        PExpr::Nested(inner) => expr_to_predicate(schema, inner),
        PExpr::Value(v) => match &v.value {
            PVal::Boolean(b) => Ok(KPred::literal(*b)),
            _ => Err(format!("Unsupported literal in predicate position: {v}").into()),
        },
        PExpr::Identifier(_) | PExpr::CompoundIdentifier(_) => {
            let (col_expr, _) =
                synthesize_expr(schema, expr).ok_or_else(|| format!("Unknown column: {expr}"))?;
            Ok(KPred::from_expr(col_expr))
        }
        PExpr::InList {
            expr,
            list,
            negated,
        } => in_list_to_pred(schema, expr, list, *negated),
        PExpr::Between {
            expr,
            negated,
            low,
            high,
        } => between_to_pred(schema, expr, *negated, low, high),
        _ => Err(format!("Unsupported expression: {expr}").into()),
    }
}

/// Converts a binary comparison into a kernel [`KPred`] with bidirectional type checking.
fn binary_op_to_predicate(
    schema: &Schema,
    left: &PExpr,
    op: &PBinOp,
    right: &PExpr,
) -> Result<KPred, Box<dyn std::error::Error>> {
    match op {
        PBinOp::Eq
        | PBinOp::NotEq
        | PBinOp::Lt
        | PBinOp::LtEq
        | PBinOp::Gt
        | PBinOp::GtEq
        | PBinOp::Spaceship => {
            let (l, r, _) = synthesize_binary_exprs(schema, left, right)?;
            Ok(match op {
                PBinOp::Eq => KPred::eq(l, r),
                PBinOp::NotEq => KPred::ne(l, r),
                PBinOp::Lt => KPred::lt(l, r),
                PBinOp::LtEq => KPred::le(l, r),
                PBinOp::Gt => KPred::gt(l, r),
                PBinOp::GtEq => KPred::ge(l, r),
                PBinOp::Spaceship => KPred::not(KPred::distinct(l, r)),
                _ => unreachable!(),
            })
        }
        PBinOp::And => {
            let l = expr_to_predicate(schema, left)?;
            let r = expr_to_predicate(schema, right)?;
            Ok(KPred::and(l, r))
        }
        PBinOp::Or => {
            let l = expr_to_predicate(schema, left)?;
            let r = expr_to_predicate(schema, right)?;
            Ok(KPred::or(l, r))
        }
        _ => Err(format!("Unsupported binary operator: {op}").into()),
    }
}

/// Converts an IN/NOT IN list into a kernel [`KPred`] with type checking.
fn in_list_to_pred(
    schema: &Schema,
    expr: &PExpr,
    list: &[PExpr],
    negated: bool,
) -> Result<KPred, Box<dyn std::error::Error>> {
    let (col_expr, col_ty) = synthesize_expr(schema, expr)
        .ok_or_else(|| format!("IN requires a column, got: {expr}"))?;

    let scalars: Vec<Scalar> = list
        .iter()
        .map(|e| -> Result<_, Box<dyn std::error::Error>> {
            match check_expr(schema, &col_ty, e)? {
                KExpr::Literal(s) => Ok(s),
                _ => Err(format!("IN list elements must be literals, got: {e}").into()),
            }
        })
        .try_collect()?;

    // Check if any scalar is null to determine array nullability
    let contains_null = scalars.iter().any(|s| s.is_null());
    let array_data = ArrayData::try_new(ArrayType::new(col_ty, contains_null), scalars)?;
    let pred = KPred::binary(
        KPredOp::In,
        col_expr,
        KExpr::literal(Scalar::Array(array_data)),
    );
    Ok(if negated { KPred::not(pred) } else { pred })
}

/// Converts BETWEEN into `col >= low AND col <= high` with type checking.
fn between_to_pred(
    schema: &Schema,
    expr: &PExpr,
    negated: bool,
    low: &PExpr,
    high: &PExpr,
) -> Result<KPred, Box<dyn std::error::Error>> {
    let (expr_checked, low_checked, common_ty) = synthesize_binary_exprs(schema, expr, low)?;
    let high_checked = check_expr(schema, &common_ty, high)?;

    let pred = KPred::and(
        KPred::ge(expr_checked.clone(), low_checked),
        KPred::le(expr_checked, high_checked),
    );
    Ok(if negated { KPred::not(pred) } else { pred })
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::{column_name as col, Scalar::*};
    use delta_kernel::schema::{MapType, Schema, StructField, StructType};
    use rstest::rstest;

    fn test_schema() -> Schema {
        Schema::new_unchecked(vec![
            // Primitive types
            StructField::new("byte_col", DataType::BYTE, true),
            StructField::new("short_col", DataType::SHORT, true),
            StructField::new("int_col", DataType::INTEGER, true),
            StructField::new("long_col", DataType::LONG, true),
            StructField::new("float_col", DataType::FLOAT, true),
            StructField::new("double_col", DataType::DOUBLE, true),
            StructField::new("str_col", DataType::STRING, true),
            StructField::new("bool_col", DataType::BOOLEAN, true),
            StructField::new("date_col", DataType::DATE, true),
            StructField::new("ts_col", DataType::TIMESTAMP, true),
            StructField::new("ts_ntz_col", DataType::TIMESTAMP_NTZ, true),
            // Struct type
            StructField::new(
                "struct_col",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::new("inner_int", DataType::INTEGER, true),
                    StructField::new("inner_str", DataType::STRING, true),
                ]))),
                true,
            ),
            // Array type
            StructField::new(
                "array_col",
                DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                true,
            ),
            // Map type
            StructField::new(
                "map_col",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::LONG,
                    true,
                ))),
                true,
            ),
        ])
    }

    // Comparisons with typed scalars
    #[rstest]
    #[case("long_col < 500", KPred::lt(col!("long_col"), Long(500)))]
    #[case("500 > long_col", KPred::gt(Long(500), col!("long_col")))]
    #[case("long_col = 999", KPred::eq(col!("long_col"), Long(999)))]
    #[case("int_col > 100", KPred::gt(col!("int_col"), Integer(100)))]
    #[case("short_col < 50", KPred::lt(col!("short_col"), Short(50)))]
    #[case("byte_col <= 127", KPred::le(col!("byte_col"), Byte(127)))]
    #[case("double_col >= 3.5", KPred::ge(col!("double_col"), Double(3.5)))]
    #[case("float_col < 1.5", KPred::lt(col!("float_col"), Float(1.5)))]
    #[case("str_col = 'hello'", KPred::eq(col!("str_col"), String("hello".to_string())))]
    #[case("bool_col = true", KPred::eq(col!("bool_col"), Boolean(true)))]
    #[case("bool_col = false", KPred::eq(col!("bool_col"), Boolean(false)))]
    #[case("long_col > -10", KPred::gt(col!("long_col"), Long(-10)))]
    #[case("long_col IS NULL", KPred::is_null(col!("long_col")))]
    #[case("long_col IS NOT NULL", KPred::is_not_null(col!("long_col")))]
    #[case("str_col IS NOT NULL", KPred::is_not_null(col!("str_col")))]
    fn parse_comparison(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Logical operators
    #[rstest]
    #[case(
        "long_col < 500 AND int_col > 10",
        KPred::and(
            KPred::lt(col!("long_col"), Long(500)),
            KPred::gt(col!("int_col"), Integer(10))
        )
    )]
    #[case(
        "long_col = 1 OR long_col = 2",
        KPred::or(
            KPred::eq(col!("long_col"), Long(1)),
            KPred::eq(col!("long_col"), Long(2))
        )
    )]
    #[case(
        "NOT long_col = 1",
        KPred::not(KPred::eq(col!("long_col"), Long(1)))
    )]
    #[case(
        "(long_col < 500) AND (int_col > 10)",
        KPred::and(
            KPred::lt(col!("long_col"), Long(500)),
            KPred::gt(col!("int_col"), Integer(10))
        )
    )]
    fn parse_logical(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // BETWEEN
    #[rstest]
    #[case(
        "long_col BETWEEN 10 AND 20",
        KPred::and(
            KPred::ge(col!("long_col"), Long(10)),
            KPred::le(col!("long_col"), Long(20))
        )
    )]
    #[case(
        "short_col BETWEEN 10 AND 20",
        KPred::and(
            KPred::ge(col!("short_col"), Short(10)),
            KPred::le(col!("short_col"), Short(20))
        )
    )]
    #[case(
        "int_col NOT BETWEEN 0 AND 10",
        KPred::not(KPred::and(
            KPred::ge(col!("int_col"), Integer(0)),
            KPred::le(col!("int_col"), Integer(10))
        ))
    )]
    fn parse_between(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    /// Helper to build an IN list predicate
    fn in_list_pred(
        col: impl Into<KExpr>,
        element_type: DataType,
        values: Vec<Scalar>,
        negated: bool,
    ) -> KPred {
        let array = ArrayData::try_new(ArrayType::new(element_type, false), values).unwrap();
        let pred = KPred::binary(KPredOp::In, col.into(), KExpr::literal(Array(array)));
        if negated {
            KPred::not(pred)
        } else {
            pred
        }
    }

    // IN list
    #[rstest]
    #[case("long_col IN (1, 2, 3)", in_list_pred(col!("long_col"), DataType::LONG, vec![Long(1), Long(2), Long(3)], false))]
    #[case("long_col NOT IN (1, 2)", in_list_pred(col!("long_col"), DataType::LONG, vec![Long(1), Long(2)], true))]
    #[case("int_col IN (10, 20, 30)", in_list_pred(col!("int_col"), DataType::INTEGER, vec![Integer(10), Integer(20), Integer(30)], false))]
    fn parse_in_list(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Null-safe equals
    #[rstest]
    #[case(
        "long_col <=> 1",
        KPred::not(KPred::distinct(col!("long_col"), Long(1)))
    )]
    #[case(
        "long_col <=> NULL",
        KPred::not(KPred::distinct(col!("long_col"), Null(DataType::LONG)))
    )]
    fn parse_null_safe_equals(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Type errors
    #[rstest]
    #[case("long_col > 'CD'", "Failed to parse")] // String literal for Long column
    #[case("long_col < 'AB'", "Failed to parse")] // String literal for Long column
    #[case("long_col = false", "Type mismatch")] // Boolean vs Long
    #[case("long_col <= false", "Type mismatch")] // Boolean vs Long
    #[case("false = long_col", "Type mismatch")] // Boolean vs Long
    #[case("str_col = 123", "Type mismatch")] // Number literal for String column
    #[case("short_col < 100000", "Failed to parse")] // Overflow: 100000 > i16::MAX
    fn type_error_rejected(#[case] sql: &str, #[case] error_contains: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "Expected error for: {}", sql);
        assert!(
            result.unwrap_err().to_string().contains(error_contains),
            "Error should contain '{}'",
            error_contains
        );
    }

    // Unknown column produces an error
    #[test]
    fn unknown_column_produces_error() {
        let schema = test_schema();
        let result = parse_predicate("unknown_col < 500", &schema);
        assert!(result.is_err(), "Unknown columns should produce an error");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot determine type"));
    }

    // Nested struct tests
    #[rstest]
    #[case("struct_col.inner_int > 25", KPred::gt(col!("struct_col.inner_int"), Integer(25)))]
    #[case("struct_col.inner_str = 'alice'", KPred::eq(col!("struct_col.inner_str"), String("alice".to_string())))]
    #[case("struct_col.inner_int IS NULL", KPred::is_null(col!("struct_col.inner_int")))]
    fn parse_nested_struct(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_nested_struct_in_list() {
        let schema = test_schema();
        let pred = parse_predicate("struct_col.inner_int IN (18, 21, 25)", &schema).unwrap();
        let expected = in_list_pred(
            col!("struct_col.inner_int"),
            DataType::INTEGER,
            vec![Integer(18), Integer(21), Integer(25)],
            false,
        );
        assert_eq!(pred, expected);
    }

    #[rstest]
    #[case(
        "struct_col.inner_int BETWEEN 18 AND 65",
        KPred::and(
            KPred::ge(col!("struct_col.inner_int"), Integer(18)),
            KPred::le(col!("struct_col.inner_int"), Integer(65))
        )
    )]
    fn parse_nested_struct_between(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // LIKE is not supported
    #[rstest]
    #[case("str_col LIKE 'b%'")]
    #[case("str_col like '%test%'")]
    fn unsupported_like(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(result.is_err(), "LIKE should not be supported: {}", sql);
    }

    // Function calls are not supported
    #[rstest]
    #[case("str_col = HEX('1111')")]
    #[case("size(array_col) > 2")]
    #[case("length(str_col) < 4")]
    fn unsupported_function_calls(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Function calls should not be supported: {}",
            sql
        );
    }

    // Arithmetic expressions
    #[rstest]
    #[case(
        "long_col + 10 > 100",
        KPred::gt(KExpr::binary(KBinOp::Plus, col!("long_col"), Long(10)), Long(100))
    )]
    #[case(
        "long_col * 2 = 10",
        KPred::eq(KExpr::binary(KBinOp::Multiply, col!("long_col"), Long(2)), Long(10))
    )]
    #[case(
        "int_col - 5 < 0",
        KPred::lt(KExpr::binary(KBinOp::Minus, col!("int_col"), Integer(5)), Integer(0))
    )]
    #[case(
        "double_col / 2.0 >= 1.5",
        KPred::ge(KExpr::binary(KBinOp::Divide, col!("double_col"), Double(2.0)), Double(1.5))
    )]
    fn parse_arithmetic(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        let pred = parse_predicate(sql, &schema).unwrap();
        assert_eq!(pred, expected);
    }

    // Modulo is transformed to a - (b * (a / b))
    #[test]
    fn parse_modulo() {
        let schema = test_schema();
        let pred = parse_predicate("long_col % 100 < 10", &schema).unwrap();
        // long_col % 100 = long_col - (100 * (long_col / 100))
        let a = col!("long_col");
        let b: KExpr = Long(100).into();
        let a_div_b = KExpr::binary(KBinOp::Divide, a.clone(), b.clone());
        let b_times_quotient = KExpr::binary(KBinOp::Multiply, b, a_div_b);
        let modulo = KExpr::binary(KBinOp::Minus, a, b_times_quotient);
        let expected = KPred::lt(modulo, Long(10));
        assert_eq!(pred, expected);
    }

    // Typed literals (TIME keyword) are not supported
    #[rstest]
    #[case("ts_col >= TIME '00:00:00'")]
    #[case("ts_col > TIME '12:00:00'")]
    fn unsupported_typed_literals(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Typed literals should not be supported: {}",
            sql
        );
    }

    // IS NULL on complex expressions (not a column) is not supported
    #[rstest]
    #[case("(long_col < 0) IS NULL")]
    #[case("(long_col > 0 AND int_col > 1) IS NULL")]
    fn unsupported_is_null_on_expr(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "IS NULL on expressions should not be supported: {}",
            sql
        );
    }

    // Modulo with negative operands (truncates toward zero like Rust/Spark)
    #[test]
    fn parse_modulo_with_negative_operands() {
        let schema = test_schema();

        // -7 % 3 = -7 - (3 * (-7 / 3)) = -7 - (3 * -2) = -7 - (-6) = -1
        let pred = parse_predicate("long_col % 3 = -1", &schema).unwrap();
        let a = col!("long_col");
        let b: KExpr = Long(3).into();
        let a_div_b = KExpr::binary(KBinOp::Divide, a.clone(), b.clone());
        let b_times_quotient = KExpr::binary(KBinOp::Multiply, b, a_div_b);
        let modulo = KExpr::binary(KBinOp::Minus, a, b_times_quotient);
        let expected = KPred::eq(modulo, Long(-1));
        assert_eq!(pred, expected);
    }

    // preprocess_timestamp_ntz tests
    #[rstest]
    #[case("TIMESTAMP_NTZ'2024-01-01'", "TIMESTAMP'2024-01-01'")]
    #[case("TIMESTAMP_NTZ '2024-01-01'", "TIMESTAMP '2024-01-01'")] // space before quote
    #[case("col = TIMESTAMP_NTZ'2024-01-01'", "col = TIMESTAMP'2024-01-01'")] // in expression
    #[case("TIMESTAMP'2024-01-01'", "TIMESTAMP'2024-01-01'")] // TIMESTAMP unchanged
    #[case("col = 'test'", "col = 'test'")] // no change
    fn preprocess_timestamp_ntz_replaces_keyword(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(preprocess_timestamp_ntz(input), expected);
    }

    // DATE and TIMESTAMP typed literals are supported via TypedString
    #[test]
    fn parse_date_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("date_col = DATE'2024-01-15'", &schema).unwrap();
        // Date is days since epoch: 2024-01-15 = 19737 days
        let expected = KPred::eq(col!("date_col"), Date(19737));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("ts_col = TIMESTAMP'2024-01-15 12:30:00'", &schema).unwrap();
        // Timestamp is microseconds since epoch
        // 2024-01-15 12:30:00 UTC = 1705321800000000 microseconds
        let expected = KPred::eq(col!("ts_col"), Timestamp(1705321800000000));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_ntz_typed_literal() {
        let schema = test_schema();
        let pred =
            parse_predicate("ts_ntz_col = TIMESTAMP_NTZ'2024-01-15 12:30:00'", &schema).unwrap();
        // TimestampNtz is microseconds since epoch (same numeric value as Timestamp)
        let expected = KPred::eq(col!("ts_ntz_col"), TimestampNtz(1705321800000000));
        assert_eq!(pred, expected);
    }
}
