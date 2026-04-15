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

// Imports use a naming convention to distinguish kernel types from sqlparser types:
// - K-prefix: Kernel types (KExpr, KPred, KBinOp, KPredOp)
// - P-prefix: Parser/sqlparser types (PExpr, PBinOp, PUnaryOp, PVal)
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
    // Local import to avoid conflict with Scalar::* variants (Long, Integer, etc.) used in tests
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
    use delta_kernel::expressions::{column_name, Scalar::*};
    use delta_kernel::schema::{MapType, Schema, StructField, StructType};
    use rstest::rstest;

    /// Test schema with columns for all test cases.
    fn test_schema() -> Schema {
        Schema::new_unchecked(vec![
            // Columns matching upstream tests
            StructField::new("id", DataType::LONG, true),
            StructField::new("a", DataType::LONG, true),
            StructField::new("b", DataType::LONG, true),
            StructField::new("c", DataType::LONG, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("value", DataType::LONG, true),
            StructField::new("flag", DataType::BOOLEAN, true),
            StructField::new("partCol", DataType::LONG, true),
            StructField::new("version_tag", DataType::STRING, true),
            // Double columns for literal_types tests
            StructField::new("c3", DataType::DOUBLE, true),
            StructField::new("c4", DataType::DOUBLE, true),
            StructField::new("val", DataType::DOUBLE, true),
            StructField::new("cc9", DataType::BOOLEAN, true),
            StructField::new("long_val", DataType::LONG, true),
            // Columns for unsupported predicate tests (these trigger other errors first)
            StructField::new("fruit", DataType::STRING, true),
            StructField::new("cc8", DataType::STRING, true),
            StructField::new("s", DataType::STRING, true),
            StructField::new("time_col", DataType::TIMESTAMP, true),
            StructField::new(
                "items",
                DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                true,
            ),
            // Nested struct for upstream tests
            StructField::new(
                "null_v_struct",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "v",
                    DataType::LONG,
                    true,
                )]))),
                true,
            ),
            // Nested structs for nested_columns tests (a.b, a.b.c, b.c.f.i, data.value)
            StructField::new(
                "data",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![StructField::new(
                    "value",
                    DataType::LONG,
                    true,
                )]))),
                true,
            ),
            // Additional typed columns for type-checking tests
            StructField::new("int_col", DataType::INTEGER, true),
            StructField::new("str_col", DataType::STRING, true),
            StructField::new("long_col", DataType::LONG, true),
            StructField::new("double_col", DataType::DOUBLE, true),
            StructField::new("short_col", DataType::SHORT, true),
            StructField::new("byte_col", DataType::BYTE, true),
            StructField::new("float_col", DataType::FLOAT, true),
            StructField::new("bool_col", DataType::BOOLEAN, true),
            StructField::new("date_col", DataType::DATE, true),
            StructField::new("ts_col", DataType::TIMESTAMP, true),
            StructField::new("ts_ntz_col", DataType::TIMESTAMP_NTZ, true),
            // Struct type for nested tests
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

    // Helper to build an IN predicate: `col IN (scalars...)`
    fn in_list(col: ColumnName, scalars: Vec<Scalar>) -> KPred {
        let element_type = scalars
            .first()
            .map(|s| s.data_type())
            .unwrap_or(DataType::LONG);
        let array = ArrayData::try_new(ArrayType::new(element_type, false), scalars).unwrap();
        KPred::binary(KPredOp::In, col, KExpr::literal(Array(array)))
    }

    // -- Comparisons --
    #[rstest]
    #[case("id < 5", KPred::lt(column_name!("id"), Long(5)))]
    #[case("id = 999", KPred::eq(column_name!("id"), Long(999)))]
    #[case("id > 250", KPred::gt(column_name!("id"), Long(250)))]
    #[case("id <= 2", KPred::le(column_name!("id"), Long(2)))]
    #[case("id >= 100", KPred::ge(column_name!("id"), Long(100)))]
    #[case("a <> 1", KPred::ne(column_name!("a"), Long(1)))]
    #[case("a != 1", KPred::ne(column_name!("a"), Long(1)))]
    // Literal on the left
    #[case("1 < a", KPred::lt(Long(1), column_name!("a")))]
    #[case("1 = a", KPred::eq(Long(1), column_name!("a")))]
    #[case("1 != a", KPred::ne(Long(1), column_name!("a")))]
    fn comparison(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- Literal types --
    #[rstest]
    #[case("name = 'bob'", KPred::eq(column_name!("name"), String("bob".to_string())))]
    #[case("c3 < 1.5", KPred::lt(column_name!("c3"), Double(1.5)))]
    #[case("val < -2.5", KPred::lt(column_name!("val"), Double(-2.5)))]
    #[case("c4 > 5.0", KPred::gt(column_name!("c4"), Double(5.0)))]
    #[case("flag = true", KPred::eq(column_name!("flag"), Boolean(true)))]
    #[case("cc9 = false", KPred::eq(column_name!("cc9"), Boolean(false)))]
    #[case("a = NULL", KPred::eq(column_name!("a"), Null(DataType::LONG)))]
    #[case("id > -100", KPred::gt(column_name!("id"), Long(-100)))]
    #[case("val > 1.0E300", KPred::gt(column_name!("val"), Double(1.0E300)))]
    #[case("a > 2147483647", KPred::gt(column_name!("a"), Long(2147483647)))]
    #[case("long_val < 50000000000", KPred::lt(column_name!("long_val"), Long(50000000000)))]
    fn literal_types(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- Compound identifiers (nested columns) --
    #[rstest]
    #[case("data.value > 150", KPred::gt(KExpr::column(["data", "value"]), Long(150)))]
    #[case("null_v_struct.v > 1", KPred::gt(KExpr::column(["null_v_struct", "v"]), Long(1)))]
    #[case("struct_col.inner_int = 2", KPred::eq(KExpr::column(["struct_col", "inner_int"]), Integer(2)))]
    fn nested_columns(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- Boolean literals --
    #[rstest]
    #[case("TRUE", KPred::literal(true))]
    #[case("FALSE", KPred::literal(false))]
    fn boolean_literals(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- IS NULL / IS NOT NULL --
    #[rstest]
    #[case("a IS NULL", KPred::is_null(column_name!("a")))]
    #[case("a IS NOT NULL", KPred::is_not_null(column_name!("a")))]
    #[case("null_v_struct.v IS NULL", KPred::is_null(KExpr::column(["null_v_struct", "v"])))]
    fn is_null(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- NOT --
    #[rstest]
    #[case("NOT a = 1", KPred::not(KPred::eq(column_name!("a"), Long(1))))]
    #[case("NOT a = NULL", KPred::not(KPred::eq(column_name!("a"), Null(DataType::LONG))))]
    #[case(
        "NOT(a < 1 OR b > 20)",
        KPred::not(KPred::or(
            KPred::lt(column_name!("a"), Long(1)),
            KPred::gt(column_name!("b"), Long(20)),
        ))
    )]
    fn not_predicate(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- AND / OR --
    #[rstest]
    #[case(
        "id < 500 AND value > 10",
        KPred::and(
            KPred::lt(column_name!("id"), Long(500)),
            KPred::gt(column_name!("value"), Long(10)),
        )
    )]
    #[case(
        "id = 1 OR id = 2",
        KPred::or(
            KPred::eq(column_name!("id"), Long(1)),
            KPred::eq(column_name!("id"), Long(2)),
        )
    )]
    #[case(
        "a = 0 AND b = 0 AND c = 0",
        KPred::and(
            KPred::and(
                KPred::eq(column_name!("a"), Long(0)),
                KPred::eq(column_name!("b"), Long(0)),
            ),
            KPred::eq(column_name!("c"), Long(0)),
        )
    )]
    #[case(
        "(a < 3 AND b < 3) OR (a > 7 AND b > 7)",
        KPred::or(
            KPred::and(
                KPred::lt(column_name!("a"), Long(3)),
                KPred::lt(column_name!("b"), Long(3)),
            ),
            KPred::and(
                KPred::gt(column_name!("a"), Long(7)),
                KPred::gt(column_name!("b"), Long(7)),
            ),
        )
    )]
    #[case(
        "(a = 5 OR a = 7) AND b < 5",
        KPred::and(
            KPred::or(
                KPred::eq(column_name!("a"), Long(5)),
                KPred::eq(column_name!("a"), Long(7)),
            ),
            KPred::lt(column_name!("b"), Long(5)),
        )
    )]
    fn and_or(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- Null-safe equals (<=>)  ->  NOT DISTINCT --
    #[rstest]
    #[case("a <=> 1", KPred::not(KPred::distinct(column_name!("a"), Long(1))))]
    #[case("a <=> NULL", KPred::not(KPred::distinct(column_name!("a"), Null(DataType::LONG))))]
    #[case("1 <=> a", KPred::not(KPred::distinct(Long(1), column_name!("a"))))]
    #[case(
        "NOT a <=> 1",
        KPred::not(KPred::not(KPred::distinct(column_name!("a"), Long(1))))
    )]
    fn null_safe_equals(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- IN / NOT IN --
    #[rstest]
    #[case(
        "a in (1, 2, 3)",
        in_list(column_name!("a"), vec![Long(1), Long(2), Long(3)])
    )]
    #[case(
        "a in (1)",
        in_list(column_name!("a"), vec![Long(1)])
    )]
    #[case(
        "value in (300, 787, 239)",
        in_list(column_name!("value"), vec![Long(300), Long(787), Long(239)])
    )]
    #[case(
        "name in ('alice', 'bob')",
        in_list(column_name!("name"), vec![String("alice".to_string()), String("bob".to_string())])
    )]
    fn in_predicate(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    #[rstest]
    #[case(
        "a NOT IN (1, 2)",
        KPred::not(in_list(column_name!("a"), vec![Long(1), Long(2)]))
    )]
    #[case(
        "a NOT IN (10, 20, 30)",
        KPred::not(in_list(column_name!("a"), vec![Long(10), Long(20), Long(30)]))
    )]
    fn not_in_predicate(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- BETWEEN  ->  col >= low AND col <= high --
    #[rstest]
    #[case(
        "id BETWEEN 10 AND 20",
        KPred::and(
            KPred::ge(column_name!("id"), Long(10)),
            KPred::le(column_name!("id"), Long(20)),
        )
    )]
    #[case(
        "id BETWEEN -10 AND -1",
        KPred::and(
            KPred::ge(column_name!("id"), Long(-10)),
            KPred::le(column_name!("id"), Long(-1)),
        )
    )]
    #[case(
        "id NOT BETWEEN 10 AND 20",
        KPred::not(KPred::and(
            KPred::ge(column_name!("id"), Long(10)),
            KPred::le(column_name!("id"), Long(20)),
        ))
    )]
    fn between(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // -- Complex predicates (multi-feature) --
    #[rstest]
    #[case(
        "id >= 0 AND id < 1000 AND version_tag = 'v0'",
        KPred::and(
            KPred::and(
                KPred::ge(column_name!("id"), Long(0)),
                KPred::lt(column_name!("id"), Long(1000)),
            ),
            KPred::eq(column_name!("version_tag"), String("v0".to_string())),
        )
    )]
    #[case(
        "int_col IS NOT NULL AND str_col IS NOT NULL",
        KPred::and(
            KPred::is_not_null(column_name!("int_col")),
            KPred::is_not_null(column_name!("str_col")),
        )
    )]
    #[case(
        "NOT (a >= 5 AND NOT (b < 5))",
        KPred::not(KPred::and(
            KPred::ge(column_name!("a"), Long(5)),
            KPred::not(KPred::lt(column_name!("b"), Long(5))),
        ))
    )]
    #[case(
        "partCol = 3 and id > 25",
        KPred::and(
            KPred::eq(column_name!("partCol"), Long(3)),
            KPred::gt(column_name!("id"), Long(25)),
        )
    )]
    fn complex(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    #[rstest]
    // LIKE (no kernel support)
    #[case("a like 'C%'")]
    #[case("fruit like 'b%'")]
    #[case("a > 0 AND b like '2016-%'")]
    // Function calls
    #[case("cc8 = HEX('1111')")]
    #[case("size(items) > 2")]
    #[case("length(s) < 4")]
    // Typed literals (TIME keyword)
    #[case("time_col >= TIME '00:00:00'")]
    #[case("time_col < TIME '12:00:00'")]
    // IS NULL on non-column expressions
    #[case("(a > 0) IS NULL")]
    #[case("(a > 0 AND b > 1) IS NULL")]
    fn unsupported_predicates_fail_gracefully(#[case] sql: &str) {
        let schema = test_schema();
        let result = parse_predicate(sql, &schema);
        assert!(
            result.is_err(),
            "Expected {sql:?} to fail, but it parsed as: {:?}",
            result.unwrap()
        );
    }

    // ========== New features: type checking, arithmetic, timestamps ==========

    // Type errors (schema-based type checking)
    #[rstest]
    #[case("long_col > 'CD'", "Failed to parse")] // String literal for Long column
    #[case("long_col = false", "Type mismatch")] // Boolean vs Long
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

    #[test]
    fn literal_only_comparison_fails() {
        // When both sides are literals (no column context), types cannot be determined
        let schema = test_schema();
        let result = parse_predicate("5 > 3", &schema);
        assert!(result.is_err(), "Literal-only comparisons should fail");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot determine types"));
    }

    // IN list with NULL values (tests ArrayType nullability)
    #[test]
    fn in_list_with_null_values() {
        let schema = test_schema();
        let pred = parse_predicate("a IN (1, 2, NULL)", &schema).unwrap();
        // The array should be nullable since it contains NULL
        let expected_array = ArrayData::try_new(
            ArrayType::new(DataType::LONG, true), // nullable = true
            vec![Long(1), Long(2), Null(DataType::LONG)],
        )
        .unwrap();
        let expected = KPred::binary(
            KPredOp::In,
            column_name!("a"),
            KExpr::literal(Scalar::Array(expected_array)),
        );
        assert_eq!(pred, expected);
    }

    // Arithmetic expressions (now supported)
    #[rstest]
    #[case(
        "a % 100 < 10 OR b > 20",
        KPred::or(
            KPred::lt(
                KExpr::binary(
                    KBinOp::Minus,
                    column_name!("a"),
                    KExpr::binary(
                        KBinOp::Multiply,
                        Long(100),
                        KExpr::binary(KBinOp::Divide, column_name!("a"), Long(100))
                    )
                ),
                Long(10)
            ),
            KPred::gt(column_name!("b"), Long(20))
        )
    )]
    #[case(
        "long_col + 10 > 100",
        KPred::gt(KExpr::binary(KBinOp::Plus, column_name!("long_col"), Long(10)), Long(100))
    )]
    #[case(
        "long_col * 2 = 10",
        KPred::eq(KExpr::binary(KBinOp::Multiply, column_name!("long_col"), Long(2)), Long(10))
    )]
    fn arithmetic(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // Typed literals (DATE, TIMESTAMP, TIMESTAMP_NTZ)
    #[test]
    fn parse_date_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("date_col = DATE'2024-01-15'", &schema).unwrap();
        let expected = KPred::eq(column_name!("date_col"), Date(19737));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_typed_literal() {
        let schema = test_schema();
        let pred = parse_predicate("ts_col = TIMESTAMP'2024-01-15 12:30:00'", &schema).unwrap();
        let expected = KPred::eq(column_name!("ts_col"), Timestamp(1705321800000000));
        assert_eq!(pred, expected);
    }

    #[test]
    fn parse_timestamp_ntz_typed_literal() {
        let schema = test_schema();
        let pred =
            parse_predicate("ts_ntz_col = TIMESTAMP_NTZ'2024-01-15 12:30:00'", &schema).unwrap();
        let expected = KPred::eq(column_name!("ts_ntz_col"), TimestampNtz(1705321800000000));
        assert_eq!(pred, expected);
    }

    #[rstest]
    #[case("TIMESTAMP_NTZ'2024-01-01'", "TIMESTAMP'2024-01-01'")]
    #[case("TIMESTAMP_NTZ '2024-01-01'", "TIMESTAMP '2024-01-01'")]
    #[case("col = TIMESTAMP_NTZ'2024-01-01'", "col = TIMESTAMP'2024-01-01'")]
    #[case("TIMESTAMP'2024-01-01'", "TIMESTAMP'2024-01-01'")]
    #[case("col = 'test'", "col = 'test'")]
    fn preprocess_timestamp_ntz_replaces_keyword(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(preprocess_timestamp_ntz(input), expected);
    }

    // Typed scalar inference from schema
    #[rstest]
    #[case("int_col > 100", KPred::gt(column_name!("int_col"), Integer(100)))]
    #[case("short_col < 50", KPred::lt(column_name!("short_col"), Short(50)))]
    #[case("byte_col <= 127", KPred::le(column_name!("byte_col"), Byte(127)))]
    #[case("float_col < 1.5", KPred::lt(column_name!("float_col"), Float(1.5)))]
    fn typed_scalar_inference(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }

    // Nested struct column tests
    #[rstest]
    #[case("struct_col.inner_int > 25", KPred::gt(KExpr::column(["struct_col", "inner_int"]), Integer(25)))]
    #[case("struct_col.inner_str = 'alice'", KPred::eq(KExpr::column(["struct_col", "inner_str"]), String("alice".to_string())))]
    fn nested_struct_columns(#[case] sql: &str, #[case] expected: KPred) {
        let schema = test_schema();
        assert_eq!(parse_predicate(sql, &schema).unwrap(), expected);
    }
}
