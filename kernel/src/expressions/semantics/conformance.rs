//! Executor-independent cases for Kernel's standard SQL expression semantics.
//!
//! Each case supplies one input row, an unresolved Kernel expression, its resolved output, and the
//! required result. A harness analyzes and executes each case according to Kernel's standard SQL
//! expression contract, then classifies the observed result as follows:
//!
//! - A portable case that matches is a pass. An explicit unsupported result is reported as
//!   unsupported, not as a pass. Any other result is a failure.
//! - An extension-boundary case rejected as invalid enforces the strict Kernel boundary. A value is
//!   reported as an executor extension, and an unsupported result as unsupported. Neither is a
//!   portable pass or a conformance failure because the input is outside Kernel's contract.
//!
//! Harnesses for executors with a configurable session time zone must configure
//! [`CONFORMANCE_SESSION_TIME_ZONE`] before analysis and execution. Its non-UTC offset makes the
//! temporal cases verify that common-type conversion does not depend on that setting. Executors
//! with no session time zone can ignore it.
//!
//! Case order is unspecified. Harnesses should identify cases by the stable `name` on
//! [`ExpressionSemanticsCase`] or [`ProjectAssignmentCase`].
//!
//! [`project_assignment_cases`] separately covers the declared output-schema boundary after
//! expression inference. A complete harness runs both suites.

use std::sync::Arc;

use super::super::{
    ArrayData, BinaryExpressionOp, BinaryPredicateOp, Expression, MapData, Predicate, Scalar,
    StructData,
};
use crate::schema::{schema_ref, ArrayType, DataType, MapType, SchemaRef, StructField, StructType};
use crate::DeltaResult;

/// Non-UTC session time zone required while running the conformance cases.
pub const CONFORMANCE_SESSION_TIME_ZONE: &str = "America/Los_Angeles";

/// The portable result or strict extension-boundary rejection for a conformance case.
#[derive(Clone, Debug, PartialEq)]
pub enum ExpectedResult {
    /// Evaluation succeeds with this scalar value, including a typed [`Scalar::Null`].
    Value(Scalar),
    /// Analysis or evaluation fails with this contract-level error. An extension-boundary case
    /// may instead be accepted with executor-specific behavior.
    Error(ExpectedError),
}

/// Contract-level errors that an executor maps to its own error types and messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedError {
    /// Expression analysis or declared-output assignment fails, including unresolved columns and
    /// invalid arity or types.
    InvalidExpression,
    /// Checked integral or decimal arithmetic, or a temporal conversion, overflows during
    /// evaluation.
    ArithmeticOverflow,
    /// A numeric divisor is positive or negative zero.
    DivideByZero,
}

/// Whether a case has portable behavior or marks the edge of Kernel's contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConformanceRequirement {
    /// The contract specifies this outcome, including required analysis errors.
    Portable,
    /// The expression is outside Kernel's contract. Rejection is expected from a strict executor,
    /// but an executor may accept it as a non-portable extension.
    ExtensionBoundary,
}

/// Type and nullability inferred before assignment to a declared output field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedOutput {
    /// Inferred expression result type.
    pub data_type: DataType,
    /// Whether the inferred result may be null.
    pub nullable: bool,
}

impl ResolvedOutput {
    fn new(data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            data_type: data_type.into(),
            nullable,
        }
    }
}

/// One input row and the expected analysis and evaluation result for an expression.
#[derive(Clone, Debug, PartialEq)]
pub struct ExpressionSemanticsCase {
    /// Stable case name suitable for a test identifier.
    pub name: &'static str,
    /// Schema used to analyze the expression and interpret `input_row`.
    pub input_schema: SchemaRef,
    /// One input row, in `input_schema` field order.
    pub input_row: Vec<Scalar>,
    /// Unresolved expression to analyze and evaluate.
    pub expression: Expression,
    /// Expected analysis result before any surrounding `Project` assignment. Extension-boundary
    /// expressions have no portable resolved output.
    pub output: Option<ResolvedOutput>,
    /// Required value or contract-level error.
    pub expected: ExpectedResult,
    /// Whether acceptance is required to follow the expected portable result.
    pub requirement: ConformanceRequirement,
}

/// One expression and the expected result of assigning it to a declared `Project` output.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectAssignmentCase {
    /// Stable case name suitable for a test identifier.
    pub name: &'static str,
    /// Schema used to analyze the source expression and interpret `input_row`.
    pub input_schema: SchemaRef,
    /// One input row, in `input_schema` field order.
    pub input_row: Vec<Scalar>,
    /// Unresolved expression to analyze before assigning it to `target`.
    pub expression: Expression,
    /// Type and nullability inferred for the source expression before assignment.
    pub source: ResolvedOutput,
    /// Declared output type and nullability.
    pub target: ResolvedOutput,
    /// Required assigned value or contract-level error.
    pub expected: ExpectedResult,
}

/// Cases for common-type resolution, [`COALESCE`](super::super::VariadicExpressionOp::Coalesce),
/// and [`ARRAY`](super::super::VariadicExpressionOp::Array).
///
/// # Errors
///
/// Returns an error if a fixture's schema or array value is internally inconsistent.
pub fn common_type_cases() -> DeltaResult<Vec<ExpressionSemanticsCase>> {
    let nullable_int = StructField::nullable("i", DataType::INTEGER);
    let required_long = StructField::not_null("l", DataType::LONG);
    let int_array = ArrayType::new(DataType::INTEGER, false);
    let long_array = ArrayType::new(DataType::LONG, false);
    let nullable_long_array = ArrayType::new(DataType::LONG, true);
    let int_value_map = MapType::new(DataType::STRING, DataType::INTEGER, false);
    let nullable_long_value_map = MapType::new(DataType::STRING, DataType::LONG, true);
    let empty_void_key_map = MapType::new(DataType::VOID, DataType::STRING, false);
    let int_key_map = MapType::new(DataType::INTEGER, DataType::STRING, false);
    let long_key_map = MapType::new(DataType::LONG, DataType::STRING, false);
    let int_struct_field = StructField::not_null("value", DataType::INTEGER);
    let long_struct_field = StructField::not_null("value", DataType::LONG);
    let long_struct_type = StructType::try_new([long_struct_field.clone()])?;
    let nullable_int_struct_field = StructField::nullable("value", DataType::INTEGER);
    let nullable_long_struct_field = StructField::nullable("value", DataType::LONG);
    let nullable_long_struct_type = StructType::try_new([nullable_long_struct_field.clone()])?;

    Ok(vec![
        ExpressionSemanticsCase {
            name: "coalesce_int_and_long",
            input_schema: Arc::new(StructType::try_new([nullable_int, required_long])?),
            input_row: vec![Scalar::Null(DataType::INTEGER), Scalar::Long(7)],
            expression: Expression::coalesce([
                crate::expressions::col!("i"),
                crate::expressions::col!("l"),
            ]),
            output: Some(ResolvedOutput::new(DataType::LONG, false)),
            expected: ExpectedResult::Value(Scalar::Long(7)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_all_null",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Null(DataType::LONG)),
            ]),
            output: Some(ResolvedOutput::new(DataType::LONG, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::LONG)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_all_void",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(Scalar::Null(DataType::VOID)),
            ]),
            output: Some(ResolvedOutput::new(DataType::VOID, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::VOID)),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "coalesce_void_and_int",
            Scalar::Null(DataType::VOID),
            Scalar::Integer(7),
            DataType::INTEGER,
            false,
            Scalar::Integer(7),
        ),
        coalesce_value_case(
            "coalesce_byte_and_short",
            Scalar::Byte(1),
            Scalar::Short(2),
            DataType::SHORT,
            false,
            Scalar::Short(1),
        ),
        coalesce_value_case(
            "coalesce_short_and_int",
            Scalar::Short(1),
            Scalar::Integer(2),
            DataType::INTEGER,
            false,
            Scalar::Integer(1),
        ),
        coalesce_value_case(
            "coalesce_float_and_double",
            Scalar::Float(1.5),
            Scalar::Double(2.5),
            DataType::DOUBLE,
            false,
            Scalar::Double(1.5),
        ),
        coalesce_value_case(
            "coalesce_decimal_and_float_returns_double",
            decimal(100, 10, 2)?,
            Scalar::Float(2.0),
            DataType::DOUBLE,
            false,
            Scalar::Double(1.0),
        ),
        ExpressionSemanticsCase {
            name: "array_int_and_long",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::array([
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Long(2)),
            ]),
            output: Some(ResolvedOutput::new(long_array.clone(), false)),
            expected: ExpectedResult::Value(array(long_array.clone(), [1i64.into(), 2i64.into()])?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_nullable_element",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::array([
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
            ]),
            output: Some(ResolvedOutput::new(
                ArrayType::new(DataType::INTEGER, true),
                false,
            )),
            expected: ExpectedResult::Value(array(
                ArrayType::new(DataType::INTEGER, true),
                [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
            )?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_int_and_void",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::array([
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Null(DataType::VOID)),
            ]),
            output: Some(ResolvedOutput::new(
                ArrayType::new(DataType::INTEGER, true),
                false,
            )),
            expected: ExpectedResult::Value(array(
                ArrayType::new(DataType::INTEGER, true),
                [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
            )?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_with_column_child",
            input_schema: schema_ref! { not_null "i": INTEGER },
            input_row: vec![Scalar::Integer(7)],
            expression: Expression::array([crate::expressions::col!("i")]),
            output: Some(ResolvedOutput::new(int_array.clone(), false)),
            expected: ExpectedResult::Value(array(int_array.clone(), [Scalar::Integer(7)])?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_empty",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::array([] as [Expression; 0]),
            output: Some(ResolvedOutput::new(
                ArrayType::new(DataType::VOID, false),
                false,
            )),
            expected: ExpectedResult::Value(array(
                ArrayType::new(DataType::VOID, false),
                [] as [Scalar; 0],
            )?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_string_and_int_is_extension_boundary",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::String("1".into())),
                Expression::Literal(Scalar::Integer(2)),
            ]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::ExtensionBoundary,
        },
        ExpressionSemanticsCase {
            name: "empty_coalesce_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([] as [Expression; 0]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_boolean_and_int_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Boolean(true)),
                Expression::Literal(Scalar::Integer(1)),
            ]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_analyzes_unselected_invalid_child",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Integer(2)),
                Expression::binary(
                    BinaryExpressionOp::Plus,
                    Expression::Literal(Scalar::Boolean(true)),
                    Expression::Literal(Scalar::Integer(1)),
                ),
            ]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_common_type_is_recursive",
            input_schema: schema_ref! {
                not_null "ints": (int_array.clone()),
                not_null "longs": (long_array.clone()),
            },
            input_row: vec![
                array(int_array.clone(), [Scalar::Integer(1)])?,
                array(long_array.clone(), [Scalar::Long(2)])?,
            ],
            expression: Expression::coalesce([
                crate::expressions::col!("ints"),
                crate::expressions::col!("longs"),
            ]),
            output: Some(ResolvedOutput::new(long_array.clone(), false)),
            expected: ExpectedResult::Value(array(long_array, [Scalar::Long(1)])?),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "array_common_type_unions_element_nullability",
            array(int_array.clone(), [Scalar::Integer(1)])?,
            array(nullable_long_array.clone(), [Scalar::Null(DataType::LONG)])?,
            nullable_long_array.clone().into(),
            false,
            array(nullable_long_array, [Scalar::Long(1)])?,
        ),
        ExpressionSemanticsCase {
            name: "map_common_type_widens_values_and_nullability",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Map(MapData::try_new(
                    int_value_map,
                    [("key", Scalar::Integer(1))],
                )?)),
                Expression::Literal(Scalar::Map(MapData::try_new(
                    nullable_long_value_map.clone(),
                    [("key", Scalar::Long(2))],
                )?)),
            ]),
            output: Some(ResolvedOutput::new(nullable_long_value_map.clone(), false)),
            expected: ExpectedResult::Value(Scalar::Map(MapData::try_new(
                nullable_long_value_map,
                [("key", Scalar::Long(1))],
            )?)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "map_common_type_widens_keys",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Map(MapData::try_new(
                    int_key_map.clone(),
                    [(Scalar::Integer(1), "first")],
                )?)),
                Expression::Literal(Scalar::Map(MapData::try_new(
                    long_key_map.clone(),
                    [(Scalar::Long(2), "second")],
                )?)),
            ]),
            output: Some(ResolvedOutput::new(long_key_map.clone(), false)),
            expected: ExpectedResult::Value(Scalar::Map(MapData::try_new(
                long_key_map,
                [(Scalar::Long(1), "first")],
            )?)),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "empty_map_void_key_widens_to_concrete_key",
            Scalar::Map(MapData::try_new(
                empty_void_key_map,
                [] as [(Scalar, Scalar); 0],
            )?),
            Scalar::Map(MapData::try_new(
                int_key_map.clone(),
                [(Scalar::Integer(1), "value")],
            )?),
            int_key_map.clone().into(),
            false,
            Scalar::Map(MapData::try_new(int_key_map, [] as [(Scalar, Scalar); 0])?),
        ),
        ExpressionSemanticsCase {
            name: "struct_common_type_is_recursive",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![int_struct_field],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![long_struct_field.clone()],
                    vec![Scalar::Long(2)],
                )?)),
            ]),
            output: Some(ResolvedOutput::new(long_struct_type, false)),
            expected: ExpectedResult::Value(Scalar::Struct(StructData::try_new(
                vec![long_struct_field.clone()],
                vec![Scalar::Long(1)],
            )?)),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "struct_common_type_unions_field_nullability",
            Scalar::Struct(StructData::try_new(
                vec![nullable_int_struct_field],
                vec![Scalar::Integer(1)],
            )?),
            Scalar::Struct(StructData::try_new(
                vec![long_struct_field.clone()],
                vec![Scalar::Long(2)],
            )?),
            nullable_long_struct_type.clone().into(),
            false,
            Scalar::Struct(StructData::try_new(
                vec![nullable_long_struct_field],
                vec![Scalar::Long(1)],
            )?),
        ),
        ExpressionSemanticsCase {
            name: "coalesce_struct_field_name_mismatch_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("left", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("right", DataType::INTEGER)],
                    vec![Scalar::Integer(2)],
                )?)),
            ]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_struct_field_count_mismatch_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![
                        StructField::not_null("value", DataType::INTEGER),
                        StructField::not_null("extra", DataType::INTEGER),
                    ],
                    vec![Scalar::Integer(1), Scalar::Integer(2)],
                )?)),
            ]),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_struct_field_names_match_case_insensitively",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("Value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("value", DataType::LONG)],
                    vec![Scalar::Long(2)],
                )?)),
            ]),
            output: Some(ResolvedOutput::new(
                StructType::try_new([StructField::not_null("Value", DataType::LONG)])?,
                false,
            )),
            expected: ExpectedResult::Value(Scalar::Struct(StructData::try_new(
                vec![StructField::not_null("Value", DataType::LONG)],
                vec![Scalar::Long(1)],
            )?)),
            requirement: ConformanceRequirement::Portable,
        },
        date_to_timestamp_ntz_case(
            "date_and_timestamp_ntz_use_temporal_common_type",
            1,
            ExpectedResult::Value(Scalar::TimestampNtz(86_400_000_000)),
        ),
        date_to_timestamp_ntz_case(
            "date_to_timestamp_ntz_max_safe_day",
            106_751_991,
            ExpectedResult::Value(Scalar::TimestampNtz(9_223_372_022_400_000_000)),
        ),
        date_to_timestamp_ntz_case(
            "date_to_timestamp_ntz_min_safe_day",
            -106_751_991,
            ExpectedResult::Value(Scalar::TimestampNtz(-9_223_372_022_400_000_000)),
        ),
        date_to_timestamp_ntz_case(
            "date_to_timestamp_ntz_positive_overflow",
            106_751_992,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        date_to_timestamp_ntz_case(
            "date_to_timestamp_ntz_negative_overflow",
            -106_751_992,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        date_to_timestamp_case(
            "date_and_timestamp_use_utc_temporal_common_type",
            1,
            ExpectedResult::Value(Scalar::Timestamp(86_400_000_000)),
        ),
        date_to_timestamp_case(
            "date_to_timestamp_max_safe_day",
            106_751_991,
            ExpectedResult::Value(Scalar::Timestamp(9_223_372_022_400_000_000)),
        ),
        date_to_timestamp_case(
            "date_to_timestamp_min_safe_day",
            -106_751_991,
            ExpectedResult::Value(Scalar::Timestamp(-9_223_372_022_400_000_000)),
        ),
        date_to_timestamp_case(
            "date_to_timestamp_positive_overflow",
            106_751_992,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        date_to_timestamp_case(
            "date_to_timestamp_negative_overflow",
            -106_751_992,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        ExpressionSemanticsCase {
            name: "timestamp_ntz_and_timestamp_use_utc",
            input_schema: schema_ref! {
                not_null "ntz": TIMESTAMP_NTZ,
                nullable "ts": TIMESTAMP,
            },
            input_row: vec![
                Scalar::TimestampNtz(86_400_000_000),
                Scalar::Null(DataType::TIMESTAMP),
            ],
            expression: Expression::coalesce([
                crate::expressions::col!("ntz"),
                crate::expressions::col!("ts"),
            ]),
            output: Some(ResolvedOutput::new(DataType::TIMESTAMP, false)),
            expected: ExpectedResult::Value(Scalar::Timestamp(86_400_000_000)),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "coalesce_decimal_and_byte_uses_full_integral_precision",
            decimal(1, 2, 2)?,
            Scalar::Byte(2),
            DataType::decimal(5, 2)?,
            false,
            decimal(1, 5, 2)?,
        ),
        coalesce_value_case(
            "coalesce_decimal_and_short_uses_full_integral_precision",
            decimal(1, 2, 2)?,
            Scalar::Short(2),
            DataType::decimal(7, 2)?,
            false,
            decimal(1, 7, 2)?,
        ),
        ExpressionSemanticsCase {
            name: "coalesce_decimal_and_int_literal_uses_full_integral_precision",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(decimal(1, 2, 2)?),
                Expression::Literal(Scalar::Integer(2)),
            ]),
            output: Some(ResolvedOutput::new(DataType::decimal(12, 2)?, false)),
            expected: ExpectedResult::Value(decimal(1, 12, 2)?),
            requirement: ConformanceRequirement::Portable,
        },
        coalesce_value_case(
            "coalesce_decimal_and_long_uses_full_integral_precision",
            decimal(1, 2, 2)?,
            Scalar::Long(2),
            DataType::decimal(22, 2)?,
            false,
            decimal(1, 22, 2)?,
        ),
        coalesce_value_case(
            "decimal_common_type_uses_maximum_scale_and_integer_digits",
            decimal(125, 5, 2)?,
            decimal(2250, 4, 3)?,
            DataType::decimal(6, 3)?,
            false,
            decimal(1250, 6, 3)?,
        ),
        ExpressionSemanticsCase {
            name: "decimal_common_type_scale_reduction_rounds_half_up",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(decimal(123_456_500_000_000_000_000, 38, 20)?),
                Expression::Literal(decimal(0, 38, 5)?),
            ]),
            output: Some(ResolvedOutput::new(DataType::decimal(38, 5)?, false)),
            expected: ExpectedResult::Value(decimal(123_457, 38, 5)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_common_type_negative_midpoint_rounds_away_from_zero",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(decimal(-123_456_500_000_000_000_000, 38, 20)?),
                Expression::Literal(decimal(0, 38, 5)?),
            ]),
            output: Some(ResolvedOutput::new(DataType::decimal(38, 5)?, false)),
            expected: ExpectedResult::Value(decimal(-123_457, 38, 5)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "coalesce_stops_before_error",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::coalesce([
                Expression::Literal(Scalar::Integer(2)),
                Expression::binary(
                    BinaryExpressionOp::Divide,
                    Expression::Literal(Scalar::Integer(5)),
                    Expression::Literal(Scalar::Integer(0)),
                ),
            ]),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(2.0)),
            requirement: ConformanceRequirement::Portable,
        },
    ])
}

/// Cases for numeric addition, subtraction, multiplication, decimal result types, and division.
///
/// # Errors
///
/// Returns an error if a fixture's schema or decimal value is internally inconsistent.
pub fn arithmetic_cases() -> DeltaResult<Vec<ExpressionSemanticsCase>> {
    let decimal_10_2 = DataType::decimal(10, 2)?;
    let decimal_11_2 = DataType::decimal(11, 2)?;
    let decimal_13_2 = DataType::decimal(13, 2)?;
    let decimal_21_4 = DataType::decimal(21, 4)?;
    let decimal_23_13 = DataType::decimal(23, 13)?;
    let decimal_38_6 = DataType::decimal(38, 6)?;
    let decimal_38_17 = DataType::decimal(38, 17)?;

    Ok(vec![
        arithmetic_case(
            "mixed_int_long_plus",
            [
                (StructField::not_null("i", DataType::INTEGER), 1i32.into()),
                (StructField::not_null("l", DataType::LONG), 2i64.into()),
            ],
            BinaryExpressionOp::Plus,
            DataType::LONG,
            Scalar::Long(3),
        )?,
        arithmetic_case(
            "mixed_long_int_minus",
            [
                (StructField::not_null("l", DataType::LONG), 3i64.into()),
                (StructField::not_null("i", DataType::INTEGER), 1i32.into()),
            ],
            BinaryExpressionOp::Minus,
            DataType::LONG,
            Scalar::Long(2),
        )?,
        arithmetic_case(
            "mixed_int_long_multiply",
            [
                (StructField::not_null("i", DataType::INTEGER), 3i32.into()),
                (StructField::not_null("l", DataType::LONG), 2i64.into()),
            ],
            BinaryExpressionOp::Multiply,
            DataType::LONG,
            Scalar::Long(6),
        )?,
        arithmetic_case(
            "long_and_float_promote_to_double",
            [
                (StructField::not_null("l", DataType::LONG), 2i64.into()),
                (StructField::not_null("f", DataType::FLOAT), 0.5f32.into()),
            ],
            BinaryExpressionOp::Plus,
            DataType::DOUBLE,
            Scalar::Double(2.5),
        )?,
        ExpressionSemanticsCase {
            name: "null_arithmetic_propagates",
            input_schema: schema_ref! {
                nullable "n": INTEGER,
                not_null "one": INTEGER,
            },
            input_row: vec![Scalar::Null(DataType::INTEGER), Scalar::Integer(1)],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                crate::expressions::col!("n"),
                crate::expressions::col!("one"),
            ),
            output: Some(ResolvedOutput::new(DataType::INTEGER, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::INTEGER)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "untyped_null_arithmetic_uses_other_operand_type",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(Scalar::Integer(1)),
            ),
            output: Some(ResolvedOutput::new(DataType::INTEGER, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::INTEGER)),
            requirement: ConformanceRequirement::Portable,
        },
        invalid_arithmetic_case(
            "boolean_arithmetic_is_rejected",
            Scalar::Boolean(true),
            Scalar::Boolean(false),
        ),
        invalid_arithmetic_case(
            "date_arithmetic_is_rejected",
            Scalar::Date(1),
            Scalar::Date(2),
        ),
        invalid_arithmetic_case(
            "year_month_interval_arithmetic_is_rejected",
            Scalar::IntervalYearMonth(1),
            Scalar::IntervalYearMonth(2),
        ),
        invalid_arithmetic_case(
            "day_time_interval_arithmetic_is_rejected",
            Scalar::IntervalDayTime(1),
            Scalar::IntervalDayTime(2),
        ),
        integer_overflow_case("int_plus_overflow", BinaryExpressionOp::Plus, i32::MAX, 1),
        integer_overflow_case("int_minus_overflow", BinaryExpressionOp::Minus, i32::MIN, 1),
        integer_overflow_case(
            "int_multiply_overflow",
            BinaryExpressionOp::Multiply,
            i32::MAX,
            2,
        ),
        ExpressionSemanticsCase {
            name: "double_multiply_overflow_is_infinity",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Multiply,
                Expression::Literal(Scalar::Double(1e308)),
                Expression::Literal(Scalar::Double(1e308)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(f64::INFINITY)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "double_arithmetic_preserves_nan",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Minus,
                Expression::Literal(Scalar::Double(f64::INFINITY)),
                Expression::Literal(Scalar::Double(f64::INFINITY)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(f64::NAN)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_double_returns_double",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::Literal(Scalar::Double(2.5)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(3.5)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_float_returns_double",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::Literal(Scalar::Float(2.5)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(3.5)),
            requirement: ConformanceRequirement::Portable,
        },
        decimal_arithmetic_case(
            "decimal_add_result_type",
            BinaryExpressionOp::Plus,
            (125, 10, 2),
            (225, 10, 2),
            decimal_11_2.clone(),
            decimal(350, 11, 2)?,
        )?,
        decimal_arithmetic_case(
            "decimal_minus_result_type",
            BinaryExpressionOp::Minus,
            (350, 10, 2),
            (125, 10, 2),
            decimal_11_2.clone(),
            decimal(225, 11, 2)?,
        )?,
        decimal_arithmetic_case(
            "decimal_add_rounds_exact_result_once",
            BinaryExpressionOp::Plus,
            (4, 38, 7),
            (4, 38, 7),
            decimal_38_6.clone(),
            decimal(1, 38, 6)?,
        )?,
        decimal_arithmetic_case(
            "decimal_add_negative_rounds_exact_result_once",
            BinaryExpressionOp::Plus,
            (-4, 38, 7),
            (-4, 38, 7),
            decimal_38_6.clone(),
            decimal(-1, 38, 6)?,
        )?,
        decimal_arithmetic_case(
            "decimal_subtract_rounds_exact_result_once",
            BinaryExpressionOp::Minus,
            (4, 38, 7),
            (-4, 38, 7),
            decimal_38_6.clone(),
            decimal(1, 38, 6)?,
        )?,
        decimal_arithmetic_case(
            "decimal_multiply_result_type",
            BinaryExpressionOp::Multiply,
            (150, 10, 2),
            (200, 10, 2),
            decimal_21_4,
            decimal(30_000, 21, 4)?,
        )?,
        decimal_arithmetic_case(
            "decimal_scale_reduction_rounds_half_up",
            BinaryExpressionOp::Multiply,
            (123_456_750_000_000_000_000, 38, 20),
            (100_000_000_000_000_000_000, 38, 20),
            decimal_38_6.clone(),
            decimal(1_234_568, 38, 6)?,
        )?,
        decimal_arithmetic_case(
            "decimal_scale_reduction_negative_midpoint_rounds_away_from_zero",
            BinaryExpressionOp::Multiply,
            (-123_456_750_000_000_000_000, 38, 20),
            (100_000_000_000_000_000_000, 38, 20),
            decimal_38_6,
            decimal(-1_234_568, 38, 6)?,
        )?,
        decimal_arithmetic_case(
            "decimal_adjusted_scale_preserves_available_fractional_digits",
            BinaryExpressionOp::Multiply,
            (10_000_000_000, 20, 10),
            (10_000_000_000, 20, 10),
            decimal_38_17,
            decimal(100_000_000_000_000_000, 38, 17)?,
        )?,
        ExpressionSemanticsCase {
            name: "decimal_add_overflow",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(
                    99_999_999_999_999_999_999_999_999_999_999_999_999,
                    38,
                    0,
                )?),
                Expression::Literal(decimal(1, 38, 0)?),
            ),
            output: Some(ResolvedOutput::new(DataType::decimal(38, 0)?, false)),
            expected: ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_int_column",
            input_schema: Arc::new(StructType::try_new([
                StructField::not_null("d", decimal_10_2.clone()),
                StructField::not_null("i", DataType::INTEGER),
            ])?),
            input_row: vec![decimal(100, 10, 2)?, Scalar::Integer(1)],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                crate::expressions::col!("d"),
                crate::expressions::col!("i"),
            ),
            output: Some(ResolvedOutput::new(decimal_13_2, false)),
            expected: ExpectedResult::Value(decimal(200, 13, 2)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_minimum_precision_int_literal",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::Literal(Scalar::Integer(1)),
            ),
            output: Some(ResolvedOutput::new(decimal_11_2.clone(), false)),
            expected: ExpectedResult::Value(decimal(200, 11, 2)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_zero_literal_uses_one_digit_precision",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::Literal(Scalar::Integer(0)),
            ),
            output: Some(ResolvedOutput::new(decimal_11_2.clone(), false)),
            expected: ExpectedResult::Value(decimal(100, 11, 2)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "decimal_plus_non_literal_int_uses_full_precision",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::binary(
                    BinaryExpressionOp::Plus,
                    Expression::Literal(Scalar::Integer(1)),
                    Expression::Literal(Scalar::Integer(1)),
                ),
            ),
            output: Some(ResolvedOutput::new(DataType::decimal(13, 2)?, false)),
            expected: ExpectedResult::Value(decimal(300, 13, 2)?),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "integer_division_is_fractional",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Integer(7)),
                Expression::Literal(Scalar::Integer(2)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(3.5)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "float_division_returns_double",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Float(7.0)),
                Expression::Literal(Scalar::Float(2.0)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(3.5)),
            requirement: ConformanceRequirement::Portable,
        },
        decimal_arithmetic_case(
            "decimal_division_result_type",
            BinaryExpressionOp::Divide,
            (300, 10, 2),
            (200, 10, 2),
            decimal_23_13.clone(),
            decimal(15_000_000_000_000, 23, 13)?,
        )?,
        decimal_arithmetic_case(
            "decimal_division_rounds_half_up",
            BinaryExpressionOp::Divide,
            (1, 1, 0),
            (128, 3, 0),
            DataType::decimal(7, 6)?,
            decimal(7_813, 7, 6)?,
        )?,
        decimal_arithmetic_case(
            "negative_decimal_division_rounds_away_from_zero",
            BinaryExpressionOp::Divide,
            (-1, 1, 0),
            (128, 3, 0),
            DataType::decimal(7, 6)?,
            decimal(-7_813, 7, 6)?,
        )?,
        ExpressionSemanticsCase {
            name: "decimal_divided_by_double_returns_double",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(decimal(300, 10, 2)?),
                Expression::Literal(Scalar::Double(2.0)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, false)),
            expected: ExpectedResult::Value(Scalar::Double(1.5)),
            requirement: ConformanceRequirement::Portable,
        },
        divide_by_zero_case(
            "integer_divide_by_zero",
            Scalar::Integer(1),
            Scalar::Integer(0),
            DataType::DOUBLE,
        ),
        divide_by_zero_case(
            "decimal_divide_by_zero",
            decimal(100, 10, 2)?,
            decimal(0, 10, 2)?,
            DataType::decimal(23, 13)?,
        ),
        divide_by_zero_case(
            "double_divide_by_positive_zero",
            Scalar::Double(1.0),
            Scalar::Double(0.0),
            DataType::DOUBLE,
        ),
        divide_by_zero_case(
            "double_divide_by_negative_zero",
            Scalar::Double(1.0),
            Scalar::Double(-0.0),
            DataType::DOUBLE,
        ),
        ExpressionSemanticsCase {
            name: "null_dividend_propagates",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Integer(2)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::DOUBLE)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "null_divisor_propagates",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Integer(2)),
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::DOUBLE)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "null_dividend_wins_over_zero_divisor",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Integer(0)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::DOUBLE)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "untyped_null_dividend_wins_over_zero_divisor",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(Scalar::Integer(0)),
            ),
            output: Some(ResolvedOutput::new(DataType::DOUBLE, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::DOUBLE)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "untyped_null_division_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(Scalar::Null(DataType::VOID)),
            ),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "untyped_null_divided_by_decimal",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(decimal(200, 10, 2)?),
            ),
            output: Some(ResolvedOutput::new(decimal_23_13.clone(), true)),
            expected: ExpectedResult::Value(Scalar::Null(decimal_23_13)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "string_plus_int_is_extension_boundary",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(Scalar::String("1".into())),
                Expression::Literal(Scalar::Integer(2)),
            ),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::ExtensionBoundary,
        },
    ])
}

/// Cases for comparison coercion, nulls, distinctness, NaNs, and signed zero.
///
/// # Errors
///
/// Returns an error if a fixture's schema or array value is internally inconsistent.
pub fn comparison_cases() -> DeltaResult<Vec<ExpressionSemanticsCase>> {
    let double_array = ArrayType::new(DataType::DOUBLE, false);
    let int_struct = StructType::try_new([StructField::not_null("value", DataType::INTEGER)])?;
    let long_struct = StructType::try_new([StructField::not_null("value", DataType::LONG)])?;
    let int_struct_array = ArrayType::new(int_struct.clone(), false);
    let long_struct_array = ArrayType::new(long_struct.clone(), false);
    let map_type = MapType::new(DataType::STRING, DataType::INTEGER, false);
    let map = Scalar::Map(MapData::try_new(map_type.clone(), [("key", 1i32)])?);
    let decimal_38_30 = DataType::decimal(38, 30)?;
    let alternate_nan = f64::from_bits(0x7ff8_0000_0000_0001);

    Ok(vec![
        ExpressionSemanticsCase {
            name: "comparison_uses_numeric_common_type",
            input_schema: schema_ref! {
                not_null "i": INTEGER,
                not_null "l": LONG,
            },
            input_row: vec![Scalar::Integer(1), Scalar::Long(1)],
            expression: Expression::from_pred(Predicate::eq(
                crate::expressions::col!("i"),
                crate::expressions::col!("l"),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        predicate_value_case(
            "date_and_timestamp_compare_in_utc",
            Predicate::eq(
                Expression::Literal(Scalar::Date(1)),
                Expression::Literal(Scalar::Timestamp(86_400_000_000)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "decimal_comparison_uses_minimum_integral_literal_precision",
            Predicate::eq(
                Expression::Literal(decimal(1_000_000_000_000_000_000_000_000_000_001, 38, 30)?),
                Expression::Literal(Scalar::Integer(1)),
            ),
            false,
            Some(false),
        ),
        ExpressionSemanticsCase {
            name: "decimal_comparison_uses_full_integral_column_precision",
            input_schema: schema_ref! {
                not_null "d": (decimal_38_30),
                not_null "i": INTEGER,
            },
            input_row: vec![
                decimal(1_000_000_000_000_000_000_000_000_000_001, 38, 30)?,
                Scalar::Integer(1),
            ],
            expression: Expression::from_pred(Predicate::eq(
                crate::expressions::col!("d"),
                crate::expressions::col!("i"),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        predicate_value_case(
            "null_equals_null_is_null",
            Predicate::eq(
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
            ),
            true,
            None,
        ),
        predicate_value_case(
            "null_less_than_value_is_null",
            Predicate::lt(
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Integer(1)),
            ),
            true,
            None,
        ),
        ExpressionSemanticsCase {
            name: "comparison_string_and_int_is_extension_boundary",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::String("1".into())),
                Expression::Literal(Scalar::Integer(1)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::ExtensionBoundary,
        },
        ExpressionSemanticsCase {
            name: "boolean_and_int_comparison_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::Boolean(true)),
                Expression::Literal(Scalar::Integer(1)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "array_and_struct_comparison_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, false),
                    [Scalar::Integer(1)],
                )?),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "arrays_with_no_common_type_are_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(array(
                    ArrayType::new(DataType::BOOLEAN, false),
                    [Scalar::Boolean(true)],
                )?),
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, false),
                    [Scalar::Integer(1)],
                )?),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "structs_with_different_field_counts_are_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("a", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![
                        StructField::not_null("a", DataType::INTEGER),
                        StructField::not_null("b", DataType::INTEGER),
                    ],
                    vec![Scalar::Integer(1), Scalar::Integer(2)],
                )?)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "structs_containing_maps_are_not_comparable",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("m", map_type.clone())],
                    vec![map.clone()],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("m", map_type.clone())],
                    vec![map.clone()],
                )?)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        predicate_value_case(
            "untyped_null_comparison_adopts_other_operand_type",
            Predicate::eq(
                Expression::Literal(Scalar::Null(DataType::VOID)),
                Expression::Literal(Scalar::String("value".into())),
            ),
            true,
            None,
        ),
        ExpressionSemanticsCase {
            name: "map_comparison_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(map.clone()),
                Expression::Literal(map),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "interval_cross_family_comparison_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::lt(
                Expression::Literal(Scalar::IntervalYearMonth(1)),
                Expression::Literal(Scalar::IntervalDayTime(1)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "differently_named_structs_requiring_leaf_widening_are_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("left", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("right", DataType::LONG)],
                    vec![Scalar::Long(1)],
                )?)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "comparison_ignores_struct_field_names_after_positional_alignment",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("left", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("right", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        predicate_value_case(
            "comparison_aligns_struct_field_nullability",
            Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nested_null_struct_fields_compare_equal",
            Predicate::eq(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("value", DataType::INTEGER)],
                    vec![Scalar::Null(DataType::INTEGER)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("value", DataType::INTEGER)],
                    vec![Scalar::Null(DataType::INTEGER)],
                )?)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nested_null_struct_field_sorts_first",
            Predicate::lt(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("value", DataType::INTEGER)],
                    vec![Scalar::Null(DataType::INTEGER)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("value", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "string_comparison_uses_binary_ordering",
            Predicate::lt(
                Expression::Literal(Scalar::String("A".into())),
                Expression::Literal(Scalar::String("a".into())),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "string_comparison_does_not_normalize_unicode",
            Predicate::lt(
                Expression::Literal(Scalar::String("e\u{301}".into())),
                Expression::Literal(Scalar::String("\u{e9}".into())),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "binary_comparison_uses_unsigned_lexicographic_order",
            Predicate::lt(
                Expression::Literal(Scalar::Binary(vec![0x7f])),
                Expression::Literal(Scalar::Binary(vec![0x80])),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "false_sorts_before_true",
            Predicate::lt(
                Expression::Literal(Scalar::Boolean(false)),
                Expression::Literal(Scalar::Boolean(true)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "year_month_intervals_use_signed_ordering",
            Predicate::lt(
                Expression::Literal(Scalar::IntervalYearMonth(-1)),
                Expression::Literal(Scalar::IntervalYearMonth(1)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "day_time_intervals_use_signed_ordering",
            Predicate::lt(
                Expression::Literal(Scalar::IntervalDayTime(-1)),
                Expression::Literal(Scalar::IntervalDayTime(1)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nested_null_array_elements_compare_equal",
            Predicate::eq(
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, true),
                    [Scalar::Null(DataType::INTEGER)],
                )?),
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, true),
                    [Scalar::Null(DataType::INTEGER)],
                )?),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nested_null_array_element_sorts_first",
            Predicate::lt(
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, true),
                    [Scalar::Null(DataType::INTEGER)],
                )?),
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, true),
                    [Scalar::Integer(1)],
                )?),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "arrays_compare_lexicographically",
            Predicate::lt(
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, false),
                    [Scalar::Integer(1), Scalar::Integer(2)],
                )?),
                Expression::Literal(array(
                    ArrayType::new(DataType::INTEGER, false),
                    [Scalar::Integer(1), Scalar::Integer(3)],
                )?),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "array_comparison_coerces_nested_struct_fields",
            Predicate::eq(
                Expression::Literal(array(
                    int_struct_array,
                    [Scalar::Struct(StructData::try_new(
                        int_struct.fields().cloned().collect(),
                        vec![Scalar::Integer(1)],
                    )?)],
                )?),
                Expression::Literal(array(
                    long_struct_array,
                    [Scalar::Struct(StructData::try_new(
                        long_struct.fields().cloned().collect(),
                        vec![Scalar::Long(1)],
                    )?)],
                )?),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "structs_compare_in_field_order",
            Predicate::lt(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![
                        StructField::not_null("first", DataType::INTEGER),
                        StructField::not_null("second", DataType::INTEGER),
                    ],
                    vec![Scalar::Integer(1), Scalar::Integer(2)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![
                        StructField::not_null("first", DataType::INTEGER),
                        StructField::not_null("second", DataType::INTEGER),
                    ],
                    vec![Scalar::Integer(1), Scalar::Integer(3)],
                )?)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "different_nan_payloads_compare_equal",
            Predicate::eq(
                Expression::Literal(Scalar::Double(f64::NAN)),
                Expression::Literal(Scalar::Double(alternate_nan)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nan_sorts_after_finite_value",
            Predicate::gt(
                Expression::Literal(Scalar::Double(f64::NAN)),
                Expression::Literal(Scalar::Double(1.0)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "finite_value_sorts_before_nan",
            Predicate::lt(
                Expression::Literal(Scalar::Double(1.0)),
                Expression::Literal(Scalar::Double(f64::NAN)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "signed_zeroes_compare_equal",
            Predicate::eq(
                Expression::Literal(Scalar::Double(-0.0)),
                Expression::Literal(Scalar::Double(0.0)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "negative_zero_is_not_less_than_positive_zero",
            Predicate::lt(
                Expression::Literal(Scalar::Double(-0.0)),
                Expression::Literal(Scalar::Double(0.0)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "negative_zero_is_not_greater_than_positive_zero",
            Predicate::gt(
                Expression::Literal(Scalar::Double(-0.0)),
                Expression::Literal(Scalar::Double(0.0)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "array_comparison_uses_nan_equality",
            Predicate::eq(
                Expression::Literal(array(double_array.clone(), [Scalar::Double(f64::NAN)])?),
                Expression::Literal(array(double_array, [Scalar::Double(alternate_nan)])?),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "equal_values_are_not_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Integer(1)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "distinct_ignores_struct_field_names_after_positional_alignment",
            Predicate::distinct(
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("left", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
                Expression::Literal(Scalar::Struct(StructData::try_new(
                    vec![StructField::not_null("right", DataType::INTEGER)],
                    vec![Scalar::Integer(1)],
                )?)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "unequal_values_are_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Integer(2)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "two_nulls_are_not_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "null_and_value_are_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Null(DataType::INTEGER)),
                Expression::Literal(Scalar::Integer(1)),
            ),
            false,
            Some(true),
        ),
        predicate_value_case(
            "nan_values_are_not_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Double(f64::NAN)),
                Expression::Literal(Scalar::Double(alternate_nan)),
            ),
            false,
            Some(false),
        ),
        predicate_value_case(
            "signed_zeroes_are_not_distinct",
            Predicate::distinct(
                Expression::Literal(Scalar::Double(-0.0)),
                Expression::Literal(Scalar::Double(0.0)),
            ),
            false,
            Some(false),
        ),
    ])
}

/// Cases for `IN` over array-valued expressions.
///
/// # Errors
///
/// Returns an error if a fixture's schema or array value is internally inconsistent.
pub fn in_cases() -> DeltaResult<Vec<ExpressionSemanticsCase>> {
    let int_array = ArrayType::new(DataType::INTEGER, false);
    let nullable_int_array = ArrayType::new(DataType::INTEGER, true);
    let boolean_array = ArrayType::new(DataType::BOOLEAN, false);
    let long_array = ArrayType::new(DataType::LONG, false);
    let float_array = ArrayType::new(DataType::FLOAT, false);
    let double_array = ArrayType::new(DataType::DOUBLE, false);
    let decimal_38_30_array = ArrayType::new(DataType::decimal(38, 30)?, false);
    let map_type = MapType::new(DataType::STRING, DataType::INTEGER, false);
    let map_array = ArrayType::new(map_type.clone(), false);
    let map = Scalar::Map(MapData::try_new(map_type, [("key", Scalar::Integer(1))])?);
    let right_struct_field = StructField::not_null("right", DataType::INTEGER);
    let right_struct_array =
        ArrayType::new(StructType::try_new([right_struct_field.clone()])?, false);
    let lowercase_long_struct_field = StructField::not_null("value", DataType::LONG);
    let lowercase_long_struct_array = ArrayType::new(
        StructType::try_new([lowercase_long_struct_field.clone()])?,
        false,
    );
    let int_array_struct =
        StructType::try_new([StructField::not_null("values", int_array.clone())])?;
    let long_array_struct =
        StructType::try_new([StructField::not_null("values", long_array.clone())])?;
    let long_array_struct_array = ArrayType::new(long_array_struct.clone(), false);

    Ok(vec![
        literal_in_case(
            "literal_in_matches_without_null",
            Scalar::Integer(1),
            int_array.clone(),
            [Scalar::Integer(1), Scalar::Integer(2)],
            false,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_match_wins_over_null_element",
            Scalar::Integer(1),
            nullable_int_array.clone(),
            [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
            true,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_no_match_with_null_element_is_null",
            Scalar::Integer(1),
            nullable_int_array.clone(),
            [Scalar::Integer(2), Scalar::Null(DataType::INTEGER)],
            true,
            None,
        )?,
        literal_in_case(
            "literal_in_declared_nullable_elements_is_nullable_without_present_null",
            Scalar::Integer(1),
            nullable_int_array.clone(),
            [Scalar::Integer(2), Scalar::Integer(3)],
            true,
            Some(false),
        )?,
        literal_in_case(
            "literal_in_null_left_operand_is_null",
            Scalar::Null(DataType::INTEGER),
            int_array.clone(),
            [Scalar::Integer(1), Scalar::Integer(2)],
            true,
            None,
        )?,
        literal_in_case(
            "literal_in_no_match_without_null_is_false",
            Scalar::Integer(1),
            int_array.clone(),
            [Scalar::Integer(2), Scalar::Integer(3)],
            false,
            Some(false),
        )?,
        literal_in_case(
            "literal_in_empty_array_is_false",
            Scalar::Integer(1),
            int_array.clone(),
            [] as [Scalar; 0],
            false,
            Some(false),
        )?,
        literal_in_case(
            "literal_in_empty_declared_nullable_array_is_nullable",
            Scalar::Integer(1),
            nullable_int_array.clone(),
            [] as [Scalar; 0],
            true,
            Some(false),
        )?,
        ExpressionSemanticsCase {
            name: "literal_in_empty_array_still_checks_declared_element_type",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(array(boolean_array, [] as [Scalar; 0])?),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        literal_in_case(
            "literal_in_empty_array_with_null_left_operand_is_false",
            Scalar::Null(DataType::INTEGER),
            int_array.clone(),
            [] as [Scalar; 0],
            true,
            Some(false),
        )?,
        literal_in_case(
            "literal_in_empty_array_with_untyped_null_left_operand_is_false",
            Scalar::Null(DataType::VOID),
            int_array.clone(),
            [] as [Scalar; 0],
            true,
            Some(false),
        )?,
        ExpressionSemanticsCase {
            name: "empty_in_array_does_not_suppress_left_child_error",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::binary(
                    BinaryExpressionOp::Divide,
                    Expression::Literal(Scalar::Integer(1)),
                    Expression::Literal(Scalar::Integer(0)),
                ),
                Expression::Literal(array(double_array.clone(), [] as [Scalar; 0])?),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Error(ExpectedError::DivideByZero),
            requirement: ConformanceRequirement::Portable,
        },
        literal_not_in_case(
            "literal_not_in_empty_array_with_null_left_operand_is_true",
            Scalar::Null(DataType::INTEGER),
            int_array.clone(),
            [] as [Scalar; 0],
            true,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_uses_numeric_common_type",
            Scalar::Integer(1),
            long_array.clone(),
            [Scalar::Long(1)],
            false,
            Some(true),
        )?,
        ExpressionSemanticsCase {
            name: "literal_in_string_and_int_is_extension_boundary",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::Literal(Scalar::String("1".into())),
                Expression::Literal(array(int_array.clone(), [Scalar::Integer(1)])?),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::ExtensionBoundary,
        },
        literal_in_case(
            "literal_in_uses_nan_equality",
            Scalar::Double(f64::NAN),
            double_array.clone(),
            [Scalar::Double(f64::NAN)],
            false,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_uses_signed_zero_equality",
            Scalar::Double(-0.0),
            double_array.clone(),
            [Scalar::Double(0.0)],
            false,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_ignores_struct_field_names_after_positional_alignment",
            Scalar::Struct(StructData::try_new(
                vec![StructField::not_null("left", DataType::INTEGER)],
                vec![Scalar::Integer(1)],
            )?),
            right_struct_array.clone(),
            [Scalar::Struct(StructData::try_new(
                vec![right_struct_field.clone()],
                vec![Scalar::Integer(1)],
            )?)],
            false,
            Some(true),
        )?,
        literal_in_case(
            "literal_in_coerces_nested_array_elements",
            Scalar::Struct(StructData::try_new(
                int_array_struct.fields().cloned().collect(),
                vec![array(int_array.clone(), [Scalar::Integer(1)])?],
            )?),
            long_array_struct_array.clone(),
            [Scalar::Struct(StructData::try_new(
                long_array_struct.fields().cloned().collect(),
                vec![array(long_array.clone(), [Scalar::Long(1)])?],
            )?)],
            false,
            Some(true),
        )?,
        literal_not_in_case(
            "literal_not_in_preserves_null",
            Scalar::Integer(1),
            nullable_int_array.clone(),
            [Scalar::Integer(2), Scalar::Null(DataType::INTEGER)],
            true,
            None,
        )?,
        column_in_case(
            "column_in_matches_without_null",
            Scalar::Integer(1),
            StructField::not_null("items", int_array.clone()),
            array(int_array.clone(), [Scalar::Integer(1), Scalar::Integer(2)])?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_match_wins_over_null_element",
            Scalar::Integer(1),
            StructField::not_null("items", nullable_int_array.clone()),
            array(
                nullable_int_array.clone(),
                [Scalar::Integer(1), Scalar::Null(DataType::INTEGER)],
            )?,
            true,
            Some(true),
        )?,
        column_in_case(
            "column_in_no_match_with_null_element_is_null",
            Scalar::Integer(1),
            StructField::not_null("items", nullable_int_array.clone()),
            array(
                nullable_int_array.clone(),
                [Scalar::Integer(2), Scalar::Null(DataType::INTEGER)],
            )?,
            true,
            None,
        )?,
        column_in_case(
            "column_in_null_left_operand_is_null",
            Scalar::Null(DataType::INTEGER),
            StructField::not_null("items", int_array.clone()),
            array(int_array.clone(), [Scalar::Integer(1), Scalar::Integer(2)])?,
            true,
            None,
        )?,
        column_in_case(
            "column_in_no_match_without_null_is_false",
            Scalar::Integer(1),
            StructField::not_null("items", int_array.clone()),
            array(int_array.clone(), [Scalar::Integer(2), Scalar::Integer(3)])?,
            false,
            Some(false),
        )?,
        column_in_case(
            "column_in_empty_list_is_false",
            Scalar::Integer(1),
            StructField::not_null("items", int_array.clone()),
            array(int_array.clone(), [] as [Scalar; 0])?,
            false,
            Some(false),
        )?,
        column_in_case(
            "column_in_empty_list_with_null_left_operand_is_false",
            Scalar::Null(DataType::INTEGER),
            StructField::not_null("items", int_array.clone()),
            array(int_array.clone(), [] as [Scalar; 0])?,
            true,
            Some(false),
        )?,
        column_in_case(
            "column_in_null_collection_is_null",
            Scalar::Integer(1),
            StructField::nullable("items", int_array.clone()),
            Scalar::Null(int_array.clone().into()),
            true,
            None,
        )?,
        column_in_case(
            "column_in_uses_numeric_common_type",
            Scalar::Integer(1),
            StructField::not_null("items", long_array.clone()),
            array(long_array, [Scalar::Long(1)])?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_integral_and_float_promote_to_double",
            Scalar::Long(16_777_217),
            StructField::not_null("items", float_array.clone()),
            array(float_array, [Scalar::Float(16_777_216.0)])?,
            false,
            Some(false),
        )?,
        column_in_case(
            "column_in_decimal_uses_minimum_integral_literal_precision",
            Scalar::Integer(1),
            StructField::not_null("items", decimal_38_30_array.clone()),
            array(
                decimal_38_30_array,
                [decimal(1_000_000_000_000_000_000_000_000_000_001, 38, 30)?],
            )?,
            false,
            Some(false),
        )?,
        column_in_case(
            "column_in_aligns_structs_with_different_field_nullability",
            Scalar::Struct(StructData::try_new(
                vec![StructField::nullable("left", DataType::INTEGER)],
                vec![Scalar::Integer(1)],
            )?),
            StructField::not_null("items", right_struct_array.clone()),
            array(
                right_struct_array.clone(),
                [Scalar::Struct(StructData::try_new(
                    vec![right_struct_field.clone()],
                    vec![Scalar::Integer(1)],
                )?)],
            )?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_case_insensitive_struct_names_allow_leaf_widening",
            Scalar::Struct(StructData::try_new(
                vec![StructField::not_null("Value", DataType::INTEGER)],
                vec![Scalar::Integer(1)],
            )?),
            StructField::not_null("items", lowercase_long_struct_array.clone()),
            array(
                lowercase_long_struct_array,
                [Scalar::Struct(StructData::try_new(
                    vec![lowercase_long_struct_field],
                    vec![Scalar::Long(1)],
                )?)],
            )?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_ignores_struct_field_names_after_positional_alignment",
            Scalar::Struct(StructData::try_new(
                vec![StructField::not_null("left", DataType::INTEGER)],
                vec![Scalar::Integer(1)],
            )?),
            StructField::not_null("items", right_struct_array.clone()),
            array(
                right_struct_array,
                [Scalar::Struct(StructData::try_new(
                    vec![right_struct_field],
                    vec![Scalar::Integer(1)],
                )?)],
            )?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_uses_nan_equality",
            Scalar::Double(f64::NAN),
            StructField::not_null("items", double_array.clone()),
            array(double_array.clone(), [Scalar::Double(f64::NAN)])?,
            false,
            Some(true),
        )?,
        column_in_case(
            "column_in_uses_signed_zero_equality",
            Scalar::Double(-0.0),
            StructField::not_null("items", double_array.clone()),
            array(double_array, [Scalar::Double(0.0)])?,
            false,
            Some(true),
        )?,
        ExpressionSemanticsCase {
            name: "in_column_left_operand_matches",
            input_schema: schema_ref! { not_null "value": INTEGER },
            input_row: vec![Scalar::Integer(1)],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                crate::expressions::col!("value"),
                Expression::Literal(array(int_array.clone(), [Scalar::Integer(1)])?),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "in_array_expression_rhs_matches",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::Literal(Scalar::Integer(1)),
                Expression::array([Expression::Literal(Scalar::Integer(1))]),
            )),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "in_scalar_rhs_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::Literal(Scalar::Integer(1)),
                Expression::Literal(Scalar::Integer(1)),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "map_membership_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::from_pred(Predicate::binary(
                BinaryPredicateOp::In,
                Expression::Literal(map.clone()),
                Expression::Literal(array(map_array, [map])?),
            )),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
    ])
}

/// Cases for Boolean input validation, three-valued logic, and `IS NULL`.
pub fn boolean_cases() -> Vec<ExpressionSemanticsCase> {
    vec![
        ExpressionSemanticsCase {
            name: "non_null_boolean_expression_preserves_value",
            input_schema: schema_ref! { not_null "b": BOOLEAN },
            input_row: vec![Scalar::Boolean(true)],
            expression: Expression::Predicate(Box::new(Predicate::BooleanExpression(
                crate::expressions::col!("b"),
            ))),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, false)),
            expected: ExpectedResult::Value(Scalar::Boolean(true)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "nullable_boolean_expression_preserves_null",
            input_schema: schema_ref! { nullable "b": BOOLEAN },
            input_row: vec![Scalar::Null(DataType::BOOLEAN)],
            expression: Expression::Predicate(Box::new(Predicate::BooleanExpression(
                crate::expressions::col!("b"),
            ))),
            output: Some(ResolvedOutput::new(DataType::BOOLEAN, true)),
            expected: ExpectedResult::Value(Scalar::Null(DataType::BOOLEAN)),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "integer_boolean_expression_is_rejected",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::Predicate(Box::new(Predicate::BooleanExpression(
                Expression::Literal(Scalar::Integer(1)),
            ))),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::Portable,
        },
        ExpressionSemanticsCase {
            name: "string_boolean_expression_is_extension_boundary",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::Predicate(Box::new(Predicate::BooleanExpression(
                Expression::Literal(Scalar::String("true".into())),
            ))),
            output: None,
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
            requirement: ConformanceRequirement::ExtensionBoundary,
        },
        predicate_value_case(
            "not_true_is_false",
            Predicate::not(Predicate::TRUE),
            false,
            Some(false),
        ),
        predicate_value_case(
            "not_false_is_true",
            Predicate::not(Predicate::FALSE),
            false,
            Some(true),
        ),
        predicate_value_case(
            "not_null_is_null",
            Predicate::not(Predicate::NULL),
            true,
            None,
        ),
        predicate_value_case(
            "null_is_null_is_true",
            Predicate::is_null(Expression::Literal(Scalar::Null(DataType::INTEGER))),
            false,
            Some(true),
        ),
        predicate_value_case(
            "value_is_null_is_false",
            Predicate::is_null(Expression::Literal(Scalar::Integer(1))),
            false,
            Some(false),
        ),
        predicate_value_case(
            "true_and_null_is_null",
            Predicate::and(Predicate::TRUE, Predicate::NULL),
            true,
            None,
        ),
        predicate_value_case(
            "false_and_null_is_false",
            Predicate::and(Predicate::FALSE, Predicate::NULL),
            true,
            Some(false),
        ),
        predicate_value_case(
            "null_and_false_is_false",
            Predicate::and(Predicate::NULL, Predicate::FALSE),
            true,
            Some(false),
        ),
        predicate_value_case(
            "null_and_null_is_null",
            Predicate::and(Predicate::NULL, Predicate::NULL),
            true,
            None,
        ),
        predicate_value_case(
            "true_or_null_is_true",
            Predicate::or(Predicate::TRUE, Predicate::NULL),
            true,
            Some(true),
        ),
        predicate_value_case(
            "false_or_null_is_null",
            Predicate::or(Predicate::FALSE, Predicate::NULL),
            true,
            None,
        ),
        predicate_value_case(
            "null_or_true_is_true",
            Predicate::or(Predicate::NULL, Predicate::TRUE),
            true,
            Some(true),
        ),
        predicate_value_case(
            "null_or_null_is_null",
            Predicate::or(Predicate::NULL, Predicate::NULL),
            true,
            None,
        ),
        predicate_value_case(
            "empty_and_is_true",
            Predicate::and_from([] as [Predicate; 0]),
            false,
            Some(true),
        ),
        predicate_value_case(
            "empty_or_is_false",
            Predicate::or_from([] as [Predicate; 0]),
            false,
            Some(false),
        ),
        predicate_value_case(
            "true_and_false_is_false",
            Predicate::and(Predicate::TRUE, Predicate::FALSE),
            false,
            Some(false),
        ),
        predicate_value_case(
            "true_and_true_is_true",
            Predicate::and(Predicate::TRUE, Predicate::TRUE),
            false,
            Some(true),
        ),
        predicate_value_case(
            "false_or_true_is_true",
            Predicate::or(Predicate::FALSE, Predicate::TRUE),
            false,
            Some(true),
        ),
        predicate_value_case(
            "false_or_false_is_false",
            Predicate::or(Predicate::FALSE, Predicate::FALSE),
            false,
            Some(false),
        ),
    ]
}

/// Cases for assignment from an inferred expression result to a declared `Project` field.
///
/// These cases are separate from [`all_cases`] because they test the schema boundary after
/// expression analysis rather than operator inference itself.
///
/// # Errors
///
/// Returns an error if a fixture's decimal or complex value is internally inconsistent.
pub fn project_assignment_cases() -> DeltaResult<Vec<ProjectAssignmentCase>> {
    let int_array = ArrayType::new(DataType::INTEGER, false);
    let long_array = ArrayType::new(DataType::LONG, false);
    let nullable_long_array = ArrayType::new(DataType::LONG, true);
    let nullable_int_array = ArrayType::new(DataType::INTEGER, true);
    let required_int_array = ArrayType::new(DataType::INTEGER, false);
    let decimal_10_2_array = ArrayType::new(DataType::decimal(10, 2)?, false);
    let decimal_12_4_array = ArrayType::new(DataType::decimal(12, 4)?, false);
    let decimal_10_1_array = ArrayType::new(DataType::decimal(10, 1)?, false);

    let int_value_map = MapType::new(DataType::STRING, DataType::INTEGER, false);
    let long_value_map = MapType::new(DataType::STRING, DataType::LONG, false);
    let int_key_map = MapType::new(DataType::INTEGER, DataType::STRING, false);
    let long_key_map = MapType::new(DataType::LONG, DataType::STRING, false);
    let nullable_int_value_map = MapType::new(DataType::STRING, DataType::INTEGER, true);
    let required_int_value_map = MapType::new(DataType::STRING, DataType::INTEGER, false);

    let required_int_struct =
        StructType::try_new([StructField::not_null("value", DataType::INTEGER)])?;
    let nullable_int_struct =
        StructType::try_new([StructField::nullable("value", DataType::INTEGER)])?;
    let nested_int_struct =
        StructType::try_new([StructField::not_null("source", int_array.clone())])?;
    let nested_long_struct =
        StructType::try_new([StructField::nullable("target", nullable_long_array.clone())])?;
    let differently_named_int_struct =
        StructType::try_new([StructField::not_null("other", DataType::INTEGER)])?;

    let empty_void_array = ArrayType::new(DataType::VOID, false);
    let void_array = ArrayType::new(DataType::VOID, true);
    let empty_void_key_map = MapType::new(DataType::VOID, DataType::STRING, false);
    let empty_void_value_map = MapType::new(DataType::STRING, DataType::VOID, false);
    let void_value_map = MapType::new(DataType::STRING, DataType::VOID, true);
    let void_struct = StructType::try_new([StructField::nullable("value", DataType::VOID)])?;

    Ok(vec![
        project_assignment_case(
            "project_exact_type",
            Scalar::Integer(1),
            false,
            DataType::INTEGER,
            false,
            ExpectedResult::Value(Scalar::Integer(1)),
        ),
        project_assignment_case(
            "project_required_to_nullable",
            Scalar::Integer(1),
            false,
            DataType::INTEGER,
            true,
            ExpectedResult::Value(Scalar::Integer(1)),
        ),
        project_assignment_case(
            "project_nullable_to_nullable",
            Scalar::Null(DataType::INTEGER),
            true,
            DataType::INTEGER,
            true,
            ExpectedResult::Value(Scalar::Null(DataType::INTEGER)),
        ),
        ProjectAssignmentCase {
            name: "project_target_does_not_change_integer_division_inference",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Divide,
                Expression::Literal(Scalar::Integer(3)),
                Expression::Literal(Scalar::Integer(2)),
            ),
            source: ResolvedOutput::new(DataType::DOUBLE, false),
            target: ResolvedOutput::new(DataType::LONG, false),
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
        },
        ProjectAssignmentCase {
            name: "project_target_does_not_change_decimal_arithmetic_inference",
            input_schema: schema_ref! {},
            input_row: vec![],
            expression: Expression::binary(
                BinaryExpressionOp::Plus,
                Expression::Literal(decimal(100, 10, 2)?),
                Expression::Literal(decimal(200, 10, 2)?),
            ),
            source: ResolvedOutput::new(DataType::decimal(11, 2)?, false),
            target: ResolvedOutput::new(DataType::decimal(10, 2)?, false),
            expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
        },
        project_assignment_case(
            "project_byte_to_short",
            Scalar::Byte(1),
            false,
            DataType::SHORT,
            false,
            ExpectedResult::Value(Scalar::Short(1)),
        ),
        project_assignment_case(
            "project_byte_to_int",
            Scalar::Byte(1),
            false,
            DataType::INTEGER,
            false,
            ExpectedResult::Value(Scalar::Integer(1)),
        ),
        project_assignment_case(
            "project_byte_to_long",
            Scalar::Byte(1),
            false,
            DataType::LONG,
            false,
            ExpectedResult::Value(Scalar::Long(1)),
        ),
        project_assignment_case(
            "project_byte_to_float",
            Scalar::Byte(1),
            false,
            DataType::FLOAT,
            false,
            ExpectedResult::Value(Scalar::Float(1.0)),
        ),
        project_assignment_case(
            "project_byte_to_double",
            Scalar::Byte(1),
            false,
            DataType::DOUBLE,
            false,
            ExpectedResult::Value(Scalar::Double(1.0)),
        ),
        project_assignment_case(
            "project_short_to_int",
            Scalar::Short(1),
            false,
            DataType::INTEGER,
            false,
            ExpectedResult::Value(Scalar::Integer(1)),
        ),
        project_assignment_case(
            "project_short_to_long",
            Scalar::Short(1),
            false,
            DataType::LONG,
            false,
            ExpectedResult::Value(Scalar::Long(1)),
        ),
        project_assignment_case(
            "project_short_to_float",
            Scalar::Short(1),
            false,
            DataType::FLOAT,
            false,
            ExpectedResult::Value(Scalar::Float(1.0)),
        ),
        project_assignment_case(
            "project_short_to_double",
            Scalar::Short(1),
            false,
            DataType::DOUBLE,
            false,
            ExpectedResult::Value(Scalar::Double(1.0)),
        ),
        project_assignment_case(
            "project_int_to_long",
            Scalar::Integer(1),
            false,
            DataType::LONG,
            false,
            ExpectedResult::Value(Scalar::Long(1)),
        ),
        project_assignment_case(
            "project_int_to_double",
            Scalar::Integer(1),
            false,
            DataType::DOUBLE,
            false,
            ExpectedResult::Value(Scalar::Double(1.0)),
        ),
        project_assignment_case(
            "project_float_to_double",
            Scalar::Float(1.5),
            false,
            DataType::DOUBLE,
            false,
            ExpectedResult::Value(Scalar::Double(1.5)),
        ),
        project_assignment_case(
            "project_date_to_timestamp_ntz",
            Scalar::Date(1),
            false,
            DataType::TIMESTAMP_NTZ,
            false,
            ExpectedResult::Value(Scalar::TimestampNtz(86_400_000_000)),
        ),
        project_assignment_case(
            "project_date_to_timestamp_ntz_max_safe_day",
            Scalar::Date(106_751_991),
            false,
            DataType::TIMESTAMP_NTZ,
            false,
            ExpectedResult::Value(Scalar::TimestampNtz(9_223_372_022_400_000_000)),
        ),
        project_assignment_case(
            "project_date_to_timestamp_ntz_min_safe_day",
            Scalar::Date(-106_751_991),
            false,
            DataType::TIMESTAMP_NTZ,
            false,
            ExpectedResult::Value(Scalar::TimestampNtz(-9_223_372_022_400_000_000)),
        ),
        project_assignment_case(
            "project_date_to_timestamp_ntz_positive_overflow",
            Scalar::Date(106_751_992),
            false,
            DataType::TIMESTAMP_NTZ,
            false,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        project_assignment_case(
            "project_date_to_timestamp_ntz_negative_overflow",
            Scalar::Date(-106_751_992),
            false,
            DataType::TIMESTAMP_NTZ,
            false,
            ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        ),
        project_assignment_case(
            "project_void_to_nullable",
            Scalar::Null(DataType::VOID),
            true,
            DataType::INTEGER,
            true,
            ExpectedResult::Value(Scalar::Null(DataType::INTEGER)),
        ),
        project_assignment_case(
            "project_int_to_decimal",
            Scalar::Integer(1),
            false,
            DataType::decimal(10, 0)?,
            false,
            ExpectedResult::Value(decimal(1, 10, 0)?),
        ),
        project_assignment_case(
            "project_decimal_to_wider_decimal",
            decimal(123, 10, 2)?,
            false,
            DataType::decimal(12, 4)?,
            false,
            ExpectedResult::Value(decimal(12_300, 12, 4)?),
        ),
        project_assignment_case(
            "project_array_decimal_assignment_is_recursive",
            array(decimal_10_2_array.clone(), [decimal(123, 10, 2)?])?,
            false,
            decimal_12_4_array.clone(),
            false,
            ExpectedResult::Value(array(decimal_12_4_array, [decimal(12_300, 12, 4)?])?),
        ),
        project_assignment_case(
            "project_array_element_widens",
            array(int_array.clone(), [Scalar::Integer(1)])?,
            false,
            long_array.clone(),
            false,
            ExpectedResult::Value(array(long_array, [Scalar::Long(1)])?),
        ),
        project_assignment_case(
            "project_map_value_widens",
            Scalar::Map(MapData::try_new(
                int_value_map.clone(),
                [("key", Scalar::Integer(1))],
            )?),
            false,
            long_value_map.clone(),
            false,
            ExpectedResult::Value(Scalar::Map(MapData::try_new(
                long_value_map,
                [("key", Scalar::Long(1))],
            )?)),
        ),
        project_assignment_case(
            "project_map_key_widens",
            Scalar::Map(MapData::try_new(
                int_key_map.clone(),
                [(Scalar::Integer(1), "value")],
            )?),
            false,
            long_key_map.clone(),
            false,
            ExpectedResult::Value(Scalar::Map(MapData::try_new(
                long_key_map,
                [(Scalar::Long(1), "value")],
            )?)),
        ),
        project_assignment_case(
            "project_struct_assignment_is_recursive_and_positional",
            Scalar::Struct(StructData::try_new(
                nested_int_struct.fields().cloned().collect(),
                vec![array(int_array.clone(), [Scalar::Integer(1)])?],
            )?),
            false,
            nested_long_struct.clone(),
            false,
            ExpectedResult::Value(Scalar::Struct(StructData::try_new(
                nested_long_struct.fields().cloned().collect(),
                vec![array(nullable_long_array, [Scalar::Long(1)])?],
            )?)),
        ),
        project_assignment_case(
            "project_empty_void_array_element_to_required_target",
            array(empty_void_array, [] as [Scalar; 0])?,
            false,
            required_int_array.clone(),
            false,
            ExpectedResult::Value(array(required_int_array.clone(), [] as [Scalar; 0])?),
        ),
        project_assignment_case(
            "project_empty_void_map_value_to_required_target",
            Scalar::Map(MapData::try_new(
                empty_void_value_map,
                [] as [(Scalar, Scalar); 0],
            )?),
            false,
            required_int_value_map.clone(),
            false,
            ExpectedResult::Value(Scalar::Map(MapData::try_new(
                required_int_value_map.clone(),
                [] as [(Scalar, Scalar); 0],
            )?)),
        ),
        project_assignment_case(
            "project_empty_void_map_key_to_concrete_target",
            Scalar::Map(MapData::try_new(
                empty_void_key_map,
                [] as [(Scalar, Scalar); 0],
            )?),
            false,
            int_key_map.clone(),
            false,
            ExpectedResult::Value(Scalar::Map(MapData::try_new(
                int_key_map.clone(),
                [] as [(Scalar, Scalar); 0],
            )?)),
        ),
        project_assignment_case(
            "project_void_array_element_to_nullable_target",
            array(void_array, [Scalar::Null(DataType::VOID)])?,
            false,
            nullable_int_array.clone(),
            false,
            ExpectedResult::Value(array(
                nullable_int_array.clone(),
                [Scalar::Null(DataType::INTEGER)],
            )?),
        ),
        project_assignment_case(
            "project_void_map_value_to_nullable_target",
            Scalar::Map(MapData::try_new(
                void_value_map,
                [("key", Scalar::Null(DataType::VOID))],
            )?),
            false,
            nullable_int_value_map.clone(),
            false,
            ExpectedResult::Value(Scalar::Map(MapData::try_new(
                nullable_int_value_map.clone(),
                [("key", Scalar::Null(DataType::INTEGER))],
            )?)),
        ),
        project_assignment_case(
            "project_void_struct_field_to_nullable_target",
            Scalar::Struct(StructData::try_new(
                void_struct.fields().cloned().collect(),
                vec![Scalar::Null(DataType::VOID)],
            )?),
            false,
            nullable_int_struct.clone(),
            false,
            ExpectedResult::Value(Scalar::Struct(StructData::try_new(
                nullable_int_struct.fields().cloned().collect(),
                vec![Scalar::Null(DataType::INTEGER)],
            )?)),
        ),
        rejected_project_assignment_case(
            "project_void_to_required_is_rejected",
            Scalar::Null(DataType::VOID),
            true,
            DataType::INTEGER,
            false,
        ),
        rejected_project_assignment_case(
            "project_nullable_to_required_is_rejected",
            Scalar::Integer(1),
            true,
            DataType::INTEGER,
            false,
        ),
        rejected_project_assignment_case(
            "project_narrowing_is_rejected",
            Scalar::Long(1),
            false,
            DataType::INTEGER,
            false,
        ),
        rejected_project_assignment_case(
            "project_int_to_float_is_rejected",
            Scalar::Integer(1),
            false,
            DataType::FLOAT,
            false,
        ),
        rejected_project_assignment_case(
            "project_long_to_double_is_rejected",
            Scalar::Long(1),
            false,
            DataType::DOUBLE,
            false,
        ),
        rejected_project_assignment_case(
            "project_date_to_timestamp_is_rejected",
            Scalar::Date(1),
            false,
            DataType::TIMESTAMP,
            false,
        ),
        rejected_project_assignment_case(
            "project_timestamp_ntz_to_timestamp_is_rejected",
            Scalar::TimestampNtz(1),
            false,
            DataType::TIMESTAMP,
            false,
        ),
        rejected_project_assignment_case(
            "project_string_parsing_is_rejected",
            Scalar::String("1".into()),
            false,
            DataType::INTEGER,
            false,
        ),
        rejected_project_assignment_case(
            "project_decimal_rounding_is_rejected",
            decimal(123, 10, 2)?,
            false,
            DataType::decimal(10, 1)?,
            false,
        ),
        rejected_project_assignment_case(
            "project_int_to_insufficient_decimal_is_rejected",
            Scalar::Integer(1),
            false,
            DataType::decimal(9, 0)?,
            false,
        ),
        rejected_project_assignment_case(
            "project_decimal_integer_loss_is_rejected",
            decimal(123, 10, 2)?,
            false,
            DataType::decimal(9, 2)?,
            false,
        ),
        rejected_project_assignment_case(
            "project_array_decimal_rounding_is_rejected",
            array(decimal_10_2_array, [decimal(123, 10, 2)?])?,
            false,
            decimal_10_1_array,
            false,
        ),
        rejected_project_assignment_case(
            "project_nullable_array_element_to_required_is_rejected",
            array(nullable_int_array, [Scalar::Integer(1)])?,
            false,
            required_int_array,
            false,
        ),
        rejected_project_assignment_case(
            "project_nullable_map_value_to_required_is_rejected",
            Scalar::Map(MapData::try_new(
                nullable_int_value_map,
                [("key", Scalar::Integer(1))],
            )?),
            false,
            required_int_value_map,
            false,
        ),
        rejected_project_assignment_case(
            "project_nullable_struct_field_to_required_is_rejected",
            Scalar::Struct(StructData::try_new(
                nullable_int_struct.fields().cloned().collect(),
                vec![Scalar::Integer(1)],
            )?),
            false,
            required_int_struct.clone(),
            false,
        ),
        project_assignment_case(
            "project_struct_fields_align_positionally",
            Scalar::Struct(StructData::try_new(
                required_int_struct.fields().cloned().collect(),
                vec![Scalar::Integer(1)],
            )?),
            false,
            differently_named_int_struct.clone(),
            false,
            ExpectedResult::Value(Scalar::Struct(StructData::try_new(
                differently_named_int_struct.fields().cloned().collect(),
                vec![Scalar::Integer(1)],
            )?)),
        ),
    ])
}

/// All standard-expression cases published by this module, in unspecified order.
///
/// # Errors
///
/// Returns an error if any fixture is internally inconsistent.
pub fn all_cases() -> DeltaResult<Vec<ExpressionSemanticsCase>> {
    let mut cases = common_type_cases()?;
    cases.extend(arithmetic_cases()?);
    cases.extend(comparison_cases()?);
    cases.extend(in_cases()?);
    cases.extend(boolean_cases());
    Ok(cases)
}

fn date_to_timestamp_ntz_case(
    name: &'static str,
    days: i32,
    expected: ExpectedResult,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {
            not_null "d": DATE,
            nullable "ts": TIMESTAMP_NTZ,
        },
        input_row: vec![Scalar::Date(days), Scalar::Null(DataType::TIMESTAMP_NTZ)],
        expression: Expression::coalesce([
            crate::expressions::col!("d"),
            crate::expressions::col!("ts"),
        ]),
        output: Some(ResolvedOutput::new(DataType::TIMESTAMP_NTZ, false)),
        expected,
        requirement: ConformanceRequirement::Portable,
    }
}

fn date_to_timestamp_case(
    name: &'static str,
    days: i32,
    expected: ExpectedResult,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {
            not_null "d": DATE,
            nullable "ts": TIMESTAMP,
        },
        input_row: vec![Scalar::Date(days), Scalar::Null(DataType::TIMESTAMP)],
        expression: Expression::coalesce([
            crate::expressions::col!("d"),
            crate::expressions::col!("ts"),
        ]),
        output: Some(ResolvedOutput::new(DataType::TIMESTAMP, false)),
        expected,
        requirement: ConformanceRequirement::Portable,
    }
}

fn project_assignment_case(
    name: &'static str,
    source_value: Scalar,
    source_nullable: bool,
    target_type: impl Into<DataType>,
    target_nullable: bool,
    expected: ExpectedResult,
) -> ProjectAssignmentCase {
    let source_type = source_value.data_type();
    ProjectAssignmentCase {
        name,
        input_schema: Arc::new(StructType::new_unchecked([StructField::new(
            "input",
            source_type.clone(),
            source_nullable,
        )])),
        input_row: vec![source_value],
        expression: Expression::column(["input"]),
        source: ResolvedOutput::new(source_type, source_nullable),
        target: ResolvedOutput::new(target_type, target_nullable),
        expected,
    }
}

fn rejected_project_assignment_case(
    name: &'static str,
    source_value: Scalar,
    source_nullable: bool,
    target_type: impl Into<DataType>,
    target_nullable: bool,
) -> ProjectAssignmentCase {
    project_assignment_case(
        name,
        source_value,
        source_nullable,
        target_type,
        target_nullable,
        ExpectedResult::Error(ExpectedError::InvalidExpression),
    )
}

fn literal_in_case(
    name: &'static str,
    value: Scalar,
    array_type: ArrayType,
    elements: impl IntoIterator<Item = Scalar>,
    output_nullable: bool,
    expected: Option<bool>,
) -> DeltaResult<ExpressionSemanticsCase> {
    Ok(predicate_value_case(
        name,
        Predicate::binary(
            BinaryPredicateOp::In,
            Expression::Literal(value),
            Expression::Literal(array(array_type, elements)?),
        ),
        output_nullable,
        expected,
    ))
}

fn literal_not_in_case(
    name: &'static str,
    value: Scalar,
    array_type: ArrayType,
    elements: impl IntoIterator<Item = Scalar>,
    output_nullable: bool,
    expected: Option<bool>,
) -> DeltaResult<ExpressionSemanticsCase> {
    Ok(predicate_value_case(
        name,
        Predicate::not(Predicate::binary(
            BinaryPredicateOp::In,
            Expression::Literal(value),
            Expression::Literal(array(array_type, elements)?),
        )),
        output_nullable,
        expected,
    ))
}

fn column_in_case(
    name: &'static str,
    value: Scalar,
    collection_field: StructField,
    collection: Scalar,
    output_nullable: bool,
    expected: Option<bool>,
) -> DeltaResult<ExpressionSemanticsCase> {
    Ok(ExpressionSemanticsCase {
        name,
        input_schema: Arc::new(StructType::try_new([collection_field])?),
        input_row: vec![collection],
        expression: Expression::from_pred(Predicate::binary(
            BinaryPredicateOp::In,
            Expression::Literal(value),
            crate::expressions::col!("items"),
        )),
        output: Some(ResolvedOutput::new(DataType::BOOLEAN, output_nullable)),
        expected: ExpectedResult::Value(match expected {
            Some(value) => Scalar::Boolean(value),
            None => Scalar::Null(DataType::BOOLEAN),
        }),
        requirement: ConformanceRequirement::Portable,
    })
}

fn predicate_value_case(
    name: &'static str,
    predicate: Predicate,
    output_nullable: bool,
    expected: Option<bool>,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::from_pred(predicate),
        output: Some(ResolvedOutput::new(DataType::BOOLEAN, output_nullable)),
        expected: ExpectedResult::Value(match expected {
            Some(value) => Scalar::Boolean(value),
            None => Scalar::Null(DataType::BOOLEAN),
        }),
        requirement: ConformanceRequirement::Portable,
    }
}

fn coalesce_value_case(
    name: &'static str,
    left: Scalar,
    right: Scalar,
    output_type: DataType,
    output_nullable: bool,
    expected: Scalar,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::coalesce([Expression::Literal(left), Expression::Literal(right)]),
        output: Some(ResolvedOutput::new(output_type, output_nullable)),
        expected: ExpectedResult::Value(expected),
        requirement: ConformanceRequirement::Portable,
    }
}

fn arithmetic_case(
    name: &'static str,
    fields_and_values: [(StructField, Scalar); 2],
    op: BinaryExpressionOp,
    output_type: DataType,
    expected: Scalar,
) -> DeltaResult<ExpressionSemanticsCase> {
    let [(left_field, left_value), (right_field, right_value)] = fields_and_values;
    let left = Expression::column([left_field.name()]);
    let right = Expression::column([right_field.name()]);
    Ok(ExpressionSemanticsCase {
        name,
        input_schema: Arc::new(StructType::try_new([left_field, right_field])?),
        input_row: vec![left_value, right_value],
        expression: Expression::binary(op, left, right),
        output: Some(ResolvedOutput::new(output_type, false)),
        expected: ExpectedResult::Value(expected),
        requirement: ConformanceRequirement::Portable,
    })
}

fn invalid_arithmetic_case(
    name: &'static str,
    left: Scalar,
    right: Scalar,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::binary(
            BinaryExpressionOp::Plus,
            Expression::Literal(left),
            Expression::Literal(right),
        ),
        output: None,
        expected: ExpectedResult::Error(ExpectedError::InvalidExpression),
        requirement: ConformanceRequirement::Portable,
    }
}

fn integer_overflow_case(
    name: &'static str,
    op: BinaryExpressionOp,
    left: i32,
    right: i32,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::binary(
            op,
            Expression::Literal(Scalar::Integer(left)),
            Expression::Literal(Scalar::Integer(right)),
        ),
        output: Some(ResolvedOutput::new(DataType::INTEGER, false)),
        expected: ExpectedResult::Error(ExpectedError::ArithmeticOverflow),
        requirement: ConformanceRequirement::Portable,
    }
}

fn decimal_arithmetic_case(
    name: &'static str,
    op: BinaryExpressionOp,
    left: (i128, u8, u8),
    right: (i128, u8, u8),
    output_type: DataType,
    expected: Scalar,
) -> DeltaResult<ExpressionSemanticsCase> {
    Ok(ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::binary(
            op,
            Expression::Literal(decimal(left.0, left.1, left.2)?),
            Expression::Literal(decimal(right.0, right.1, right.2)?),
        ),
        output: Some(ResolvedOutput::new(output_type, false)),
        expected: ExpectedResult::Value(expected),
        requirement: ConformanceRequirement::Portable,
    })
}

fn divide_by_zero_case(
    name: &'static str,
    numerator: Scalar,
    denominator: Scalar,
    output_type: DataType,
) -> ExpressionSemanticsCase {
    ExpressionSemanticsCase {
        name,
        input_schema: schema_ref! {},
        input_row: vec![],
        expression: Expression::binary(
            BinaryExpressionOp::Divide,
            Expression::Literal(numerator),
            Expression::Literal(denominator),
        ),
        output: Some(ResolvedOutput::new(output_type, false)),
        expected: ExpectedResult::Error(ExpectedError::DivideByZero),
        requirement: ConformanceRequirement::Portable,
    }
}

fn decimal(bits: i128, precision: u8, scale: u8) -> DeltaResult<Scalar> {
    Scalar::decimal(bits, precision, scale)
}

fn array(array_type: ArrayType, elements: impl IntoIterator<Item = Scalar>) -> DeltaResult<Scalar> {
    Ok(Scalar::Array(ArrayData::try_new(array_type, elements)?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn all_cases_are_well_formed() {
        let mut names = HashSet::new();
        for case in all_cases().unwrap() {
            assert!(
                names.insert(case.name),
                "duplicate case name: {}",
                case.name
            );
            assert_eq!(case.input_schema.num_fields(), case.input_row.len());

            for (field, value) in case.input_schema.fields().zip(&case.input_row) {
                assert_eq!(field.data_type(), &value.data_type(), "case: {}", case.name);
                assert!(
                    field.is_nullable() || !value.is_null(),
                    "case: {}",
                    case.name
                );
            }

            if let ExpectedResult::Value(value) = &case.expected {
                let output = case.output.as_ref().expect("value case must resolve");
                assert_eq!(output.data_type, value.data_type(), "case: {}", case.name);
                assert!(!value.is_null() || output.nullable, "case: {}", case.name);
            }

            match (case.requirement, &case.expected, &case.output) {
                (
                    ConformanceRequirement::ExtensionBoundary,
                    ExpectedResult::Error(ExpectedError::InvalidExpression),
                    None,
                ) => {}
                (ConformanceRequirement::Portable, ExpectedResult::Value(_), Some(_)) => {}
                (
                    ConformanceRequirement::Portable,
                    ExpectedResult::Error(ExpectedError::InvalidExpression),
                    None,
                ) => {}
                (
                    ConformanceRequirement::Portable,
                    ExpectedResult::Error(
                        ExpectedError::ArithmeticOverflow | ExpectedError::DivideByZero,
                    ),
                    Some(_),
                ) => {}
                state => panic!("inconsistent expected state for {}: {state:?}", case.name),
            }
        }
    }

    #[test]
    fn boolean_expression_cases_preserve_predicate_context() {
        let cases = boolean_cases();
        for name in [
            "non_null_boolean_expression_preserves_value",
            "nullable_boolean_expression_preserves_null",
            "integer_boolean_expression_is_rejected",
            "string_boolean_expression_is_extension_boundary",
        ] {
            let case = cases
                .iter()
                .find(|case| case.name == name)
                .expect("Boolean expression case");
            assert!(matches!(
                &case.expression,
                Expression::Predicate(predicate)
                    if matches!(predicate.as_ref(), Predicate::BooleanExpression(_))
            ));
        }
    }

    #[test]
    fn project_assignment_cases_are_well_formed() {
        let mut names: HashSet<_> = all_cases()
            .unwrap()
            .into_iter()
            .map(|case| case.name)
            .collect();

        for case in project_assignment_cases().unwrap() {
            assert!(
                names.insert(case.name),
                "duplicate case name: {}",
                case.name
            );
            assert_eq!(case.input_schema.num_fields(), case.input_row.len());

            for (field, value) in case.input_schema.fields().zip(&case.input_row) {
                assert_eq!(field.data_type(), &value.data_type(), "case: {}", case.name);
                assert!(
                    field.is_nullable() || !value.is_null(),
                    "case: {}",
                    case.name
                );
            }

            match &case.expected {
                ExpectedResult::Value(value) => {
                    assert_eq!(
                        case.target.data_type,
                        value.data_type(),
                        "case: {}",
                        case.name
                    );
                    assert!(
                        !value.is_null() || case.target.nullable,
                        "case: {}",
                        case.name
                    );
                }
                ExpectedResult::Error(
                    ExpectedError::InvalidExpression | ExpectedError::ArithmeticOverflow,
                ) => {}
                error => panic!("invalid assignment result for {}: {error:?}", case.name),
            }
        }
    }
}
