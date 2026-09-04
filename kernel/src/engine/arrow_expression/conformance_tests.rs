//! Runs the shared expression fixtures against the Arrow evaluation handler.
//!
//! The shared module owns the executor-independent inputs and expected results. This module adapts
//! each case to Arrow data, invokes `EvaluationHandler`, and compares the returned value or error.
//! Unsupported, divergent, and untestable cases are listed explicitly so fixture additions cannot
//! be skipped silently. `EvaluationHandler` accepts a caller-resolved output type and exposes no
//! analysis result, so this harness verifies eventual values and errors but cannot verify inferred
//! types, nullability, or when an invalid expression is rejected. See #3210.

use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use super::{ArrowEvaluationHandler, StructArray};
use crate::arrow::array::{ArrayRef, AsArray};
use crate::arrow::error::ArrowError;
use crate::engine::arrow_data::extract_record_batch;
use crate::expressions::semantics::conformance::{
    all_cases, project_assignment_cases, ConformanceRequirement, ExpectedError, ExpectedResult,
};
use crate::expressions::{Expression, Predicate, Scalar};
use crate::schema::{schema_ref, DataType, StructField, StructType};
use crate::{EngineData, Error, EvaluationHandler};

// These cases need expression analysis without a caller-provided output type. EvaluationHandler
// has no such entry point, so its Arrow implementation cannot execute them directly.
const UNTESTABLE_ANALYSIS_CASES: &[&str] = &[
    "coalesce_string_and_int_is_extension_boundary",
    "empty_coalesce_is_rejected",
    "coalesce_boolean_and_int_is_rejected",
    "coalesce_analyzes_unselected_invalid_child",
    "coalesce_struct_field_name_mismatch_is_rejected",
    "coalesce_struct_field_count_mismatch_is_rejected",
    "string_plus_int_is_extension_boundary",
    "untyped_null_division_is_rejected",
    "boolean_arithmetic_is_rejected",
    "date_arithmetic_is_rejected",
    "year_month_interval_arithmetic_is_rejected",
    "day_time_interval_arithmetic_is_rejected",
];

// Portable cases that require functionality the Arrow evaluator does not provide. Each case still
// runs and must return `Error::Unsupported`; returning a value or another error fails the harness.
const UNSUPPORTED_STANDARD_CASES: &[&str] = &[
    // Common-type conversion.
    "coalesce_int_and_long",
    "coalesce_all_null",
    "coalesce_void_and_int",
    "coalesce_byte_and_short",
    "coalesce_short_and_int",
    "coalesce_float_and_double",
    "coalesce_decimal_and_float_returns_double",
    "array_int_and_long",
    "array_int_and_void",
    "array_common_type_is_recursive",
    "array_common_type_unions_element_nullability",
    "map_common_type_widens_values_and_nullability",
    "map_common_type_widens_keys",
    "empty_map_void_key_widens_to_concrete_key",
    "struct_common_type_is_recursive",
    "struct_common_type_unions_field_nullability",
    "coalesce_struct_field_names_match_case_insensitively",
    "date_and_timestamp_ntz_use_temporal_common_type",
    "date_to_timestamp_ntz_max_safe_day",
    "date_to_timestamp_ntz_min_safe_day",
    "date_to_timestamp_ntz_positive_overflow",
    "date_to_timestamp_ntz_negative_overflow",
    "date_and_timestamp_use_utc_temporal_common_type",
    "date_to_timestamp_max_safe_day",
    "date_to_timestamp_min_safe_day",
    "date_to_timestamp_positive_overflow",
    "date_to_timestamp_negative_overflow",
    "timestamp_ntz_and_timestamp_use_utc",
    "coalesce_decimal_and_byte_uses_full_integral_precision",
    "coalesce_decimal_and_short_uses_full_integral_precision",
    "coalesce_decimal_and_int_literal_uses_full_integral_precision",
    "coalesce_decimal_and_long_uses_full_integral_precision",
    "decimal_common_type_uses_maximum_scale_and_integer_digits",
    "decimal_common_type_scale_reduction_rounds_half_up",
    "decimal_common_type_negative_midpoint_rounds_away_from_zero",
    "coalesce_stops_before_error",
    // Arithmetic coercion and decimal result adjustment.
    "decimal_add_rounds_exact_result_once",
    "decimal_add_negative_rounds_exact_result_once",
    "decimal_subtract_rounds_exact_result_once",
    "decimal_scale_reduction_rounds_half_up",
    "decimal_scale_reduction_negative_midpoint_rounds_away_from_zero",
    "decimal_adjusted_scale_preserves_available_fractional_digits",
    "decimal_plus_int_column",
    "decimal_plus_minimum_precision_int_literal",
    "decimal_plus_zero_literal_uses_one_digit_precision",
    "decimal_plus_non_literal_int_uses_full_precision",
    "decimal_division_result_type",
    "decimal_division_rounds_half_up",
    "negative_decimal_division_rounds_away_from_zero",
    // Common-type and nested comparison.
    "date_and_timestamp_compare_in_utc",
    "decimal_comparison_uses_minimum_integral_literal_precision",
    "decimal_comparison_uses_full_integral_column_precision",
    "comparison_ignores_struct_field_names_after_positional_alignment",
    "comparison_aligns_struct_field_nullability",
    "nested_null_struct_fields_compare_equal",
    "nested_null_struct_field_sorts_first",
    "nested_null_array_elements_compare_equal",
    "nested_null_array_element_sorts_first",
    "arrays_compare_lexicographically",
    "array_comparison_coerces_nested_struct_fields",
    "structs_compare_in_field_order",
    "array_comparison_uses_nan_equality",
    "distinct_ignores_struct_field_names_after_positional_alignment",
    // IN common-type and nested comparison.
    "literal_in_uses_numeric_common_type",
    "literal_in_ignores_struct_field_names_after_positional_alignment",
    "literal_in_coerces_nested_array_elements",
    "column_in_uses_numeric_common_type",
    "column_in_integral_and_float_promote_to_double",
    "column_in_decimal_uses_minimum_integral_literal_precision",
    "column_in_aligns_structs_with_different_field_nullability",
    "column_in_case_insensitive_struct_names_allow_leaf_widening",
    "column_in_ignores_struct_field_names_after_positional_alignment",
];

// Portable cases that execute but return the wrong value or error category. The harness requires
// the mismatch to remain visible and fails when a fixed case is not removed from this inventory.
const STANDARD_DIVERGENCES: &[&str] = &["interval_cross_family_comparison_is_rejected"];

// Valid Project assignments that the Arrow evaluator cannot perform. Each case must return
// `Error::Unsupported`, rather than being rejected as an invalid assignment.
const UNSUPPORTED_PROJECT_CASES: &[&str] = &[
    "project_byte_to_short",
    "project_byte_to_int",
    "project_byte_to_long",
    "project_byte_to_float",
    "project_byte_to_double",
    "project_short_to_int",
    "project_short_to_long",
    "project_short_to_float",
    "project_short_to_double",
    "project_int_to_long",
    "project_int_to_double",
    "project_float_to_double",
    "project_date_to_timestamp_ntz",
    "project_date_to_timestamp_ntz_max_safe_day",
    "project_date_to_timestamp_ntz_min_safe_day",
    "project_date_to_timestamp_ntz_positive_overflow",
    "project_date_to_timestamp_ntz_negative_overflow",
    "project_void_to_nullable",
    "project_int_to_decimal",
    "project_decimal_to_wider_decimal",
    "project_array_decimal_assignment_is_recursive",
    "project_array_element_widens",
    "project_map_value_widens",
    "project_map_key_widens",
    "project_struct_assignment_is_recursive_and_positional",
    "project_empty_void_array_element_to_required_target",
    "project_empty_void_map_value_to_required_target",
    "project_empty_void_map_key_to_concrete_target",
    "project_void_array_element_to_nullable_target",
    "project_void_map_value_to_nullable_target",
    "project_void_struct_field_to_nullable_target",
    "project_struct_fields_align_positionally",
];

// Project cases that execute but differ from the contract. Input-schema nullability is not
// flow-sensitive, so the evaluator cannot distinguish an unsafe assignment from a projection
// reached after a filter has established a non-null value.
const PROJECT_DIVERGENCES: &[&str] = &[
    "project_nullable_to_required_is_rejected",
    "project_target_does_not_change_decimal_arithmetic_inference",
];

#[test]
fn standard_cases_match_arrow_support_inventory() {
    let unsupported: HashSet<_> = UNSUPPORTED_STANDARD_CASES.iter().copied().collect();
    let divergences: HashSet<_> = STANDARD_DIVERGENCES.iter().copied().collect();
    let analysis_gaps: HashSet<_> = UNTESTABLE_ANALYSIS_CASES.iter().copied().collect();
    assert_eq!(unsupported.len(), UNSUPPORTED_STANDARD_CASES.len());
    assert_eq!(divergences.len(), STANDARD_DIVERGENCES.len());
    assert_eq!(analysis_gaps.len(), UNTESTABLE_ANALYSIS_CASES.len());

    let mut seen_unsupported = HashSet::new();
    let mut seen_divergences = HashSet::new();
    let mut seen_analysis_gaps = HashSet::new();
    let mut failures = Vec::new();
    for case in all_cases().unwrap() {
        let expectation = if analysis_gaps.contains(case.name) {
            seen_analysis_gaps.insert(case.name);
            CaseExpectation::AnalysisApiGap
        } else if case.requirement == ConformanceRequirement::ExtensionBoundary {
            CaseExpectation::ExtensionBoundary
        } else if unsupported.contains(case.name) {
            seen_unsupported.insert(case.name);
            CaseExpectation::Unsupported
        } else if divergences.contains(case.name) {
            seen_divergences.insert(case.name);
            CaseExpectation::Diverges
        } else {
            CaseExpectation::Conform
        };

        if expectation == CaseExpectation::AnalysisApiGap {
            if case.output.is_some() || matches!(case.expression, Expression::Predicate(_)) {
                failures.push(format!(
                    "{} is executable and should not be an API gap",
                    case.name
                ));
            }
            continue;
        }

        if expectation == CaseExpectation::Unsupported
            && matches!(
                &case.expected,
                ExpectedResult::Error(ExpectedError::InvalidExpression)
            )
        {
            failures.push(format!(
                "{} is invalid and must not be inventoried as unsupported",
                case.name
            ));
        }

        let mut executed = false;
        if let Some(output) = &case.output {
            executed = true;
            if let Some(failure) = capture_failure(|| {
                expression_failure(
                    case.input_schema.clone(),
                    case.input_row.clone(),
                    case.expression.clone(),
                    output.data_type.clone(),
                    &case.expected,
                    expectation,
                )
            }) {
                failures.push(format!("{} [expression]: {failure}", case.name));
            }
        }

        if let Expression::Predicate(predicate) = &case.expression {
            executed = true;
            if let Some(failure) = capture_failure(|| {
                predicate_failure(
                    case.input_schema,
                    case.input_row,
                    predicate,
                    &case.expected,
                    expectation,
                )
            }) {
                failures.push(format!("{} [predicate]: {failure}", case.name));
            }
        }
        if !executed {
            failures.push(format!("{} was not executed or inventoried", case.name));
        }
    }

    inventory_failure(
        "unsupported standard",
        &unsupported,
        &seen_unsupported,
        &mut failures,
    );
    inventory_failure(
        "standard divergence",
        &divergences,
        &seen_divergences,
        &mut failures,
    );
    inventory_failure(
        "untestable analysis",
        &analysis_gaps,
        &seen_analysis_gaps,
        &mut failures,
    );
    assert!(
        failures.is_empty(),
        "standard-expression conformance failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn project_cases_match_arrow_support_inventory() {
    let unsupported: HashSet<_> = UNSUPPORTED_PROJECT_CASES.iter().copied().collect();
    let divergences: HashSet<_> = PROJECT_DIVERGENCES.iter().copied().collect();
    assert_eq!(unsupported.len(), UNSUPPORTED_PROJECT_CASES.len());
    assert_eq!(divergences.len(), PROJECT_DIVERGENCES.len());

    let mut seen_unsupported = HashSet::new();
    let mut seen_divergences = HashSet::new();
    let mut failures = Vec::new();
    for case in project_assignment_cases().unwrap() {
        let expectation = if unsupported.contains(case.name) {
            seen_unsupported.insert(case.name);
            CaseExpectation::Unsupported
        } else if divergences.contains(case.name) {
            seen_divergences.insert(case.name);
            CaseExpectation::Diverges
        } else {
            CaseExpectation::Conform
        };
        if expectation == CaseExpectation::Unsupported
            && matches!(
                &case.expected,
                ExpectedResult::Error(ExpectedError::InvalidExpression)
            )
        {
            failures.push(format!(
                "{} is invalid and must not be inventoried as unsupported",
                case.name
            ));
        }
        let target_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "output",
            case.target.data_type.clone(),
            case.target.nullable,
        )]));
        if let Some(failure) = capture_failure(|| {
            let result = evaluate(
                case.input_schema,
                case.input_row,
                Expression::struct_from([case.expression]),
                DataType::from(target_schema),
            );
            check_result(result, &case.expected, &case.target.data_type, expectation)
        }) {
            failures.push(format!("{}: {failure}", case.name));
        }
    }

    inventory_failure(
        "unsupported Project",
        &unsupported,
        &seen_unsupported,
        &mut failures,
    );
    inventory_failure(
        "Project divergence",
        &divergences,
        &seen_divergences,
        &mut failures,
    );
    assert!(
        failures.is_empty(),
        "Project-assignment conformance failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn coalesce_per_row_short_circuit_divergence_is_inventoried() {
    let handler = ArrowEvaluationHandler;
    let input_schema = schema_ref! {
        nullable "a": DOUBLE,
        not_null "d": DOUBLE,
    };
    let rows = [
        [Scalar::Double(1.0), Scalar::Double(0.0)],
        [Scalar::Null(DataType::DOUBLE), Scalar::Double(2.0)],
    ];
    let row_refs: Vec<_> = rows.iter().map(|row| row.as_slice()).collect();
    let input = handler
        .create_many(input_schema.clone(), &row_refs)
        .unwrap();
    let expression = Expression::coalesce([
        crate::expressions::col!("a"),
        Expression::binary(
            crate::expressions::BinaryExpressionOp::Divide,
            Expression::Literal(Scalar::Double(1.0)),
            crate::expressions::col!("d"),
        ),
    ]);
    let evaluator = handler
        .new_expression_evaluator(input_schema, expression.into(), DataType::DOUBLE)
        .unwrap();

    assert!(matches!(
        evaluator.evaluate(input.as_ref()),
        Err(Error::Arrow(ArrowError::DivideByZero))
    ));
}

#[test]
fn interval_arithmetic_analysis_gap_is_inventoried() {
    let cases = [
        (
            Scalar::IntervalYearMonth(1),
            Scalar::IntervalYearMonth(2),
            DataType::INTERVAL_YEAR_MONTH,
        ),
        (
            Scalar::IntervalDayTime(1),
            Scalar::IntervalDayTime(2),
            DataType::INTERVAL_DAY_TIME,
        ),
    ];
    for (left, right, output_type) in cases {
        let expression = Expression::binary(
            crate::expressions::BinaryExpressionOp::Plus,
            Expression::Literal(left),
            Expression::Literal(right),
        );
        assert!(evaluate(schema_ref! {}, vec![], expression, output_type).is_ok());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CaseExpectation {
    // A portable case must return exactly the documented value or error.
    Conform,
    // A valid portable case must explicitly report missing evaluator support.
    Unsupported,
    // An executed portable case is known to return the wrong value or error category.
    Diverges,
    // Behavior is outside the portable contract; strict rejection or an extension is allowed.
    ExtensionBoundary,
    // The handler API cannot expose the analysis result needed to run this case.
    AnalysisApiGap,
}

fn inventory_failure<'a>(
    label: &str,
    expected: &HashSet<&'a str>,
    seen: &HashSet<&'a str>,
    failures: &mut Vec<String>,
) {
    let mut missing: Vec<_> = expected.difference(seen).copied().collect();
    missing.sort_unstable();
    if !missing.is_empty() {
        failures.push(format!(
            "stale {label} inventory entries: {}",
            missing.join(", ")
        ));
    }
}

fn capture_failure(run: impl FnOnce() -> Option<String>) -> Option<String> {
    // Capture panics so one broken fixture does not hide failures from the remaining fixtures.
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(failure) => failure,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            Some(format!("panicked: {message}"))
        }
    }
}

fn expression_failure(
    input_schema: crate::schema::SchemaRef,
    input_row: Vec<Scalar>,
    expression: Expression,
    output_type: DataType,
    expected: &ExpectedResult,
    expectation: CaseExpectation,
) -> Option<String> {
    check_result(
        evaluate(input_schema, input_row, expression, output_type.clone()),
        expected,
        &output_type,
        expectation,
    )
}

fn predicate_failure(
    input_schema: crate::schema::SchemaRef,
    input_row: Vec<Scalar>,
    predicate: &Predicate,
    expected: &ExpectedResult,
    expectation: CaseExpectation,
) -> Option<String> {
    let handler = ArrowEvaluationHandler;
    let input = match handler.create_many(input_schema.clone(), &[input_row.as_slice()]) {
        Ok(input) => input,
        Err(error) => return Some(format!("could not create input: {error}")),
    };
    let evaluator = match handler.new_predicate_evaluator(input_schema, predicate.clone().into()) {
        Ok(evaluator) => evaluator,
        Err(error) => return check_error(error, expected, expectation),
    };
    check_result(
        evaluator.evaluate(input.as_ref()),
        expected,
        &DataType::BOOLEAN,
        expectation,
    )
}

fn evaluate(
    input_schema: crate::schema::SchemaRef,
    input_row: Vec<Scalar>,
    expression: Expression,
    output_type: DataType,
) -> Result<Box<dyn EngineData>, Error> {
    let handler = ArrowEvaluationHandler;
    let input = handler.create_many(input_schema.clone(), &[input_row.as_slice()])?;
    let evaluator =
        handler.new_expression_evaluator(input_schema, expression.into(), output_type)?;
    evaluator.evaluate(input.as_ref())
}

fn check_result(
    result: Result<Box<dyn EngineData>, Error>,
    expected: &ExpectedResult,
    output_type: &DataType,
    expectation: CaseExpectation,
) -> Option<String> {
    match expectation {
        CaseExpectation::Unsupported => match result {
            Err(error) if is_unsupported(&error) => None,
            Err(error) => Some(format!("expected Unsupported, returned {error}")),
            Ok(_) => Some("expected Unsupported, returned a value".to_string()),
        },
        CaseExpectation::ExtensionBoundary => match result {
            Ok(_) => None,
            Err(error)
                if is_unsupported(&error)
                    || error_matches(&error, ExpectedError::InvalidExpression) =>
            {
                None
            }
            Err(error) => Some(format!("unexpected extension-boundary error: {error}")),
        },
        CaseExpectation::Conform => conforming_result_failure(result, expected, output_type),
        CaseExpectation::Diverges => conforming_result_failure(result, expected, output_type)
            .is_none()
            .then(|| "case now conforms; remove it from the divergence inventory".to_string()),
        CaseExpectation::AnalysisApiGap => Some("analysis API gap was executed".to_string()),
    }
}

fn check_error(
    error: Error,
    expected: &ExpectedResult,
    expectation: CaseExpectation,
) -> Option<String> {
    match expectation {
        CaseExpectation::Unsupported if is_unsupported(&error) => None,
        CaseExpectation::ExtensionBoundary
            if is_unsupported(&error)
                || error_matches(&error, ExpectedError::InvalidExpression) =>
        {
            None
        }
        CaseExpectation::Conform => match expected {
            ExpectedResult::Error(expected) => error_failure(error, *expected),
            ExpectedResult::Value(_) => Some(format!("returned error: {error}")),
        },
        CaseExpectation::Diverges => match expected {
            ExpectedResult::Error(expected) if error_failure(error, *expected).is_none() => {
                Some("case now conforms; remove it from the divergence inventory".to_string())
            }
            _ => None,
        },
        _ => Some(format!("unexpected evaluator-construction error: {error}")),
    }
}

fn conforming_result_failure(
    result: Result<Box<dyn EngineData>, Error>,
    expected: &ExpectedResult,
    output_type: &DataType,
) -> Option<String> {
    match (result, expected) {
        (Ok(output), ExpectedResult::Value(expected)) => {
            let actual = output_array(output.as_ref(), output_type);
            let expected = match expected.to_array(1) {
                Ok(expected) => expected,
                Err(error) => return Some(format!("invalid expected value: {error}")),
            };
            (!arrays_equal(&actual, &expected))
                .then(|| format!("expected {expected:?}, got {actual:?}"))
        }
        (Err(error), ExpectedResult::Value(_)) => Some(format!("returned error: {error}")),
        (Ok(_), ExpectedResult::Error(expected)) => {
            Some(format!("expected {expected:?}, returned a value"))
        }
        (Err(error), ExpectedResult::Error(expected)) => error_failure(error, *expected),
    }
}

fn error_failure(error: Error, expected: ExpectedError) -> Option<String> {
    (!error_matches(&error, expected)).then(|| format!("expected {expected:?}, returned {error}"))
}

fn error_matches(error: &Error, expected: ExpectedError) -> bool {
    match (error, expected) {
        (Error::InvalidExpressionEvaluation(_), ExpectedError::InvalidExpression) => true,
        (
            Error::Arrow(ArrowError::InvalidArgumentError(message)),
            ExpectedError::InvalidExpression,
        ) if is_arrow_analysis_error(message) => true,
        (Error::Arrow(ArrowError::ArithmeticOverflow(_)), ExpectedError::ArithmeticOverflow) => {
            true
        }
        (Error::Arrow(ArrowError::DivideByZero), ExpectedError::DivideByZero) => true,
        (Error::Backtraced { source, .. }, _) => error_matches(source, expected),
        _ => false,
    }
}

fn is_arrow_analysis_error(message: &str) -> bool {
    message.starts_with("Incorrect datatype.")
        || message.starts_with("Invalid arithmetic operation:")
        || message.starts_with("Missing Struct fields ")
        || message.starts_with("Requested result type ")
}

fn is_unsupported(error: &Error) -> bool {
    match error {
        Error::Unsupported(_) => true,
        Error::Backtraced { source, .. } => is_unsupported(source),
        _ => false,
    }
}

fn arrays_equal(actual: &ArrayRef, expected: &ArrayRef) -> bool {
    if actual.data_type() != expected.data_type() || actual.len() != expected.len() {
        return false;
    }
    match actual.data_type() {
        crate::arrow::datatypes::DataType::Float32 => {
            let actual = actual.as_primitive::<crate::arrow::datatypes::Float32Type>();
            let expected = expected.as_primitive::<crate::arrow::datatypes::Float32Type>();
            actual.iter().zip(expected).all(|(actual, expected)| {
                matches!((actual, expected), (None, None))
                    || matches!((actual, expected), (Some(actual), Some(expected)) if actual == expected || (actual.is_nan() && expected.is_nan()))
            })
        }
        crate::arrow::datatypes::DataType::Float64 => {
            let actual = actual.as_primitive::<crate::arrow::datatypes::Float64Type>();
            let expected = expected.as_primitive::<crate::arrow::datatypes::Float64Type>();
            actual.iter().zip(expected).all(|(actual, expected)| {
                matches!((actual, expected), (None, None))
                    || matches!((actual, expected), (Some(actual), Some(expected)) if actual == expected || (actual.is_nan() && expected.is_nan()))
            })
        }
        _ => actual.to_data() == expected.to_data(),
    }
}

fn output_array(output: &dyn EngineData, output_type: &DataType) -> ArrayRef {
    let batch = extract_record_batch(output).unwrap();
    match output_type {
        DataType::Struct(_) => Arc::new(StructArray::from(batch.clone())),
        _ => batch.column(0).clone(),
    }
}
