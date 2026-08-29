//! Runs the shared standard-expression fixtures through DataFusion.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::{EmptyRelation, Expr as DFExpr, LogicalPlan, LogicalPlanBuilder};
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::semantics::conformance::{
    all_cases, project_assignment_cases, ConformanceRequirement, ExpectedError, ExpectedResult,
    ExpressionSemanticsCase, ProjectAssignmentCase, ResolvedOutput, CONFORMANCE_SESSION_TIME_ZONE,
};
use delta_kernel::expressions::{Expression, Scalar};
use delta_kernel::plans::ir::nodes::{Operator as KernelOperator, Values as KernelValues};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Error as KernelError;

use crate::expression::to_df_expr;
use crate::operator::lower_operator;

// Valid standard expressions that DataFusion currently rejects during lowering or planning. A
// returned value is a harness failure.
const UNSUPPORTED_STANDARD_CASES: &[&str] = &[
    // Recursive common-type conversion.
    "map_common_type_widens_values_and_nullability",
    "map_common_type_widens_keys",
    "empty_map_void_key_widens_to_concrete_key",
    "struct_common_type_unions_field_nullability",
    "coalesce_struct_field_names_match_case_insensitively",
    // Decimal result adjustment.
    "decimal_scale_reduction_rounds_half_up",
    "decimal_scale_reduction_negative_midpoint_rounds_away_from_zero",
    // Nested and interval comparison.
    "comparison_ignores_struct_field_names_after_positional_alignment",
    "year_month_intervals_use_signed_ordering",
    "day_time_intervals_use_signed_ordering",
    "distinct_ignores_struct_field_names_after_positional_alignment",
    // Nested and decimal IN coercion.
    "literal_in_ignores_struct_field_names_after_positional_alignment",
    "literal_in_coerces_nested_array_elements",
    "column_in_decimal_uses_minimum_integral_literal_precision",
    "column_in_aligns_structs_with_different_field_nullability",
    "column_in_case_insensitive_struct_names_allow_leaf_widening",
    "column_in_ignores_struct_field_names_after_positional_alignment",
];

// Standard expressions that DataFusion executes with behavior different from Kernel's contract.
// A case that starts conforming must be removed from this list.
const STANDARD_DIVERGENCES: &[&str] = &[
    // Common types and ARRAY nullability.
    "coalesce_decimal_and_float_returns_double",
    "array_int_and_long",
    "array_nullable_element",
    "array_int_and_void",
    "array_with_column_child",
    "array_empty",
    "date_and_timestamp_ntz_use_temporal_common_type",
    "date_to_timestamp_ntz_max_safe_day",
    "date_to_timestamp_ntz_min_safe_day",
    "date_and_timestamp_use_utc_temporal_common_type",
    "date_to_timestamp_max_safe_day",
    "date_to_timestamp_min_safe_day",
    "decimal_common_type_scale_reduction_rounds_half_up",
    "decimal_common_type_negative_midpoint_rounds_away_from_zero",
    // Arithmetic coercion, overflow, and decimal result types.
    "long_and_float_promote_to_double",
    "year_month_interval_arithmetic_is_rejected",
    "day_time_interval_arithmetic_is_rejected",
    "int_plus_overflow",
    "int_minus_overflow",
    "int_multiply_overflow",
    "decimal_plus_float_returns_double",
    "decimal_add_rounds_exact_result_once",
    "decimal_add_negative_rounds_exact_result_once",
    "decimal_subtract_rounds_exact_result_once",
    "decimal_adjusted_scale_preserves_available_fractional_digits",
    "decimal_add_overflow",
    "decimal_plus_minimum_precision_int_literal",
    "decimal_plus_zero_literal_uses_one_digit_precision",
    "decimal_division_result_type",
    "decimal_division_rounds_half_up",
    "negative_decimal_division_rounds_away_from_zero",
    "untyped_null_divided_by_decimal",
    // Comparison validity and floating-point ordering.
    "decimal_comparison_uses_full_integral_column_precision",
    "structs_containing_maps_are_not_comparable",
    "map_comparison_is_rejected",
    "interval_cross_family_comparison_is_rejected",
    "differently_named_structs_requiring_leaf_widening_are_rejected",
    "different_nan_payloads_compare_equal",
    "signed_zeroes_compare_equal",
    "negative_zero_is_not_less_than_positive_zero",
    "array_comparison_uses_nan_equality",
    "two_nulls_are_not_distinct",
    "null_and_value_are_distinct",
    "nan_values_are_not_distinct",
    "signed_zeroes_are_not_distinct",
    // IN evaluation and operand validation.
    "empty_in_array_does_not_suppress_left_child_error",
    "literal_in_uses_signed_zero_equality",
    "column_in_integral_and_float_promote_to_double",
    "column_in_uses_signed_zero_equality",
    "in_array_expression_rhs_matches",
    "map_membership_is_rejected",
];

// Valid Project assignments that DataFusion currently rejects because it cannot perform the
// recursive or positional cast. A returned value is a harness failure.
const UNSUPPORTED_PROJECT_CASES: &[&str] = &[
    "project_struct_assignment_is_recursive_and_positional",
    "project_struct_fields_align_positionally",
];

// Project assignments that DataFusion accepts or evaluates differently from Kernel's contract. A
// case that starts conforming must be removed from this list.
const PROJECT_DIVERGENCES: &[&str] = &["project_nullable_to_required_is_rejected"];

#[tokio::test]
async fn standard_cases_match_datafusion_support_inventory() {
    let unsupported: HashSet<_> = UNSUPPORTED_STANDARD_CASES.iter().copied().collect();
    let divergences: HashSet<_> = STANDARD_DIVERGENCES.iter().copied().collect();
    assert_eq!(unsupported.len(), UNSUPPORTED_STANDARD_CASES.len());
    assert_eq!(divergences.len(), STANDARD_DIVERGENCES.len());

    let mut seen_unsupported = HashSet::new();
    let mut seen_divergences = HashSet::new();
    let mut failures = Vec::new();
    for case in all_cases().unwrap() {
        let expectation = if case.requirement == ConformanceRequirement::ExtensionBoundary {
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
        if expectation == CaseExpectation::Unsupported
            && matches!(
                case.expected,
                ExpectedResult::Error(ExpectedError::InvalidExpression)
            )
        {
            failures.push(format!(
                "{} is invalid and must not be inventoried as unsupported",
                case.name
            ));
            continue;
        }

        let result = evaluate_expression_case(&case).await;
        if let Some(failure) =
            check_result(result, &case.expected, case.output.as_ref(), expectation)
        {
            failures.push(format!("{}: {failure}", case.name));
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
    assert!(
        failures.is_empty(),
        "standard-expression conformance failures:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn project_cases_match_datafusion_support_inventory() {
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
                case.expected,
                ExpectedResult::Error(ExpectedError::InvalidExpression)
            )
        {
            failures.push(format!(
                "{} is invalid and must not be inventoried as unsupported",
                case.name
            ));
            continue;
        }

        let result = evaluate_project_case(&case).await;
        if let Some(failure) = check_result(result, &case.expected, Some(&case.target), expectation)
        {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CaseExpectation {
    // The case must return exactly the documented type, nullability, value, or error.
    Conform,
    // A valid expression must explicitly report that the executor does not support it.
    Unsupported,
    // The executor is known to return the wrong value, type, nullability, or error.
    Diverges,
    // Strict rejection or executor-specific behavior is allowed outside Kernel's contract.
    ExtensionBoundary,
}

struct EvaluatedValue {
    output: ResolvedOutput,
    value: ArrayRef,
}

async fn evaluate_expression_case(
    case: &ExpressionSemanticsCase,
) -> Result<EvaluatedValue, DataFusionError> {
    let input = lower_input(case.input_schema.as_ref(), case.input_row.clone())?;
    let expression = to_df_expr(
        &case.expression,
        case.input_schema.as_ref(),
        case.output.as_ref().map(|output| &output.data_type),
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    evaluate_projection(input, expression).await
}

async fn evaluate_project_case(
    case: &ProjectAssignmentCase,
) -> Result<EvaluatedValue, DataFusionError> {
    let input = Arc::new(lower_input(
        case.input_schema.as_ref(),
        case.input_row.clone(),
    )?);
    let target = Arc::new(StructType::new_unchecked([StructField::new(
        "output",
        case.target.data_type.clone(),
        case.target.nullable,
    )]));
    let project = delta_kernel::plans::ir::nodes::Project {
        expr: Expression::struct_from([case.expression.clone()]).into(),
        schema: target,
    };
    let plan = lower_operator(&KernelOperator::Project(project), &[input])?;
    evaluate_plan(plan).await
}

fn lower_input(schema: &StructType, row: Vec<Scalar>) -> Result<LogicalPlan, DataFusionError> {
    if schema.num_fields() == 0 {
        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        }));
    }
    lower_operator(
        &KernelOperator::Values(KernelValues::new(schema.clone(), vec![row])),
        &[],
    )
}

async fn evaluate_projection(
    input: LogicalPlan,
    expression: DFExpr,
) -> Result<EvaluatedValue, DataFusionError> {
    let plan = LogicalPlanBuilder::from(input)
        .project([expression.alias("output")])?
        .build()?;
    evaluate_plan(plan).await
}

async fn evaluate_plan(plan: LogicalPlan) -> Result<EvaluatedValue, DataFusionError> {
    let field = plan.schema().field(0);
    let data_type: DataType = field.data_type().try_into_kernel()?;
    let output = ResolvedOutput {
        data_type,
        nullable: field.is_nullable(),
    };
    let batches = session_context()
        .execute_logical_plan(plan)
        .await?
        .collect()
        .await?;
    if batches.len() != 1 || batches[0].num_columns() != 1 || batches[0].num_rows() != 1 {
        return Err(DataFusionError::Execution(format!(
            "expected one value, got {} batch(es)",
            batches.len()
        )));
    }
    Ok(EvaluatedValue {
        output,
        value: batches[0].column(0).clone(),
    })
}

fn session_context() -> SessionContext {
    let mut config = SessionConfig::new().with_enable_ansi_mode(true);
    config.options_mut().execution.time_zone = Some(CONFORMANCE_SESSION_TIME_ZONE.to_string());
    SessionContext::new_with_config(config)
}

fn check_result(
    result: Result<EvaluatedValue, DataFusionError>,
    expected: &ExpectedResult,
    output: Option<&ResolvedOutput>,
    expectation: CaseExpectation,
) -> Option<String> {
    match expectation {
        CaseExpectation::Unsupported => match result {
            Err(error) if is_analysis_rejection(&error) => None,
            Err(error) => Some(format!("expected a planning rejection, returned {error}")),
            Ok(_) => Some("expected a planning rejection, returned a value".to_string()),
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
        CaseExpectation::Conform => conforming_result_failure(result, expected, output),
        CaseExpectation::Diverges => conforming_result_failure(result, expected, output)
            .is_none()
            .then(|| "case now conforms; remove it from the divergence inventory".to_string()),
    }
}

fn conforming_result_failure(
    result: Result<EvaluatedValue, DataFusionError>,
    expected: &ExpectedResult,
    output: Option<&ResolvedOutput>,
) -> Option<String> {
    match (result, expected) {
        (Ok(actual), ExpectedResult::Value(expected)) => {
            if Some(&actual.output) != output {
                return Some(format!(
                    "expected output {output:?}, got {:?}",
                    actual.output
                ));
            }
            let expected = match expected.to_array(1) {
                Ok(expected) => expected,
                Err(error) => return Some(format!("invalid expected value: {error}")),
            };
            (!arrays_equal(&actual.value, &expected))
                .then(|| format!("expected {expected:?}, got {:?}", actual.value))
        }
        (Err(error), ExpectedResult::Value(_)) => Some(format!("returned error: {error}")),
        (Ok(_), ExpectedResult::Error(expected)) => {
            Some(format!("expected {expected:?}, returned a value"))
        }
        (Err(error), ExpectedResult::Error(expected)) => (!error_matches(&error, *expected))
            .then(|| format!("expected {expected:?}, returned {error}")),
    }
}

fn error_matches(error: &DataFusionError, expected: ExpectedError) -> bool {
    match (error, expected) {
        (DataFusionError::ArrowError(error, _), ExpectedError::ArithmeticOverflow)
            if matches!(error.as_ref(), ArrowError::ArithmeticOverflow(_)) =>
        {
            true
        }
        (DataFusionError::ArrowError(error, _), ExpectedError::DivideByZero)
            if matches!(error.as_ref(), ArrowError::DivideByZero) =>
        {
            true
        }
        (DataFusionError::Execution(message), ExpectedError::ArithmeticOverflow)
            if message.contains("converted value exceeds the representable i64 range") =>
        {
            true
        }
        (
            DataFusionError::Plan(_)
            | DataFusionError::SchemaError(_, _)
            | DataFusionError::External(_),
            ExpectedError::InvalidExpression,
        ) if !is_unsupported(error) => true,
        (DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source), _) => {
            error_matches(source, expected)
        }
        (DataFusionError::Shared(source), _) => error_matches(source, expected),
        (DataFusionError::Collection(errors), _) => {
            errors.iter().any(|error| error_matches(error, expected))
        }
        _ => false,
    }
}

fn is_analysis_rejection(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Plan(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::NotImplemented(_) => true,
        DataFusionError::External(_) => is_unsupported(error),
        DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
            is_analysis_rejection(source)
        }
        DataFusionError::Shared(source) => is_analysis_rejection(source),
        DataFusionError::Collection(errors) => errors.iter().any(is_analysis_rejection),
        _ => false,
    }
}

fn is_unsupported(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::NotImplemented(_) => true,
        DataFusionError::External(source) => source
            .downcast_ref::<KernelError>()
            .is_some_and(is_kernel_unsupported),
        DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
            is_unsupported(source)
        }
        DataFusionError::Shared(source) => is_unsupported(source),
        DataFusionError::Collection(errors) => errors.iter().any(is_unsupported),
        _ => false,
    }
}

fn is_kernel_unsupported(error: &KernelError) -> bool {
    match error {
        KernelError::Unsupported(_) => true,
        KernelError::Backtraced { source, .. } => is_kernel_unsupported(source),
        _ => false,
    }
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

fn arrays_equal(actual: &ArrayRef, expected: &ArrayRef) -> bool {
    if actual.data_type() != expected.data_type() || actual.len() != expected.len() {
        return false;
    }
    match actual.data_type() {
        datafusion::arrow::datatypes::DataType::Float32 => {
            let actual = actual.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            let expected = expected.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            actual.iter().zip(expected).all(|(actual, expected)| {
                matches!((actual, expected), (None, None))
                    || matches!((actual, expected), (Some(actual), Some(expected)) if actual == expected || (actual.is_nan() && expected.is_nan()))
            })
        }
        datafusion::arrow::datatypes::DataType::Float64 => {
            let actual = actual.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            let expected = expected.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            actual.iter().zip(expected).all(|(actual, expected)| {
                matches!((actual, expected), (None, None))
                    || matches!((actual, expected), (Some(actual), Some(expected)) if actual == expected || (actual.is_nan() && expected.is_nan()))
            })
        }
        _ => actual.to_data() == expected.to_data(),
    }
}
