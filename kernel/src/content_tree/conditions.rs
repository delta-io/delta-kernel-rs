//! Validation of content-tree entry batches against predicate conditions.
//!
//! Each [`AssertCondition`] is a predicate every row must satisfy. A [`ConditionChecker`] pairs
//! conditions with the errors they report and finds the first row of a batch that fails one.

use std::sync::{Arc, LazyLock};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::error::{KernelError, KernelResult};
use crate::expressions::{ColumnName, Expression, Predicate, column_name, lit, null_lit};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType};
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator};

/// The most diagnostic columns one condition can report.
const MAX_DIAGNOSTICS: usize = 4;

/// A predicate every row of a batch must satisfy. A row fails when the predicate is FALSE or NULL,
/// and the values of `diagnostic_cols` in that row are passed to the condition's error.
///
/// The predicate must not fail evaluation on any row (no strict casts or checked arithmetic):
/// all conditions are evaluated together, so one evaluation error would hide every failure.
#[derive(Debug, Clone)]
pub(crate) struct AssertCondition {
    pub(crate) id: &'static str,
    pub(crate) predicate: Predicate,
    pub(crate) diagnostic_cols: Vec<ColumnName>,
}

/// Builds the error for a failed condition from its diagnostic values, in `diagnostic_cols` order
/// and read as strings. NULL values are `None`.
pub(crate) type ConditionErrorFn = fn(&[Option<&str>]) -> KernelError;

/// The diagnostic value at `index`, or `NULL` when it is null or absent.
pub(crate) fn diagnostic<'a>(values: &[Option<&'a str>], index: usize) -> &'a str {
    values.get(index).copied().flatten().unwrap_or("NULL")
}

/// The INTEGER diagnostic value at `index`, read from the `column` it was taken from.
///
/// # Errors
/// Returns a `MissingData` error naming `column` if the value is null, and an internal error if
/// it does not parse as an integer.
pub(crate) fn diagnostic_i32(
    values: &[Option<&str>],
    index: usize,
    column: &str,
) -> KernelResult<i32> {
    let value = values
        .get(index)
        .copied()
        .flatten()
        .ok_or_else(|| KernelError::missing_data(format!("Data missing for field {column}")))?;
    value.parse().map_err(|_| {
        KernelError::internal_error(format!(
            "Expected an integer diagnostic for {column}, got '{value}'"
        ))
    })
}

/// A condition and the error it reports.
#[derive(Debug, Clone)]
pub(crate) struct Check {
    pub(crate) condition: AssertCondition,
    pub(crate) error: ConditionErrorFn,
}

impl Check {
    /// A check that rows satisfy `predicate`, reporting `error` with the values of
    /// `diagnostic_cols` for the first row that does not.
    pub(crate) fn new(
        id: &'static str,
        predicate: Predicate,
        diagnostic_cols: Vec<ColumnName>,
        error: ConditionErrorFn,
    ) -> Self {
        Self {
            condition: AssertCondition {
                id,
                predicate,
                diagnostic_cols,
            },
            error,
        }
    }
}

/// Checks batches with one input schema against a list of [`Check`]s.
pub(crate) struct ConditionChecker {
    checks: Vec<Check>,
    /// Yields, per row, the index of the first failing condition, or NULL when every one passes.
    first_failure: Arc<dyn ExpressionEvaluator>,
    /// Per condition, yields its diagnostics as [`MAX_DIAGNOSTICS`] STRING fields, NULL-padded.
    diagnostics: Vec<Arc<dyn ExpressionEvaluator>>,
}

impl ConditionChecker {
    /// Creates a checker for batches with `input_schema`. Errors if a condition has more than
    /// [`MAX_DIAGNOSTICS`] diagnostic columns, or one that is missing or not a primitive type
    /// other than BINARY, since only those render as STRING.
    pub(crate) fn try_new(
        handler: &dyn EvaluationHandler,
        input_schema: SchemaRef,
        checks: Vec<Check>,
    ) -> KernelResult<Self> {
        // `CASE WHEN p0 IS DISTINCT FROM TRUE THEN 0 WHEN p1 .. THEN 1 .. END`: a condition fails
        // when it is FALSE or NULL. Branches match failures, so a passing batch matches none.
        let failure_branches = checks
            .iter()
            .enumerate()
            .map(|(index, check)| {
                let index = i32::try_from(index).map_err(|_| {
                    KernelError::internal_error(format!("Too many conditions: {}", checks.len()))
                })?;
                let failed = Predicate::distinct(
                    Expression::from_pred(check.condition.predicate.clone()),
                    lit(true),
                );
                Ok((failed, lit(index)))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let first_failure = handler.new_expression_evaluator(
            input_schema.clone(),
            Arc::new(Expression::case_when(
                failure_branches,
                null_lit(DataType::INTEGER),
            )),
            DataType::INTEGER,
        )?;
        let diagnostics = checks
            .iter()
            .map(|check| {
                let diagnostics = diagnostic_expression(&check.condition, &input_schema)?;
                handler.new_expression_evaluator(
                    input_schema.clone(),
                    Arc::new(diagnostics),
                    DIAGNOSTICS_SCHEMA.clone().into(),
                )
            })
            .collect::<KernelResult<_>>()?;
        Ok(Self {
            checks,
            first_failure,
            diagnostics,
        })
    }

    /// Errors for the first row of `batch` that fails a condition. Within a row, conditions are
    /// checked in the order they were given.
    pub(crate) fn check(&self, batch: &dyn EngineData) -> KernelResult<()> {
        if self.checks.is_empty() {
            return Ok(());
        }
        let mut first_failure = FirstFailureVisitor(None);
        first_failure.visit_rows_of(self.first_failure.evaluate(batch)?.as_ref())?;
        let Some((row, index)) = first_failure.0 else {
            return Ok(());
        };
        let check = self.checks.get(index).ok_or_else(|| {
            KernelError::internal_error(format!("Condition index {index} is out of range"))
        })?;
        let mut diagnostics = DiagnosticsVisitor {
            row,
            count: check.condition.diagnostic_cols.len(),
            error: check.error,
            result: Ok(()),
        };
        diagnostics.visit_rows_of(self.diagnostics[index].evaluate(batch)?.as_ref())?;
        diagnostics.result
    }
}

/// `STRUCT(CAST(d0 AS STRING), ..)` over `condition`'s diagnostic columns, NULL-padded to
/// [`MAX_DIAGNOSTICS`] fields.
fn diagnostic_expression(
    condition: &AssertCondition,
    input_schema: &StructType,
) -> KernelResult<Expression> {
    let cols = &condition.diagnostic_cols;
    if cols.len() > MAX_DIAGNOSTICS {
        return Err(KernelError::internal_error(format!(
            "Condition '{}' has {} diagnostic columns, more than the {MAX_DIAGNOSTICS} supported",
            condition.id,
            cols.len()
        )));
    }
    let values = cols.iter().map(|col| match column_type(input_schema, col) {
        Some(data_type @ DataType::Primitive(_)) if *data_type != DataType::BINARY => Ok(
            Expression::cast(Expression::column(col.clone()), DataType::STRING),
        ),
        other => Err(KernelError::internal_error(format!(
            "Condition '{}' diagnostic column {col} has type {other:?}, which cannot be \
                 reported as STRING",
            condition.id
        ))),
    });
    let padding = (cols.len()..MAX_DIAGNOSTICS).map(|_| Ok(null_lit(DataType::STRING)));
    Ok(Expression::struct_from(
        values.chain(padding).collect::<KernelResult<Vec<_>>>()?,
    ))
}

/// The type of the (possibly nested) column `col` in `schema`, if it exists.
fn column_type<'a>(schema: &'a StructType, col: &ColumnName) -> Option<&'a DataType> {
    let (first, rest) = col.path().split_first()?;
    let mut data_type = schema.field(first)?.data_type();
    for name in rest {
        let DataType::Struct(inner) = data_type else {
            return None;
        };
        data_type = inner.field(name)?.data_type();
    }
    Some(data_type)
}

static FIRST_FAILURE_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| (vec![column_name!("output")], vec![DataType::INTEGER]).into());

static DIAGNOSTICS_SCHEMA: LazyLock<StructType> = LazyLock::new(|| {
    StructType::new_unchecked(
        (0..MAX_DIAGNOSTICS).map(|i| StructField::nullable(format!("d{i}"), DataType::STRING)),
    )
});

static DIAGNOSTICS_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    DIAGNOSTICS_SCHEMA
        .fields()
        .map(|field| (ColumnName::new([field.name()]), DataType::STRING))
        .unzip::<_, _, Vec<_>, Vec<_>>()
        .into()
});

/// Finds the first row with a failed condition, as `(row, condition index)`.
struct FirstFailureVisitor(Option<(usize, usize)>);

impl RowVisitor for FirstFailureVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        FIRST_FAILURE_COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..row_count {
            let index: Option<i32> = getters[0].get_opt(row, "output")?;
            if let Some(index) = index {
                let index = usize::try_from(index).map_err(|_| {
                    KernelError::internal_error(format!("Negative condition index {index}"))
                })?;
                self.0 = Some((row, index));
                return Ok(());
            }
        }
        Ok(())
    }
}

/// Reads one row's diagnostics and builds the failed condition's error from them.
struct DiagnosticsVisitor {
    row: usize,
    count: usize,
    error: ConditionErrorFn,
    result: KernelResult<()>,
}

impl RowVisitor for DiagnosticsVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        DIAGNOSTICS_COLUMNS.as_ref()
    }

    fn visit<'a>(
        &mut self,
        _row_count: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> KernelResult<()> {
        let values: Vec<Option<&str>> = getters[..self.count]
            .iter()
            .enumerate()
            .map(|(i, getter)| getter.get_opt(self.row, &format!("d{i}")))
            .collect::<KernelResult<_>>()?;
        self.result = Err((self.error)(&values));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{
        ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray, StructArray,
    };
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::expressions::col;
    use crate::schema::schema_ref;

    type Row = (&'static str, Option<&'static str>, Option<i32>);

    fn input_schema() -> SchemaRef {
        schema_ref! {
            nullable "location": STRING,
            nullable "format": STRING,
            nullable "spec": INTEGER,
            nullable "meta": { nullable "x": INTEGER },
            nullable "raw": BINARY,
        }
    }

    fn batch(rows: &[Row]) -> ArrowEngineData {
        let meta_fields = Fields::from(vec![ArrowField::new("x", ArrowDataType::Int32, true)]);
        let meta = StructArray::new_null(meta_fields, rows.len());
        let batch = RecordBatch::try_from_iter([
            (
                "location",
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            ),
            (
                "format",
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))) as ArrayRef,
            ),
            (
                "spec",
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.2))) as ArrayRef,
            ),
            ("meta", Arc::new(meta) as ArrayRef),
            (
                "raw",
                Arc::new(BinaryArray::from_iter(rows.iter().map(|_| None::<&[u8]>))) as ArrayRef,
            ),
        ])
        .unwrap();
        ArrowEngineData::new(batch)
    }

    fn checks() -> Vec<Check> {
        vec![
            Check {
                condition: AssertCondition {
                    id: "NonParquetDataFile",
                    predicate: Predicate::eq(col!("format"), lit("parquet")),
                    diagnostic_cols: vec![column_name!("location"), column_name!("format")],
                },
                error: |values| {
                    KernelError::unsupported(format!(
                        "entry '{}' has file format '{}'",
                        diagnostic(values, 0),
                        diagnostic(values, 1)
                    ))
                },
            },
            Check {
                condition: AssertCondition {
                    id: "NonZeroPartitionSpec",
                    predicate: Predicate::or(
                        Predicate::is_null(col!("spec")),
                        Predicate::eq(col!("spec"), lit(0)),
                    ),
                    diagnostic_cols: vec![column_name!("location"), column_name!("spec")],
                },
                error: |values| {
                    KernelError::unsupported(format!(
                        "entry '{}' has spec {}",
                        diagnostic(values, 0),
                        diagnostic(values, 1)
                    ))
                },
            },
        ]
    }

    fn checker(checks: Vec<Check>) -> KernelResult<ConditionChecker> {
        ConditionChecker::try_new(&ArrowEvaluationHandler, input_schema(), checks)
    }

    #[rstest]
    #[case::no_rows(&[], None)]
    #[case::all_pass(&[("a", Some("parquet"), None), ("b", Some("parquet"), Some(0))], None)]
    #[case::false_predicate(
        &[("a", Some("parquet"), None), ("b", Some("orc"), None)],
        Some("entry 'b' has file format 'orc'")
    )]
    #[case::null_predicate(&[("a", None, None)], Some("entry 'a' has file format 'NULL'"))]
    #[case::first_failing_row_wins(
        &[("a", Some("parquet"), Some(3)), ("b", Some("orc"), None)],
        Some("entry 'a' has spec 3")
    )]
    #[case::null_predicate_in_later_row(
        &[("a", Some("parquet"), None), ("b", None, None)],
        Some("entry 'b' has file format 'NULL'")
    )]
    #[case::null_condition_before_false_condition_wins(
        &[("a", None, Some(3))],
        Some("entry 'a' has file format 'NULL'")
    )]
    #[case::earlier_condition_wins_within_a_row(
        &[("a", Some("orc"), Some(3))],
        Some("entry 'a' has file format 'orc'")
    )]
    fn test_check_reports_the_first_failure(#[case] rows: &[Row], #[case] expected: Option<&str>) {
        let result = checker(checks()).unwrap().check(&batch(rows));
        match expected {
            None => result.unwrap(),
            Some(message) => assert!(
                matches!(&result, Err(KernelError::Unsupported(m)) if m == message),
                "{result:?}"
            ),
        }
    }

    #[test]
    fn test_checker_is_reusable_across_batches() {
        let checker = checker(checks()).unwrap();
        checker
            .check(&batch(&[("a", Some("parquet"), None)]))
            .unwrap();
        checker
            .check(&batch(&[("b", Some("orc"), None)]))
            .unwrap_err();
        checker
            .check(&batch(&[("c", Some("parquet"), Some(0))]))
            .unwrap();
    }

    #[test]
    fn test_condition_without_diagnostics_gets_no_values() {
        let check = Check {
            condition: AssertCondition {
                id: "AlwaysFails",
                predicate: Predicate::FALSE,
                diagnostic_cols: vec![],
            },
            error: |values| KernelError::unsupported(format!("{} values", values.len())),
        };
        let err = checker(vec![check])
            .unwrap()
            .check(&batch(&[("a", None, None)]))
            .unwrap_err();
        assert!(
            matches!(&err, KernelError::Unsupported(m) if m == "0 values"),
            "{err}"
        );
    }

    #[test]
    fn test_no_conditions_always_pass() {
        checker(vec![])
            .unwrap()
            .check(&batch(&[("a", None, None)]))
            .unwrap();
    }

    #[rstest]
    #[case::missing(vec![column_name!("nope")], "nope")]
    #[case::struct_type(vec![column_name!("meta")], "meta")]
    #[case::binary(vec![column_name!("raw")], "raw")]
    #[case::too_many(vec![column_name!("location"); MAX_DIAGNOSTICS + 1], "more than")]
    fn test_try_new_rejects_unreportable_diagnostics(
        #[case] diagnostic_cols: Vec<ColumnName>,
        #[case] message: &str,
    ) {
        let check = Check {
            condition: AssertCondition {
                id: "Bad",
                predicate: Predicate::TRUE,
                diagnostic_cols,
            },
            error: |_| KernelError::unsupported("unused"),
        };
        let err = checker(vec![check]).err().unwrap();
        assert!(err.to_string().contains(message), "{err}");
    }

    #[rstest]
    #[case::integer(&[Some("x"), Some("-3")], Ok(-3))]
    #[case::null(&[Some("x"), None], Err("Data missing for field col"))]
    #[case::absent(&[Some("x")], Err("Data missing for field col"))]
    #[case::not_an_integer(&[Some("x"), Some("y")], Err("Expected an integer diagnostic"))]
    fn test_diagnostic_i32(#[case] values: &[Option<&str>], #[case] expected: Result<i32, &str>) {
        match (diagnostic_i32(values, 1, "col"), expected) {
            (Ok(value), Ok(expected)) => assert_eq!(value, expected),
            (Err(e), Err(message)) => assert!(e.to_string().contains(message), "{e}"),
            (actual, expected) => panic!("{actual:?} != {expected:?}"),
        }
    }
}
