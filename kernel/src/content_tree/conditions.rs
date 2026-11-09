//! Validation of content-tree entry batches with predicates instead of row visitors.
//!
//! Each check is an [`AssertCondition`]: a predicate every row must satisfy, an id, and the
//! columns whose values its error reports. A [`ConditionSet`] evaluates all of its conditions with
//! one evaluator per input schema, and the first failing row becomes the error that the set's
//! error table builds for that condition's id.

use std::ops::Range;
use std::sync::Arc;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::error::{KernelError, KernelResult};
use crate::expressions::{ColumnName, Expression, ExpressionRef, Predicate};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType};
use crate::{EngineData, EvaluationHandler, ExpressionEvaluator};

/// A predicate every row of a batch must satisfy. A row fails when the predicate is FALSE or NULL.
/// When a row fails, the values of `diagnostic_cols` in that row are passed to the error the
/// condition's `id` selects.
#[derive(Debug, Clone)]
pub(crate) struct AssertCondition {
    pub(crate) id: &'static str,
    pub(crate) predicate: Predicate,
    pub(crate) diagnostic_cols: Vec<ColumnName>,
}

/// Builds the error for a failed condition from its diagnostic values, in `diagnostic_cols` order,
/// read as strings. NULL values are `None`.
pub(crate) type ConditionError = fn(&[Option<String>]) -> KernelError;

/// A fixed list of conditions and the error table their ids select from. Row visitors need
/// `'static` column lists, so a set is built once, typically in a `LazyLock`.
pub(crate) struct ConditionSet {
    conditions: Vec<AssertCondition>,
    errors: &'static [(&'static str, ConditionError)],
    expression: ExpressionRef,
    output_type: DataType,
    columns: ColumnNamesAndTypes,
    field_names: Vec<String>,
}

impl ConditionSet {
    /// Collects `conditions` into one struct expression: a BOOLEAN field per condition, then each
    /// condition's diagnostic columns cast to STRING.
    pub(crate) fn new(
        conditions: Vec<AssertCondition>,
        errors: &'static [(&'static str, ConditionError)],
    ) -> Self {
        let predicates = conditions.iter().enumerate().map(|(i, condition)| {
            let field = StructField::nullable(format!("c{i}"), DataType::BOOLEAN);
            (field, Expression::from(condition.predicate.clone()))
        });
        let diagnostics = conditions.iter().enumerate().flat_map(|(i, condition)| {
            condition
                .diagnostic_cols
                .iter()
                .enumerate()
                .map(move |(j, col)| {
                    let field = StructField::nullable(format!("c{i}_d{j}"), DataType::STRING);
                    (
                        field,
                        Expression::cast(Expression::column(col.clone()), DataType::STRING),
                    )
                })
        });
        let (fields, exprs): (Vec<_>, Vec<_>) = predicates.chain(diagnostics).unzip();
        let field_names: Vec<String> = fields.iter().map(|field| field.name().clone()).collect();
        let columns = fields
            .iter()
            .map(|field| (ColumnName::new([field.name()]), field.data_type().clone()))
            .unzip::<_, _, Vec<_>, Vec<_>>()
            .into();
        Self {
            conditions,
            errors,
            expression: Arc::new(Expression::struct_from(exprs)),
            output_type: StructType::new_unchecked(fields).into(),
            columns,
            field_names,
        }
    }

    /// Creates a checker for batches with `input_schema`.
    pub(crate) fn checker(
        &'static self,
        handler: &dyn EvaluationHandler,
        input_schema: SchemaRef,
    ) -> KernelResult<ConditionChecker> {
        let evaluator = handler.new_expression_evaluator(
            input_schema,
            self.expression.clone(),
            self.output_type.clone(),
        )?;
        Ok(ConditionChecker {
            set: self,
            evaluator,
        })
    }

    /// The error the table builds for a failure of the condition at `index`.
    fn error(&self, index: usize, diagnostics: &[Option<String>]) -> KernelError {
        let id = self.conditions[index].id;
        match self.errors.iter().find(|(entry_id, _)| *entry_id == id) {
            Some((_, error)) => error(diagnostics),
            None => {
                KernelError::internal_error(format!("No error is registered for condition '{id}'"))
            }
        }
    }

    /// The output columns holding the condition at `index`'s diagnostic values.
    fn diagnostic_range(&self, index: usize) -> Range<usize> {
        let start = self.conditions.len()
            + self.conditions[..index]
                .iter()
                .map(|condition| condition.diagnostic_cols.len())
                .sum::<usize>();
        start..start + self.conditions[index].diagnostic_cols.len()
    }
}

/// Checks batches with one input schema against a [`ConditionSet`].
pub(crate) struct ConditionChecker {
    set: &'static ConditionSet,
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl ConditionChecker {
    /// Errors for the first row of `batch` that fails a condition. Within a row, conditions are
    /// checked in the order the set lists them.
    pub(crate) fn check(&self, batch: &dyn EngineData) -> KernelResult<()> {
        let evaluated = self.evaluator.evaluate(batch)?;
        let mut visitor = FirstFailureVisitor {
            set: self.set,
            failure: None,
        };
        visitor.visit_rows_of(evaluated.as_ref())?;
        match visitor.failure {
            Some((index, diagnostics)) => Err(self.set.error(index, &diagnostics)),
            None => Ok(()),
        }
    }
}

/// Finds the first row where a condition is not TRUE and reads that condition's diagnostics.
struct FirstFailureVisitor {
    set: &'static ConditionSet,
    failure: Option<(usize, Vec<Option<String>>)>,
}

impl RowVisitor for FirstFailureVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.set.columns.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let names = &self.set.field_names;
        for row in 0..row_count {
            for index in 0..self.set.conditions.len() {
                let passed: Option<bool> = getters[index].get_opt(row, &names[index])?;
                if passed == Some(true) {
                    continue;
                }
                let diagnostics = self
                    .set
                    .diagnostic_range(index)
                    .map(|column| getters[column].get_opt(row, &names[column]))
                    .collect::<KernelResult<_>>()?;
                self.failure = Some((index, diagnostics));
                return Ok(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::arrow_expression::ArrowEvaluationHandler;
    use crate::expressions::{col, column_name, lit};
    use crate::schema::schema_ref;

    const NON_PARQUET: &str = "NonParquetDataFile";
    const PARTITION_SPEC: &str = "NonZeroPartitionSpec";
    const UNREGISTERED: &str = "Unregistered";

    fn non_parquet(values: &[Option<String>]) -> KernelError {
        KernelError::unsupported(format!(
            "entry {:?} has file format {:?}",
            values[0], values[1]
        ))
    }

    fn partition_spec(values: &[Option<String>]) -> KernelError {
        KernelError::unsupported(format!("entry {:?} has spec {:?}", values[0], values[1]))
    }

    static ERRORS: &[(&str, ConditionError)] =
        &[(NON_PARQUET, non_parquet), (PARTITION_SPEC, partition_spec)];

    static CONDITIONS: LazyLock<ConditionSet> = LazyLock::new(|| {
        ConditionSet::new(
            vec![
                AssertCondition {
                    id: NON_PARQUET,
                    predicate: Predicate::eq(col!("format"), lit("parquet")),
                    diagnostic_cols: vec![column_name!("location"), column_name!("format")],
                },
                AssertCondition {
                    id: PARTITION_SPEC,
                    predicate: Predicate::or(
                        Predicate::is_null(col!("spec")),
                        Predicate::eq(col!("spec"), lit(0)),
                    ),
                    diagnostic_cols: vec![column_name!("location"), column_name!("spec")],
                },
            ],
            ERRORS,
        )
    });

    static UNREGISTERED_CONDITIONS: LazyLock<ConditionSet> = LazyLock::new(|| {
        ConditionSet::new(
            vec![AssertCondition {
                id: UNREGISTERED,
                predicate: Predicate::FALSE,
                diagnostic_cols: vec![],
            }],
            ERRORS,
        )
    });

    /// Checks one batch of `(location, format, spec)` rows against `set`.
    fn check(
        set: &'static ConditionSet,
        rows: &[(&str, Option<&str>, Option<i32>)],
    ) -> KernelResult<()> {
        let schema = schema_ref! {
            nullable "location": STRING,
            nullable "format": STRING,
            nullable "spec": INTEGER,
        };
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
        ])
        .unwrap();
        set.checker(&ArrowEvaluationHandler, schema)?
            .check(&ArrowEngineData::new(batch))
    }

    #[rstest]
    #[case::no_rows(&[])]
    #[case::all_pass(&[("a", Some("parquet"), None), ("b", Some("parquet"), Some(0))])]
    fn test_check_passes_when_every_row_satisfies_every_condition(
        #[case] rows: &[(&str, Option<&str>, Option<i32>)],
    ) {
        check(&CONDITIONS, rows).unwrap();
    }

    #[rstest]
    #[case::false_predicate(
        &[("a", Some("parquet"), None), ("b", Some("orc"), None)],
        "entry Some(\"b\") has file format Some(\"orc\")"
    )]
    #[case::null_predicate(&[("a", None, None)], "entry Some(\"a\") has file format None")]
    #[case::first_failing_row_wins(
        &[("a", Some("parquet"), Some(3)), ("b", Some("orc"), None)],
        "entry Some(\"a\") has spec Some(\"3\")"
    )]
    #[case::earlier_condition_wins_within_a_row(
        &[("a", Some("orc"), Some(3))],
        "entry Some(\"a\") has file format Some(\"orc\")"
    )]
    fn test_check_reports_first_failure_with_its_diagnostics(
        #[case] rows: &[(&str, Option<&str>, Option<i32>)],
        #[case] message: &str,
    ) {
        let err = check(&CONDITIONS, rows).unwrap_err();
        assert!(
            matches!(&err, KernelError::Unsupported(m) if m == message),
            "{err}"
        );
    }

    #[test]
    fn test_check_errors_when_a_condition_has_no_registered_error() {
        let err = check(&UNREGISTERED_CONDITIONS, &[("a", None, None)]).unwrap_err();
        assert!(err.to_string().contains("Unregistered"), "{err}");
    }
}
