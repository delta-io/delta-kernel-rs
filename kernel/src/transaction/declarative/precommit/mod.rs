//! Validates staged file actions for a declarative transaction.
//!
//! The main components of this module are:
//!
//! 1. A shared plan builder (`build_plan`) which unions all staged inputs and feeds them to a set
//!    of checks.
//! 2. A row-level check framework (see `StagedDataCheck` trait) whose checks read the shared input
//!    schema and project errors into fixed result slots declared in `schemas`.
//! 3. A shared validation function (see `validate_result`) which validates the result of the plan
//!    against the set of checks.
//!
//! ## Core Plan Shape
//!
//! The overall pre-commit validation plan has the following shape:
//!
//! 1. Validate staged schemas and select common input fields plus the table's statistics subtree,
//!    then UNION staged adds and removes into a single input stream.
//! 2. PROJECT the input stream into rows containing a single column for each check (dictated by the
//!    check's `project_error` expression).
//! 3. AGGREGATE into a single row containing a single column for each check (taking the
//!    FIRST_NON_NULL value for each check).
//! 4. PROJECT the aggregate row into two columns per check:
//!    - a non-null `is_valid` Boolean indicating whether the check passed or failed
//!    - a nullable error struct (derived from the check's error type) containing error information
//!      for the failed check
//!
//! The single-row output of the plan is then visited by the `validate_result` function, which
//! returns an error with the first failing check.
//! Disabled checks retain their result slots with `is_valid = true` and a null error struct.

mod checks;
mod schemas;

use std::sync::Arc;

pub(super) use checks::{
    AddFileFieldsCheck, AppendOnlyCheck, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};
use schemas::{
    ERROR_SCHEMA, ERROR_SUFFIX, INPUT_SCHEMA, IS_VALID_SUFFIX, RESULT_COLUMNS, RESULT_SCHEMA,
};

use crate::engine_data::{GetData, RowVisitor};
use crate::expressions::{ColumnName, Expression, Predicate, Scalar, StructData, col, null_lit};
use crate::plans::ir::nodes::{Agg, Aggregate};
use crate::plans::{Plan, PlanBuilder, RelationRef};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::struct_patch::StructPatchBuilder;
use crate::{EngineData, KernelError, KernelResult, KernelResultIterator};

/// Checks selected from the transaction's effective configuration, in validation order.
pub(super) type CheckList = Vec<Box<dyn DynStagedDataCheck>>;

/// Defines a row-level pre-commit rule over the shared input schema.
///
/// Each check projects a null error struct if the check passed, or a present error struct if the
/// check failed, even when all error fields are null.
///
/// Each check must ensure that its required inputs are present in [`schemas::INPUT_SCHEMA`] and
/// it is registered in `schemas::CHECK_SCHEMAS` to include its output in the shared schemas.
pub(super) trait StagedDataCheck {
    /// Typed details from a failing row, with conversion and schema derived from its fields.
    type ErrorInfo: TryFrom<StructData, Error = KernelError> + ToSchema;

    /// The fixed name used to prefix this check's result columns.
    const NAME: &'static str;
    /// Returns an expression which projects a null error struct if the check passed, or a present
    /// error struct if the check failed, matching the schema of `ErrorInfo`.
    fn project_error(&self) -> Expression;
    /// Constructs an error from the typed details of a failing row.
    fn build_error(&self, sample: Self::ErrorInfo) -> KernelResult<KernelError>;
}

/// Wrapper around [`StagedDataCheck`] which provides a dyn compatible interface.
/// Auto-implemented for any struct which implements [`StagedDataCheck`].
pub(super) trait DynStagedDataCheck {
    /// Returns the fixed name used to prefix this check's result columns.
    fn name(&self) -> &'static str;
    /// Returns the schema derived from this check's error payload type.
    fn output_schema(&self) -> SchemaRef;
    /// Returns the check's error projection.
    fn project_error(&self) -> Expression;
    /// Converts the projected fields into the check's typed payload and constructs its error.
    fn decode_error(&self, sample: StructData) -> KernelResult<KernelError>;
}

impl<C: StagedDataCheck> DynStagedDataCheck for C {
    fn name(&self) -> &'static str {
        C::NAME
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(C::ErrorInfo::to_schema())
    }

    fn project_error(&self) -> Expression {
        StagedDataCheck::project_error(self)
    }

    fn decode_error(&self, sample: StructData) -> KernelResult<KernelError> {
        self.build_error(C::ErrorInfo::try_from(sample)?)
    }
}

/// Builds a declarative plan that resolves down to a single-row summary containing results from the
/// given `checks`. See the module documentation for more details on the plan shape.
/// `stats_schema` comes from the transaction's table configuration, independently of the checks.
pub(super) fn build_plan(
    adds: &[RelationRef],
    removes: &[RelationRef],
    checks: &[Box<dyn DynStagedDataCheck>],
    stats_schema: &StructType,
) -> KernelResult<Plan> {
    if checks.is_empty() {
        return Err(KernelError::internal_error(
            "Pre-commit validation requires checks",
        ));
    }
    let input = build_input(adds, removes, stats_schema)?;

    // Project the input with each of the check's error expressions.
    // The resulting schema is a struct with one field per check, each field named as
    // "<check_name>_err"
    let expressions = ERROR_SCHEMA.fields().map(|field| {
        checks
            .iter()
            .find(|check| field.name().strip_suffix(ERROR_SUFFIX) == Some(check.name()))
            .map(|check| check.project_error())
            .unwrap_or_else(|| null_lit(field.data_type().clone()))
    });
    let input = input.project(Expression::struct_from(expressions), ERROR_SCHEMA.clone())?;

    // Aggregate the results into a single row containing a single column for each check (taking the
    // FIRST_NON_NULL value for each check).
    let mut aggregate = Aggregate::ungrouped(ERROR_SCHEMA.clone());
    for field in ERROR_SCHEMA.fields() {
        let name = field.name();
        aggregate = aggregate.aggregate_as(Agg::first_non_null(ColumnName::new([name])), name);
    }

    // The final result must include a `<check_name>_is_valid` column in addition to the
    // `<check_name>_err` column because RowVisitors only access leaf values and cannot
    // differentiate between a null struct and a struct with all null leaf values. Adding an
    // is_valid column allows us to directly verify the result of a check without relying on
    // decoding the error struct.
    let summary_exprs = ERROR_SCHEMA.fields().flat_map(|field| {
        let error = Expression::column([field.name()]);
        [Predicate::is_null(error.clone()).into(), error]
    });
    input
        .aggregate(aggregate)?
        .project(
            Expression::struct_from(summary_exprs),
            RESULT_SCHEMA.clone(),
        )?
        .build()
}

/// Validates the resolved output of running the pre-commit validation plan (from `build_plan`)
/// using the given `checks`.
/// Empty batches are ignored, but the stream must contain exactly one row in total. Stream errors
/// are propagated even when they occur after the summary row.
pub(super) fn validate_result(
    results: impl KernelResultIterator<Box<dyn EngineData>>,
    checks: &[Box<dyn DynStagedDataCheck>],
) -> KernelResult<()> {
    let mut summary = None;
    let mut row_count = 0usize;
    for batch in results {
        let batch = batch?;
        if batch.is_empty() {
            continue;
        }
        row_count = row_count.saturating_add(batch.len());
        if row_count == 1 {
            summary = Some(batch);
        }
    }
    if row_count != 1 {
        return Err(invalid_summary(format!(
            "Expected exactly one pre-commit validation row, but got {}",
            row_count,
        )));
    }
    let batch = summary.ok_or_else(|| invalid_summary("Missing pre-commit validation row"))?;
    CheckResultVisitor { checks }.visit_rows_of(batch.as_ref())
}

fn build_input(
    adds: &[RelationRef],
    removes: &[RelationRef],
    stats_schema: &StructType,
) -> KernelResult<PlanBuilder> {
    let schema = Arc::new(
        StructPatchBuilder::<StructField>::new()
            .replace_at(
                ["add"],
                "stats",
                StructField::nullable("stats", stats_schema.clone()),
            )
            .build(&INPUT_SCHEMA)?,
    );
    let add_type = schema.field_at(&ColumnName::new(["add"]))?.data_type();
    let remove_type = schema.field_at(&ColumnName::new(["remove"]))?.data_type();
    let add_row = Expression::struct_from([
        Expression::struct_from([
            col!("path"),
            col!("size"),
            col!("modificationTime"),
            col!("stats"),
        ]),
        null_lit(remove_type.clone()),
    ]);
    let remove_row = Expression::struct_from([
        null_lit(add_type.clone()),
        Expression::struct_from([col!("path"), col!("size")]),
    ]);
    let mut inputs = Vec::new();
    for (relations, row, data_type) in [
        (adds, add_row, add_type),
        (removes, remove_row, remove_type),
    ] {
        let DataType::Struct(required) = data_type else {
            return Err(KernelError::internal_error("Expected an action struct"));
        };
        let required = required.leaves(None);
        let (columns, types) = required.as_ref();
        for relation in relations {
            for (column, expected) in columns.iter().zip(types) {
                let actual = relation.schema().field_at(column)?.data_type();
                if actual != expected {
                    return Err(KernelError::schema(format!(
                        "Unexpected input type at {column}; expected {expected}, got {actual}"
                    )));
                }
            }
            inputs.push(
                PlanBuilder::relation_source(relation.clone())
                    .project(row.clone(), schema.clone())?,
            );
        }
    }
    if inputs.is_empty() {
        PlanBuilder::values(schema, vec![])
    } else {
        PlanBuilder::union_all(inputs)
    }
}

fn invalid_summary(message: impl AsRef<str>) -> KernelError {
    KernelError::internal_error(format!(
        "Invalid pre-commit validation result: {}",
        message.as_ref()
    ))
}

/// Reads the fixed summary projection in one visit, checking rules in selection order.
// Only column metadata needs a static lifetime. Getters stay borrowed inside `visit`, where error
// fields are copied into owned Scalars before constructing the error.
struct CheckResultVisitor<'a> {
    checks: &'a [Box<dyn DynStagedDataCheck>],
}

impl CheckResultVisitor<'_> {
    fn read<'a>(getter: &'a dyn GetData<'a>, data_type: &DataType) -> KernelResult<Scalar> {
        let value = match *data_type {
            DataType::STRING => getter
                .get_str(0, "check result")?
                .map(|value| Scalar::String(value.to_owned())),
            DataType::LONG => getter.get_long(0, "check result")?.map(Scalar::Long),
            DataType::BOOLEAN => getter.get_bool(0, "check result")?.map(Scalar::Boolean),
            _ => return Err(invalid_summary("Unsupported error-info field type")),
        };
        Ok(value.unwrap_or_else(|| Scalar::Null(data_type.clone())))
    }
}

impl RowVisitor for CheckResultVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        RESULT_COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if row_count != 1 {
            return Err(invalid_summary("Expected one check result row"));
        }
        if getters.len() != RESULT_COLUMNS.as_ref().0.len() {
            return Err(invalid_summary("Missing check result fields"));
        }
        let (columns, _) = RESULT_COLUMNS.as_ref();
        let read = |column: ColumnName, data_type: &DataType| {
            let index = columns
                .iter()
                .position(|name| *name == column)
                .ok_or_else(|| {
                    invalid_summary(format!("Missing check result column '{column}'"))
                })?;
            Self::read(getters[index], data_type)
        };
        for check in self.checks {
            let valid_column = ColumnName::new([format!("{}{IS_VALID_SUFFIX}", check.name())]);
            match read(valid_column, &DataType::BOOLEAN)? {
                Scalar::Boolean(true) => continue,
                Scalar::Boolean(false) => (),
                _ => return Err(invalid_summary("Missing check validity")),
            }
            let error_name = format!("{}{ERROR_SUFFIX}", check.name());
            let schema = check.output_schema();
            let values = schema
                .fields()
                .map(|field| {
                    read(
                        ColumnName::new([error_name.as_str(), field.name()]),
                        field.data_type(),
                    )
                })
                .collect::<KernelResult<Vec<_>>>()?;
            let sample = StructData::try_new(schema, values)?;
            return Err(check.decode_error(sample)?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
