//! Validates staged file actions for a declarative transaction.
//!
//! The main components of this module are:
//!
//! 1. A shared plan builder (`build_plan`) which unions all staged inputs and feeds them to a set
//!    of checks.
//! 2. A row-level check framework (see `StagedDataCheck` trait) which defines an interface for each
//!    check to declare its inputs, outputs, and validation logic.
//! 3. A shared validation function (see `validate_result`) which validates the result of the plan
//!    against the set of checks.
//!
//! ## Core Plan Shape
//!
//! The overall pre-commit validation plan has the following shape:
//!
//! 1. UNION staged inputs (e.g. adds and removes) into a single input stream.
//! 2. PROJECT the input stream into rows containing a single column for each check (dictated by the
//!    check's `project_error` expression).
//! 3. AGGREGATE into a single row containing a single column for each check (taking the
//!    FIRST_NON_NULL value for each check).
//! 4. PROJECT the aggregate row into two columns per check:
//!    - a non-null `is_valid` Boolean indicating whether the check passed or failed
//!    - a nullable error struct (dictated by the check's `output_schema`) containing error
//!      information for the failed check
//!
//! The single-row output of the plan is then visited by the `validate_result` function, which
//! returns an error with the first failing check.

mod checks;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

pub(super) use checks::{
    AddFileFieldsCheck, AppendOnlyCheck, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};

use crate::engine_data::{GetData, RowVisitor};
use crate::expressions::{ColumnName, Expression, Predicate, Scalar, StructData, null_lit};
use crate::plans::ir::nodes::{Agg, Aggregate};
use crate::plans::{Plan, PlanBuilder, RelationRef};
use crate::schema::{DataType, StructField, StructType};
use crate::{EngineData, KernelError, KernelResult, KernelResultIterator};

pub(super) type CheckList = Vec<Box<dyn StagedDataCheck>>;

/// Defines inputs, outputs, and error expression for performing a single, row-level, pre-commit
/// check.
///
/// Each check projects a null error struct if the check passed, or a present error struct if the
/// check failed.
pub(super) trait StagedDataCheck {
    /// Returns a unique name for this check (used to prefix this check's result columns).
    fn name(&self) -> &str;
    /// Returns the list of input columns used by this check.
    fn input_cols(&self) -> Vec<(ColumnName, DataType)>;
    /// Returns the flat error-info schema, whose fields must be STRING, LONG, or BOOLEAN.
    fn output_schema(&self) -> StructType;
    /// Returns an expression which projects a null error struct if the check passed, or a present
    /// error struct if the check failed (output should match `output_schema`).
    fn project_error(&self) -> Expression;
    /// Constructs an error from a non-null error struct containing the projected fields from the
    /// failing row.
    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError>;
}

/// Builds a declarative plan that resolves down to a single-row summary containing results from the
/// given `checks`. See the module documentation for more details on the plan shape.
pub(super) fn build_plan(
    adds: &[RelationRef],
    removes: &[RelationRef],
    checks: &[Box<dyn StagedDataCheck>],
) -> KernelResult<Plan> {
    if checks.is_empty() {
        return Err(KernelError::internal_error(
            "Pre-commit validation requires checks",
        ));
    }
    let input = build_input(adds, removes, checks)?;

    // Project the input with each of the check's error expressions.
    // The resulting schema is a struct with one field per check, each field named as
    // "<check_name>.err"
    let error_schema = Arc::new(StructType::try_new(checks.iter().map(|check| {
        StructField::nullable(format!("{}.err", check.name()), check.output_schema())
    }))?);
    let expressions = checks.iter().map(|check| check.project_error());
    let input = input.project(Expression::struct_from(expressions), error_schema.clone())?;

    // Aggregate the results into a single row containing a single column for each check (taking the
    // FIRST_NON_NULL value for each check).
    let mut aggregate = Aggregate::ungrouped(error_schema);
    for check in checks {
        let err_name = format!("{}.err", check.name());
        aggregate =
            aggregate.aggregate_as(Agg::first_non_null(ColumnName::new([&err_name])), &err_name);
    }

    // Project the aggregate row into one `<check_name>.is_valid` and one `<check_name>.err` column
    // per check.
    let summary_schema = Arc::new(StructType::try_new(checks.iter().flat_map(|check| {
        [
            StructField::not_null(format!("{}.is_valid", check.name()), DataType::BOOLEAN),
            StructField::nullable(format!("{}.err", check.name()), check.output_schema()),
        ]
    }))?);
    let summary_exprs = checks.iter().flat_map(|check| {
        let error = Expression::column([format!("{}.err", check.name())]);
        [Predicate::is_null(error.clone()).into(), error]
    });
    input
        .aggregate(aggregate)?
        .project(Expression::struct_from(summary_exprs), summary_schema)?
        .build()
}

/// Validates the resolved output of running the pre-commit validation plan (from `build_plan`)
/// using the given `checks`.
/// Empty batches are ignored, but the stream must contain exactly one row in total. Stream errors
/// are propagated even when they occur after the summary row.
pub(super) fn validate_result(
    results: impl KernelResultIterator<Box<dyn EngineData>>,
    checks: &[Box<dyn StagedDataCheck>],
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
    for check in checks {
        CheckResultVisitor::validate_check(batch.as_ref(), check.as_ref())?;
    }
    Ok(())
}

fn build_input(
    adds: &[RelationRef],
    removes: &[RelationRef],
    checks: &[Box<dyn StagedDataCheck>],
) -> KernelResult<PlanBuilder> {
    let mut columns: BTreeMap<_, _> = checks.iter().flat_map(|check| check.input_cols()).collect();
    // Ensure there is at least one column for each action kind (add or remove) so that the presence
    // of the action distinguishes the action-kind.
    columns
        .entry(ColumnName::new(["add", "path"]))
        .or_insert(DataType::STRING);
    columns
        .entry(ColumnName::new(["remove", "path"]))
        .or_insert(DataType::STRING);
    let columns: Vec<_> = columns
        .iter()
        .map(|(column, data_type)| (column.as_ref(), data_type.clone()))
        .collect();

    // Project each action kind into the same shared schema, ensuring that all request input columns
    // are present and nested under the appropriate action kind.
    let schema = Arc::new(build_nested_schema(&columns)?);
    let add_type = schema.field_at(&ColumnName::new(["add"]))?.data_type();
    let remove_type = schema.field_at(&ColumnName::new(["remove"]))?.data_type();
    let add_row = Expression::struct_from([
        build_nested_expr(ColumnName::default(), add_type),
        null_lit(remove_type.clone()),
    ]);
    let remove_row = Expression::struct_from([
        null_lit(add_type.clone()),
        build_nested_expr(ColumnName::default(), remove_type),
    ]);
    let mut inputs = Vec::new();
    for (relations, row) in [(adds, add_row), (removes, remove_row)] {
        for relation in relations {
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

// Builds a nested schema from leaf paths, merging shared prefixes into the same struct.
// For example, stats.minValues.x and stats.minValues.y share stats and minValues structs.
fn build_nested_schema(columns: &[(&[String], DataType)]) -> KernelResult<StructType> {
    let mut fields = BTreeMap::new();
    let mut children = BTreeMap::<_, Vec<_>>::new();
    for (path, data_type) in columns {
        let (name, rest) = path
            .split_first()
            .ok_or_else(|| KernelError::schema("Empty pre-commit input column path"))?;
        if rest.is_empty() {
            fields.insert(name, StructField::nullable(name, data_type.clone()));
        } else {
            children
                .entry(name)
                .or_default()
                .push((rest, data_type.clone()));
        }
    }
    for (name, columns) in children {
        fields.insert(
            name,
            StructField::nullable(name, build_nested_schema(&columns)?),
        );
    }
    StructType::try_new(fields.into_values())
}

// Starting from an empty source path, build one struct expression per shared parent in the
// output schema. Leaves read the original relation path without the `add` or `remove` prefix.
// For example, the output `add.stats.numRecords` leaf reads `stats.numRecords` from an add
// relation; `add_row` wraps that source expression under the output `add` struct.
fn build_nested_expr(column: ColumnName, data_type: &DataType) -> Expression {
    if let DataType::Struct(schema) = data_type {
        return Expression::struct_from(schema.fields().map(|field| {
            let path = ColumnName::new(column.iter().chain(std::iter::once(field.name())));
            build_nested_expr(path, field.data_type())
        }));
    }
    Expression::column(column)
}

fn invalid_summary(message: impl AsRef<str>) -> KernelError {
    KernelError::internal_error(format!(
        "Invalid pre-commit validation result: {}",
        message.as_ref()
    ))
}

/// A RowVisitor which reads a single check result column from a single EngineData batch.
/// Used by `validate_result` to parse the expected output for a check and build an error if the
/// check failed.
struct CheckResultVisitor {
    data_type: &'static DataType,
    value: Option<Scalar>,
}

impl CheckResultVisitor {
    fn validate_check(batch: &dyn EngineData, check: &dyn StagedDataCheck) -> KernelResult<()> {
        let valid_column = ColumnName::new([format!("{}.is_valid", check.name())]);
        match Self::read(batch, valid_column, &DataType::BOOLEAN)? {
            Scalar::Boolean(true) => return Ok(()),
            Scalar::Boolean(false) => (),
            _ => return Err(invalid_summary("Missing check validity")),
        }
        let error_name = format!("{}.err", check.name());
        let fields: Vec<_> = check.output_schema().fields().cloned().collect();
        let values = fields
            .iter()
            .map(|field| {
                Self::read(
                    batch,
                    ColumnName::new([error_name.as_str(), field.name()]),
                    field.data_type(),
                )
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let sample = StructData::try_new(fields, values)?;
        Err(check.build_error(&sample)?)
    }

    fn read(
        batch: &dyn EngineData,
        column: ColumnName,
        data_type: &DataType,
    ) -> KernelResult<Scalar> {
        let data_type = match *data_type {
            DataType::STRING => &DataType::STRING,
            DataType::LONG => &DataType::LONG,
            DataType::BOOLEAN => &DataType::BOOLEAN,
            _ => return Err(invalid_summary("Unsupported error-info field type")),
        };
        let mut visitor = Self {
            data_type,
            value: None,
        };
        batch.visit_rows(&[column], &mut visitor)?;
        visitor
            .value
            .ok_or_else(|| invalid_summary("Missing check result"))
    }
}

impl RowVisitor for CheckResultVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMN: LazyLock<[ColumnName; 1]> = LazyLock::new(|| [ColumnName::new(["value"])]);
        (&*COLUMN, std::slice::from_ref(self.data_type))
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [getter] = getters else {
            return Err(invalid_summary("Expected one check result field"));
        };
        if row_count != 1 {
            return Err(invalid_summary("Expected one check result row"));
        }
        let value = match *self.data_type {
            DataType::STRING => getter
                .get_str(0, "check result")?
                .map(|value| Scalar::String(value.to_owned())),
            DataType::LONG => getter.get_long(0, "check result")?.map(Scalar::Long),
            DataType::BOOLEAN => getter.get_bool(0, "check result")?.map(Scalar::Boolean),
            _ => return Err(invalid_summary("Unsupported error-info field type")),
        };
        self.value = Some(value.unwrap_or_else(|| Scalar::Null(self.data_type.clone())));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
