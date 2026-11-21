//! Validates staged file actions with a shared plan.
//!
//! The plan unions staged adds and removes, projects nullable error structs, then aggregates the
//! first failure for each check. [`CheckResults`] supplies the schema and typed decoder. A present
//! error struct remains a failure even when all its fields are null. Validation returns the first
//! failing check in selection order.

mod checks;

use std::sync::{Arc, LazyLock};

pub(super) use checks::{
    AddFileFieldsCheck, AppendOnlyCheck, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};
use checks::{
    AddFileFieldsError, AppendOnlyError, ClusteringStatsError, NumRecordsError,
    RemoveFileFieldsError,
};
use delta_kernel_derive::{ToSchema, TryFromStructData};

use crate::expressions::{ColumnName, Expression, StructData, col, null_lit};
use crate::plans::ir::nodes::{Agg, Aggregate};
use crate::plans::{Plan, PlanBuilder, RelationRef};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema, lazy_schema_ref};
use crate::struct_patch::StructPatchBuilder;
use crate::{KernelError, KernelResult};

/// Checks selected from the transaction's effective configuration, in validation order.
pub(super) type CheckList = Vec<Box<dyn DynStagedDataCheck>>;

/// One optional failure per check; several checks may fail in the same summary.
#[derive(ToSchema, TryFromStructData)]
pub(super) struct CheckResults {
    add_fields: Option<AddFileFieldsError>,
    remove_fields: Option<RemoveFileFieldsError>,
    append_only: Option<AppendOnlyError>,
    num_records: Option<NumRecordsError>,
    clustering_stats: Option<ClusteringStatsError>,
}

/// Defines a row-level pre-commit rule over the shared input schema.
///
/// Each check projects a null error struct if the check passed, or a present error struct if the
/// check failed, even when all error fields are null.
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
pub(super) trait DynStagedDataCheck {
    /// Returns the fixed name used to prefix this check's result columns.
    fn name(&self) -> &'static str;
    /// Returns the check's error projection.
    fn project_error(&self) -> Expression;
    /// Constructs an error from this check's optional summary payload.
    fn decode_error(&self, result: &mut CheckResults) -> KernelResult<Option<KernelError>>;
}

macro_rules! impl_dyn_check {
    ($($check:ty => $field:ident: $name:literal),* $(,)?) => {
        fn result_field_name(name: &str) -> Option<&'static str> {
            match name {
                $(<$check as StagedDataCheck>::NAME => Some($name),)*
                _ => None,
            }
        }

        $(
        impl DynStagedDataCheck for $check {
            fn name(&self) -> &'static str {
                Self::NAME
            }

            fn project_error(&self) -> Expression {
                StagedDataCheck::project_error(self)
            }

            fn decode_error(&self, result: &mut CheckResults) -> KernelResult<Option<KernelError>> {
                result.$field.take().map(|sample| self.build_error(sample)).transpose()
            }
        }
        )*
    };
}

impl_dyn_check! {
    AddFileFieldsCheck => add_fields: "addFields",
    RemoveFileFieldsCheck => remove_fields: "removeFields",
    AppendOnlyCheck => append_only: "appendOnly",
    NumRecordsCheck => num_records: "numRecords",
    ClusteringStatsCheck => clustering_stats: "clusteringStats",
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

    let schema = CheckResults::schema_ref();
    let expressions = schema.fields().map(|field| {
        checks
            .iter()
            .find(|check| result_field_name(check.name()) == Some(field.name().as_str()))
            .map(|check| check.project_error())
            .unwrap_or_else(|| null_lit(field.data_type().clone()))
    });
    let input = input.project(Expression::struct_from(expressions), schema.clone())?;

    let mut aggregate = Aggregate::ungrouped(schema.clone());
    for field in schema.fields() {
        let name = field.name();
        aggregate = aggregate.aggregate_as(Agg::first_non_null(ColumnName::new([name])), name);
    }
    input.aggregate(aggregate)?.build()
}

/// Returns the first failing check in selection order.
pub(super) fn validate_result(
    mut result: CheckResults,
    checks: &[Box<dyn DynStagedDataCheck>],
) -> KernelResult<()> {
    for check in checks {
        if let Some(error) = check.decode_error(&mut result)? {
            return Err(error);
        }
    }
    Ok(())
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

/// Common action fields. Plan construction replaces `add.stats` with the table's statistics schema.
static INPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "add": {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": { nullable "numRecords": LONG },
    },
    nullable "remove": {
        nullable "path": STRING,
        nullable "size": LONG,
    },
};

#[cfg(test)]
mod tests;
