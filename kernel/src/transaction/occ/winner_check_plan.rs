//! The winner check plan returns one summary row for each winner that has an action.
//!
//! ```text
//! ScanJson -> Project(action rows) -> Aggregate(version)
//! ```

use std::sync::LazyLock;

use super::{WinningCommit, scan_winners, to_version, visit_batches, winner_versions};
use crate::action_reconciliation::plan::VERSION;
use crate::actions::COMMIT_INFO_NAME;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, Expression, col, column_name};
use crate::plans::{Plan, PlanResult};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, lazy_schema_ref};
use crate::{KernelError, KernelResult, Version};

const OPERATION: &str = "operation";
const IS_BLIND_APPEND: &str = "is_blind_append";
const IN_COMMIT_TIMESTAMP: &str = "in_commit_timestamp";

/// The output schema of the winner check plan: one row for each winner that has an action.
pub(super) static WINNER_CHECK_OUTPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
};

/// The action columns that the winner check plan reads from each winner.
///
/// Every action field is nullable, so an action that lacks a canonical field still gets a row.
static WINNER_ACTIONS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (COMMIT_INFO_NAME): {
        nullable "operation": STRING,
        nullable "isBlindAppend": BOOLEAN,
        nullable "inCommitTimestamp": LONG,
    },
};

static ACTION_ROW_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
};

/// The summary of one winner: one row of the winner check plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WinnerSummary {
    pub(crate) version: Version,
    pub(crate) operation: Option<String>,
    /// False when the winner has no `commitInfo.isBlindAppend`.
    pub(crate) is_blind_append: bool,
    pub(crate) in_commit_timestamp: Option<i64>,
}

pub(super) fn winner_check_plan(winners: &[WinningCommit]) -> KernelResult<Plan> {
    scan_winners(winners, WINNER_ACTIONS_SCHEMA.clone())?
        .project(action_row_expr(), ACTION_ROW_SCHEMA.clone())?
        .aggregate_by([column_name!(VERSION)], |aggs| {
            aggs.first_non_null(column_name!(OPERATION))
                .first_non_null(column_name!(IS_BLIND_APPEND))
                .first_non_null(column_name!(IN_COMMIT_TIMESTAMP))
        })?
        .build()
}

/// Reads the winner check plan output in ascending version order.
///
/// The engine can return the aggregate groups in any order.
pub(super) fn read_winner_summaries(result: PlanResult) -> KernelResult<Vec<WinnerSummary>> {
    let mut summaries = visit_batches::<WinnerSummaryVisitor>(result)?.0;
    summaries.sort_by_key(|summary| summary.version);
    Ok(summaries)
}

/// Returns an error unless `summaries` has one summary for each winner, in ascending version
/// order.
///
/// A winner whose commit file has no action, such as an empty file, gets no summary, and Kernel
/// can't check it.
pub(super) fn ensure_winner_summaries(
    winners: &[WinningCommit],
    summaries: &[WinnerSummary],
) -> KernelResult<()> {
    let returned = Vec::from_iter(summaries.iter().map(|summary| summary.version));
    let expected = winner_versions(winners);
    if let Some(missing) = expected.iter().find(|version| !returned.contains(version)) {
        return Err(KernelError::generic(format!(
            "Conflict detection read no action from the commit file of version {missing}"
        )));
    }
    if returned != expected {
        return Err(KernelError::internal_error(format!(
            "The winner check returned summaries for versions {returned:?}, but the winning \
             commits are at versions {expected:?}"
        )));
    }
    Ok(())
}

/// One row for each action. Columns that don't apply to the action are NULL.
fn action_row_expr() -> Expression {
    Expression::struct_from([
        col!(VERSION),
        col!(COMMIT_INFO_NAME, "operation"),
        col!(COMMIT_INFO_NAME, "isBlindAppend"),
        col!(COMMIT_INFO_NAME, "inCommitTimestamp"),
    ])
}

#[derive(Default)]
struct WinnerSummaryVisitor(Vec<WinnerSummary>);

impl RowVisitor for WinnerSummaryVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| WINNER_CHECK_OUTPUT_SCHEMA.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [version, operation, is_blind_append, in_commit_timestamp] = getters else {
            return Err(KernelError::internal_error(format!(
                "Unexpected winner check getter count {}",
                getters.len()
            )));
        };
        for row in 0..row_count {
            self.0.push(WinnerSummary {
                version: to_version(version.get(row, VERSION)?)?,
                operation: operation.get_opt(row, OPERATION)?,
                is_blind_append: is_blind_append
                    .get_opt(row, IS_BLIND_APPEND)?
                    .unwrap_or(false),
                in_commit_timestamp: in_commit_timestamp.get_opt(row, IN_COMMIT_TIMESTAMP)?,
            });
        }
        Ok(())
    }
}
