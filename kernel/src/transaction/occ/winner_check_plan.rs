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
use crate::expressions::{ColumnName, Expression, Predicate, col, column_name, lit, null_lit};
use crate::plans::{Plan, PlanResult};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, lazy_schema_ref};
use crate::{KernelError, KernelResult, Version};

const OPERATION: &str = "operation";
const IS_BLIND_APPEND: &str = "is_blind_append";
const IN_COMMIT_TIMESTAMP: &str = "in_commit_timestamp";
pub(super) const COMMIT_INFO_ACTIONS: &str = "commit_info_actions";

/// The output schema of the winner check plan: one row for each winner that has an action.
pub(super) static WINNER_CHECK_OUTPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
    not_null (COMMIT_INFO_ACTIONS): LONG,
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
    nullable (COMMIT_INFO_ACTIONS): LONG,
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
                .count(column_name!(COMMIT_INFO_ACTIONS))
        })?
        .build()
}

/// Reads the winner check plan output into one summary for each winner, in ascending version
/// order. The engine can return the aggregate groups in any order. `winners` must be in ascending
/// version order.
///
/// Returns an error for a winner that Kernel can't check. A winner whose commit file has no action,
/// such as an empty file, gets no summary. Each `first_non_null` aggregate picks its field on its
/// own, so a winner with more than one `commitInfo` action could get a mixed summary.
pub(super) fn read_winner_summaries(
    result: PlanResult,
    winners: &[WinningCommit],
) -> KernelResult<Vec<WinnerSummary>> {
    let WinnerSummaryVisitor {
        mut summaries,
        mut multi_commit_info_versions,
    } = visit_batches(result)?;
    summaries.sort_by_key(|summary| summary.version);
    let returned = Vec::from_iter(summaries.iter().map(|summary| summary.version));
    let expected = winner_versions(winners);
    if returned != expected {
        if let Some(missing) = expected.iter().find(|version| !returned.contains(version)) {
            return Err(KernelError::generic(format!(
                "Conflict detection read no action from the commit file of version {missing}"
            )));
        }
        return Err(KernelError::internal_error(format!(
            "The winner check returned summaries for versions {returned:?}, but the winning \
             commits are at versions {expected:?}"
        )));
    }
    if !multi_commit_info_versions.is_empty() {
        multi_commit_info_versions.sort_unstable();
        return Err(KernelError::generic(format!(
            "Conflict detection can't check the winning commits at versions \
             {multi_commit_info_versions:?}, because each has more than one `commitInfo` action"
        )));
    }
    Ok(summaries)
}

/// One row for each action. Columns that don't apply to the action are NULL.
fn action_row_expr() -> Expression {
    Expression::struct_from([
        col!(VERSION),
        col!(COMMIT_INFO_NAME, "operation"),
        col!(COMMIT_INFO_NAME, "isBlindAppend"),
        col!(COMMIT_INFO_NAME, "inCommitTimestamp"),
        marker(col!(COMMIT_INFO_NAME).is_not_null()),
    ])
}

/// 1 when `predicate` holds, NULL otherwise, so `count` over a marker column gives the matches.
fn marker(predicate: Predicate) -> Expression {
    Expression::case_when([(predicate, lit(1i64))], null_lit(DataType::LONG))
}

#[derive(Default)]
struct WinnerSummaryVisitor {
    summaries: Vec<WinnerSummary>,
    multi_commit_info_versions: Vec<Version>,
}

impl RowVisitor for WinnerSummaryVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| WINNER_CHECK_OUTPUT_SCHEMA.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [
            version,
            operation,
            is_blind_append,
            in_commit_timestamp,
            commit_info_actions,
        ] = getters
        else {
            return Err(KernelError::internal_error(format!(
                "Unexpected winner check getter count {}",
                getters.len()
            )));
        };
        for row in 0..row_count {
            let version = to_version(version.get(row, VERSION)?)?;
            let commit_info_actions: i64 = commit_info_actions.get(row, COMMIT_INFO_ACTIONS)?;
            if commit_info_actions > 1 {
                self.multi_commit_info_versions.push(version);
            }
            self.summaries.push(WinnerSummary {
                version,
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
