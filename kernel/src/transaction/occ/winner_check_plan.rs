//! The winner check plan returns one summary row for each winner that has an action. The row
//! includes the first winner rule that the winner breaks.
//!
//! ```text
//! ScanJson -> Project(action rows) -> Aggregate(version) -> Project(summary)
//! ```

use std::sync::LazyLock;

use strum::{EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

use super::{
    Conflict, ConflictKind, LoserFacts, WinningCommit, conflict_at, scan_winners, to_version,
    visit_batches, winner_versions,
};
use crate::action_reconciliation::plan::VERSION;
use crate::actions::{
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME,
};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, Expression, Predicate, col, column_name, lit, null_lit};
use crate::plans::{Plan, PlanResult};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, lazy_schema_ref};
use crate::{KernelError, KernelResult, Version};

const IDENTITY_ONLY_METADATA_UPDATE_TAG: &str = "delta.identity.schemaUpdate";
const IDENTITY_ONLY_METADATA_UPDATE_TAG_VALUE: &str = "true";

const OPERATION: &str = "operation";
const IS_BLIND_APPEND: &str = "is_blind_append";
const IN_COMMIT_TIMESTAMP: &str = "in_commit_timestamp";
pub(super) const COMMIT_INFO_ACTIONS: &str = "commit_info_actions";
const FIRST_VIOLATION: &str = "first_violation";

const IDENTITY_ONLY_TAG: &str = "identity_only_tag";
const PROTOCOL_ACTIONS: &str = "protocol_actions";
const METADATA_ACTIONS: &str = "metadata_actions";
const CONFLICTING_DOMAIN_ACTIONS: &str = "conflicting_domain_actions";
const CONFLICTING_TXN_ACTIONS: &str = "conflicting_txn_actions";

/// The output schema of the winner check plan: one row for each winner that has an action.
///
/// `first_violation` holds the snake_case name of the first [`WinnerRule`] that the winner breaks,
/// or NULL.
pub(super) static WINNER_CHECK_OUTPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
    not_null (COMMIT_INFO_ACTIONS): LONG,
    nullable (FIRST_VIOLATION): STRING,
};

/// The action columns that the winner check plan reads from each winner.
///
/// Every action field is nullable, so an action that lacks a canonical field still gets a row.
/// The rules read the values of `domain`, `appId` and the identity tag, and only test whether
/// `protocol` and `metaData` are present.
static WINNER_ACTIONS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (COMMIT_INFO_NAME): {
        nullable "operation": STRING,
        nullable "isBlindAppend": BOOLEAN,
        nullable "inCommitTimestamp": LONG,
        nullable "tags": {
            nullable (IDENTITY_ONLY_METADATA_UPDATE_TAG): STRING,
        },
    },
    nullable (PROTOCOL_NAME): {
        nullable "minReaderVersion": INTEGER,
    },
    nullable (METADATA_NAME): {
        nullable "id": STRING,
    },
    nullable (DOMAIN_METADATA_NAME): {
        nullable "domain": STRING,
    },
    nullable (SET_TRANSACTION_NAME): {
        nullable "appId": STRING,
    },
};

static ACTION_ROW_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
    nullable (IDENTITY_ONLY_TAG): STRING,
    nullable (COMMIT_INFO_ACTIONS): LONG,
    nullable (PROTOCOL_ACTIONS): LONG,
    nullable (METADATA_ACTIONS): LONG,
    nullable (CONFLICTING_DOMAIN_ACTIONS): LONG,
    nullable (CONFLICTING_TXN_ACTIONS): LONG,
};

/// A rule that fails T. The variant order is the order in which Kernel reports one winner's rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum WinnerRule {
    /// The winner writes `protocol`.
    ProtocolChanged,
    /// The winner writes `metaData`.
    ///
    /// It doesn't count if its `delta.identity.schemaUpdate` tag is exactly `true` and T changes no
    /// metadata.
    MetadataChanged,
    /// The winner writes a domain that T writes, other than `delta.rowTracking`.
    ///
    /// Delta Spark exempts `delta.rowTracking`, because a retry assigns T's row IDs again.
    DomainConflict,
    /// The winner writes a `txn` for an app ID that T read.
    TxnConflict,
}

/// The summary of one winner: one row of the winner check plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WinnerSummary {
    pub(crate) version: Version,
    pub(crate) operation: Option<String>,
    /// False when the winner has no `commitInfo.isBlindAppend`.
    pub(crate) is_blind_append: bool,
    pub(crate) in_commit_timestamp: Option<i64>,
    /// The first rule in [`WinnerRule`] order that the winner breaks.
    pub(crate) first_violation: Option<WinnerRule>,
}

pub(super) fn winner_check_plan(
    facts: &LoserFacts,
    winners: &[WinningCommit],
) -> KernelResult<Plan> {
    scan_winners(winners, WINNER_ACTIONS_SCHEMA.clone())?
        .project(action_row_expr(facts), ACTION_ROW_SCHEMA.clone())?
        .aggregate_by([column_name!(VERSION)], |aggs| {
            aggs.first_non_null(column_name!(OPERATION))
                .first_non_null(column_name!(IS_BLIND_APPEND))
                .first_non_null(column_name!(IN_COMMIT_TIMESTAMP))
                .first_non_null(column_name!(IDENTITY_ONLY_TAG))
                .count(column_name!(COMMIT_INFO_ACTIONS))
                .count(column_name!(PROTOCOL_ACTIONS))
                .count(column_name!(METADATA_ACTIONS))
                .count(column_name!(CONFLICTING_DOMAIN_ACTIONS))
                .count(column_name!(CONFLICTING_TXN_ACTIONS))
        })?
        .project(summary_expr(facts), WINNER_CHECK_OUTPUT_SCHEMA.clone())?
        .build()
}

/// Reads the winner check plan output into one summary for each winner, in ascending version
/// order. `winners` must be in ascending version order.
///
/// Returns an error for a winner that Kernel can't check: one whose commit file has no action,
/// such as an empty file, or one with more than one `commitInfo` action.
pub(super) fn read_winner_summaries(
    result: PlanResult,
    winners: &[WinningCommit],
) -> KernelResult<Vec<WinnerSummary>> {
    let WinnerSummaryVisitor {
        mut summaries,
        mut multi_commit_info_versions,
    } = visit_batches(result)?;
    // The engine can return the aggregate groups in any order.
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
    // Each `first_non_null` aggregate picks its field on its own, so several `commitInfo` actions
    // could give a mixed summary.
    if !multi_commit_info_versions.is_empty() {
        multi_commit_info_versions.sort_unstable();
        return Err(KernelError::generic(format!(
            "Conflict detection can't check the winning commits at versions \
             {multi_commit_info_versions:?}, because each has more than one `commitInfo` action"
        )));
    }
    Ok(summaries)
}

/// Returns the earliest winner that breaks a winner rule. `summaries` must be in version order.
pub(super) fn first_winner_conflict(summaries: &[WinnerSummary]) -> Option<Conflict> {
    let (version, rule) = summaries
        .iter()
        .find_map(|summary| Some((summary.version, summary.first_violation?)))?;
    Some(conflict_at(summaries, version, ConflictKind::Winner(rule)))
}

/// One row for each action. Columns that don't apply to the action are NULL.
fn action_row_expr(facts: &LoserFacts) -> Expression {
    let conflicting_domains = facts
        .written_domains_except_row_tracking()
        .map(|domain| col!(DOMAIN_METADATA_NAME, "domain").eq(lit(domain)));
    let conflicting_app_ids = facts
        .read_app_ids
        .iter()
        .map(|app_id| col!(SET_TRANSACTION_NAME, "appId").eq(lit(app_id.as_str())));
    Expression::struct_from([
        col!(VERSION),
        col!(COMMIT_INFO_NAME, "operation"),
        col!(COMMIT_INFO_NAME, "isBlindAppend"),
        col!(COMMIT_INFO_NAME, "inCommitTimestamp"),
        Expression::column([COMMIT_INFO_NAME, "tags", IDENTITY_ONLY_METADATA_UPDATE_TAG]),
        marker(col!(COMMIT_INFO_NAME).is_not_null()),
        marker(col!(PROTOCOL_NAME).is_not_null()),
        marker(col!(METADATA_NAME).is_not_null()),
        marker(Predicate::or_from(conflicting_domains)),
        marker(Predicate::or_from(conflicting_app_ids)),
    ])
}

fn summary_expr(facts: &LoserFacts) -> Expression {
    let first_violation = Expression::case_when(
        WinnerRule::iter()
            .map(|rule| (rule_predicate(rule, facts), lit(<&'static str>::from(rule)))),
        null_lit(DataType::STRING),
    );
    Expression::struct_from([
        col!(VERSION),
        col!(OPERATION),
        col!(IS_BLIND_APPEND),
        col!(IN_COMMIT_TIMESTAMP),
        col!(COMMIT_INFO_ACTIONS),
        first_violation,
    ])
}

/// The predicate that holds when a winner breaks `rule`.
fn rule_predicate(rule: WinnerRule, facts: &LoserFacts) -> Predicate {
    match rule {
        WinnerRule::ProtocolChanged => has_matches(PROTOCOL_ACTIONS),
        WinnerRule::MetadataChanged if facts.changes_metadata => has_matches(METADATA_ACTIONS),
        WinnerRule::MetadataChanged => Predicate::and(
            has_matches(METADATA_ACTIONS),
            col!(IDENTITY_ONLY_TAG).distinct(lit(IDENTITY_ONLY_METADATA_UPDATE_TAG_VALUE)),
        ),
        WinnerRule::DomainConflict => has_matches(CONFLICTING_DOMAIN_ACTIONS),
        WinnerRule::TxnConflict => has_matches(CONFLICTING_TXN_ACTIONS),
    }
}

fn has_matches(count: &str) -> Predicate {
    Expression::column([count]).gt(lit(0i64))
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
            first_violation,
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
            let rule_name: Option<&str> = first_violation.get_opt(row, FIRST_VIOLATION)?;
            let rule = rule_name
                .map(|name| {
                    name.parse().map_err(|_| {
                        KernelError::internal_error(format!("Unknown winner rule {name:?}"))
                    })
                })
                .transpose()?;
            self.summaries.push(WinnerSummary {
                version,
                operation: operation.get_opt(row, OPERATION)?,
                is_blind_append: is_blind_append
                    .get_opt(row, IS_BLIND_APPEND)?
                    .unwrap_or(false),
                in_commit_timestamp: in_commit_timestamp.get_opt(row, IN_COMMIT_TIMESTAMP)?,
                first_violation: rule,
            });
        }
        Ok(())
    }
}
