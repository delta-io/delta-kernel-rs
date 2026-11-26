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
    CHECKPOINT_ACTION_NAME, CHECKPOINT_METADATA_NAME, COMMIT_INFO_NAME, DOMAIN_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME,
};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, Expression, Predicate, col, column_name, lit, null_lit};
use crate::plans::{Plan, PlanResult};
use crate::row_tracking::{
    ROW_ID_HIGH_WATER_MARK_FIELD, ROW_TRACKING_CONFIGURATION_SCHEMA, ROW_TRACKING_DOMAIN_NAME,
};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, lazy_schema_ref};
use crate::{KernelError, KernelResult, Version};

pub(super) const ROW_TRACKING_BACKFILL_OPERATION: &str = "ROW TRACKING BACKFILL";
pub(super) const ROW_TRACKING_UNBACKFILL_OPERATION: &str = "ROW TRACKING UNBACKFILL";
const ROW_TRACKING_BACKFILL_OPERATIONS: [&str; 2] = [
    ROW_TRACKING_BACKFILL_OPERATION,
    ROW_TRACKING_UNBACKFILL_OPERATION,
];
const IDENTITY_ONLY_METADATA_UPDATE_TAG: &str = "delta.identity.schemaUpdate";
const IDENTITY_ONLY_METADATA_UPDATE_TAG_VALUE: &str = "true";

const OPERATION: &str = "operation";
const IS_BLIND_APPEND: &str = "is_blind_append";
const IN_COMMIT_TIMESTAMP: &str = "in_commit_timestamp";
pub(super) const COMMIT_INFO_ACTIONS: &str = "commit_info_actions";
const IS_MANIFEST_COMMIT: &str = "is_manifest_commit";
const WRITES_ROW_TRACKING: &str = "writes_row_tracking";
const REMOVES_ROW_TRACKING: &str = "removes_row_tracking";
const FIRST_VIOLATION: &str = "first_violation";

const IDENTITY_ONLY_TAG: &str = "identity_only_tag";
const PROTOCOL_ACTIONS: &str = "protocol_actions";
const METADATA_ACTIONS: &str = "metadata_actions";
const CONFLICTING_DOMAIN_ACTIONS: &str = "conflicting_domain_actions";
const CONFLICTING_TXN_ACTIONS: &str = "conflicting_txn_actions";
const CHECKPOINT_ACTIONS: &str = "checkpoint_actions";
const ROW_TRACKING_ACTIONS: &str = "row_tracking_actions";
const ROW_TRACKING_TOMBSTONES: &str = "row_tracking_tombstones";
const ROW_TRACKING_STATE: &str = "row_tracking_state";

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
    not_null (IS_MANIFEST_COMMIT): BOOLEAN,
    nullable (ROW_ID_HIGH_WATER_MARK_FIELD): LONG,
    not_null (WRITES_ROW_TRACKING): BOOLEAN,
    not_null (REMOVES_ROW_TRACKING): BOOLEAN,
    nullable (FIRST_VIOLATION): STRING,
};

/// The action columns that the winner check plan reads from each winner.
///
/// Every action field is nullable, so an action that lacks a canonical field still gets a row.
/// The rules read the values of `domain`, `appId`, `operation` and the identity tag, and only test
/// whether `protocol`, `metaData` and `checkpoint` are present. The row-tracking checks also read
/// `removed` and `configuration`.
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
        nullable "configuration": STRING,
        nullable "removed": BOOLEAN,
    },
    nullable (SET_TRANSACTION_NAME): {
        nullable "appId": STRING,
    },
    nullable (CHECKPOINT_ACTION_NAME): [ nullable {
        nullable (CHECKPOINT_METADATA_NAME): { nullable "version": LONG },
    } ],
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
    nullable (CHECKPOINT_ACTIONS): LONG,
    nullable (ROW_TRACKING_ACTIONS): LONG,
    nullable (ROW_TRACKING_TOMBSTONES): LONG,
    nullable (ROW_TRACKING_STATE): (ROW_TRACKING_CONFIGURATION_SCHEMA.as_ref().clone()),
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
    /// The winner is a row-tracking backfill or unbackfill, and T rewrites files.
    BackfillConflict,
}

/// The summary of one winner: one row of the winner check plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WinnerSummary {
    pub(crate) version: Version,
    pub(crate) operation: Option<String>,
    /// False when the winner has no `commitInfo.isBlindAppend`.
    pub(crate) is_blind_append: bool,
    pub(crate) in_commit_timestamp: Option<i64>,
    /// The winner has a top-level `checkpoint` action.
    pub(crate) is_manifest_commit: bool,
    pub(crate) row_id_high_water_mark: Option<i64>,
    /// The winner writes a `delta.rowTracking` domain action that isn't a removal.
    pub(crate) writes_row_tracking: bool,
    pub(crate) removes_row_tracking: bool,
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
                .count(column_name!(CHECKPOINT_ACTIONS))
                .count(column_name!(ROW_TRACKING_ACTIONS))
                .count(column_name!(ROW_TRACKING_TOMBSTONES))
                .max(column_name!(
                    ROW_TRACKING_STATE,
                    ROW_ID_HIGH_WATER_MARK_FIELD
                ))
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

/// Returns an error when a winner is a manifest commit.
///
/// Its file changes can live in a content tree that the plans don't read, and its `checkpoint`
/// array holds the table state, not changes.
pub(super) fn ensure_no_manifest_commit(summaries: &[WinnerSummary]) -> KernelResult<()> {
    if let Some(summary) = summaries.iter().find(|summary| summary.is_manifest_commit) {
        return Err(KernelError::unsupported(format!(
            "Conflict detection does not support the manifest commit at version {}",
            summary.version
        )));
    }
    Ok(())
}

/// Returns an error when a winner removes the `delta.rowTracking` domain, or writes it without a
/// readable high-water mark. The next attempt needs the mark to continue T's row IDs. The error
/// lists every winner that writes `delta.rowTracking` without a readable mark.
pub(super) fn ensure_row_tracking_marks(summaries: &[WinnerSummary]) -> KernelResult<()> {
    if let Some(summary) = summaries
        .iter()
        .find(|summary| summary.removes_row_tracking)
    {
        return Err(KernelError::unsupported(format!(
            "The winning commit at version {} removes the `delta.rowTracking` domain",
            summary.version
        )));
    }
    let versions: Vec<Version> = summaries
        .iter()
        .filter(|summary| summary.writes_row_tracking && summary.row_id_high_water_mark.is_none())
        .map(|summary| summary.version)
        .collect();
    if !versions.is_empty() {
        return Err(KernelError::generic(format!(
            "Conflict detection could not read the rowIdHighWaterMark of the `delta.rowTracking` \
             domain in the winning commits at versions {versions:?}"
        )));
    }
    Ok(())
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
    let is_row_tracking = col!(DOMAIN_METADATA_NAME, "domain").eq(lit(ROW_TRACKING_DOMAIN_NAME));
    let not_removed = col!(DOMAIN_METADATA_NAME, "removed").distinct(lit(true));
    let live_row_tracking = Predicate::and(is_row_tracking.clone(), not_removed.clone());
    // Parse only the configuration of a `delta.rowTracking` action that isn't a removal. In the
    // default engine, one malformed configuration nulls the parse result of its whole batch.
    let row_tracking_configuration = Expression::case_when(
        [(
            live_row_tracking.clone(),
            col!(DOMAIN_METADATA_NAME, "configuration"),
        )],
        null_lit(DataType::STRING),
    );
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
        marker(col!(CHECKPOINT_ACTION_NAME).is_not_null()),
        marker(live_row_tracking),
        marker(Predicate::and(is_row_tracking, Predicate::not(not_removed))),
        Expression::parse_json(
            row_tracking_configuration,
            ROW_TRACKING_CONFIGURATION_SCHEMA.clone(),
        ),
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
        Expression::from_pred(has_matches(CHECKPOINT_ACTIONS)),
        col!(ROW_ID_HIGH_WATER_MARK_FIELD),
        Expression::from_pred(has_matches(ROW_TRACKING_ACTIONS)),
        Expression::from_pred(has_matches(ROW_TRACKING_TOMBSTONES)),
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
        WinnerRule::BackfillConflict if facts.rewrites_files => Predicate::or_from(
            ROW_TRACKING_BACKFILL_OPERATIONS.map(|operation| col!(OPERATION).eq(lit(operation))),
        ),
        WinnerRule::BackfillConflict => Predicate::FALSE,
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
            is_manifest_commit,
            row_id_high_water_mark,
            writes_row_tracking,
            removes_row_tracking,
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
                is_manifest_commit: is_manifest_commit.get(row, IS_MANIFEST_COMMIT)?,
                row_id_high_water_mark: row_id_high_water_mark
                    .get_opt(row, ROW_ID_HIGH_WATER_MARK_FIELD)?,
                writes_row_tracking: writes_row_tracking.get(row, WRITES_ROW_TRACKING)?,
                removes_row_tracking: removes_row_tracking.get(row, REMOVES_ROW_TRACKING)?,
                first_violation: rule,
            });
        }
        Ok(())
    }
}
