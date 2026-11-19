//! Builds the winner check plan.
//!
//! ```text
//! ScanJson -> Project(action rows) -> Aggregate(version) -> Project(rules)
//! ```

use std::sync::LazyLock;

use super::{LoserFacts, VERSION, WinningCommit, scan_files};
use crate::KernelResult;
use crate::actions::{
    COMMIT_INFO_NAME, DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME,
};
use crate::expressions::{Expression, Predicate, col, column_name, lit, null_lit};
use crate::plans::ir::nodes::Agg;
use crate::plans::{Plan, PlanBuilder};
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::{DataType, SchemaRef, lazy_schema_ref};

/// The `commitInfo.operation` value of a row-tracking backfill.
pub(crate) const ROW_TRACKING_BACKFILL_OPERATION: &str = "ROW TRACKING BACKFILL";
/// The `commitInfo.tags` key of an identity-only metadata update. Delta Spark uses the same key as
/// `DeltaSourceUtils.IDENTITY_COMMITINFO_TAG`.
const IDENTITY_ONLY_METADATA_UPDATE_TAG: &str = "delta.identity.schemaUpdate";
/// The tag value that marks an identity-only metadata update. Kernel matches it exactly. Delta
/// Spark also accepts `TRUE`, so Kernel fails some commits that Delta Spark accepts.
const IDENTITY_ONLY_METADATA_UPDATE_TAG_VALUE: &str = "true";

// === Output columns ===
pub(crate) const OPERATION: &str = "operation";
pub(crate) const IS_BLIND_APPEND: &str = "is_blind_append";
pub(crate) const IN_COMMIT_TIMESTAMP: &str = "in_commit_timestamp";
pub(crate) const IS_MANIFEST_COMMIT: &str = "is_manifest_commit";
pub(crate) const ROW_ID_HIGH_WATER_MARK: &str = "row_id_high_water_mark";
pub(crate) const ROW_TRACKING_COUNT: &str = "row_tracking_count";
pub(crate) const REMOVES_ROW_TRACKING: &str = "removes_row_tracking";
pub(crate) const PROTOCOL_CHANGED: &str = "protocol_changed";
pub(crate) const METADATA_CHANGED: &str = "metadata_changed";
pub(crate) const DOMAIN_CONFLICT: &str = "domain_conflict";
pub(crate) const TXN_CONFLICT: &str = "txn_conflict";
pub(crate) const BACKFILL_CONFLICT: &str = "backfill_conflict";

// === Per-action columns ===
// A marker is 1 when the action matches and NULL otherwise, so `count` gives the matches.
const IDENTITY_ONLY_TAG: &str = "identity_only_tag";
const PROTOCOL_MARKER: &str = "protocol_marker";
const METADATA_MARKER: &str = "metadata_marker";
const DOMAIN_CONFLICT_MARKER: &str = "domain_conflict_marker";
const TXN_CONFLICT_MARKER: &str = "txn_conflict_marker";
const CHECKPOINT_MARKER: &str = "checkpoint_marker";
const ROW_TRACKING_MARKER: &str = "row_tracking_marker";
const ROW_TRACKING_TOMBSTONE_MARKER: &str = "row_tracking_tombstone_marker";
const ROW_TRACKING: &str = "row_tracking";

// === Aggregated count columns ===
const PROTOCOL_COUNT: &str = "protocol_count";
const METADATA_COUNT: &str = "metadata_count";
const DOMAIN_CONFLICT_COUNT: &str = "domain_conflict_count";
const TXN_CONFLICT_COUNT: &str = "txn_conflict_count";
const CHECKPOINT_COUNT: &str = "checkpoint_count";
const ROW_TRACKING_TOMBSTONE_COUNT: &str = "row_tracking_tombstone_count";

// === JSON keys ===
const ROW_ID_HIGH_WATER_MARK_FIELD: &str = "rowIdHighWaterMark";
/// The adaptiveMetadata action of a manifest commit. `actions::CHECKPOINT_ACTION_NAME` exists only
/// with the `adaptive-metadata-in-dev` feature.
const CHECKPOINT: &str = "checkpoint";

/// The schema of the plan output: one row for each winner. Every rule column and the row-tracking
/// count are not null, so a rule expression that yields NULL fails the plan.
pub(crate) static WINNER_CHECK_OUTPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
    not_null (IS_MANIFEST_COMMIT): BOOLEAN,
    nullable (ROW_ID_HIGH_WATER_MARK): LONG,
    not_null (ROW_TRACKING_COUNT): LONG,
    not_null (REMOVES_ROW_TRACKING): BOOLEAN,
    not_null (PROTOCOL_CHANGED): BOOLEAN,
    not_null (METADATA_CHANGED): BOOLEAN,
    not_null (DOMAIN_CONFLICT): BOOLEAN,
    not_null (TXN_CONFLICT): BOOLEAN,
    not_null (BACKFILL_CONFLICT): BOOLEAN,
};

/// The winner action fields. All are nullable, so an action that lacks a canonical field still
/// gets a row. `protocol.minReaderVersion` and `metaData.id` exist only to give each struct a
/// field. The rules test the whole struct with `is_not_null`.
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
    nullable (CHECKPOINT): [ nullable {} ],
};

static ROW_TRACKING_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable (ROW_ID_HIGH_WATER_MARK_FIELD): LONG,
};

static ACTION_ROW_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (OPERATION): STRING,
    nullable (IS_BLIND_APPEND): BOOLEAN,
    nullable (IN_COMMIT_TIMESTAMP): LONG,
    nullable (IDENTITY_ONLY_TAG): STRING,
    nullable (PROTOCOL_MARKER): LONG,
    nullable (METADATA_MARKER): LONG,
    nullable (DOMAIN_CONFLICT_MARKER): LONG,
    nullable (TXN_CONFLICT_MARKER): LONG,
    nullable (CHECKPOINT_MARKER): LONG,
    nullable (ROW_TRACKING_MARKER): LONG,
    nullable (ROW_TRACKING_TOMBSTONE_MARKER): LONG,
    nullable (ROW_TRACKING): (ROW_TRACKING_SCHEMA.as_ref().clone()),
};

/// Builds the winner check plan over `winners`, or returns `None` when there are no winners.
///
/// The plan output follows [`WINNER_CHECK_OUTPUT_SCHEMA`], with one row for each winner that has
/// at least one action. T's domains and app IDs enter the plan as literals. T's `changes_metadata`
/// and `rewrites_files` select the rule expressions in Rust.
pub(crate) fn winner_check_plan(
    facts: &LoserFacts,
    winners: &[WinningCommit],
) -> KernelResult<Option<Plan>> {
    let files = scan_files(winners, winners.iter().map(|winner| winner.version))?;
    PlanBuilder::scan_json(files, &[VERSION], WINNER_ACTIONS_SCHEMA.clone())?
        .project(action_row_expr(facts), ACTION_ROW_SCHEMA.clone())?
        .aggregate_by([column_name!(VERSION)], |aggs| {
            aggs.first_non_null(column_name!(OPERATION))
                .first_non_null(column_name!(IS_BLIND_APPEND))
                .first_non_null(column_name!(IN_COMMIT_TIMESTAMP))
                .first_non_null(column_name!(IDENTITY_ONLY_TAG))
                .aggregate_as(Agg::count(column_name!(PROTOCOL_MARKER)), PROTOCOL_COUNT)
                .aggregate_as(Agg::count(column_name!(METADATA_MARKER)), METADATA_COUNT)
                .aggregate_as(
                    Agg::count(column_name!(DOMAIN_CONFLICT_MARKER)),
                    DOMAIN_CONFLICT_COUNT,
                )
                .aggregate_as(
                    Agg::count(column_name!(TXN_CONFLICT_MARKER)),
                    TXN_CONFLICT_COUNT,
                )
                .aggregate_as(
                    Agg::count(column_name!(CHECKPOINT_MARKER)),
                    CHECKPOINT_COUNT,
                )
                .aggregate_as(
                    Agg::count(column_name!(ROW_TRACKING_MARKER)),
                    ROW_TRACKING_COUNT,
                )
                .aggregate_as(
                    Agg::count(column_name!(ROW_TRACKING_TOMBSTONE_MARKER)),
                    ROW_TRACKING_TOMBSTONE_COUNT,
                )
                .aggregate_as(
                    Agg::max(column_name!(ROW_TRACKING, ROW_ID_HIGH_WATER_MARK_FIELD)),
                    ROW_ID_HIGH_WATER_MARK,
                )
        })?
        .project(rule_expr(facts), WINNER_CHECK_OUTPUT_SCHEMA.clone())?
        .build_opt()
}

/// One row for each action. Columns that do not apply to the action are NULL.
fn action_row_expr(facts: &LoserFacts) -> Expression {
    let conflicting_domains = facts
        .written_domains
        .iter()
        .filter(|domain| domain.as_str() != ROW_TRACKING_DOMAIN_NAME)
        .map(|domain| col!(DOMAIN_METADATA_NAME, "domain").eq(lit(domain.as_str())));
    let conflicting_app_ids = facts
        .read_app_ids
        .iter()
        .map(|app_id| col!(SET_TRANSACTION_NAME, "appId").eq(lit(app_id.as_str())));
    let is_row_tracking = col!(DOMAIN_METADATA_NAME, "domain").eq(lit(ROW_TRACKING_DOMAIN_NAME));
    let is_removed = Predicate::not(col!(DOMAIN_METADATA_NAME, "removed").distinct(lit(true)));
    let live_row_tracking =
        Predicate::and(is_row_tracking.clone(), Predicate::not(is_removed.clone()));
    // Only a live `delta.rowTracking` action passes its configuration to the parser, so a
    // malformed configuration of another domain can't null the marks.
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
        marker(col!(PROTOCOL_NAME).is_not_null()),
        marker(col!(METADATA_NAME).is_not_null()),
        marker(Predicate::or_from(conflicting_domains)),
        marker(Predicate::or_from(conflicting_app_ids)),
        marker(col!(CHECKPOINT).is_not_null()),
        marker(live_row_tracking),
        marker(Predicate::and(is_row_tracking, is_removed)),
        Expression::parse_json(row_tracking_configuration, ROW_TRACKING_SCHEMA.clone()),
    ])
}

/// Passes the summary columns through, then adds one boolean column for each winner rule. A rule
/// column is true when the winner breaks that rule.
fn rule_expr(facts: &LoserFacts) -> Expression {
    let identity_only = Predicate::not(
        col!(IDENTITY_ONLY_TAG).distinct(lit(IDENTITY_ONLY_METADATA_UPDATE_TAG_VALUE)),
    );
    let has_metadata = col!(METADATA_COUNT).gt(lit(0i64));
    // Delta Spark's ConflictChecker accepts an identity-only metadata update when T changes no
    // metadata.
    let metadata_changed = match facts.changes_metadata {
        true => has_metadata,
        false => Predicate::and(has_metadata, Predicate::not(identity_only)),
    };
    let is_backfill =
        Predicate::not(col!(OPERATION).distinct(lit(ROW_TRACKING_BACKFILL_OPERATION)));
    let backfill_conflict = match facts.rewrites_files {
        true => is_backfill,
        false => Predicate::FALSE,
    };
    Expression::struct_from([
        col!(VERSION),
        col!(OPERATION),
        col!(IS_BLIND_APPEND),
        col!(IN_COMMIT_TIMESTAMP),
        Expression::from_pred(col!(CHECKPOINT_COUNT).gt(lit(0i64))),
        col!(ROW_ID_HIGH_WATER_MARK),
        col!(ROW_TRACKING_COUNT),
        Expression::from_pred(col!(ROW_TRACKING_TOMBSTONE_COUNT).gt(lit(0i64))),
        Expression::from_pred(col!(PROTOCOL_COUNT).gt(lit(0i64))),
        Expression::from_pred(metadata_changed),
        Expression::from_pred(col!(DOMAIN_CONFLICT_COUNT).gt(lit(0i64))),
        Expression::from_pred(col!(TXN_CONFLICT_COUNT).gt(lit(0i64))),
        Expression::from_pred(backfill_conflict),
    ])
}

/// 1 when `predicate` holds, NULL otherwise.
fn marker(predicate: Predicate) -> Expression {
    Expression::case_when([(predicate, lit(1i64))], null_lit(DataType::LONG))
}
