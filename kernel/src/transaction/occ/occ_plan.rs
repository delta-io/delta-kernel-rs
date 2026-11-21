//! The OCC plan returns one row for each check and version whose file actions conflict with T.
//!
//! ```text
//! read_append:   ScanJson -> Filter(add) -> Project(part) -> Filter(predicate) -> Project -+
//! read_delete:   ScanJson -> Filter(remove) -+-> SemiJoin(read_files) -> Project ----------+
//! delete_delete:                             +-> SemiJoin(file_actions) -> Project --------+
//!                                                          UnionAll -> Aggregate(check, version)
//! ```
//!
//! Project(part) and Filter(predicate) run only for a partitioned `ReadScope::Files` with a
//! predicate, and SemiJoin(read_files) runs only for `ReadScope::Files`. No check filters on
//! `dataChange`.
//!
//! For `ReadScope::Nothing`, `select_read_append_versions` selects no version and `read_delete` has
//! no read files. `delete_delete` still runs, as a backstop for inconsistent facts.

use std::sync::LazyLock;

use strum::{EnumString, IntoStaticStr};

use super::winner_check_plan::is_row_tracking_backfill_or_unbackfill;
use super::{
    Conflict, ConflictKind, LoserFacts, ReadScope, WinnerSummary, WinningCommit, conflict_at,
    scan_winners, to_version, visit_batches,
};
use crate::action_reconciliation::file_metadata::PARTITION_VALUES_FIELD;
use crate::action_reconciliation::plan::VERSION;
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, Expression, col, column_name, lit};
use crate::plans::{Plan, PlanBuilder, PlanResult, RelationRef};
use crate::scan::PrefixColumns;
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, lazy_schema_ref, try_schema,
};
use crate::table_properties::IsolationLevel;
use crate::transforms::ExpressionTransform as _;
use crate::{KernelError, KernelResult, Version};

const CHECK: &str = "check";
const PART: &str = "part";
const PATH: &str = "path";

static WINNER_ADDS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (ADD_NAME): {
        not_null (PARTITION_VALUES_FIELD): { STRING => nullable STRING },
    },
};

static WINNER_REMOVES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    nullable (REMOVE_NAME): {
        not_null (PATH): STRING,
    },
};

static VIOLATION_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null (VERSION): LONG,
    not_null (CHECK): STRING,
};

/// A file-level check. The variant order is the order in which Kernel reports one version's checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumString, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum OccCheck {
    /// The winner adds a file that T should have read.
    ReadAppend,
    /// The winner removes a file that T read.
    ReadDelete,
    /// The winner removes a file that T also removes.
    DeleteDelete,
}

/// Builds the OCC plan over `winners`.
///
/// `read_append` scans only the winners at `read_append_versions`. `file_actions` must have a
/// `remove.path` column.
pub(super) fn occ_plan(
    facts: &LoserFacts,
    winners: &[WinningCommit],
    read_append_versions: &[Version],
    file_actions: &RelationRef,
) -> KernelResult<Plan> {
    let read_append = read_append_branch(&facts.read_scope, winners, read_append_versions)?;
    let removes = winner_removes(winners)?;
    let read_delete = read_delete_branch(&facts.read_scope, removes.clone())?;
    let delete_delete = delete_delete_branch(removes, file_actions)?;
    PlanBuilder::union_all([
        violations(read_append, OccCheck::ReadAppend)?,
        violations(read_delete, OccCheck::ReadDelete)?,
        violations(delete_delete, OccCheck::DeleteDelete)?,
    ])?
    // One row for each (check, version). Nothing reads the count.
    .aggregate_by([column_name!(CHECK), column_name!(VERSION)], |aggs| {
        aggs.count_star()
    })?
    .build()
}

/// Returns the winner versions that `read_append` checks.
///
/// If T read nothing, or T's effective isolation level is SnapshotIsolation, there are none. Under
/// WriteSerializable, a blind-append winner counts only if T changes metadata. A row-tracking
/// backfill or unbackfill never counts.
pub(super) fn select_read_append_versions(
    facts: &LoserFacts,
    summaries: &[WinnerSummary],
) -> Vec<Version> {
    let isolation_level = effective_isolation_level(facts);
    if facts.read_scope.is_nothing() || isolation_level == IsolationLevel::SnapshotIsolation {
        return Vec::new();
    }
    Vec::from_iter(
        summaries
            .iter()
            .filter(|summary| {
                let is_backfill = summary
                    .operation
                    .as_deref()
                    .is_some_and(is_row_tracking_backfill_or_unbackfill);
                let skipped_as_blind_append = isolation_level == IsolationLevel::WriteSerializable
                    && !facts.changes_metadata
                    && summary.is_blind_append;
                !is_backfill && !skipped_as_blind_append
            })
            .map(|summary| summary.version),
    )
}

/// Returns the earliest violation of the OCC plan, in (version, [`OccCheck`]) order.
pub(super) fn first_occ_conflict(
    result: PlanResult,
    summaries: &[WinnerSummary],
) -> KernelResult<Option<Conflict>> {
    let first = visit_batches::<OccViolationVisitor>(result)?
        .0
        .into_iter()
        .min();
    Ok(first.map(|(version, check)| conflict_at(summaries, version, ConflictKind::Occ(check))))
}

/// Winner adds that T should have read.
fn read_append_branch(
    read_scope: &ReadScope,
    winners: &[WinningCommit],
    versions: &[Version],
) -> KernelResult<PlanBuilder> {
    let checked_winners = winners
        .iter()
        .filter(|winner| versions.contains(&winner.version));
    let adds = scan_winners(checked_winners, WINNER_ADDS_SCHEMA.clone())?
        .filter(col!(ADD_NAME).is_not_null())?;
    let (partition_schema, partition_predicate, map_to_struct_options) = match read_scope {
        ReadScope::Files {
            partition_schema,
            partition_predicate: Some(partition_predicate),
            map_to_struct_options,
            ..
        } if partition_schema.num_fields() > 0 => {
            (partition_schema, partition_predicate, map_to_struct_options)
        }
        _ => return Ok(adds),
    };
    // A winner add can hold a null or no value for any partition column.
    let part_type = StructType::try_new(
        partition_schema
            .fields()
            .map(|field| StructField::nullable(field.name(), field.data_type().clone())),
    )?;
    let part_schema = try_schema! {
        not_null (VERSION): LONG,
        nullable (PART): (part_type),
    }?;
    let part = Expression::struct_from([
        col!(VERSION),
        Expression::map_to_struct(
            col!(ADD_NAME, PARTITION_VALUES_FIELD),
            map_to_struct_options.clone(),
        ),
    ]);
    let mut prefixer = PrefixColumns::new(column_name!(PART));
    let predicate = prefixer.transform_pred(partition_predicate).into_owned();
    adds.project(part, part_schema)?.filter(predicate)
}

/// Winner removes of files that T read.
fn read_delete_branch(read_scope: &ReadScope, removes: PlanBuilder) -> KernelResult<PlanBuilder> {
    match read_scope {
        ReadScope::WholeTable => Ok(removes),
        ReadScope::Files { read_files, .. } => removes.semi_join(
            PlanBuilder::relation_source(read_files.clone()),
            [column_name!(REMOVE_NAME, PATH)],
            [column_name!(PATH)],
        ),
        ReadScope::Nothing => PlanBuilder::values(WINNER_REMOVES_SCHEMA.clone(), vec![]),
    }
}

/// Winner removes of paths that T also removes.
fn delete_delete_branch(
    removes: PlanBuilder,
    file_actions: &RelationRef,
) -> KernelResult<PlanBuilder> {
    let loser_removes = PlanBuilder::relation_source(file_actions.clone())
        .filter(col!(REMOVE_NAME).is_not_null())?;
    removes.semi_join(
        loser_removes,
        [column_name!(REMOVE_NAME, PATH)],
        [column_name!(REMOVE_NAME, PATH)],
    )
}

fn winner_removes(winners: &[WinningCommit]) -> KernelResult<PlanBuilder> {
    scan_winners(winners, WINNER_REMOVES_SCHEMA.clone())?.filter(col!(REMOVE_NAME).is_not_null())
}

fn violations(branch: PlanBuilder, check: OccCheck) -> KernelResult<PlanBuilder> {
    let name: &'static str = check.into();
    branch.project(
        Expression::struct_from([col!(VERSION), lit(name)]),
        VIOLATION_SCHEMA.clone(),
    )
}

/// Returns the isolation level that `read_append` uses.
///
/// T drops to SnapshotIsolation when no file action has `dataChange=true` and T writes only file
/// actions, `delta.rowTracking` domain metadata and `commitInfo`. Under WriteSerializable, T's
/// operation must also change no data.
fn effective_isolation_level(facts: &LoserFacts) -> IsolationLevel {
    let blocks_downgrade = facts.has_other_actions
        || facts.changes_metadata
        || facts.written_domains_except_row_tracking().next().is_some();
    let can_downgrade = !facts.has_data_change
        && !blocks_downgrade
        && match facts.isolation_level {
            IsolationLevel::Serializable => true,
            IsolationLevel::WriteSerializable => !facts.operation_changes_data,
            IsolationLevel::SnapshotIsolation => false,
        };
    if can_downgrade {
        IsolationLevel::SnapshotIsolation
    } else {
        facts.isolation_level
    }
}

#[derive(Default)]
struct OccViolationVisitor(Vec<(Version, OccCheck)>);

impl RowVisitor for OccViolationVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| VIOLATION_SCHEMA.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [version, check] = getters else {
            return Err(KernelError::internal_error(format!(
                "Unexpected OCC getter count {}",
                getters.len()
            )));
        };
        for row in 0..row_count {
            let check_name: &str = check.get(row, CHECK)?;
            let occ_check = check_name.parse().map_err(|_| {
                KernelError::internal_error(format!("Unknown OCC check {check_name:?}"))
            })?;
            self.0
                .push((to_version(version.get(row, VERSION)?)?, occ_check));
        }
        Ok(())
    }
}
