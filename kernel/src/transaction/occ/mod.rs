//! Conflict detection for a transaction that lost a commit race.
//!
//! T is the losing transaction. The winners are the commits after T's read version. Kernel checks
//! T against the winners with declarative plans:
//!
//! 1. The winner check plan ([`winner_check_plan()`]) returns one row for each winner. Each row
//!    holds the winner's commit facts and one flag for each winner rule.
//! 2. If no winner breaks a winner rule, the OCC plan ([`occ_plan()`]) checks the winners' file
//!    actions. It returns one row for each check and version with a violation.
//!
//! The plan builders only describe the plans. [`decision`] reads the rows and decides the result.
//! [`detect_conflicts()`] runs the two plans in order. Kernel reports the earliest winner-rule
//! violation before any OCC violation.

mod decision;
mod occ_plan;
mod winner_check_plan;

#[cfg(test)]
mod real_table_tests;
#[cfg(test)]
mod tests;

use decision::{
    Conflict, WinnerSummary, first_occ_conflict, first_winner_conflict, read_occ_violations,
    read_winner_summaries, select_occ_branch_versions,
};
use occ_plan::occ_plan;
use winner_check_plan::winner_check_plan;

use crate::commit_range::CommitRange;
use crate::expressions::MapToStructOptions;
use crate::plans::ir::nodes::ScanFile;
use crate::plans::{Operation, RelationRef, ScopedPlanExecutor};
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::SchemaRef;
use crate::table_properties::IsolationLevel;
use crate::{FileMeta, KernelError, KernelResult, PredicateRef, Version, version_as_i64};

/// Decides whether T conflicts with any of `winners`.
///
/// `file_actions` holds T's file actions with a `remove.path` column. Retain it in the same
/// `ScopedPlanExecutor` scope that `executor` belongs to. Each `path` must be exactly as written in
/// the log.
pub(crate) fn detect_conflicts(
    executor: &dyn ScopedPlanExecutor,
    facts: &LoserFacts,
    winners: &[WinningCommit],
    file_actions: &RelationRef,
) -> KernelResult<ConflictDetection> {
    check_winners_follow_read_version(facts.read_version, winners)?;
    let summaries = match winner_check_plan(facts, winners)? {
        Some(plan) => read_winner_summaries(executor.execute_op(Operation::QueryPlan(plan))?)?,
        None => Vec::new(),
    };
    check_winner_rows(winners, &summaries)?;
    let conflict = match first_winner_conflict(&summaries) {
        Some(conflict) => Some(conflict),
        None => occ_conflict(executor, facts, winners, &summaries, file_actions)?,
    };
    match conflict {
        Some(conflict) => Ok(ConflictDetection::Conflict(conflict)),
        None => {
            check_row_tracking_marks(&summaries)?;
            Ok(ConflictDetection::NoConflict(summaries))
        }
    }
}

/// The result of conflict detection.
#[derive(Debug)]
pub(crate) enum ConflictDetection {
    /// No winner conflicts with T. The summaries hold each winner's facts, including its row ID
    /// high-water mark.
    NoConflict(Vec<WinnerSummary>),
    /// A winner conflicts with T.
    Conflict(Conflict),
}

/// A commit that took a version after T's read version.
#[derive(Debug, Clone)]
pub(crate) struct WinningCommit {
    pub(crate) version: Version,
    pub(crate) file: FileMeta,
}

impl WinningCommit {
    /// Returns the commit files of `range` in ascending version order.
    pub(crate) fn from_commit_range(range: &CommitRange) -> KernelResult<Vec<Self>> {
        range
            .commit_files()?
            .map(|result| {
                result.map(|commit| Self {
                    version: commit.version,
                    file: commit.location,
                })
            })
            .collect()
    }

    /// Returns the commit file as a scan input, with `version` as a constant on every row.
    pub(crate) fn scan_file(&self) -> KernelResult<ScanFile> {
        Ok(ScanFile {
            meta: self.file.clone(),
            file_constants: vec![version_as_i64(self.version)?.into()],
        })
    }
}

/// The facts about T that conflict detection needs.
#[derive(Debug, Clone)]
pub(crate) struct LoserFacts {
    /// T's read version. The winners start at `read_version + 1`.
    pub(crate) read_version: Version,
    /// The table's isolation level. [`LoserFacts::effective_isolation_level`] applies the
    /// downgrade.
    pub(crate) isolation_level: IsolationLevel,
    /// T only adds files, so it reads nothing. Pass `ReadScope::Files` with an empty `read_files`,
    /// not `WholeTable`.
    pub(crate) is_blind_append: bool,
    /// At least one file action of T has `dataChange=true`.
    pub(crate) has_data_change: bool,
    /// T's operation changes table data. This matches Delta Spark's
    /// `DeltaOperations.Operation.changesData`.
    pub(crate) operation_changes_data: bool,
    /// T writes a `metaData` action.
    pub(crate) changes_metadata: bool,
    /// T writes an action other than a file action or `delta.rowTracking` domain metadata.
    /// A `commitInfo` action does not count.
    pub(crate) has_other_actions: bool,
    /// T's operation is a row-tracking backfill.
    pub(crate) is_row_tracking_backfill: bool,
    /// T removes a file or adds a file again.
    pub(crate) rewrites_files: bool,
    /// The domains that T writes. A removal tombstone (`removed: true`) counts as a write.
    pub(crate) written_domains: Vec<String>,
    /// The app IDs whose `txn` version T read.
    pub(crate) read_app_ids: Vec<String>,
    /// The part of the table that T read.
    pub(crate) read_scope: ReadScope,
}

impl LoserFacts {
    /// Returns the isolation level that the `read_append` check uses.
    ///
    /// T drops to SnapshotIsolation when no file action has `dataChange=true` and T writes no
    /// other action. A metadata change or a domain other than `delta.rowTracking` is another
    /// action, even when `has_other_actions` is false. Under WriteSerializable, T's operation must
    /// also change no data.
    pub(crate) fn effective_isolation_level(&self) -> IsolationLevel {
        let has_other_actions = self.has_other_actions
            || self.changes_metadata
            || self
                .written_domains
                .iter()
                .any(|domain| domain != ROW_TRACKING_DOMAIN_NAME);
        let can_downgrade = !self.has_data_change
            && !has_other_actions
            && match self.isolation_level {
                IsolationLevel::Serializable | IsolationLevel::SnapshotIsolation => true,
                IsolationLevel::WriteSerializable => !self.operation_changes_data,
            };
        match can_downgrade {
            true => IsolationLevel::SnapshotIsolation,
            false => self.isolation_level,
        }
    }
}

/// The part of the table that T read.
#[derive(Debug, Clone)]
pub(crate) enum ReadScope {
    /// T read the whole table. Any winner add or remove can conflict with T.
    WholeTable,
    /// T read the files in `read_files`.
    Files {
        /// The files that T's scans returned, with a `path` column. Each `path` must be exactly
        /// as written in the log. Retain it in the same `ScopedPlanExecutor` scope that
        /// `detect_conflicts` receives.
        read_files: RelationRef,
        /// T's read-time partition schema, keyed by physical names.
        partition_schema: SchemaRef,
        /// The partition conjuncts of T's read predicate, over the physical partition columns.
        ///
        /// `None` matches every partition. A scan without a filter reads every partition, so it
        /// passes `None`. With several scans, pass the OR of their partition conjuncts, or `None`
        /// when any scan has none. On a partitioned table, a T that read no data passes
        /// `Some(Predicate::FALSE)`, because Delta Spark finds no append conflict then. On an
        /// unpartitioned table, the predicate is ignored and every add matches.
        partition_predicate: Option<PredicateRef>,
        /// Parses offset-less TIMESTAMP partition values in `partition_schema`. Use the options of
        /// T's scan.
        map_to_struct_options: MapToStructOptions,
    },
}

// === Output columns ===

/// The winner version column, shared by the winner check plan and the OCC plan.
pub(crate) const VERSION: &str = "version";

// === Helpers ===

/// Returns an internal error unless the winners start at `read_version + 1` and their versions
/// ascend with no gap.
///
/// Runs before any plan, so a bad winner list fails before any I/O.
fn check_winners_follow_read_version(
    read_version: Version,
    winners: &[WinningCommit],
) -> KernelResult<()> {
    let starts_after_read_version = match winners.first() {
        Some(first) => read_version.checked_add(1) == Some(first.version),
        None => true,
    };
    let contiguous = winners.windows(2).all(|pair| {
        matches!(pair, [previous, next] if previous.version.checked_add(1) == Some(next.version))
    });
    if !(starts_after_read_version && contiguous) {
        return Err(KernelError::internal_error(
            "Winning commits must start at the read version + 1 with no version gap",
        ));
    }
    Ok(())
}

/// Returns an internal error unless `summaries` has one row for each winner, in order.
///
/// A winner gets no row when the executor reads no action from its commit file, for example an
/// empty file. Kernel fails then, because it can't check that winner.
fn check_winner_rows(winners: &[WinningCommit], summaries: &[WinnerSummary]) -> KernelResult<()> {
    let rows = summaries.iter().map(|summary| summary.version);
    if !rows.eq(winners.iter().map(|winner| winner.version)) {
        return Err(KernelError::internal_error(
            "Winner check rows do not match the winning commits",
        ));
    }
    Ok(())
}

/// Returns an error when a winner writes `delta.rowTracking` without a readable high-water mark.
///
/// A `configuration` that fails to parse yields no mark. Kernel fails here, because the next
/// attempt would otherwise use an older mark. The error omits the version, because a parse failure
/// can null the marks of other winners in the same batch. Runs only when no winner conflicts, so
/// a conflict is reported first.
fn check_row_tracking_marks(summaries: &[WinnerSummary]) -> KernelResult<()> {
    let missing = summaries
        .iter()
        .any(|summary| summary.row_tracking_count > 0 && summary.row_id_high_water_mark.is_none());
    if missing {
        return Err(KernelError::generic(
            "a winning commit has a `delta.rowTracking` domain without a \
             readable rowIdHighWaterMark",
        ));
    }
    Ok(())
}

/// Returns the earliest OCC violation, or `None` when no OCC check fails T.
fn occ_conflict(
    executor: &dyn ScopedPlanExecutor,
    facts: &LoserFacts,
    winners: &[WinningCommit],
    summaries: &[WinnerSummary],
    file_actions: &RelationRef,
) -> KernelResult<Option<Conflict>> {
    let branches = select_occ_branch_versions(facts, summaries);
    let Some(plan) = occ_plan(facts, winners, &branches, file_actions)? else {
        return Ok(None);
    };
    let violations = read_occ_violations(executor.execute_op(Operation::QueryPlan(plan))?)?;
    Ok(first_occ_conflict(&violations, summaries))
}
