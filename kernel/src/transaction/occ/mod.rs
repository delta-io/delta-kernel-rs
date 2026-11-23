//! Conflict detection for a transaction T that lost a commit race.
//!
//! A winner is a commit after T's checked version. The checked version is T's read version on the
//! first check, then the last winner version of the previous check. [`detect_conflicts`] checks
//! every winner against the winner rules before it checks the winners' file actions against T's
//! reads and removes. So the reported winner can differ from Delta Spark's.
//!
//! Other differences from Delta Spark:
//! - Kernel returns an error for a winner with more than one `commitInfo` action. Delta Spark uses
//!   the first one.
//! - Any winner `protocol` action fails T.
//! - A `metaData` winner fails T unless its `delta.identity.schemaUpdate` tag is exactly `true` and
//!   T changes no metadata.
//! - A row-tracking backfill or unbackfill winner fails a T that rewrites any file.
//! - Detection returns an error, not a conflict, for a T or a winner that Kernel can't check.
//!
//! `detect_conflicts` runs the checks in this order: the input checks on T and on the winner list,
//! the winner summaries, the winner rules, the manifest-commit check, the OCC plan, and the
//! row-tracking check. The first check that fails decides the result. So a winner-rule conflict at
//! a later version is reported before an OCC conflict at an earlier one, and a manifest-commit
//! winner fails detection even when an OCC check would fail T.

mod occ_plan;
mod winner_check_plan;

#[cfg(test)]
mod tests;

pub(crate) use occ_plan::OccCheck;
use occ_plan::{first_occ_conflict, occ_plan, select_read_append_versions};
pub(crate) use winner_check_plan::{WinnerRule, WinnerSummary};
use winner_check_plan::{
    ensure_no_manifest_commit, ensure_row_tracking_marks, first_winner_conflict,
    read_winner_summaries, winner_check_plan,
};

use crate::action_reconciliation::plan::VERSION;
use crate::engine_data::RowVisitor;
use crate::expressions::MapToStructOptions;
use crate::plans::ir::nodes::ScanFile;
use crate::plans::{Operation, PlanBuilder, PlanResult, RelationRef, ScopedPlanExecutor};
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::SchemaRef;
use crate::table_properties::IsolationLevel;
use crate::{FileMeta, KernelError, KernelResult, PredicateRef, Version, version_as_i64};

/// Checks T against `winners`, and returns the conflict that Kernel reports or the winner
/// summaries.
///
/// An error means that Kernel couldn't check T against the winners, and the caller must fail T.
///
/// `winners` must hold every commit from `facts.checked_version + 1`, in ascending version order.
/// `file_actions` and any `ReadScope::Files` `read_files` must be retained in `executor`'s scope.
/// `file_actions` must have a `remove.path` column. The paths in both must match the `path`
/// values written in the log.
pub(crate) fn detect_conflicts(
    executor: &dyn ScopedPlanExecutor,
    facts: &LoserFacts,
    winners: &[WinningCommit],
    file_actions: &RelationRef,
) -> KernelResult<ConflictDetection> {
    ensure_supported_loser(facts)?;
    ensure_winners_follow_checked_version(facts.checked_version, winners)?;
    if winners.is_empty() {
        return Ok(ConflictDetection::NoConflict(Vec::new()));
    }
    let plan = winner_check_plan(facts, winners)?;
    let result = executor.execute_op(Operation::QueryPlan(plan))?;
    let summaries = read_winner_summaries(result, winners)?;
    if let Some(conflict) = first_winner_conflict(&summaries) {
        return Ok(ConflictDetection::Conflict(conflict));
    }
    ensure_no_manifest_commit(&summaries)?;
    let read_append_versions = select_read_append_versions(facts, &summaries);
    let plan = occ_plan(facts, winners, &read_append_versions, file_actions)?;
    let violations = executor.execute_op(Operation::QueryPlan(plan))?;
    if let Some(conflict) = first_occ_conflict(violations, &summaries)? {
        return Ok(ConflictDetection::Conflict(conflict));
    }
    ensure_row_tracking_marks(&summaries)?;
    Ok(ConflictDetection::NoConflict(summaries))
}

#[derive(Debug)]
pub(crate) enum ConflictDetection {
    /// No winner conflicts with T.
    ///
    /// Only a winner that writes the `delta.rowTracking` domain has a high-water mark in its
    /// summary. Before T commits, the caller must move T's row IDs past the winners' marks and T's
    /// in-commit timestamp past the winners' timestamps.
    NoConflict(Vec<WinnerSummary>),
    Conflict(Conflict),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Conflict {
    pub(crate) version: Version,
    /// The winner's `commitInfo.operation`, for the caller's error message.
    pub(crate) operation: Option<String>,
    pub(crate) kind: ConflictKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictKind {
    Winner(WinnerRule),
    Occ(OccCheck),
}

/// A commit that took a version after [`LoserFacts::checked_version`].
#[derive(Debug, Clone)]
pub(crate) struct WinningCommit {
    pub(crate) version: Version,
    pub(crate) file: FileMeta,
}

/// The facts about T that conflict detection needs.
///
/// Only `checked_version` changes between checks. T is never a row-tracking backfill or
/// unbackfill and never drops a feature: Kernel reserves those operation names.
#[derive(Debug, Clone)]
pub(crate) struct LoserFacts {
    /// T's read version on the first check, then the last winner version of the previous check.
    pub(crate) checked_version: Version,
    /// The table's `delta.isolationLevel`, or Serializable when the property is unset.
    ///
    /// Compare the value case-insensitively, and fail T when it matches no level.
    pub(crate) isolation_level: IsolationLevel,
    /// T writes a top-level `checkpoint` action, as an adaptiveMetadata manifest commit does.
    pub(crate) is_manifest_commit: bool,
    /// At least one of T's file actions has `dataChange=true`.
    pub(crate) has_data_change: bool,
    /// T's operation changes data, like Delta Spark's `DeltaOperations.Operation.changesData`.
    pub(crate) operation_changes_data: bool,
    /// T writes a `metaData` action.
    pub(crate) changes_metadata: bool,
    /// T writes an action other than a file action, `metaData`, `domainMetadata` or `commitInfo`,
    /// such as `protocol` or `txn`.
    ///
    /// Count `cdc` as a file action.
    pub(crate) has_other_actions: bool,
    /// T removes a file, updates a deletion vector or adds a file again.
    ///
    /// Derive it from T's staged actions. It must be true when `file_actions` holds a remove.
    pub(crate) rewrites_files: bool,
    /// The domains that T writes, including removal tombstones.
    pub(crate) written_domains: Vec<String>,
    /// The app IDs whose `txn` version T read.
    ///
    /// Include every app ID that T writes in a `txn` action: a writer reads the version first.
    pub(crate) read_app_ids: Vec<String>,
    pub(crate) read_scope: ReadScope,
}

impl LoserFacts {
    fn written_domains_except_row_tracking(&self) -> impl Iterator<Item = &str> {
        self.written_domains
            .iter()
            .map(String::as_str)
            .filter(|domain| *domain != ROW_TRACKING_DOMAIN_NAME)
    }
}

/// The part of the table that T read.
#[derive(Debug, Clone)]
pub(crate) enum ReadScope {
    /// Any winner add or remove can conflict with T.
    WholeTable,
    Files {
        /// The files that T's scans returned, with a `path` column.
        ///
        /// Include every file that T adds again, so that a winner's remove of it fails T.
        read_files: RelationRef,
        /// T's read-time partition schema, keyed by physical names.
        partition_schema: SchemaRef,
        /// The conjuncts of T's read predicate that reference only partition columns, by
        /// physical name.
        ///
        /// - If any of T's scans has no partition filter, pass `None`, which matches every add.
        /// - If T ran several filtered scans, pass the OR of each scan's partition predicate.
        /// - On an unpartitioned table, Kernel ignores it, and every checked add conflicts.
        partition_predicate: Option<PredicateRef>,
        /// The options that T's scans used to read offset-less TIMESTAMP partition values.
        ///
        /// Build them from the scans' time zone with `scan::map_to_struct_options`, as
        /// `incremental_scan` does. All of T's scans must use the same options. Kernel can't check
        /// this.
        map_to_struct_options: MapToStructOptions,
    },
    /// T ran no scan, for example a blind append. `read_app_ids` still applies. If T ran a scan,
    /// pass `Files`, even if the scan returned no file. If T removes or re-adds a file, pass
    /// `Files` or `WholeTable`.
    Nothing,
}

impl ReadScope {
    fn is_nothing(&self) -> bool {
        matches!(self, ReadScope::Nothing)
    }
}

/// Rejects a T that Kernel can't check, or that read nothing but rewrites files.
///
/// Delta Spark doesn't let a writer set SnapshotIsolation as the table level. Kernel can't check T
/// at that level. A manifest commit's `checkpoint` restates all txns and domains, so a retry would
/// drop a winner's changes.
fn ensure_supported_loser(facts: &LoserFacts) -> KernelResult<()> {
    if facts.isolation_level == IsolationLevel::SnapshotIsolation {
        return Err(KernelError::unsupported(
            "Conflict detection does not support the table isolation level SnapshotIsolation",
        ));
    }
    if facts.is_manifest_commit {
        return Err(KernelError::unsupported(
            "Conflict detection does not support a transaction that writes a manifest commit",
        ));
    }
    if facts.read_scope.is_nothing() && facts.rewrites_files {
        return Err(KernelError::internal_error(
            "A transaction that removes or re-adds files must pass the files that it read",
        ));
    }
    Ok(())
}

fn ensure_winners_follow_checked_version(
    checked_version: Version,
    winners: &[WinningCommit],
) -> KernelResult<()> {
    let follows_checked_version = winners
        .iter()
        .zip(1..)
        .all(|(winner, offset)| checked_version.checked_add(offset) == Some(winner.version));
    if !follows_checked_version {
        return Err(KernelError::internal_error(format!(
            "Winning commits {:?} must start at checked version {checked_version} + 1 with no \
             version gap",
            winner_versions(winners),
        )));
    }
    Ok(())
}

/// A JSON scan of the winners' commit files, with each row's winner version in `VERSION`.
fn scan_winners<'a>(
    winners: impl IntoIterator<Item = &'a WinningCommit>,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let files = winners
        .into_iter()
        .map(|winner| {
            Ok(ScanFile {
                meta: winner.file.clone(),
                file_constants: vec![version_as_i64(winner.version)?.into()],
            })
        })
        .collect::<KernelResult<Vec<_>>>()?;
    PlanBuilder::scan_json(files, &[VERSION], schema)
}

fn winner_versions(winners: &[WinningCommit]) -> Vec<Version> {
    Vec::from_iter(winners.iter().map(|winner| winner.version))
}

fn conflict_at(summaries: &[WinnerSummary], version: Version, kind: ConflictKind) -> Conflict {
    let operation = summaries
        .iter()
        .find(|summary| summary.version == version)
        .and_then(|summary| summary.operation.clone());
    Conflict {
        version,
        operation,
        kind,
    }
}

fn visit_batches<V: RowVisitor + Default>(result: PlanResult) -> KernelResult<V> {
    let mut visitor = V::default();
    for batch in result.into_data()? {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    Ok(visitor)
}

fn to_version(version: i64) -> KernelResult<Version> {
    Version::try_from(version)
        .map_err(|_| KernelError::internal_error(format!("Negative winner version {version}")))
}
