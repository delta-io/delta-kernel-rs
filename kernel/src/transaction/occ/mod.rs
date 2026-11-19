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
//!
//! # Differences from Delta Spark
//!
//! - Any winner protocol action fails T.
//! - A winner metadata action fails T, except an identity-only update whose tag is exactly `true`
//!   while T changes no metadata. Delta Spark also accepts a winner that only enables some
//!   features, such as row tracking, while T changes no metadata.
//! - A row-tracking backfill winner fails any T that removes or adds a file again, whatever its
//!   paths.
//! - A winner's remove of a file that a row-tracking backfill T adds again fails T. Delta Spark
//!   drops that file from the backfill.
//! - For a row-tracking backfill T, Kernel doesn't check a winner that adds a file again without
//!   removing it. The retry must drop T's re-add of that file, as Delta Spark does.
//! - Kernel checks every winner rule before any file check. So it can report a later winner, or a
//!   different rule, than Delta Spark reports.
//! - A table-level SnapshotIsolation and, when no winner rule fails, a manifest-commit winner or a
//!   `delta.rowTracking` tombstone fail detection with an `unsupported` error.
//!
//! T's facts don't depend on the winners, because Kernel never rebases T between winners.

mod decision;
mod occ_plan;
mod winner_check_plan;

#[cfg(test)]
mod real_table_tests;
#[cfg(test)]
mod tests;

use decision::{
    Conflict, WinnerSummary, first_occ_conflict, first_winner_conflict, read_occ_violations,
    read_winner_summaries, select_read_append_versions,
};
use occ_plan::occ_plan;
use serde::de::Error as _;
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
/// Detection runs in this order:
///
/// 1. the table isolation level and the winner list;
/// 2. the winner rules;
/// 3. the manifest commits;
/// 4. the OCC checks;
/// 5. the row-tracking checks.
///
/// An error means that Kernel can't check T against the winners. The caller must fail T and not
/// retry.
///
/// `file_actions` holds T's file actions with a `remove.path` column. Each `path` in
/// `file_actions` and in the `read_files` of [`ReadScope::Files`] must be exactly as written in
/// the log. Retain both relations in the same `ScopedPlanExecutor` scope that `executor` belongs
/// to.
pub(crate) fn detect_conflicts(
    executor: &dyn ScopedPlanExecutor,
    facts: &LoserFacts,
    winners: &[WinningCommit],
    file_actions: &RelationRef,
) -> KernelResult<ConflictDetection> {
    if facts.isolation_level == IsolationLevel::SnapshotIsolation {
        return Err(KernelError::unsupported(
            "Conflict detection does not support the table isolation level SnapshotIsolation",
        ));
    }
    ensure_winners_follow_read_version(facts.read_version, winners)?;
    let summaries = match winner_check_plan(facts, winners)? {
        Some(plan) => read_winner_summaries(executor.execute_op(Operation::QueryPlan(plan))?)?,
        None => Vec::new(),
    };
    ensure_winner_rows(facts.read_version, winners, &summaries)?;
    if let Some(conflict) = first_winner_conflict(&summaries) {
        return Ok(ConflictDetection::Conflict(conflict));
    }
    ensure_no_manifest_commit(&summaries)?;
    let read_append_versions = select_read_append_versions(facts, &summaries);
    if let Some(plan) = occ_plan(facts, winners, &read_append_versions, file_actions)? {
        let violations = read_occ_violations(executor.execute_op(Operation::QueryPlan(plan))?)?;
        if let Some(conflict) = first_occ_conflict(&violations, &summaries) {
            return Ok(ConflictDetection::Conflict(conflict));
        }
    }
    ensure_row_tracking_marks(&summaries)?;
    Ok(ConflictDetection::NoConflict(summaries))
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
    /// The table's isolation level. It must be Serializable or WriteSerializable. When
    /// `delta.isolationLevel` is unset, pass Serializable, the Delta default.
    /// [`LoserFacts::effective_isolation_level`] applies the downgrade.
    pub(crate) isolation_level: IsolationLevel,
    /// T only adds files, so it reads nothing.
    pub(crate) is_blind_append: bool,
    /// At least one file action of T has `dataChange=true`.
    pub(crate) has_data_change: bool,
    /// T's operation changes table data. This matches Delta Spark's
    /// `DeltaOperations.Operation.changesData`.
    pub(crate) operation_changes_data: bool,
    /// T writes a `metaData` action.
    pub(crate) changes_metadata: bool,
    /// T writes an action that no other field describes, for example `protocol` or `txn`. A
    /// `commitInfo` action does not count.
    pub(crate) has_other_actions: bool,
    /// T's operation is a row-tracking backfill.
    pub(crate) is_row_tracking_backfill: bool,
    /// T removes a file or adds a file again.
    pub(crate) rewrites_files: bool,
    /// The domains that T writes. A removal tombstone (`removed: true`) counts as a write.
    pub(crate) written_domains: Vec<String>,
    /// The app IDs whose `txn` version T read. Include every app ID that T writes in a `txn`
    /// action. A writer reads the app's version before it writes the next one.
    pub(crate) read_app_ids: Vec<String>,
    /// The part of the table that T read.
    pub(crate) read_scope: ReadScope,
}

impl LoserFacts {
    /// Returns the isolation level that the `read_append` check uses.
    ///
    /// T drops to SnapshotIsolation when no file action has `dataChange=true` and T writes no
    /// action other than file actions, `delta.rowTracking` domain metadata and `commitInfo`. Under
    /// WriteSerializable, T's operation must also change no data.
    pub(crate) fn effective_isolation_level(&self) -> IsolationLevel {
        let blocks_downgrade = self.has_other_actions
            || self.changes_metadata
            || self
                .written_domains
                .iter()
                .any(|domain| domain != ROW_TRACKING_DOMAIN_NAME);
        let can_downgrade = !self.has_data_change
            && !blocks_downgrade
            && match self.isolation_level {
                IsolationLevel::WriteSerializable => !self.operation_changes_data,
                _ => true,
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
        /// The files that T's scans returned, with a `path` column. Include every file that T adds
        /// again, for example in a row-tracking backfill. Then a winner's remove of that file
        /// fails T with `read_delete`.
        read_files: RelationRef,
        /// T's read-time partition schema, keyed by physical names.
        partition_schema: SchemaRef,
        /// The partition conjuncts of T's read predicate, over the physical partition columns.
        ///
        /// - `None` matches every add. Pass `None` for a scan without a partition filter, or when
        ///   any of several scans has none.
        /// - With several filtered scans, pass the OR of their partition conjuncts.
        /// - On a partitioned table, a T that read no data passes `Some(Predicate::FALSE)`. Then
        ///   no add conflicts.
        /// - On an unpartitioned table, Kernel ignores the predicate, and every add conflicts, as
        ///   in Delta Spark.
        partition_predicate: Option<PredicateRef>,
        /// Parses offset-less TIMESTAMP partition values in `partition_schema`. Use the options of
        /// T's scan.
        map_to_struct_options: MapToStructOptions,
    },
}

// === Columns ===

/// The column that holds the winner version. Each scan of a winner commit adds it as a constant.
pub(crate) const VERSION: &str = "version";

// === Helpers ===

/// Returns an internal error unless the winners start at `read_version + 1` and their versions
/// ascend with no gap.
///
/// Runs before any plan, so a bad winner list fails before any I/O.
fn ensure_winners_follow_read_version(
    read_version: Version,
    winners: &[WinningCommit],
) -> KernelResult<()> {
    let follows_read_version = winners
        .iter()
        .zip(1..)
        .all(|(winner, offset)| read_version.checked_add(offset) == Some(winner.version));
    if !follows_read_version {
        return Err(KernelError::internal_error(format!(
            "Winning commits {:?} must start at read version {read_version} + 1 with no version \
             gap",
            winner_versions(winners),
        )));
    }
    Ok(())
}

/// Returns an internal error unless `summaries` has one row for each winner, in order.
///
/// A winner gets no row when the executor reads no action from its commit file, for example an
/// empty file. Kernel fails then, because it can't check that winner.
fn ensure_winner_rows(
    read_version: Version,
    winners: &[WinningCommit],
    summaries: &[WinnerSummary],
) -> KernelResult<()> {
    let rows = Vec::from_iter(summaries.iter().map(|summary| summary.version));
    let versions = winner_versions(winners);
    if rows != versions {
        return Err(KernelError::internal_error(format!(
            "Winner check rows {rows:?} do not match the winning commits {versions:?} after read \
             version {read_version}"
        )));
    }
    Ok(())
}

/// Returns an error when a winner is a manifest commit, because the plans don't read the actions
/// nested in its `checkpoint` action.
fn ensure_no_manifest_commit(summaries: &[WinnerSummary]) -> KernelResult<()> {
    match summaries.iter().find(|summary| summary.is_manifest_commit) {
        Some(summary) => Err(KernelError::unsupported(format!(
            "Conflict detection does not support the manifest commit at version {}",
            summary.version
        ))),
        None => Ok(()),
    }
}

/// Returns an error when a winner removes the `delta.rowTracking` domain, or writes it without a
/// readable high-water mark.
///
/// The next attempt needs a row ID high-water mark. A removed `delta.rowTracking` domain gives
/// none, so Kernel can't tell how T's row IDs continue. A `configuration` that fails to parse also
/// yields no mark. Kernel fails then, because the next attempt would otherwise use an older mark.
/// The missing-mark error omits the version, because a parse failure can null the marks of other
/// winners in the same batch.
fn ensure_row_tracking_marks(summaries: &[WinnerSummary]) -> KernelResult<()> {
    if let Some(summary) = summaries
        .iter()
        .find(|summary| summary.removes_row_tracking)
    {
        return Err(KernelError::unsupported(format!(
            "The winning commit at version {} removes the `delta.rowTracking` domain",
            summary.version
        )));
    }
    let missing = summaries
        .iter()
        .any(|summary| summary.writes_row_tracking && summary.row_id_high_water_mark.is_none());
    if missing {
        return Err(KernelError::MalformedJson(serde_json::Error::custom(
            "A winning commit has a `delta.rowTracking` domain without a readable \
             rowIdHighWaterMark",
        )));
    }
    Ok(())
}

/// The commit files of the winners at `versions`, as scan inputs. `winners` must be in ascending
/// version order.
fn scan_files(
    winners: &[WinningCommit],
    versions: impl IntoIterator<Item = Version>,
) -> KernelResult<Vec<ScanFile>> {
    versions
        .into_iter()
        .map(|version| {
            winners
                .binary_search_by_key(&version, |winner| winner.version)
                .ok()
                .and_then(|index| winners.get(index))
                .ok_or_else(|| {
                    KernelError::internal_error(format!("No winning commit at version {version}"))
                })?
                .scan_file()
        })
        .collect()
}

fn winner_versions(winners: &[WinningCommit]) -> Vec<Version> {
    Vec::from_iter(winners.iter().map(|winner| winner.version))
}
