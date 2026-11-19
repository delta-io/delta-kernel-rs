//! Reads the rows of the conflict-detection plans and decides whether T fails.

use std::sync::LazyLock;

use super::occ_plan::{CHECK, OccCheck, VIOLATION_SCHEMA};
use super::winner_check_plan::{
    BACKFILL_CONFLICT, DOMAIN_CONFLICT, IN_COMMIT_TIMESTAMP, IS_BLIND_APPEND, IS_MANIFEST_COMMIT,
    METADATA_CHANGED, OPERATION, PROTOCOL_CHANGED, REMOVES_ROW_TRACKING, ROW_ID_HIGH_WATER_MARK,
    TXN_CONFLICT, WINNER_CHECK_OUTPUT_SCHEMA, WRITES_ROW_TRACKING,
    is_row_tracking_backfill_operation,
};
use super::{LoserFacts, WinningCommit, winner_versions};
use crate::action_reconciliation::plan::VERSION;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::ColumnName;
use crate::plans::PlanResult;
use crate::schema::{ColumnNamesAndTypes, DataType};
use crate::table_properties::IsolationLevel;
use crate::{KernelError, KernelResult, Version};

/// A winner check rule that fails T.
///
/// The variant order is the order in which Kernel reports the rules of one winner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WinnerRule {
    /// The winner has a protocol action.
    ProtocolChanged,
    /// The winner has a `metaData` action. An identity-only update doesn't count when T changes no
    /// metadata.
    MetadataChanged,
    /// The winner writes a domain that T writes, other than `delta.rowTracking`.
    DomainConflict,
    /// The winner writes a `txn` for an app ID that T read.
    TxnConflict,
    /// The winner is a row-tracking backfill or unbackfill, and T removes or adds a file again.
    BackfillConflict,
}

/// The reason that a winner fails T.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictKind {
    Winner(WinnerRule),
    Occ(OccCheck),
}

/// The winner that fails T, and the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Conflict {
    pub(crate) version: Version,
    /// The winner's `commitInfo.operation`, for T's error.
    pub(crate) operation: Option<String>,
    pub(crate) kind: ConflictKind,
}

/// One row of the winner check plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WinnerSummary {
    pub(crate) version: Version,
    pub(crate) operation: Option<String>,
    /// `None` when the winner has no `commitInfo.isBlindAppend`.
    pub(crate) is_blind_append: Option<bool>,
    pub(crate) in_commit_timestamp: Option<i64>,
    /// The winner has a top-level `checkpoint` action, so it is an adaptiveMetadata manifest
    /// commit.
    pub(crate) is_manifest_commit: bool,
    /// The winner's `delta.rowTracking` high-water mark, if the winner writes one.
    pub(crate) row_id_high_water_mark: Option<i64>,
    /// The winner writes a live `delta.rowTracking` domain action. A tombstone does not count.
    pub(crate) writes_row_tracking: bool,
    /// The winner removes the `delta.rowTracking` domain.
    pub(crate) removes_row_tracking: bool,
    /// The first rule in [`WinnerRule`] order that the winner breaks.
    pub(crate) first_violation: Option<WinnerRule>,
}

/// One row of the OCC plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OccViolation {
    version: Version,
    check: OccCheck,
}

/// Reads the winner check plan output, in ascending version order.
pub(super) fn read_winner_summaries(result: PlanResult) -> KernelResult<Vec<WinnerSummary>> {
    let mut summaries = visit_batches::<WinnerSummaryVisitor>(result)?.0;
    summaries.sort_by_key(|summary| summary.version);
    Ok(summaries)
}

/// Returns the earliest winner that breaks a winner rule. Within one winner, [`WinnerRule`] order
/// decides.
///
/// `summaries` must be in ascending version order.
pub(super) fn first_winner_conflict(summaries: &[WinnerSummary]) -> Option<Conflict> {
    let (version, rule) = summaries
        .iter()
        .find_map(|summary| Some((summary.version, summary.first_violation?)))?;
    Some(conflict_at(summaries, version, ConflictKind::Winner(rule)))
}

/// Returns the winner versions that `read_append` checks.
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
                    .is_some_and(is_row_tracking_backfill_operation);
                let skipped_as_blind_append = isolation_level == IsolationLevel::WriteSerializable
                    && !facts.changes_metadata
                    && summary.is_blind_append == Some(true);
                !is_backfill && !skipped_as_blind_append
            })
            .map(|summary| summary.version),
    )
}

/// Reads the OCC plan output and returns the earliest violation. Within one version, [`OccCheck`]
/// order decides.
pub(super) fn first_occ_conflict(
    result: PlanResult,
    summaries: &[WinnerSummary],
) -> KernelResult<Option<Conflict>> {
    let violations = visit_batches::<OccViolationVisitor>(result)?.0;
    let first = violations
        .iter()
        .min_by_key(|violation| (violation.version, violation.check));
    Ok(first.map(|first| conflict_at(summaries, first.version, ConflictKind::Occ(first.check))))
}

/// Returns an error unless `summaries` has one row for each winner, in order.
///
/// A winner gets no row when the plan reads no action from its commit file, for example an empty
/// file. Kernel fails then, because it can't check that winner.
pub(super) fn ensure_winner_rows(
    checked_version: Version,
    winners: &[WinningCommit],
    summaries: &[WinnerSummary],
) -> KernelResult<()> {
    let rows = Vec::from_iter(summaries.iter().map(|summary| summary.version));
    let versions = winner_versions(winners);
    if let Some(missing) = versions.iter().find(|version| !rows.contains(version)) {
        return Err(KernelError::generic(format!(
            "Conflict detection read no action from the commit file of version {missing}"
        )));
    }
    if rows != versions {
        return Err(KernelError::internal_error(format!(
            "Winner check rows {rows:?} do not match the winning commits {versions:?} after \
             checked version {checked_version}"
        )));
    }
    Ok(())
}

/// Returns an error when a winner is a manifest commit.
///
/// A manifest commit's file changes can live in its content tree, which the plans don't read. T's
/// backreferences to the table state also go stale. The `checkpoint` array holds the full table
/// state, not the commit's changes.
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
/// readable high-water mark.
///
/// The next attempt needs a row ID high-water mark to continue T's row IDs.
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
    let missing = summaries
        .iter()
        .any(|summary| summary.writes_row_tracking && summary.row_id_high_water_mark.is_none());
    if missing {
        return Err(KernelError::generic(
            "A winning commit has a `delta.rowTracking` domain without a readable \
             rowIdHighWaterMark",
        ));
    }
    Ok(())
}

// === Row visitors ===

#[derive(Default)]
struct WinnerSummaryVisitor(Vec<WinnerSummary>);

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
            is_manifest_commit,
            row_id_high_water_mark,
            writes_row_tracking,
            removes_row_tracking,
            protocol_changed,
            metadata_changed,
            domain_conflict,
            txn_conflict,
            backfill_conflict,
        ] = getters
        else {
            return Err(KernelError::internal_error(format!(
                "Unexpected winner check getter count {}",
                getters.len()
            )));
        };
        let rules = [
            (
                WinnerRule::ProtocolChanged,
                protocol_changed,
                PROTOCOL_CHANGED,
            ),
            (
                WinnerRule::MetadataChanged,
                metadata_changed,
                METADATA_CHANGED,
            ),
            (WinnerRule::DomainConflict, domain_conflict, DOMAIN_CONFLICT),
            (WinnerRule::TxnConflict, txn_conflict, TXN_CONFLICT),
            (
                WinnerRule::BackfillConflict,
                backfill_conflict,
                BACKFILL_CONFLICT,
            ),
        ];
        for row in 0..row_count {
            let mut first_violation: Option<WinnerRule> = None;
            for (rule, getter, name) in &rules {
                let violated: bool = getter.get(row, name)?;
                if violated {
                    first_violation = Some(first_violation.map_or(*rule, |first| first.min(*rule)));
                }
            }
            self.0.push(WinnerSummary {
                version: to_version(version.get(row, VERSION)?)?,
                operation: operation.get_opt(row, OPERATION)?,
                is_blind_append: is_blind_append.get_opt(row, IS_BLIND_APPEND)?,
                in_commit_timestamp: in_commit_timestamp.get_opt(row, IN_COMMIT_TIMESTAMP)?,
                is_manifest_commit: is_manifest_commit.get(row, IS_MANIFEST_COMMIT)?,
                row_id_high_water_mark: row_id_high_water_mark
                    .get_opt(row, ROW_ID_HIGH_WATER_MARK)?,
                writes_row_tracking: writes_row_tracking.get(row, WRITES_ROW_TRACKING)?,
                removes_row_tracking: removes_row_tracking.get(row, REMOVES_ROW_TRACKING)?,
                first_violation,
            });
        }
        Ok(())
    }
}

#[derive(Default)]
struct OccViolationVisitor(Vec<OccViolation>);

impl RowVisitor for OccViolationVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| VIOLATION_SCHEMA.leaves(None));
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [version_getter, check_getter] = getters else {
            return Err(KernelError::internal_error(format!(
                "Unexpected OCC getter count {}",
                getters.len()
            )));
        };
        for row in 0..row_count {
            let check: &str = check_getter.get(row, CHECK)?;
            let check = check
                .parse()
                .map_err(|_| KernelError::internal_error(format!("Unknown OCC check {check:?}")))?;
            self.0.push(OccViolation {
                version: to_version(version_getter.get(row, VERSION)?)?,
                check,
            });
        }
        Ok(())
    }
}

/// Returns the isolation level that the `read_append` check uses.
///
/// T drops to SnapshotIsolation when no file action has `dataChange=true` and T writes no action
/// other than file actions, `delta.rowTracking` domain metadata and `commitInfo`. Under
/// WriteSerializable, T's operation must also change no data.
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

/// The conflict of the winner at `version`, with that winner's `commitInfo.operation`.
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

/// Visits every batch of `result` with a new `V`.
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
