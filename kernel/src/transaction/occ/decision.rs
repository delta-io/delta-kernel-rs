//! Reads the rows of the conflict-detection plans and decides whether T fails.

use std::sync::LazyLock;

use super::occ_plan::{CHECK, OccBranchVersions, OccCheck};
use super::winner_check_plan::{
    BACKFILL_CONFLICT, DOMAIN_CONFLICT, IN_COMMIT_TIMESTAMP, IS_BLIND_APPEND, METADATA_CHANGED,
    OPERATION, PROTOCOL_CHANGED, ROW_ID_HIGH_WATER_MARK, ROW_TRACKING_BACKFILL_OPERATION,
    ROW_TRACKING_COUNT, TXN_CONFLICT, WINNER_CHECK_OUTPUT_SCHEMA,
};
use super::{LoserFacts, VERSION};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{ColumnName, column_name};
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
    /// The winner has a `metaData` action. The rule does not apply when T changes no metadata and
    /// the winner's commit tags mark an identity-only update.
    MetadataChanged,
    /// The winner writes a domain that T writes, other than `delta.rowTracking`.
    DomainConflict,
    /// The winner writes a `txn` for an app ID that T read.
    TxnConflict,
    /// The winner is a row-tracking backfill, and T removes or adds a file again.
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
    /// The winner's `delta.rowTracking` high-water mark, if the winner writes one.
    pub(crate) row_id_high_water_mark: Option<i64>,
    /// The number of `delta.rowTracking` domain actions in the winner.
    pub(crate) row_tracking_count: i64,
    /// The rules that the winner breaks, in [`WinnerRule`] order.
    pub(crate) violations: Vec<WinnerRule>,
}

impl WinnerSummary {
    fn is_row_tracking_backfill(&self) -> bool {
        self.operation.as_deref() == Some(ROW_TRACKING_BACKFILL_OPERATION)
    }
}

/// One row of the OCC plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OccViolation {
    pub(crate) version: Version,
    pub(crate) check: OccCheck,
}

/// Reads the winner check plan output, in ascending version order.
pub(crate) fn read_winner_summaries(result: PlanResult) -> KernelResult<Vec<WinnerSummary>> {
    let mut visitor = WinnerSummaryVisitor::default();
    for batch in result.into_data()? {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    let mut summaries = visitor.0;
    summaries.sort_by_key(|summary| summary.version);
    Ok(summaries)
}

/// Returns the earliest winner that breaks a winner rule.
///
/// `summaries` must be in ascending version order.
pub(crate) fn first_winner_conflict(summaries: &[WinnerSummary]) -> Option<Conflict> {
    summaries.iter().find_map(|summary| {
        let rule = summary.violations.first()?;
        Some(Conflict {
            version: summary.version,
            operation: summary.operation.clone(),
            kind: ConflictKind::Winner(*rule),
        })
    })
}

/// Picks the winner versions that each OCC branch checks.
///
/// A violation at any checked version fails T. `read_delete` and `delete_delete` check every
/// winner. `read_append` checks no winner when T is a blind append, T is a row-tracking backfill,
/// or T runs at SnapshotIsolation. It also skips backfill winners. Under WriteSerializable, it
/// skips blind-append winners unless T changes metadata.
pub(crate) fn select_occ_branch_versions(
    facts: &LoserFacts,
    summaries: &[WinnerSummary],
) -> OccBranchVersions {
    let all = Vec::from_iter(summaries.iter().map(|summary| summary.version));
    let isolation_level = facts.effective_isolation_level();
    let check_read_append = !facts.is_blind_append
        && !facts.is_row_tracking_backfill
        && isolation_level != IsolationLevel::SnapshotIsolation;
    let read_append = match check_read_append {
        false => Vec::new(),
        true => Vec::from_iter(
            summaries
                .iter()
                .filter(|summary| !summary.is_row_tracking_backfill())
                .filter(|summary| {
                    isolation_level != IsolationLevel::WriteSerializable
                        || facts.changes_metadata
                        || summary.is_blind_append != Some(true)
                })
                .map(|summary| summary.version),
        ),
    };
    OccBranchVersions {
        read_append,
        read_delete: all.clone(),
        delete_delete: all,
    }
}

/// Reads the OCC plan output.
pub(crate) fn read_occ_violations(result: PlanResult) -> KernelResult<Vec<OccViolation>> {
    let mut visitor = OccViolationVisitor::default();
    for batch in result.into_data()? {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    Ok(visitor.0)
}

/// Returns the earliest violation. Within one version, [`OccCheck`] order decides.
pub(crate) fn first_occ_conflict(
    violations: &[OccViolation],
    summaries: &[WinnerSummary],
) -> Option<Conflict> {
    let first = violations
        .iter()
        .min_by_key(|violation| (violation.version, violation.check))?;
    let operation = summaries
        .iter()
        .find(|summary| summary.version == first.version)
        .and_then(|summary| summary.operation.clone());
    Some(Conflict {
        version: first.version,
        operation,
        kind: ConflictKind::Occ(first.check),
    })
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
            row_id_high_water_mark,
            row_tracking_count,
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
            let mut violations = Vec::new();
            for (rule, getter, name) in &rules {
                let violated: bool = getter.get(row, name)?;
                if violated {
                    violations.push(*rule);
                }
            }
            self.0.push(WinnerSummary {
                version: to_version(version.get(row, VERSION)?)?,
                operation: operation.get_opt(row, OPERATION)?,
                is_blind_append: is_blind_append.get_opt(row, IS_BLIND_APPEND)?,
                in_commit_timestamp: in_commit_timestamp.get_opt(row, IN_COMMIT_TIMESTAMP)?,
                row_id_high_water_mark: row_id_high_water_mark
                    .get_opt(row, ROW_ID_HIGH_WATER_MARK)?,
                row_tracking_count: row_tracking_count.get(row, ROW_TRACKING_COUNT)?,
                violations,
            });
        }
        Ok(())
    }
}

#[derive(Default)]
struct OccViolationVisitor(Vec<OccViolation>);

impl RowVisitor for OccViolationVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![column_name!(CHECK), column_name!(VERSION)],
                vec![DataType::STRING, DataType::LONG],
            )
                .into()
        });
        COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        let [check_getter, version_getter] = getters else {
            return Err(KernelError::internal_error(format!(
                "Unexpected OCC getter count {}",
                getters.len()
            )));
        };
        for row in 0..row_count {
            let check: &str = check_getter.get(row, CHECK)?;
            self.0.push(OccViolation {
                version: to_version(version_getter.get(row, VERSION)?)?,
                check: OccCheck::parse(check)?,
            });
        }
        Ok(())
    }
}

fn to_version(version: i64) -> KernelResult<Version> {
    Version::try_from(version)
        .map_err(|_| KernelError::internal_error(format!("Negative winner version {version}")))
}
