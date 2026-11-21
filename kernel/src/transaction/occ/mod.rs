//! Conflict detection for a transaction T that lost a commit race.
//!
//! A winner is a commit after T's checked version. The checked version is T's read version on the
//! first check, then the last winner version of the previous check. The winner check plan returns
//! one summary for each winner that has an action, with the first winner rule that the winner
//! breaks.
//!
//! Differences from Delta Spark:
//! - Kernel returns an error for a winner with more than one `commitInfo` action. Delta Spark uses
//!   the first one.
//! - Any winner `protocol` action fails T.
//! - A `metaData` winner fails T unless its `delta.identity.schemaUpdate` tag is exactly `true` and
//!   T changes no metadata.

mod winner_check_plan;

#[cfg(test)]
mod tests;

pub(crate) use winner_check_plan::{WinnerRule, WinnerSummary};

use crate::action_reconciliation::plan::VERSION;
use crate::engine_data::RowVisitor;
use crate::plans::ir::nodes::ScanFile;
use crate::plans::{PlanBuilder, PlanResult};
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::SchemaRef;
use crate::{FileMeta, KernelError, KernelResult, Version, version_as_i64};

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
}

/// A commit that took a version after T's checked version.
#[derive(Debug, Clone)]
pub(crate) struct WinningCommit {
    pub(crate) version: Version,
    pub(crate) file: FileMeta,
}

/// The facts about T that conflict detection needs.
///
/// T is never a row-tracking backfill or unbackfill and never drops a feature: Kernel reserves
/// those operation names.
#[derive(Debug, Clone)]
pub(crate) struct LoserFacts {
    /// T writes a `metaData` action.
    pub(crate) changes_metadata: bool,
    /// The domains that T writes, including removal tombstones.
    pub(crate) written_domains: Vec<String>,
    /// The app IDs whose `txn` version T read.
    ///
    /// Include every app ID that T writes in a `txn` action: a writer reads the version first.
    pub(crate) read_app_ids: Vec<String>,
}

impl LoserFacts {
    fn written_domains_except_row_tracking(&self) -> impl Iterator<Item = &str> {
        self.written_domains
            .iter()
            .map(String::as_str)
            .filter(|domain| *domain != ROW_TRACKING_DOMAIN_NAME)
    }
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
