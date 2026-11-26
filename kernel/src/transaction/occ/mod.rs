//! Conflict detection for a transaction T that lost a commit race.
//!
//! A winner is a commit after T's checked version. The checked version is T's read version on the
//! first check, then the last winner version of the previous check. The winner check plan returns
//! one summary for each winner that has an action.
//!
//! Differences from Delta Spark:
//! - Kernel returns an error for a winner with more than one `commitInfo` action. Delta Spark uses
//!   the first one.

mod winner_check_plan;

#[cfg(test)]
mod tests;

use crate::action_reconciliation::plan::VERSION;
use crate::engine_data::RowVisitor;
use crate::plans::ir::nodes::ScanFile;
use crate::plans::{PlanBuilder, PlanResult};
use crate::schema::SchemaRef;
use crate::{FileMeta, KernelError, KernelResult, Version, version_as_i64};

/// A commit that took a version after T's checked version.
#[derive(Debug, Clone)]
pub(crate) struct WinningCommit {
    pub(crate) version: Version,
    pub(crate) file: FileMeta,
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
