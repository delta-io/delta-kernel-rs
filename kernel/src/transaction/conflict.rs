//! Saved winning history from a confirmed conflict response.

use std::sync::Arc;

use url::Url;

use crate::commit_range::CommitRange;
use crate::{KernelError, KernelResult, Version};

/// Winning history or its discovery error, retained without a committer or execution context.
///
/// Getters inspect only the saved response. If discovery failed, the connector must restart from
/// a fresh Snapshot and rebuild its transaction instead of refetching through this result.
#[derive(Debug)]
pub struct WinningCommitHistory {
    result: KernelResult<Arc<CommitRange>, Arc<KernelError>>,
}

impl WinningCommitHistory {
    /// Returns the saved discovery error without performing I/O.
    pub fn load_error(&self) -> Option<&KernelError> {
        self.result.as_ref().err().map(AsRef::as_ref)
    }

    /// Returns saved winning history without performing I/O.
    pub fn cached(&self) -> Option<&Arc<CommitRange>> {
        self.result.as_ref().ok()
    }

    pub(super) fn new(
        table_root: &Url,
        conflict_version: Version,
        commits: KernelResult<CommitRange>,
    ) -> Self {
        let result = commits.and_then(|range| {
            if range.table_root() != table_root
                || range.start_version() != conflict_version
                || range.end_version() < conflict_version
            {
                return Err(KernelError::internal_error(
                    "Committer returned history for a different conflict",
                ));
            }
            let mut expected = Some(conflict_version);
            let mut last = None;
            for file in range.commit_files()? {
                let file = file?;
                if Some(file.version) != expected {
                    return Err(KernelError::CommitRange(
                        "Winning history contains a version gap".into(),
                    ));
                }
                last = Some(file.version);
                expected = file.version.checked_add(1);
            }
            if last != Some(range.end_version()) {
                return Err(KernelError::CommitRange(
                    "Winning history is incomplete".into(),
                ));
            }
            Ok(range)
        });
        Self {
            result: result.map(Arc::new).map_err(Arc::new),
        }
    }

    pub(super) fn get(&self) -> KernelResult<&Arc<CommitRange>> {
        self.result
            .as_ref()
            .map_err(|error| KernelError::generic_err(error.clone()))
    }
}
