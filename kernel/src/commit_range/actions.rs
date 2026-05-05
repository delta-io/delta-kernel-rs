//! [`DeltaAction`] — selects which Delta log action to read.
//! [`CommitActions`] — per-commit handle exposing version, timestamp, and a lazy
//! iterator over the commit's action batches.

use crate::{DeltaResult, EngineData, FileDataReadResultIterator, Version};

/// A Delta log action kind.
///
/// Callers that need to read multiple action types pass a slice
/// (e.g. `&[DeltaAction::Add, DeltaAction::Remove]`).
///
/// `Protocol` and `CommitInfo` are always read internally — protocol for per-batch
/// validation, commit-info to extract the in-commit timestamp. They are stripped from
/// emitted batches unless the caller explicitly includes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaAction {
    /// AddFile action.
    Add,
    /// RemoveFile action.
    Remove,
    /// Metadata (schema/properties) action.
    Metadata,
    /// Protocol action.
    Protocol,
    /// CommitInfo action.
    CommitInfo,
    /// AddCDCFile action.
    Cdc,
}

/// Per-commit handle returned by [`super::CommitRange::commits`].
///
/// Owns a lazy iterator over the commit's action batches (already projected to the
/// requested action kinds and validated for protocol compatibility per batch).
/// Single-pass: consume via [`Self::into_actions`].
pub struct CommitActions {
    pub(crate) version: Version,
    pub(crate) timestamp: i64,
    pub(crate) actions: FileDataReadResultIterator,
}

impl CommitActions {
    /// Commit version of this commit.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Resolved commit timestamp in milliseconds since epoch.
    ///
    /// Source of truth: `commitInfo.inCommitTimestamp` if present in the commit;
    /// otherwise the commit file's modification time.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Consume self and return the iterator over action batches.
    pub fn into_actions(self) -> FileDataReadResultIterator {
        self.actions
    }
}
