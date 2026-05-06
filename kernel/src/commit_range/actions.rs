//! [`DeltaAction`] — selects which Delta log action to read.
//! [`CommitActions`] — per-commit handle exposing version, timestamp, and a lazy
//! iterator over the commit's action batches.

use crate::{FileDataReadResultIterator, Version};

/// A Delta log action kind.
///
/// Callers that need to read multiple action types pass a slice
/// (e.g. `&[DeltaAction::Add, DeltaAction::Remove]`).
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

    /// Commit timestamp in milliseconds since epoch.
    ///
    /// Currently always the commit file's `last_modified` time. In-commit timestamp (ICT)
    /// extraction from `commitInfo.inCommitTimestamp` is planned (TODO) and will become the
    /// source of truth when the table has ICT enabled.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Consume self and return the iterator over action batches.
    pub fn into_actions(self) -> FileDataReadResultIterator {
        self.actions
    }
}
