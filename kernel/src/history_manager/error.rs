//! Error types for the history manager module.

use url::Url;

use super::Timestamp;
use crate::Version;

/// Represents errors that can occur when converting commit timestamps to versions.
#[derive(Debug, thiserror::Error)]
pub enum LogHistoryError {
    /// The provided timestamp range is invalid (start > end).
    #[error("Invalid timestamp range: ({start_timestamp}, {end_timestamp})")]
    InvalidTimestampRange {
        /// The start timestamp that was greater than the end timestamp.
        start_timestamp: Timestamp,
        /// The end timestamp that was less than the start timestamp.
        end_timestamp: Timestamp,
    },
    /// The timestamp range contains no commits - the entire range falls between two adjacent
    /// versions.
    #[error(
        "There are no commits in the timestamp range ({start_timestamp}, {end_timestamp}). \
         The entire timestamp range falls between versions {between_version} and {}.",
        .between_version + 1
    )]
    EmptyTimestampRange {
        /// The start of the empty timestamp range.
        start_timestamp: Timestamp,
        /// The end of the empty timestamp range.
        end_timestamp: Timestamp,
        /// The version immediately before the timestamp range (the next version is
        /// `between_version + 1`).
        between_version: Version,
    },
    /// The timestamp is outside the range of available commits.
    #[error("Timestamp {timestamp} is out of range: {reason}")]
    TimestampOutOfRange {
        /// The timestamp that was out of range.
        timestamp: Timestamp,
        /// Description of why the timestamp is out of range.
        reason: &'static str,
    },
    /// The log directory contains no commit files. Either the directory is empty or it
    /// contains only checkpoint files. Returned by [`get_earliest_recreatable_commit`].
    ///
    /// [`get_earliest_recreatable_commit`]: super::get_earliest_recreatable_commit
    #[error("No commit files found at {log_root}")]
    NoCommitsFound {
        /// The log root URL that was scanned.
        log_root: Url,
    },
    /// Commit files exist in the log but the table cannot be reconstructed: commit version 0
    /// is missing and no complete checkpoint is present to anchor the surviving commits.
    /// Returned by [`get_earliest_recreatable_commit`].
    ///
    /// [`get_earliest_recreatable_commit`]: super::get_earliest_recreatable_commit
    #[error(
        "No recreatable commits found at {log_root}: commits exist but version 0 is missing \
         and no complete checkpoint is present"
    )]
    NoRecreatableCommit {
        /// The log root URL that was scanned.
        log_root: Url,
    },
    /// An internal error occurred during timestamp conversion.
    #[error("{context}{}", source.as_ref().map(|e| format!(": {e}")).unwrap_or_default())]
    Internal {
        /// Description of the operation that failed.
        context: &'static str,
        /// The underlying error, if any.
        #[source]
        source: Option<Box<crate::Error>>,
    },
}

impl LogHistoryError {
    /// Creates an internal error with context and an underlying cause.
    pub(crate) fn internal(context: &'static str, source: crate::Error) -> Self {
        Self::Internal {
            context,
            source: Some(Box::new(source)),
        }
    }

    /// Creates an internal error with just a context message.
    pub(crate) fn internal_message(context: &'static str) -> Self {
        Self::Internal {
            context,
            source: None,
        }
    }
}

impl From<LogHistoryError> for crate::Error {
    fn from(e: LogHistoryError) -> Self {
        crate::Error::LogHistory(Box::new(e))
    }
}
