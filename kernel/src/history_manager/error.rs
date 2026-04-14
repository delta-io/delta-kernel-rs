//! Error types for the history manager module.

use super::search::Bound;
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
    #[error("Timestamp {timestamp} is out of range: {}", match .bound {
        Bound::LeastUpper => "no version exists at or after this timestamp",
        Bound::GreatestLower => "no version exists at or before this timestamp",
    })]
    TimestampOutOfRange {
        /// The timestamp that was out of range.
        timestamp: Timestamp,
        /// The type of bound search that failed.
        bound: Bound,
    },
    /// An internal error occurred during timestamp conversion.
    #[error("{context}: {source}")]
    Internal {
        /// Description of the operation that failed.
        context: &'static str,
        /// The underlying error.
        #[source]
        source: Box<crate::Error>,
    },
}

impl LogHistoryError {
    /// Creates an internal error with context describing the failed operation.
    pub(crate) fn internal(context: &'static str, source: crate::Error) -> Self {
        Self::Internal {
            context,
            source: Box::new(source),
        }
    }
}

impl From<LogHistoryError> for crate::Error {
    fn from(e: LogHistoryError) -> Self {
        crate::Error::LogHistory(Box::new(e))
    }
}
