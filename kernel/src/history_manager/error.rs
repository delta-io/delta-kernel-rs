//! Defines errors that can occur when performing timestamp to version conversion.
use std::fmt::Debug;

use crate::Version;

use super::Timestamp;

/// Represents errors that can occur when converting commit timestamps to version
#[derive(Debug, thiserror::Error)]
pub enum LogHistoryError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(Timestamp),
    #[error("Invalid timestamp range: ({start_timestamp}, {end_timestamp})")]
    InvalidTimestampRange {
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
    },
    #[error("Table configuration is invalid: {0}")]
    InvalidTableConfiguration(#[from] Box<crate::Error>),
    #[error(
        r#"There are no commits in the timestamp range ({start_timestamp}, {end_timestamp}).
            The entire timestamp range is between the versions ({between_left}, {between_right})."#
    )]
    EmptyTimestampRange {
        end_timestamp: Timestamp,
        start_timestamp: Timestamp,
        between_left: Version,
        between_right: Version,
    },
    #[error("Failed to read the timestamp for a commit at version {version}: {error}")]
    FailedToReadTimestampForCommit {
        version: Version,
        #[source]
        error: Box<crate::Error>,
    },
    #[error("No In-commit timestamp found for commit at version {version}")]
    InCommitTimestampNotFoundError { version: Version },
    #[error("Failed to construct Log Segment with end version {version}: {error}")]
    FailedToBuildLogSegment {
        version: Version,
        #[source]
        error: Box<crate::Error>,
    },
}
