//! Defines errors that can occur when performing timestamp to version conversion.
use std::fmt::{Debug, Display};

use crate::path::ParsedLogPath;
use crate::Version;

use super::search::Bound;
use super::Timestamp;

/// Represents errors that can occur when converting commit timestamps to version
#[derive(Debug, thiserror::Error)]
pub enum LogHistoryError {
    #[allow(dead_code)]
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(Timestamp),
    #[allow(dead_code)]
    #[error("Invalid timestamp range: ({start_timestamp}, {end_timestamp})")]
    InvalidTimestampRange {
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
    },
    #[error("Table configuration is invalid: {0}")]
    InvalidTableConfiguration(#[from] Box<crate::Error>),
    #[allow(dead_code)]
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
    #[error("{0}")]
    TimestampOutOfRange(TimestampOutOfRangeError),
    #[error("Failed to read the timestamp for a commit at version {version}: {error}")]
    FailedToReadTimestampForCommit {
        version: Version,
        #[source]
        error: Box<crate::Error>,
    },
    #[allow(dead_code)]
    #[error("No In-commit timestamp found for commit at version {version}")]
    InCommitTimestampNotFoundError { version: Version },
    #[error("Failed to construct Log Segment with end version {version}: {error}")]
    FailedToBuildLogSegment {
        version: Version,
        #[source]
        error: Box<crate::Error>,
    },
}

#[derive(Debug)]
pub struct TimestampOutOfRangeError {
    pub(crate) timestamp: Timestamp,
    pub(crate) nearest_timestamp: Timestamp,
    pub(crate) nearest_version: Version,
    pub(crate) bound: Bound,
    pub(crate) _read_ict: bool,
}

impl TimestampOutOfRangeError {
    pub(crate) fn try_new(
        commit_range: &[ParsedLogPath],
        timestamp: Timestamp,
        key_fun: impl Fn(&ParsedLogPath) -> Result<Timestamp, LogHistoryError>,
        bound: Bound,
        read_ict: bool,
    ) -> Result<Self, LogHistoryError> {
        let idx = match bound {
            // If the bound is [[Bound::GreatestLower]], then all the commits must have had greater
            // timestams than the query.
            Bound::GreatestLower => 0,
            // If the bound is [[Bound::LeastUpper]], then all the commits must have had smaller
            // timestams than the query.
            Bound::LeastUpper => commit_range.len() - 1,
        };
        // This is the commit that was the closest to the out of range timestamp query. Get its
        // version and timestamp.
        let nearest_commit = &commit_range[idx];
        let nearest_version = nearest_commit.version;
        let nearest_timestamp = key_fun(nearest_commit)?;

        Ok(TimestampOutOfRangeError {
            timestamp,
            nearest_timestamp,
            nearest_version,
            bound,
            _read_ict: read_ict,
        })
    }
}
impl Display for TimestampOutOfRangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.bound {
            Bound::LeastUpper => {
                write!(
                    f,
                    r#"The start timestamp {} is greater than all the commits in the log.
                        The max timestamp is {} at version {}"#,
                    self.timestamp, self.nearest_timestamp, self.nearest_version
                )
            }
            Bound::GreatestLower => {
                write!(
                    f,
                    r#"The end timestamp {} is smaller than all the commits in the log.
                        The min timestamp is {} at version {}"#,
                    self.timestamp, self.nearest_timestamp, self.nearest_version
                )
            }
        }
    }
}
