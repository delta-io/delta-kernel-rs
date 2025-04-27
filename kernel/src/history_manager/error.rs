use std::fmt::{Debug, Display};
use std::num::ParseIntError;

use crate::path::ParsedLogPath;
use crate::Version;

use super::{Bound, Timestamp};

#[derive(Debug, thiserror::Error)]
pub enum LogHistoryError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(Timestamp),
    #[error("Invalid timestamp range: ({start_timestamp}, {end_timestamp})")]
    InvalidTimestampRange {
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
    },
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
    #[error("Failed to parse the In-Commit Timestamp enablement configuration for snapshot at version {version}: {parse_error}")]
    FailedToParseConfiguration {
        #[source]
        parse_error: ParseIntError,
        version: Version,
    },
}

#[derive(Debug)]
pub struct TimestampOutOfRangeError {
    pub(crate) timestamp: Timestamp,
    pub(crate) bound_timestamp: Timestamp,
    pub(crate) bound_version: Version,
    pub(crate) bound: Bound,
    pub(crate) _read_ict: bool,
}

impl TimestampOutOfRangeError {
    pub(crate) fn try_new<'a>(
        commit_range: &'a [ParsedLogPath],
        timestamp: Timestamp,
        key_fun: impl Fn(&'a ParsedLogPath) -> Result<i64, LogHistoryError>,
        bound: Bound,
        read_ict: bool,
    ) -> Result<Self, LogHistoryError> {
        let idx = match bound {
            Bound::GreatestLower => 0,
            Bound::LeastUpper => commit_range.len() - 1,
        };
        let bound_commit = &commit_range[idx];
        let bound_version = bound_commit.version;
        // FIXME: undo the unwrap
        let bound_timestamp = key_fun(bound_commit)?;
        Ok(TimestampOutOfRangeError {
            timestamp,
            bound_timestamp,
            bound_version,
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
                    self.timestamp, self.bound_timestamp, self.bound_version
                )
            }
            Bound::GreatestLower => {
                write!(
                    f,
                    r#"The end timestamp {} is smaller than all the commits in the log.
                        The min timestamp is {} at version {}"#,
                    self.timestamp, self.bound_timestamp, self.bound_version
                )
            }
        }
    }
}
