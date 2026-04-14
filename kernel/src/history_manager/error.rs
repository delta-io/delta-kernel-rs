//! Error types for the history manager module.

use crate::Version;

use super::search::Bound;

type Timestamp = i64;

/// Represents errors that can occur when converting commit timestamps to version
#[derive(Debug, thiserror::Error)]
pub enum LogHistoryError {
    #[error("Invalid timestamp range: ({start_timestamp}, {end_timestamp})")]
    InvalidTimestampRange {
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
    },
    #[error(
        r#"There are no commits in the timestamp range ({start_timestamp}, {end_timestamp}). The entire timestamp range is between versions ({between_left}, {between_right})."#
    )]
    EmptyTimestampRange {
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        between_left: Version,
        between_right: Version,
    },
    #[error("Timestamp {timestamp} is out of range: {}", match .bound {
        Bound::LeastUpper => "no version exists at or after this timestamp",
        Bound::GreatestLower => "no version exists at or before this timestamp",
    })]
    TimestampOutOfRange { timestamp: Timestamp, bound: Bound },
    #[error("{context}: {source}")]
    Internal {
        context: &'static str,
        #[source]
        source: Box<crate::Error>,
    },
}

impl From<LogHistoryError> for crate::Error {
    fn from(e: LogHistoryError) -> Self {
        crate::Error::LogHistory(Box::new(e))
    }
}
