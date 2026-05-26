//! Error types for the history manager module.

use super::search::Bound;
use super::Timestamp;
use crate::Version;

/// The nearest retained timestamp on the side of the search bound that an
/// out-of-range search failed against. Engines surface this to users so the
/// error message can point at a valid timestamp.
///
/// `Earliest` and `Latest` carry the boundary commit's timestamp. `Unknown`
/// means no boundary timestamp was available, e.g. empty log or a failure
/// reading the boundary commit's In-Commit Timestamp at the error site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NearestTimestamp {
    /// The earliest retained commit's timestamp. Returned for a
    /// `GreatestLower` search whose input fell below the retained range.
    Earliest(Timestamp),
    /// The latest retained commit's timestamp. Returned for a `LeastUpper`
    /// search whose input fell above the retained range.
    Latest(Timestamp),
    /// No boundary timestamp could be determined.
    Unknown,
}

impl NearestTimestamp {
    /// Tags `ts` with the variant matching `bound`.
    pub(crate) fn from_boundary(bound: Bound, ts: Timestamp) -> Self {
        match bound {
            Bound::GreatestLower => Self::Earliest(ts),
            Bound::LeastUpper => Self::Latest(ts),
        }
    }
}

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
        /// The nearest retained commit's timestamp on the side of the search
        /// bound. For example, given a retained range `[100, 500]` and a
        /// `GreatestLower` search at timestamp `50`, this is
        /// `NearestTimestamp::Earliest(100)`.
        nearest_timestamp: NearestTimestamp,
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

    /// Creates a `TimestampOutOfRange` error. The reason is derived from `bound`.
    pub(crate) fn out_of_range(
        timestamp: Timestamp,
        bound: Bound,
        nearest_timestamp: NearestTimestamp,
    ) -> Self {
        Self::TimestampOutOfRange {
            timestamp,
            reason: bound.out_of_range_reason(),
            nearest_timestamp,
        }
    }
}

impl From<LogHistoryError> for crate::Error {
    fn from(e: LogHistoryError) -> Self {
        crate::Error::LogHistory(Box::new(e))
    }
}
