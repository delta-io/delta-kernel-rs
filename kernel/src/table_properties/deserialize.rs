//! For now we just use simple functions to deserialize table properties from strings. This allows
//! us to relatively simply implement the functionality described in the protocol and expose
//! 'simple' types to the user in the [`TableProperties`] struct. E.g. we can expose a `bool`
//! directly instead of a `BoolConfig` type that we implement `Deserialize` for.
use super::*;
use crate::expressions::ColumnName;
use crate::table_features::ColumnMappingMode;
use crate::utils::require;
use crate::{DeltaResult, Error};
use std::num::NonZero;
use std::time::Duration;

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

impl<K, V, I> From<I> for TableProperties
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from(unparsed: I) -> Self {
        let mut props = TableProperties::default();
        let unparsed = unparsed.into_iter().filter(|(k, v)| {
            // Only keep elements that fail to parse
            try_parse(&mut props, k.as_ref(), v.as_ref()).is_err()
        });
        props.unknown_properties = unparsed.map(|(k, v)| (k.into(), v.into())).collect();
        props
    }
}

// attempt to parse a key-value pair into a `TableProperties` struct. Returns Some(()) if the key
// was successfully parsed, and None otherwise.
fn try_parse(props: &mut TableProperties, k: &str, v: &str) -> DeltaResult<()> {
    // NOTE!! we do Some(parse(v)?) instead of just parse(v) because we want to return None if the
    // parsing fails. If we simply call 'parse(v)', then we would (incorrectly) return Some(()) and
    // just set the property to None.
    match k {
        "delta.appendOnly" => props.append_only = parse_bool(v)?,
        "delta.autoOptimize.autoCompact" => props.auto_compact = parse_bool(v)?,
        "delta.autoOptimize.optimizeWrite" => props.optimize_write = parse_bool(v)?,
        "delta.checkpointInterval" => props.checkpoint_interval = parse_positive_int(v)?,
        "delta.checkpoint.writeStatsAsJson" => {
            props.checkpoint_write_stats_as_json = parse_bool(v)?
        }
        "delta.checkpoint.writeStatsAsStruct" => {
            props.checkpoint_write_stats_as_struct = parse_bool(v)?
        }
        "delta.columnMapping.mode" => {
            props.column_mapping_mode = ColumnMappingMode::try_from(v).ok()
        }
        "delta.dataSkippingNumIndexedCols" => {
            props.data_skipping_num_indexed_cols = DataSkippingNumIndexedCols::try_from(v).ok()
        }
        "delta.dataSkippingStatsColumns" => {
            props.data_skipping_stats_columns = parse_column_names(v)?
        }
        "delta.deletedFileRetentionDuration" => {
            props.deleted_file_retention_duration = parse_interval(v)?
        }
        "delta.enableChangeDataFeed" => props.enable_change_data_feed = parse_bool(v)?,
        "delta.enableDeletionVectors" => props.enable_deletion_vectors = parse_bool(v)?,
        "delta.isolationLevel" => props.isolation_level = IsolationLevel::try_from(v).ok(),
        "delta.logRetentionDuration" => props.log_retention_duration = parse_interval(v)?,
        "delta.enableExpiredLogCleanup" => props.enable_expired_log_cleanup = parse_bool(v)?,
        "delta.randomizeFilePrefixes" => props.randomize_file_prefixes = parse_bool(v)?,
        "delta.randomPrefixLength" => props.random_prefix_length = parse_positive_int(v)?,
        "delta.setTransactionRetentionDuration" => {
            props.set_transaction_retention_duration = parse_interval(v)?
        }
        "delta.targetFileSize" => props.target_file_size = parse_positive_int(v)?,
        "delta.tuneFileSizesForRewrites" => props.tune_file_sizes_for_rewrites = parse_bool(v)?,
        "delta.checkpointPolicy" => props.checkpoint_policy = CheckpointPolicy::try_from(v).ok(),
        "delta.enableRowTracking" => props.enable_row_tracking = parse_bool(v)?,
        "delta.enableInCommitTimestamps" => props.enable_in_commit_timestamps = parse_bool(v)?,
        "delta.inCommitTimestampEnablementVersion" => {
            props.in_commit_timestamp_enablement_version = parse_non_negative(v)?
        }
        "delta.inCommitTimestampEnablementTimestamp" => {
            props.in_commit_timestamp_enablement_timestamp = parse_non_negative(v)?
        }
        _ => return Err(Error::ParsePropertyError(ParsePropertyError(k.to_string()))),
    }
    Ok(())
}

/// The input string is not a valid interval
#[derive(thiserror::Error, Debug)]
#[error("'{0}' is not a valid property")]
pub struct ParsePropertyError(String);

/// Deserialize a string representing a positive (> 0) integer into an `Option<u64>`. Returns `Some`
/// if successfully parses, and `None` otherwise.
pub(crate) fn parse_positive_int(s: &str) -> DeltaResult<Option<NonZero<u64>>> {
    // parse as non-negative and verify the result is non-zero
    parse_non_negative(s).map(|maybe_num| maybe_num.and_then(NonZero::<u64>::new))
}

/// Deserialize a string representing a non-negative integer into an `Option<u64>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_non_negative<T>(s: &str) -> DeltaResult<Option<T>>
where
    i64: TryInto<T>,
{
    // parse to i64 since java doesn't even allow u64
    Ok(s.parse::<i64>()
        .ok()
        .filter(|&i| i >= 0)
        .and_then(|value| value.try_into().ok()))
}

/// Deserialize a string representing a boolean into an `Option<bool>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_bool(s: &str) -> DeltaResult<Option<bool>> {
    parse_bool_impl(s).map(Some).map_err(Error::ParseBoolError)
}

/// The input string is not a valid interval
#[derive(thiserror::Error, Debug)]
#[error("'{0}' is not an bool")]
pub struct ParseBoolError(String);

fn parse_bool_impl(s: &str) -> Result<bool, ParseBoolError> {
    match s {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ParseBoolError(s.to_string())),
    }
}

/// Deserialize a comma-separated list of column names into an `Option<Vec<ColumnName>>`. Returns
/// `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_column_names(s: &str) -> DeltaResult<Option<Vec<ColumnName>>> {
    ColumnName::parse_column_name_list(s).map(Some)
}

/// Deserialize an interval string of the form "interval 5 days" into an `Option<Duration>`.
/// Returns `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_interval(s: &str) -> DeltaResult<Option<Duration>> {
    parse_interval_impl(s)
        .map(Some)
        .map_err(Error::ParseIntervalError)
}

#[derive(thiserror::Error, Debug)]
pub enum ParseIntervalError {
    /// The input string is not a valid interval
    #[error("'{0}' is not an interval")]
    NotAnInterval(String),
    /// Couldn't parse the input string as an integer
    #[error("Unable to parse '{0}' as an integer")]
    ParseIntError(String),
    /// Negative intervals aren't supported
    #[error("Interval '{0}' cannot be negative")]
    NegativeInterval(String),
    /// Unsupported interval
    #[error("Unsupported interval '{0}'")]
    UnsupportedInterval(String),
    /// Unknown unit
    #[error("Unknown interval unit '{0}'")]
    UnknownUnit(String),
}

/// This is effectively a simpler version of spark's `CalendarInterval` parser. See spark's
/// `stringToInterval`:
/// https://github.com/apache/spark/blob/5a57efdcee9e6569d8de4272bda258788cf349e3/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkIntervalUtils.scala#L134
///
/// Notably we don't support months nor years, nor do we support fractional values, and negative
/// intervals aren't supported.
///
/// For now this is adapted from delta-rs' `parse_interval` function:
/// https://github.com/delta-io/delta-rs/blob/d4f18b3ae9d616e771b5d0e0fa498d0086fd91eb/crates/core/src/table/config.rs#L474
///
/// See issue delta-kernel-rs/#507 for details: https://github.com/delta-io/delta-kernel-rs/issues/507
fn parse_interval_impl(value: &str) -> Result<Duration, ParseIntervalError> {
    let mut it = value.split_whitespace();
    if it.next() != Some("interval") {
        return Err(ParseIntervalError::NotAnInterval(value.to_string()));
    }
    let number = it
        .next()
        .ok_or_else(|| ParseIntervalError::NotAnInterval(value.to_string()))?;
    let number: i64 = number
        .parse()
        .map_err(|_| ParseIntervalError::ParseIntError(number.into()))?;

    // TODO(zach): spark allows negative intervals, but we don't
    require!(
        number >= 0,
        ParseIntervalError::NegativeInterval(value.to_string())
    );

    // convert to u64 since Duration expects it
    let number = number as u64; // non-negative i64 => always safe

    let duration = match it
        .next()
        .ok_or_else(|| ParseIntervalError::NotAnInterval(value.to_string()))?
    {
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit @ ("month" | "months") => {
            return Err(ParseIntervalError::UnsupportedInterval(unit.to_string()));
        }
        unit => {
            return Err(ParseIntervalError::UnknownUnit(unit.to_string()));
        }
    };

    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("true").unwrap(), Some(true));
        assert_eq!(parse_bool("false").unwrap(), Some(false));
        assert_eq!(
            parse_bool("whatever").unwrap_err().to_string(),
            "'whatever' is not an bool".to_string()
        );
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_positive_int("12").unwrap(), NonZero::<u64>::new(12));
        assert_eq!(parse_positive_int("0").unwrap(), None);
        assert_eq!(parse_positive_int("-12").unwrap(), None);
        assert_eq!(parse_non_negative::<u64>("12").unwrap(), Some(12));
        assert_eq!(parse_non_negative::<u64>("0").unwrap(), Some(0));
        assert_eq!(parse_non_negative::<u64>("-12").unwrap(), None);
        assert_eq!(parse_non_negative::<i64>("12").unwrap(), Some(12));
        assert_eq!(parse_non_negative::<i64>("0").unwrap(), Some(0));
        assert_eq!(parse_non_negative::<i64>("-12").unwrap(), None);
    }

    #[test]
    fn test_parse_interval() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Some(Duration::from_nanos(123))
        );

        assert_eq!(
            parse_interval("interval 123 nanoseconds").unwrap(),
            Some(Duration::from_nanos(123))
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Some(Duration::from_micros(123))
        );

        assert_eq!(
            parse_interval("interval 123 microseconds").unwrap(),
            Some(Duration::from_micros(123))
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Some(Duration::from_millis(123))
        );

        assert_eq!(
            parse_interval("interval 123 milliseconds").unwrap(),
            Some(Duration::from_millis(123))
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Some(Duration::from_secs(123))
        );

        assert_eq!(
            parse_interval("interval 123 seconds").unwrap(),
            Some(Duration::from_secs(123))
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Some(Duration::from_secs(123 * 60))
        );

        assert_eq!(
            parse_interval("interval 123 minutes").unwrap(),
            Some(Duration::from_secs(123 * 60))
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Some(Duration::from_secs(123 * 3600))
        );

        assert_eq!(
            parse_interval("interval 123 hours").unwrap(),
            Some(Duration::from_secs(123 * 3600))
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Some(Duration::from_secs(123 * 86400))
        );

        assert_eq!(
            parse_interval("interval 123 days").unwrap(),
            Some(Duration::from_secs(123 * 86400))
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Some(Duration::from_secs(123 * 604800))
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Some(Duration::from_secs(123 * 604800))
        );
    }

    #[test]
    fn test_invalid_parse_interval() {
        assert_eq!(
            parse_interval_impl("whatever").err().unwrap().to_string(),
            "'whatever' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval").err().unwrap().to_string(),
            "'interval' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2").err().unwrap().to_string(),
            "'interval 2' is not an interval".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2 months")
                .err()
                .unwrap()
                .to_string(),
            "Unsupported interval 'months'".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval 2 years")
                .err()
                .unwrap()
                .to_string(),
            "Unknown interval unit 'years'".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval two years")
                .err()
                .unwrap()
                .to_string(),
            "Unable to parse 'two' as an integer".to_string()
        );

        assert_eq!(
            parse_interval_impl("interval -25 hours")
                .err()
                .unwrap()
                .to_string(),
            "Interval 'interval -25 hours' cannot be negative".to_string()
        );
    }
}
