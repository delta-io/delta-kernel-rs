//! For now we just use simple functions to deserialize table properties from strings. This allows
//! us to relatively simply implement the functionality described in the protocol and expose
//! 'simple' types to the user in the [`TableProperties`] struct. E.g. we can expose a `bool`
//! directly instead of a `BoolConfig` type that we implement `Deserialize` for.
use std::num::NonZero;
use std::time::Duration;

use super::*;
use crate::expressions::ColumnName;
use crate::table_features::ColumnMappingMode;
use crate::utils::require;

use tracing::warn;

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
            try_parse(&mut props, k.as_ref(), v.as_ref()).is_none()
        });
        props.unknown_properties = unparsed.map(|(k, v)| (k.into(), v.into())).collect();
        props
    }
}

/// Generates the `try_parse` function from the table property definitions in
/// [`with_table_properties!`].
///
/// NOTE: Properties using `Some(parse_fn(v)?)` will return `None` from `try_parse` if parsing
/// fails, causing the key-value pair to be placed in `unknown_properties`. Properties using
/// `.ok()` (like enum conversions) will silently set the field to `None` while still consuming
/// the key — this preserves the existing behavior where some property types tolerate invalid values
/// gracefully.
macro_rules! generate_try_parse {
    ($v:ident, $(($const_name:ident, $_key:expr, $field:ident, $parse:block, $_help:expr)),* $(,)?) => {
        // Attempt to parse a key-value pair into a `TableProperties` struct. Returns Some(()) if
        // the key was successfully parsed, and None otherwise.
        fn try_parse(props: &mut TableProperties, k: &str, $v: &str) -> Option<()> {
            match k {
                $($const_name => props.$field = $parse,)*
                _ => return None,
            }
            Some(())
        }
    };
}

with_table_properties!(generate_try_parse, v);

/// Generates the `property_help_message` function that returns the help message for a known
/// table property key. Each help message describes what constitutes a valid value, inspired by
/// Spark's `DeltaConfig[T].helpMessage`.
macro_rules! generate_help_messages {
    ($_v:ident, $(($const_name:ident, $_key:expr, $_field:ident, $_parse:block, $help:expr)),* $(,)?) => {
        /// Returns the help message for a known table property key, or `None` for unknown keys.
        fn property_help_message(key: &str) -> Option<&'static str> {
            match key {
                $($const_name => Some($help),)*
                _ => None,
            }
        }
    };
}

with_table_properties!(generate_help_messages, v);

/// Generates the `is_field_none_for_key` helper that checks whether the field corresponding to a
/// given key is `None` in a [`TableProperties`] struct. This is used by [`validate_property_value`]
/// to detect properties that use `.ok()` parsing (which silently consumes invalid values in
/// `try_parse` — the key is accepted but the field stays `None`).
macro_rules! generate_field_none_check {
    ($_v:ident, $(($const_name:ident, $_key:expr, $field:ident, $_parse:block, $_help:expr)),* $(,)?) => {
        /// Returns `true` if the field for the given key is `None` in the given properties.
        fn is_field_none_for_key(props: &TableProperties, key: &str) -> bool {
            match key {
                $($const_name => props.$field.is_none(),)*
                _ => false,
            }
        }
    };
}

with_table_properties!(generate_field_none_check, v);

/// Validates that a string value is valid for a given table property key, returning a descriptive
/// error message (including the help message) on failure. This mirrors Spark's
/// `DeltaConfig[T].validate()` method which combines `fromString` + `validationFunction` +
/// `helpMessage`.
///
/// Validation happens in two phases:
/// 1. **Type parsing**: Checks that the value can be parsed into the expected type (e.g., bool,
///    integer, interval). This is analogous to Spark's `fromString`.
/// 2. **Semantic validation**: For properties with additional constraints beyond type parsing
///    (e.g., `isolationLevel` must be `Serializable`), a post-parse check is applied. This is
///    analogous to Spark's `validationFunction`. Most properties use `_ => true` in Spark,
///    meaning parsing alone is sufficient.
///
/// Intended for use in write paths (e.g., CREATE TABLE, ALTER TABLE) to validate property values
/// before they are stored in the Delta log.
///
/// Returns `Ok(())` if:
/// - The key is a known delta property and the value parses and validates successfully
/// - The key is not a known delta property (unknown keys are always valid)
///
/// Returns `Err(message)` if:
/// - The key is a known delta property but the value fails to parse or validate
pub(crate) fn validate_property_value(key: &str, value: &str) -> Result<(), String> {
    let mut temp = TableProperties::default();
    if try_parse(&mut temp, key, value).is_none() {
        // Key was not consumed — either it's unknown (always valid) or parsing failed via `?`
        return match property_help_message(key) {
            Some(help) => Err(format!(
                "Invalid value '{}' for table property '{}': {}",
                value, key, help
            )),
            None => Ok(()), // Unknown key, always valid
        };
    }

    // Key was consumed by try_parse. For properties that use `.ok()` parsing (e.g. enum
    // conversions), try_parse returns Some(()) even when the value is invalid — it just sets
    // the field to None. Detect this by checking if the field is still None after parsing.
    if is_field_none_for_key(&temp, key) {
        return match property_help_message(key) {
            Some(help) => Err(format!(
                "Invalid value '{}' for table property '{}': {}",
                value, key, help
            )),
            None => Ok(()),
        };
    }

    // Phase 2: Post-parse semantic validation (Spark's validationFunction).
    // Most properties use `_ => true` in Spark, meaning parsing alone is sufficient.
    // The few exceptions are handled here.
    validate_parsed_value(&temp, key, value)?;

    Ok(())
}

/// Additional semantic validation beyond type parsing, matching Spark's `validationFunction`.
///
/// Most properties in Spark use `validationFunction = _ => true`, meaning that if the value
/// parses successfully, it's valid. The following properties have stricter validation:
///
/// - `delta.isolationLevel`: Spark restricts to `Serializable` only (`_ == Serializable`)
///
/// Properties where Spark has validation but kernel already enforces via parsing:
/// - `delta.checkpointInterval`: Spark validates `_ > 0`; kernel uses `parse_positive_int`
/// - `delta.randomPrefixLength`: Spark validates `_ > 0`; kernel uses `parse_positive_int`
/// - `delta.dataSkippingNumIndexedCols`: Spark validates `_ >= -1`; kernel uses `TryFrom`
fn validate_parsed_value(props: &TableProperties, key: &str, value: &str) -> Result<(), String> {
    if key == ISOLATION_LEVEL {
        if let Some(level) = &props.isolation_level {
            if *level != IsolationLevel::Serializable {
                return Err(format!(
                    "Invalid value '{}' for table property '{}': {}",
                    value,
                    key,
                    property_help_message(key).unwrap_or("must be Serializable.")
                ));
            }
        }
    }
    Ok(())
}

/// Validates compatibility between related table properties, matching Spark's cross-property
/// validation in `DeltaConfigs.validateTombstoneAndLogRetentionDurationCompatibility`.
///
/// Currently validates:
/// - `delta.logRetentionDuration` must be >= `delta.deletedFileRetentionDuration`
///
/// This should be called after individual property values have been validated via
/// [`validate_property_value`].
pub(crate) fn validate_properties_compatibility(
    properties: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    let log_retention = properties
        .get(LOG_RETENTION_DURATION)
        .and_then(|v| parse_interval(v));
    let tombstone_retention = properties
        .get(DELETED_FILE_RETENTION_DURATION)
        .and_then(|v| parse_interval(v));

    if let (Some(log), Some(tombstone)) = (log_retention, tombstone_retention) {
        if log < tombstone {
            return Err(format!(
                "The table property '{}' ({:?}) needs to be greater than or equal to '{}' ({:?}).",
                LOG_RETENTION_DURATION, log, DELETED_FILE_RETENTION_DURATION, tombstone
            ));
        }
    }

    Ok(())
}

/// Deserialize a string representing a positive (> 0) integer into an `Option<u64>`. Returns `Some`
/// if successfully parses, and `None` otherwise.
pub(crate) fn parse_positive_int(s: &str) -> Option<NonZero<u64>> {
    // parse as non-negative and verify the result is non-zero
    NonZero::new(parse_non_negative(s)?)
}

/// Deserialize a string representing a non-negative integer into an `Option<u64>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_non_negative<T>(s: &str) -> Option<T>
where
    i64: TryInto<T>,
{
    // parse to i64 since java doesn't even allow u64
    let n: i64 = s.parse().ok().filter(|&i| i >= 0)?;
    n.try_into().ok()
}

/// Deserialize a string representing a boolean into an `Option<bool>`. Returns `Some` if
/// successfully parses, and `None` otherwise.
pub(crate) fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Deserialize a comma-separated list of column names into an `Option<Vec<ColumnName>>`. Returns
/// `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_column_names(s: &str) -> Option<Vec<ColumnName>> {
    ColumnName::parse_column_name_list(s)
        .inspect_err(|e| warn!("column name list failed to parse: {e}"))
        .ok()
}

/// Deserialize an interval string of the form "interval 5 days" into an `Option<Duration>`.
/// Returns `Some` if successfully parses, and `None` otherwise.
pub(crate) fn parse_interval(s: &str) -> Option<Duration> {
    parse_interval_impl(s).ok()
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
        assert!(parse_bool("true").unwrap());
        assert!(!parse_bool("false").unwrap());
        assert_eq!(parse_bool("whatever"), None);
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_positive_int("12").unwrap().get(), 12);
        assert_eq!(parse_positive_int("0"), None);
        assert_eq!(parse_positive_int("-12"), None);
        assert_eq!(parse_non_negative::<u64>("12").unwrap(), 12);
        assert_eq!(parse_non_negative::<u64>("0").unwrap(), 0);
        assert_eq!(parse_non_negative::<u64>("-12"), None);
        assert_eq!(parse_non_negative::<i64>("12").unwrap(), 12);
        assert_eq!(parse_non_negative::<i64>("0").unwrap(), 0);
        assert_eq!(parse_non_negative::<i64>("-12"), None);
    }

    #[test]
    fn test_parse_interval() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 nanoseconds").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 microseconds").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 milliseconds").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 seconds").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 minutes").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 hours").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 days").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
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
