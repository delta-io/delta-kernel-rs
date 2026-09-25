//! Engine-independent timezone handling for `TIMESTAMP` partition values.
//!
//! Kernel never infers a timezone from the host or the Delta table. A value can carry an embedded
//! zone; otherwise callers provide a [`TimestampTimezone`], whose default is UTC. A timestamp
//! with an explicit offset, including `Z`, identifies an absolute instant.
//!
//! Timestamp strings are parsed with Jiff. Sub-microsecond precision is truncated because Kernel
//! scalars store microseconds.
//!
//! # Terminology
//!
//! An Internet Assigned Numbers Authority (IANA) timezone is a named region such as
//! `America/Los_Angeles`. Its UTC offset is resolved for each timestamp, including clock changes.

use std::str::FromStr;
use std::sync::LazyLock;

use jiff::civil::DateTime;
use jiff::fmt::temporal::{DateTimeParser, Pieces};
use jiff::tz::TimeZone;
use jiff::{RoundMode, Timestamp, TimestampRound, Unit, Zoned};
use regex::Regex;

use crate::{DeltaResult, Error};

static FIXED_OFFSET_REGEX: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"^[+-](?:(?:0[0-9]|1[0-7]):[0-5][0-9]|18:00)$").ok());

/// A validated timezone used to interpret a local `TIMESTAMP` value.
#[derive(Clone, Debug)]
pub(crate) struct TimestampTimezone(TimeZone);

impl Default for TimestampTimezone {
    fn default() -> Self {
        Self(TimeZone::UTC)
    }
}

impl FromStr for TimestampTimezone {
    type Err = Error;

    fn from_str(value: &str) -> DeltaResult<Self> {
        let time_zone = if FIXED_OFFSET_REGEX
            .as_ref()
            .is_some_and(|regex| regex.is_match(value))
        {
            DateTimeParser::new().parse_time_zone(value)
        } else {
            TimeZone::get(value)
        };
        time_zone
            .map(Self)
            .map_err(|_| Error::generic(format!("Invalid timestamp timezone: {value}")))
    }
}

/// Parses a partition timestamp into microseconds since the Unix epoch.
///
/// A value with an explicit offset is parsed as an absolute instant. A local value uses its
/// embedded timezone, if present, or `timezone` otherwise.
/// When clocks move backward, a repeated local time uses the earlier instant. When clocks move
/// forward, a skipped local time uses the pre-transition offset.
pub(crate) fn parse_partition_timestamp(raw: &str, timezone: &TimestampTimezone) -> Option<i64> {
    let pieces = Pieces::parse(raw).ok()?;
    let timestamp = if pieces.time_zone_annotation().is_some() {
        raw.parse::<Zoned>().ok()?.timestamp()
    } else if pieces.offset().is_some() {
        raw.parse::<Timestamp>().ok()?
    } else {
        let local_datetime = raw.parse::<DateTime>().ok()?;
        timezone.0.to_timestamp(local_datetime).ok()?
    };
    // Preserve the leading six fractional digits, including before the Unix epoch.
    timestamp
        .round(
            TimestampRound::new()
                .smallest(Unit::Microsecond)
                .mode(RoundMode::Floor),
        )
        .ok()
        .map(Timestamp::as_microsecond)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn expected_timestamp_micros(timestamp: &str) -> i64 {
        timestamp.parse::<Timestamp>().unwrap().as_microsecond()
    }

    #[test]
    fn fixed_offset_regex_compiles() {
        assert!(FIXED_OFFSET_REGEX.is_some());
    }

    #[rstest]
    #[case::utc("UTC")]
    #[case::named("America/Los_Angeles")]
    #[case::hour_offset("-04:00")]
    #[case::minute_offset("+05:30")]
    #[case::positive_limit("+18:00")]
    #[case::negative_limit("-18:00")]
    fn parses_configured_timezones(#[case] timezone: &str) {
        assert!(timezone.parse::<TimestampTimezone>().is_ok());
    }

    #[rstest]
    #[case::empty("")]
    #[case::bare_sign("+")]
    #[case::malformed_sign("+-5:00")]
    #[case::short_offset("+05")]
    #[case::compact_offset("+0530")]
    #[case::invalid_minutes("+05:60")]
    #[case::positive_limit_with_minutes("+18:01")]
    #[case::outside_positive_limit("+19:00")]
    #[case::offset_with_seconds("+18:00:01")]
    #[case::extra_component("+05:00:00:00")]
    #[case::posix_rule("EST5EDT,M3.2.0,M11.1.0")]
    #[case::unknown_name("Not/AZone")]
    fn rejects_invalid_timezones(#[case] timezone: &str) {
        assert!(timezone.parse::<TimestampTimezone>().is_err());
    }

    #[rstest]
    #[case::local_seconds("2024-01-15 12:30:45", "2024-01-15T12:30:45Z")]
    #[case::local("2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
    #[case::local_short_fraction("2024-01-15 12:30:45.1", "2024-01-15T12:30:45.1Z")]
    #[case::utc_seconds("2024-01-15T12:30:45Z", "2024-01-15T12:30:45Z")]
    #[case::utc("2024-01-15T12:30:45.123456Z", "2024-01-15T12:30:45.123456Z")]
    #[case::utc_short_fraction("2024-01-15T12:30:45.1Z", "2024-01-15T12:30:45.1Z")]
    #[case::local_iso("2024-01-15T12:30:45", "2024-01-15T12:30:45Z")]
    #[case::numeric_offset("2024-01-15T12:30:45+02:00", "2024-01-15T10:30:45Z")]
    #[case::embedded_zone("2024-01-15T12:30:45[America/Los_Angeles]", "2024-01-15T20:30:45Z")]
    #[case::embedded_zone_with_offset(
        "2024-01-15T12:30:45-08:00[America/Los_Angeles]",
        "2024-01-15T20:30:45Z"
    )]
    #[case::embedded_zone_with_z(
        "2024-01-15T12:30:45Z[America/Los_Angeles]",
        "2024-01-15T12:30:45Z"
    )]
    #[case::embedded_zone_dst_overlap(
        "2024-11-03T01:30:00[America/Los_Angeles]",
        "2024-11-03T08:30:00Z"
    )]
    #[case::embedded_zone_dst_gap(
        "2024-03-10T02:30:00[America/Los_Angeles]",
        "2024-03-10T10:30:00Z"
    )]
    #[case::nanoseconds("2024-01-15 12:30:45.123456789", "2024-01-15T12:30:45.123456Z")]
    #[case::pre_epoch_local_submicrosecond(
        "1969-12-31 23:59:59.999999500",
        "1969-12-31T23:59:59.999999Z"
    )]
    #[case::pre_epoch_offset_submicrosecond(
        "1970-01-01T00:59:59.999999500+01:00",
        "1969-12-31T23:59:59.999999Z"
    )]
    fn parses_partition_timestamps(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(
            parse_partition_timestamp(raw, &TimestampTimezone::default()),
            Some(expected_timestamp_micros(expected))
        );
    }

    #[test]
    fn explicit_partition_timestamp_offset_overrides_configured_timezone() {
        let timezone = "America/Los_Angeles".parse().unwrap();
        assert_eq!(
            parse_partition_timestamp("2024-01-15T12:30:45+02:00", &timezone),
            Some(expected_timestamp_micros("2024-01-15T10:30:45Z"))
        );
    }

    #[test]
    fn embedded_partition_timezone_overrides_configured_timezone() {
        let timezone = "America/New_York".parse().unwrap();
        assert_eq!(
            parse_partition_timestamp("2024-01-15T12:30:45[America/Los_Angeles]", &timezone),
            Some(expected_timestamp_micros("2024-01-15T20:30:45Z"))
        );
    }

    #[test]
    fn named_partition_timezone_uses_bundled_timezone_rules() {
        let timezone = "America/Vancouver".parse().unwrap();
        assert_eq!(
            parse_partition_timestamp("2050-12-01 12:00:00", &timezone),
            Some(expected_timestamp_micros("2050-12-01T19:00:00Z"))
        );
    }

    #[rstest]
    #[case::invalid("not a timestamp")]
    #[case::invalid_suffix("2024-01-15T12:30:45XYZ")]
    #[case::trailing_garbage_after_z("2024-01-15T12:30:45ZXYZ")]
    #[case::named_timezone("2024-01-15 12:30:45 America/Los_Angeles")]
    #[case::unknown_embedded_timezone("2024-06-30T08:30:00[Not/AZone]")]
    #[case::conflicting_embedded_offset("2024-01-15T12:30:45+02:00[America/Los_Angeles]")]
    #[case::absolute_normalization_overflow("9999-12-30T22:00:00-01:00")]
    fn rejects_invalid_partition_timestamps(#[case] raw: &str) {
        assert_eq!(
            parse_partition_timestamp(raw, &TimestampTimezone::default()),
            None
        );
    }
}
