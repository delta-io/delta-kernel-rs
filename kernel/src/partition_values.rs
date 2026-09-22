//! Engine-independent timezone handling for `TIMESTAMP` partition values.
//!
//! Kernel never infers this timezone from the host or the Delta table. A connector supplies its
//! timezone through [`MapToStructOptions`], and no option means UTC. A protocol-formatted ISO 8601
//! timestamp carries its own offset, which takes precedence.
//!
//! The parser accepts the Delta protocol's partition timestamp encodings. `CONVERT TO DELTA` can
//! preserve other timestamp strings from partition directories after validating them; those
//! non-protocol encodings are intentionally outside this parser's contract. Fractions beyond
//! microsecond precision are truncated because Kernel timestamp scalars store microseconds.
//!
//! # Terminology
//!
//! An Internet Assigned Numbers Authority (IANA) timezone is a named region such as
//! `America/Los_Angeles`. Its UTC offset is resolved for each timestamp, including clock changes.

use std::borrow::Cow;
use std::str::FromStr;

use jiff::civil::DateTime;
use jiff::tz::{Offset, TimeZone};
use jiff::Timestamp;

#[cfg(feature = "arrow-expression")]
use crate::expressions::MapToStructOptions;
use crate::{DeltaResult, Error};

const MAX_CONFIGURED_OFFSET_HOURS: i32 = 18;
const MAX_TIMESTAMP_OFFSET_HOURS: i32 = 23;
const MICROSECOND_DIGITS: usize = 6;
const SECONDS_PER_HOUR: i32 = 3_600;
const SECONDS_PER_MINUTE: i32 = 60;

/// A validated timezone used to interpret an offset-less `TIMESTAMP` value.
#[derive(Clone, Debug)]
pub(crate) enum TimestampTimezone {
    /// A named timezone whose offset is resolved at each local timestamp.
    ///
    /// For example, `America/Los_Angeles` normally resolves to `-08:00` in winter and `-07:00`
    /// in summer.
    Named(TimeZone),
    /// A constant offset from UTC, such as `+05:30` or `-03:30`.
    Fixed(Offset),
}

impl Default for TimestampTimezone {
    fn default() -> Self {
        Self::Fixed(Offset::UTC)
    }
}

impl FromStr for TimestampTimezone {
    type Err = Error;

    fn from_str(value: &str) -> DeltaResult<Self> {
        let timezone = if value.starts_with(['+', '-']) {
            parse_normalized_fixed_offset(value).map(Self::Fixed)
        } else {
            parse_named_timezone(value).map(Self::Named)
        };
        timezone.ok_or_else(|| Error::generic(format!("Invalid timestamp timezone: {value}")))
    }
}

impl TimestampTimezone {
    /// Resolves the timezone from map-to-struct options, defaulting to UTC.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured value is neither a recognized IANA timezone nor a
    /// fixed offset in `+HH:MM` or `-HH:MM` form.
    #[cfg(feature = "arrow-expression")]
    pub(crate) fn try_from_options(options: &MapToStructOptions) -> DeltaResult<Self> {
        match options.timestamp_timezone() {
            Some(value) => value.parse(),
            None => Ok(Self::default()),
        }
    }

    /// Parses a partition timestamp into microseconds since the Unix epoch.
    ///
    /// A protocol-formatted ISO 8601 value carries its own offset. A space-separated value uses
    /// the configured timezone. For an offset-less value in a named timezone, ambiguous or
    /// nonexistent local times are resolved as follows:
    ///
    /// - If a backward clock change makes a local time occur twice, use the earlier instant.
    /// - If a forward clock change skips a local time, use the offset from before the change. For
    ///   example, `02:30` in a one-hour spring-forward gap is interpreted as `03:30`.
    pub(crate) fn parse_timestamp(&self, raw: &str) -> Option<i64> {
        parse_timestamp(raw, self)
    }
}

/// Parses a normalized fixed offset within the conventional `-18:00` through `+18:00` range.
fn parse_normalized_fixed_offset(value: &str) -> Option<Offset> {
    let (sign, hours, minutes) = parse_normalized_offset_components(value)?;
    if hours > MAX_CONFIGURED_OFFSET_HOURS || (hours == MAX_CONFIGURED_OFFSET_HOURS && minutes != 0)
    {
        return None;
    }
    Offset::from_seconds(sign * (hours * SECONDS_PER_HOUR + minutes * SECONDS_PER_MINUTE)).ok()
}

fn parse_normalized_offset_components(value: &str) -> Option<(i32, i32, i32)> {
    let sign = match value.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let mut components = value.get(1..)?.split(':');
    let hours = parse_two_digits(components.next()?)?;
    let minutes = parse_two_digits(components.next()?)?;
    if components.next().is_some() || minutes > 59 {
        return None;
    }
    Some((sign, hours, minutes))
}

/// Parses exactly two ASCII decimal digits.
fn parse_two_digits(value: &str) -> Option<i32> {
    let [tens, ones] = value.as_bytes() else {
        return None;
    };
    if !tens.is_ascii_digit() || !ones.is_ascii_digit() {
        return None;
    }
    Some(i32::from(*tens - b'0') * 10 + i32::from(*ones - b'0'))
}

fn parse_timestamp(raw: &str, timezone: &TimestampTimezone) -> Option<i64> {
    if has_local_timestamp_syntax(raw) {
        let local_datetime = truncate_subseconds(raw).parse().ok()?;
        return resolve_local_timestamp(local_datetime, timezone);
    }
    parse_explicit_offset_timestamp(raw)
}

fn has_local_timestamp_syntax(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if !has_timestamp_prefix_syntax(bytes, b' ') {
        return false;
    }
    match &bytes[19..] {
        [] => true,
        [b'.', fraction @ ..] => !fraction.is_empty() && fraction.iter().all(u8::is_ascii_digit),
        _ => false,
    }
}

fn parse_explicit_offset_timestamp(raw: &str) -> Option<i64> {
    let bytes = raw.as_bytes();
    if !has_timestamp_prefix_syntax(bytes, b'T') {
        return None;
    }

    let suffix = raw.get(19..)?;
    let offset = if let Some(fraction_and_offset) = suffix.strip_prefix('.') {
        let fraction_len = fraction_and_offset
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        if fraction_len == 0 {
            return None;
        }
        fraction_and_offset.get(fraction_len..)?
    } else {
        suffix
    };
    if offset != "Z" {
        let (_, hours, _) = parse_normalized_offset_components(offset)?;
        if hours > MAX_TIMESTAMP_OFFSET_HOURS {
            return None;
        }
    }

    truncate_subseconds(raw)
        .parse::<Timestamp>()
        .ok()
        .map(Timestamp::as_microsecond)
}

fn truncate_subseconds(raw: &str) -> Cow<'_, str> {
    let Some(decimal) = raw.find('.') else {
        return Cow::Borrowed(raw);
    };
    let fraction_start = decimal + 1;
    let fraction_len = raw[fraction_start..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    if fraction_len <= MICROSECOND_DIGITS {
        return Cow::Borrowed(raw);
    }

    let fraction_end = fraction_start + fraction_len;
    let mut truncated = String::with_capacity(raw.len() - fraction_len + MICROSECOND_DIGITS);
    truncated.push_str(&raw[..fraction_start + MICROSECOND_DIGITS]);
    truncated.push_str(&raw[fraction_end..]);
    Cow::Owned(truncated)
}

fn has_timestamp_prefix_syntax(bytes: &[u8], separator: u8) -> bool {
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != separator
        || bytes[13] != b':'
        || bytes[16] != b':'
        || &bytes[17..19] == b"60"
    {
        return false;
    }
    let digits = [
        &bytes[..4],
        &bytes[5..7],
        &bytes[8..10],
        &bytes[11..13],
        &bytes[14..16],
        &bytes[17..19],
    ];
    digits.into_iter().flatten().all(u8::is_ascii_digit)
}

fn parse_named_timezone(value: &str) -> Option<TimeZone> {
    let timezone = TimeZone::get(value).ok()?;
    (timezone.iana_name() == Some(value)).then_some(timezone)
}

fn resolve_local_timestamp(local_datetime: DateTime, timezone: &TimestampTimezone) -> Option<i64> {
    match timezone {
        TimestampTimezone::Fixed(offset) => offset
            .to_timestamp(local_datetime)
            .ok()
            .map(Timestamp::as_microsecond),
        TimestampTimezone::Named(timezone) => timezone
            .to_ambiguous_timestamp(local_datetime)
            .compatible()
            .ok()
            .map(Timestamp::as_microsecond),
    }
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;
    use rstest::rstest;

    use super::*;

    fn expected_timestamp_micros(timestamp: &str) -> i64 {
        timestamp.parse::<Timestamp>().unwrap().as_microsecond()
    }

    #[rstest]
    #[case::named("America/Los_Angeles")]
    #[case::minute_offset("+05:30")]
    #[case::positive_limit("+18:00")]
    #[case::negative_limit("-18:00")]
    fn accepts_normalized_timezones(#[case] timezone: &str) {
        assert!(timezone.parse::<TimestampTimezone>().is_ok());
    }

    #[rstest]
    #[case::empty("")]
    #[case::bare_sign("+")]
    #[case::malformed_sign("+-5:00")]
    #[case::compact_hour("+05")]
    #[case::compact_hour_minute("+0530")]
    #[case::invalid_minutes("+05:60")]
    #[case::second_precision("+05:00:01")]
    #[case::past_limit("+18:00:01")]
    #[case::past_limit_first_minute("+18:01")]
    #[case::past_limit_minutes("+18:30")]
    #[case::invalid_hours("+19:00")]
    #[case::extra_component("+05:00:00:00")]
    #[case::lowercase_utc("utc")]
    #[case::unknown_name("Not/AZone")]
    fn rejects_noncanonical_timezones(#[case] timezone: &str) {
        assert!(timezone.parse::<TimestampTimezone>().is_err());
    }

    #[rstest]
    #[case::local("2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
    #[case::utc("2024-01-15T12:30:45.123456Z", "2024-01-15T12:30:45.123456Z")]
    #[case::positive_offset("2024-01-15T17:30:45+05:30", "2024-01-15T12:00:45Z")]
    #[case::negative_offset("2024-01-15T07:00:45-05:30", "2024-01-15T12:30:45Z")]
    #[case::offset_beyond_timezone_limit("2024-01-15T12:30:45+19:00", "2024-01-14T17:30:45Z")]
    #[case::positive_offset_limit("2024-01-15T23:59:59+23:59", "2024-01-15T00:00:59Z")]
    #[case::negative_offset_limit("2024-01-15T00:00:00-23:59", "2024-01-15T23:59:00Z")]
    #[case::excess_local_fraction("2024-01-15 12:30:45.123456789", "2024-01-15T12:30:45.123456Z")]
    #[case::excess_explicit_fraction(
        "2024-01-15T12:30:45.123456789Z",
        "2024-01-15T12:30:45.123456Z"
    )]
    #[case::pre_epoch_excess_fraction(
        "1969-12-31 23:59:59.999999500",
        "1969-12-31T23:59:59.999999Z"
    )]
    fn parses_supported_partition_timestamps(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(
            TimestampTimezone::default().parse_timestamp(raw),
            Some(expected_timestamp_micros(expected))
        );
    }

    #[rstest]
    #[case::embedded_timezone("2024-01-15 12:30:45 America/New_York")]
    #[case::date_only("2024-01-15")]
    #[case::zoneless_t("2024-01-15T12:30:45")]
    #[case::lowercase_t("2024-01-15t12:30:45")]
    #[case::compact_clock("2024-01-15 123045")]
    #[case::compact_offset("2024-01-15T12:30:45+0530")]
    #[case::second_precision_offset("2024-01-15T12:30:45+05:30:15")]
    #[case::single_digit_month("2024-1-15 12:30:45")]
    #[case::offset_past_limit("2024-01-15T12:30:45+24:00")]
    #[case::invalid_suffix("2024-01-15T12:30:45XYZ")]
    #[case::unicode_offset_sign("2024-01-15T12:30:45−07:00")]
    #[case::lowercase_z("2024-01-15T12:30:45z")]
    #[case::leap_second("2024-01-15T12:30:60Z")]
    #[case::local_leap_second("2024-01-15 12:30:60")]
    fn rejects_unsupported_partition_timestamps(#[case] raw: &str) {
        assert_eq!(TimestampTimezone::default().parse_timestamp(raw), None);
    }
}
