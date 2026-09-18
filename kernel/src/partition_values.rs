//! Engine-independent timezone handling for `TIMESTAMP` partition values.
//!
//! Kernel never infers this timezone from the host or the Delta table. A connector supplies its
//! timezone through [`MapToStructOptions`], and no option means UTC. A protocol-formatted ISO 8601
//! timestamp carries its own offset, which takes precedence.
//!
//! # Terminology
//!
//! An Internet Assigned Numbers Authority (IANA) timezone is a named region such as
//! `America/Los_Angeles`. Its UTC offset is resolved for each timestamp, including clock changes.

use std::borrow::Cow;
use std::str::FromStr;

use chrono::{
    DateTime, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone,
};
use chrono_tz::Tz;

#[cfg(feature = "arrow-expression")]
use crate::expressions::MapToStructOptions;
use crate::{DeltaResult, Error};

/// A validated timezone used to interpret an offset-less `TIMESTAMP` value.
///
/// `chrono_tz::Tz` represents named zones but not fixed offsets, so the enum carries both forms.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimestampTimezone {
    /// A named timezone whose offset is resolved at each local timestamp.
    ///
    /// For example, `America/Los_Angeles` normally resolves to `-08:00` in winter and `-07:00`
    /// in summer.
    Named(Tz),
    /// A constant offset from UTC, such as `+05:30` or `-03:30`.
    Fixed(FixedOffset),
}

impl Default for TimestampTimezone {
    fn default() -> Self {
        Self::Named(chrono_tz::UTC)
    }
}

impl FromStr for TimestampTimezone {
    type Err = Error;

    fn from_str(value: &str) -> DeltaResult<Self> {
        let timezone = if value.starts_with(['+', '-']) {
            parse_normalized_fixed_offset(value).map(Self::Fixed)
        } else {
            value.parse::<Tz>().ok().map(Self::Named)
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
    /// ISO 8601 timestamps are interpreted using their explicit offset, ignoring the configured
    /// timezone. Offset-less timestamps are interpreted in the configured timezone. When that
    /// timezone is named, ambiguous or nonexistent local times are resolved as follows:
    ///
    /// - If a backward clock change makes a local time occur twice, use the earlier instant.
    /// - If a forward clock change skips a local time, use the offset from before the change. For
    ///   example, `02:30` in a one-hour spring-forward gap is interpreted as `03:30`.
    pub(crate) fn parse_timestamp(self, raw: &str) -> Option<i64> {
        parse_timestamp(raw, self)
    }
}

/// Parses a normalized fixed offset within the conventional `-18:00` through `+18:00` range.
fn parse_normalized_fixed_offset(value: &str) -> Option<FixedOffset> {
    let sign = match value.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let mut components = value.get(1..)?.split(':');
    let hours = parse_two_digits(components.next()?)?;
    let minutes = parse_two_digits(components.next()?)?;
    if components.next().is_some() || hours > 18 || minutes > 59 || (hours == 18 && minutes != 0) {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60))
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

fn parse_timestamp(raw: &str, timezone: TimestampTimezone) -> Option<i64> {
    let raw = truncate_fraction(raw);
    if let Some((local_datetime, embedded_timezone)) = parse_embedded_timezone(&raw) {
        return resolve_local_timestamp(
            local_datetime,
            TimestampTimezone::Named(embedded_timezone),
        );
    }
    if let Some(timestamp) = parse_explicit_offset_timestamp(&raw) {
        return Some(timestamp);
    }
    if let Some(local_datetime) = parse_local_timestamp(&raw) {
        return resolve_local_timestamp(local_datetime, timezone);
    }
    None
}

fn parse_explicit_offset_timestamp(raw: &str) -> Option<i64> {
    if !matches!(raw.as_bytes().get(10), Some(b'T' | b' ')) || raw.ends_with('z') {
        return None;
    }
    let timestamp = DateTime::parse_from_str(raw, "%+")
        .or_else(|_| DateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f%#z"))
        .ok()?;
    // java.time.ZoneOffset, and therefore Spark, limits offsets to this range.
    (timestamp.offset().local_minus_utc().unsigned_abs() <= 18 * 3_600)
        .then(|| timestamp.timestamp_micros())
}

fn parse_embedded_timezone(raw: &str) -> Option<(NaiveDateTime, Tz)> {
    let separator = raw.rfind(char::is_whitespace)?;
    let timezone = raw.get(separator + 1..)?.parse().ok()?;
    let local_datetime =
        NaiveDateTime::parse_from_str(raw.get(..separator)?.trim_end(), "%Y-%m-%d %H:%M:%S%.f")
            .ok()?;
    Some((local_datetime, timezone))
}

fn parse_local_timestamp(raw: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
        })
}

fn truncate_fraction(raw: &str) -> Cow<'_, str> {
    let Some(decimal_index) = raw.find('.') else {
        return Cow::Borrowed(raw);
    };
    let fraction_start = decimal_index + 1;
    let fraction_len = raw[fraction_start..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    if fraction_len <= 6 {
        return Cow::Borrowed(raw);
    }
    let mut truncated = String::with_capacity(raw.len() - (fraction_len - 6));
    truncated.push_str(&raw[..fraction_start + 6]);
    truncated.push_str(&raw[fraction_start + fraction_len..]);
    Cow::Owned(truncated)
}

fn resolve_local_timestamp(
    local_datetime: NaiveDateTime,
    timezone: TimestampTimezone,
) -> Option<i64> {
    match timezone {
        TimestampTimezone::Fixed(timezone) => timezone
            .from_local_datetime(&local_datetime)
            .single()
            .map(|timestamp| timestamp.timestamp_micros()),
        TimestampTimezone::Named(timezone) => match timezone.from_local_datetime(&local_datetime) {
            LocalResult::Ambiguous(first, second) => Some(first.min(second).timestamp_micros()),
            LocalResult::None => resolve_nonexistent_local_timestamp(local_datetime, timezone),
            LocalResult::Single(timestamp) => Some(timestamp.timestamp_micros()),
        },
    }
}

/// Resolves a local timestamp skipped by a forward clock transition using the prior offset.
fn resolve_nonexistent_local_timestamp(local_datetime: NaiveDateTime, timezone: Tz) -> Option<i64> {
    // Walking back 48 hours finds the prior offset even when a zone skips a full calendar day.
    let offset = (1..=48).find_map(|hours| {
        let before_transition = local_datetime.checked_sub_signed(TimeDelta::hours(hours))?;
        timezone
            .from_local_datetime(&before_transition)
            .earliest()
            .map(|timestamp| timestamp.offset().fix())
    })?;
    local_datetime
        .checked_sub_signed(TimeDelta::seconds(i64::from(offset.local_minus_utc())))
        .map(|utc_datetime| utc_datetime.and_utc().timestamp_micros())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn expected_timestamp_micros(timestamp: &str) -> i64 {
        DateTime::parse_from_rfc3339(timestamp)
            .unwrap()
            .timestamp_micros()
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
    #[case::past_limit_minutes("+18:30")]
    #[case::invalid_hours("+19:00")]
    #[case::extra_component("+05:00:00:00")]
    #[case::unknown_name("Not/AZone")]
    fn rejects_noncanonical_timezones(#[case] timezone: &str) {
        assert!(timezone.parse::<TimestampTimezone>().is_err());
    }

    #[rstest]
    #[case::local("2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
    #[case::utc("2024-01-15T12:30:45.123456Z", "2024-01-15T12:30:45.123456Z")]
    #[case::positive_offset("2024-01-15T17:30:45+05:30", "2024-01-15T12:00:45Z")]
    #[case::negative_offset("2024-01-15T07:00:45-05:30", "2024-01-15T12:30:45Z")]
    #[case::excess_fraction("2024-01-15 12:30:45.123456789", "2024-01-15T12:30:45.123456Z")]
    #[case::excess_fraction_with_offset(
        "2024-01-15T12:30:45.123456789+05:30",
        "2024-01-15T07:00:45.123456Z"
    )]
    #[case::space_separated_offset("2024-01-15 12:30:45+02:00", "2024-01-15T10:30:45Z")]
    #[case::spaced_offset("2024-01-15 17:30:45 +05:30", "2024-01-15T12:00:45Z")]
    #[case::embedded_timezone("2024-01-15 12:30:45 America/New_York", "2024-01-15T17:30:45Z")]
    #[case::date_only("2024-01-15", "2024-01-15T00:00:00Z")]
    #[case::zoneless_t("2024-01-15T12:30:45", "2024-01-15T12:30:45Z")]
    #[case::compact_offset("2024-01-15T12:30:45+0530", "2024-01-15T07:00:45Z")]
    #[case::hour_only_offset("2024-01-15T12:30:45+07", "2024-01-15T05:30:45Z")]
    #[case::pre_epoch_excess_fraction(
        "1969-12-31 23:59:59.999999500",
        "1969-12-31T23:59:59.999999Z"
    )]
    #[case::ten_digit_fraction("2024-01-15 12:30:45.1234567891", "2024-01-15T12:30:45.123456Z")]
    fn parses_supported_partition_timestamps(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(
            TimestampTimezone::default().parse_timestamp(raw),
            Some(expected_timestamp_micros(expected))
        );
    }

    #[rstest]
    #[case::lowercase_t("2024-01-15t12:30:45")]
    #[case::compact_clock("2024-01-15 123045")]
    #[case::offset_past_limit("2024-01-15T12:30:45+19:00")]
    #[case::extra_offset_component("2024-01-15T12:30:45+05:30:15:00")]
    #[case::bad_compact_offset("2024-01-15T12:30:45+053")]
    #[case::invalid_suffix("2024-01-15T12:30:45XYZ")]
    #[case::trailing_garbage_after_zone("2024-01-15T12:30:45ZXYZ")]
    fn rejects_unsupported_partition_timestamps(#[case] raw: &str) {
        assert_eq!(TimestampTimezone::default().parse_timestamp(raw), None);
    }
}
