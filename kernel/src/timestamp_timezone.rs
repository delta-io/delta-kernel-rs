//! Reader-timezone handling for `TIMESTAMP` partition values.
//!
//! Kernel never infers this timezone from the host or the Delta table. A connector supplies its
//! reader/session timezone through [`MapToStructOptions`], and no option means UTC. Timestamp
//! strings may carry their own offset or named timezone. This preserves the Arrow evaluator's
//! partition-value compatibility; the embedded value takes precedence.

use chrono::{
    Datelike, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone, Utc,
};
use chrono_tz::Tz;

use crate::arrow::compute::kernels::cast_utils::string_to_datetime;
use crate::expressions::{MapToStructOptions, Scalar};
use crate::schema::{DataType, PrimitiveType};
use crate::{DeltaResult, Error};

/// A validated timezone used to interpret an offset-less `TIMESTAMP` value.
///
/// `chrono_tz::Tz` represents named zones but not normalized second-precision fixed offsets, so
/// the enum carries both forms.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimestampTimezone {
    /// A named timezone whose offset is resolved at each local timestamp.
    ///
    /// For example, `America/Los_Angeles` normally resolves to `-08:00` in winter and `-07:00`
    /// in summer.
    Named(Tz),
    /// A constant offset from UTC, such as `+05:30` or `-13:33:33`.
    Fixed(FixedOffset),
}

impl Default for TimestampTimezone {
    fn default() -> Self {
        Self::Named(chrono_tz::UTC)
    }
}

impl TimestampTimezone {
    /// Resolves the reader timezone from map-to-struct options, defaulting to UTC.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured value is neither a recognized IANA timezone nor a
    /// fixed offset in `+HH:MM`, `-HH:MM`, `+HH:MM:SS`, or `-HH:MM:SS` form.
    pub(crate) fn try_from_options(options: &MapToStructOptions) -> DeltaResult<Self> {
        options
            .timestamp_timezone()
            .map_or(Ok(Self::default()), Self::parse)
    }

    /// Parses a recognized IANA timezone identifier or normalized fixed offset.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not a known IANA timezone or a fixed offset in normalized
    /// `+HH:MM`, `-HH:MM`, `+HH:MM:SS`, or `-HH:MM:SS` form.
    pub(crate) fn parse(value: &str) -> DeltaResult<Self> {
        let timezone = if value.starts_with(['+', '-']) {
            parse_normalized_fixed_offset(value).map(Self::Fixed)
        } else {
            value.parse::<Tz>().ok().map(Self::Named)
        };
        timezone.ok_or_else(|| Error::generic(format!("Invalid timestamp timezone: {value}")))
    }

    /// Parses a partition timestamp into microseconds since the Unix epoch.
    ///
    /// Arrow handles the timestamp grammar and any offset or timezone carried by `raw`. For an
    /// offset-less value in a configured named timezone, clock transitions resolve as follows:
    ///
    /// - If a backward clock change makes a local time occur twice, use the earlier instant.
    /// - If a forward clock change skips a local time, use the offset from before the change. For
    ///   example, `02:30` in a one-hour spring-forward gap is interpreted as `03:30`.
    pub(crate) fn parse_timestamp(self, raw: &str) -> Option<i64> {
        match self {
            Self::Named(timezone) => parse_timestamp_in_named_timezone(raw, timezone),
            Self::Fixed(timezone) => string_to_datetime(&timezone, raw)
                .ok()
                .map(|timestamp| timestamp.timestamp_micros()),
        }
    }
}

/// Parses a raw partition value using map-to-struct's empty-string and timestamp semantics.
/// `timestamp_timezone` applies only to `TIMESTAMP` values without an embedded offset or named
/// timezone; it does not affect `DATE` or `TIMESTAMP_NTZ`.
pub(crate) fn parse_partition_scalar(
    primitive: &PrimitiveType,
    raw: &str,
    timestamp_timezone: TimestampTimezone,
) -> DeltaResult<Option<Scalar>> {
    if raw.is_empty() {
        return Ok(primitive.empty_string_partition_cast());
    }
    let parsed = match primitive {
        PrimitiveType::Date => parse_partition_date(raw).map(Scalar::Date),
        PrimitiveType::Timestamp => timestamp_timezone
            .parse_timestamp(raw)
            .map(Scalar::Timestamp),
        // Preserve the timestamp forms accepted by the Arrow evaluator without applying the
        // reader timezone to a timezone-independent value.
        PrimitiveType::TimestampNtz => TimestampTimezone::default()
            .parse_timestamp(raw)
            .map(Scalar::TimestampNtz),
        _ => {
            let scalar = primitive.parse_scalar(raw)?;
            return Ok((!matches!(scalar, Scalar::Null(_))).then_some(scalar));
        }
    };
    parsed
        .map(Some)
        .ok_or_else(|| Error::ParseError(raw.to_string(), DataType::Primitive(primitive.clone())))
}

/// Preserves the date forms accepted by Arrow's partition-value parser.
fn parse_partition_date(raw: &str) -> Option<i32> {
    if raw.starts_with(['+', '-']) {
        let rest = raw.get(1..)?;
        let year_end = rest.find('-')?;
        if year_end < 4 {
            return None;
        }
        let year: i32 = raw.get(..=year_end)?.parse().ok()?;
        let (month, day) = raw.get(year_end + 2..)?.split_once('-')?;
        let month = month.parse().ok()?;
        let day = day.parse().ok()?;

        // The Gregorian calendar repeats every 400 years, which lets Date32 represent years
        // outside chrono's range while still validating the month and day.
        let era = i64::from(year).div_euclid(400);
        let year_in_era = year.rem_euclid(400);
        let date = NaiveDate::from_ymd_opt(year_in_era, month, day)?;
        let days = era * 146_097 + i64::from(date.num_days_from_ce() - 719_163);
        return i32::try_from(days).ok();
    }
    if raw.len() > 10 {
        let micros = TimestampTimezone::default().parse_timestamp(raw)?;
        return i32::try_from(micros.div_euclid(86_400_000_000)).ok();
    }
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(raw, "%Y%m%d"))
        .ok()
        .map(|date| date.num_days_from_ce() - 719_163)
}

/// Parses a normalized fixed offset within the conventional `-18:00` through `+18:00` range.
fn parse_normalized_fixed_offset(value: &str) -> Option<FixedOffset> {
    let sign = match value.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let mut components = value.get(1..)?.split(':');
    let hours = parse_digits(components.next()?, 2, 2)?;
    let minutes = parse_digits(components.next()?, 2, 2)?;
    let seconds = match components.next() {
        Some(component) => parse_digits(component, 2, 2)?,
        None => 0,
    };
    if components.next().is_some() || hours > 18 || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    build_fixed_offset(sign, hours, minutes, seconds)
}

fn build_fixed_offset(sign: i32, hours: i32, minutes: i32, seconds: i32) -> Option<FixedOffset> {
    if minutes > 59 || seconds > 59 {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60 + seconds))
}

fn parse_digits(value: &str, min_len: usize, max_len: usize) -> Option<i32> {
    ((min_len..=max_len).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

/// Parses with Arrow, resolving only named-zone clock transitions that Arrow rejects.
fn parse_timestamp_in_named_timezone(raw: &str, timezone: Tz) -> Option<i64> {
    if let Ok(timestamp) = string_to_datetime(&timezone, raw) {
        return Some(timestamp.timestamp_micros());
    }
    let local_datetime = string_to_datetime(&Utc, raw).ok()?.naive_utc();
    match timezone.from_local_datetime(&local_datetime) {
        LocalResult::Ambiguous(first, second) => Some(first.min(second).timestamp_micros()),
        LocalResult::None => resolve_nonexistent_local_timestamp(local_datetime, timezone),
        LocalResult::Single(_) => None,
    }
}

/// Resolves a local timestamp skipped by a forward clock transition using the prior offset.
fn resolve_nonexistent_local_timestamp(local_datetime: NaiveDateTime, timezone: Tz) -> Option<i64> {
    // This covers full-day timezone transitions while still reaching a valid prior instant.
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
    use chrono::DateTime;
    use rstest::rstest;

    use super::*;

    fn expected_timestamp_micros(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_micros()
    }

    fn options(timezone: &str) -> MapToStructOptions {
        MapToStructOptions::default().with_timestamp_timezone(timezone)
    }

    #[rstest]
    #[case::named("America/Los_Angeles")]
    #[case::minute_offset("+05:30")]
    #[case::second_offset("-13:33:33")]
    #[case::positive_limit("+18:00")]
    #[case::negative_limit("-18:00")]
    fn accepts_normalized_reader_timezones(#[case] timezone: &str) {
        assert!(TimestampTimezone::try_from_options(&options(timezone)).is_ok());
    }

    #[rstest]
    #[case::empty("")]
    #[case::bare_sign("+")]
    #[case::compact_hour("+05")]
    #[case::compact_hour_minute("+0530")]
    #[case::invalid_minutes("+05:60")]
    #[case::past_limit("+18:00:01")]
    #[case::past_limit_minutes("+18:30")]
    #[case::invalid_hours("+19:00")]
    #[case::invalid_seconds("+05:00:60")]
    #[case::extra_component("+05:00:00:00")]
    #[case::unknown_name("Not/AZone")]
    fn rejects_noncanonical_reader_timezones(#[case] timezone: &str) {
        assert!(TimestampTimezone::try_from_options(&options(timezone)).is_err());
    }

    #[rstest]
    #[case::local("2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
    #[case::compact_offset("2024-01-15T17:30:45+0530", "2024-01-15T12:00:45Z")]
    #[case::offset_beyond_reader_limit("2024-01-15T12:30:45+19:00", "2024-01-14T17:30:45Z")]
    #[case::negative_offset("2024-01-15T07:00:45-05:30", "2024-01-15T12:30:45Z")]
    #[case::spaced_offset("2024-01-15 17:30:45 +05:30", "2024-01-15T12:00:45Z")]
    #[case::named("2024-01-15 12:30:45 America/New_York", "2024-01-15T17:30:45Z")]
    #[case::lowercase_t("2024-01-15t12:30:45", "2024-01-15T12:30:45Z")]
    #[case::compact_clock("2024-01-15 123045", "2024-01-15T12:30:45Z")]
    #[case::date_only("2024-01-15", "2024-01-15T00:00:00Z")]
    #[case::excess_fraction("2024-01-15 12:30:45.123456789123", "2024-01-15T12:30:45.123456Z")]
    fn parses_compatible_partition_timestamps(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(
            TimestampTimezone::default().parse_timestamp(raw),
            Some(expected_timestamp_micros(expected))
        );
    }

    #[rstest]
    #[case::conflicting("2024-01-15 12:30:45+02:00 America/New_York")]
    #[case::unknown_embedded("2024-01-15 12:30:45 Foo/Bar")]
    #[case::multibyte_prefix("日時 America/New_York")]
    #[case::extra_offset_component("2024-01-15T12:30:45+05:30:15:00")]
    #[case::bad_compact_offset("2024-01-15T12:30:45+053")]
    #[case::ampm("2024-01-15 01:30:45 PM")]
    #[case::day_first("15/01/2024 12:30:45")]
    #[case::time_only("12:30:45")]
    fn rejects_unsupported_partition_timestamps(#[case] raw: &str) {
        assert_eq!(TimestampTimezone::default().parse_timestamp(raw), None);
    }

    #[rstest]
    #[case::compact_date(PrimitiveType::Date, "20240115", Scalar::Date(19_737))]
    #[case::extended_date(PrimitiveType::Date, "+2739877-01-03", Scalar::Date(1_000_000_000))]
    #[case::timestamp_date(PrimitiveType::Date, "2024-01-15T23:30:00-02:00", Scalar::Date(19_738))]
    #[case::ntz_t_separator(
        PrimitiveType::TimestampNtz,
        "2024-01-15T12:30:45",
        Scalar::TimestampNtz(1_705_321_845_000_000)
    )]
    #[case::ntz_explicit_offset(
        PrimitiveType::TimestampNtz,
        "2024-01-15T12:30:45+02:00",
        Scalar::TimestampNtz(1_705_314_645_000_000)
    )]
    fn preserves_compatible_date_and_timestamp_ntz_partition_values(
        #[case] primitive: PrimitiveType,
        #[case] raw: &str,
        #[case] expected: Scalar,
    ) {
        let reader_timezone = TimestampTimezone::parse("America/Los_Angeles").unwrap();
        assert_eq!(
            parse_partition_scalar(&primitive, raw, reader_timezone).unwrap(),
            Some(expected)
        );
    }
}
