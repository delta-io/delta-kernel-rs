//! Reader-timezone handling for offset-less timestamp strings.

use chrono::{
    DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeDelta, TimeZone,
};
use chrono_tz::Tz;

use crate::expressions::Scalar;
use crate::schema::{DataType, PrimitiveType};
use crate::{DeltaResult, Error};

/// A validated IANA timezone or fixed UTC offset used to interpret local timestamps.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimestampTimezone {
    Iana(Tz),
    Fixed(FixedOffset),
}

/// Parses a raw partition value using map-to-struct's empty-string and timestamp semantics.
pub(crate) fn parse_partition_scalar(
    primitive: &PrimitiveType,
    raw: &str,
    timestamp_timezone: TimestampTimezone,
) -> DeltaResult<Option<Scalar>> {
    if raw.is_empty() {
        return Ok(primitive.empty_string_partition_cast());
    }
    if primitive == &PrimitiveType::Timestamp {
        return timestamp_timezone
            .parse_timestamp(raw)
            .map(|timestamp| Some(Scalar::Timestamp(timestamp)))
            .ok_or_else(|| {
                Error::ParseError(raw.to_string(), DataType::Primitive(primitive.clone()))
            });
    }
    let scalar = primitive.parse_scalar(raw)?;
    Ok((!matches!(scalar, Scalar::Null(_))).then_some(scalar))
}

impl Default for TimestampTimezone {
    fn default() -> Self {
        Self::Iana(chrono_tz::UTC)
    }
}

impl TimestampTimezone {
    /// Parses an IANA timezone name or a fixed offset.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is neither a known IANA timezone nor a supported fixed
    /// offset.
    pub(crate) fn parse(value: &str) -> DeltaResult<Self> {
        if value.starts_with('+') || value.starts_with('-') {
            return parse_fixed_offset(value)
                .map(Self::Fixed)
                .ok_or_else(|| Error::generic(format!("Invalid timestamp timezone: {value}")));
        }
        value
            .parse::<Tz>()
            .map(Self::Iana)
            .map_err(|_| Error::generic(format!("Invalid timestamp timezone: {value}")))
    }

    /// Parses a timestamp string, honoring an explicit offset in `raw` when present.
    ///
    /// Returns `None` when `raw` is not a valid timestamp or cannot be represented.
    pub(crate) fn parse_timestamp(self, raw: &str) -> Option<i64> {
        if let Ok(timestamp) = DateTime::parse_from_str(raw, "%+") {
            return Some(timestamp.timestamp_micros());
        }
        let local_datetime = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
            .or_else(|_| {
                NaiveDate::parse_from_str(raw, "%Y-%m-%d").map(|date| date.and_time(NaiveTime::MIN))
            })
            .ok()?;
        match self {
            Self::Iana(timezone) => resolve_local_timestamp(local_datetime, timezone),
            Self::Fixed(timezone) => resolve_local_timestamp(local_datetime, timezone),
        }
    }
}

fn parse_fixed_offset(value: &str) -> Option<FixedOffset> {
    let (sign, value) = match value.as_bytes().first()? {
        b'+' => (1, &value[1..]),
        b'-' => (-1, &value[1..]),
        _ => return None,
    };
    let mut parts = value.split(':');
    let parse_component = |part: &str| {
        (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| part.parse::<i32>().ok())
            .flatten()
    };
    let hours = parse_component(parts.next()?)?;
    let minutes = parts.next().map_or(Some(0), parse_component)?;
    let seconds = parts.next().map_or(Some(0), parse_component)?;
    if parts.next().is_some()
        || value.is_empty()
        || hours > 18
        || minutes > 59
        || seconds > 59
        || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60 + seconds))
}

fn resolve_local_timestamp<T: TimeZone + Copy>(
    local_datetime: NaiveDateTime,
    timezone: T,
) -> Option<i64> {
    if let Some(timestamp) = timezone.from_local_datetime(&local_datetime).earliest() {
        return Some(timestamp.timestamp_micros());
    }

    // A forward transition can skip more than one hour, so walk backward until reaching the
    // pre-transition side of the gap.
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

    fn expected_timestamp_micros(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_micros()
    }

    #[rstest]
    #[case::utc("UTC", "2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
    #[case::iana_winter(
        "America/Los_Angeles",
        "2024-01-15 12:30:45.123456",
        "2024-01-15T20:30:45.123456Z"
    )]
    #[case::iana_summer(
        "America/Los_Angeles",
        "2024-06-15 08:00:00.500500",
        "2024-06-15T15:00:00.500500Z"
    )]
    #[case::fixed_offset(
        "+12:45:30",
        "2024-01-15 12:30:45.123456",
        "2024-01-14T23:45:15.123456Z"
    )]
    #[case::explicit_offset_wins(
        "America/Los_Angeles",
        "2024-01-15 12:30:45+02:00",
        "2024-01-15T10:30:45Z"
    )]
    #[case::iso_t_separator("UTC", "2024-01-15T12:30:45", "2024-01-15T12:30:45Z")]
    #[case::date_only("UTC", "2024-01-15", "2024-01-15T00:00:00Z")]
    #[case::dst_overlap("America/Los_Angeles", "2024-11-03 01:30:00", "2024-11-03T08:30:00Z")]
    #[case::dst_gap("America/Los_Angeles", "2024-03-10 02:30:00", "2024-03-10T10:30:00Z")]
    #[case::skipped_day("Pacific/Apia", "2011-12-30 12:00:00", "2011-12-30T22:00:00Z")]
    fn parses_timestamp_in_reader_timezone(
        #[case] timezone: &str,
        #[case] raw: &str,
        #[case] expected: &str,
    ) {
        assert_eq!(
            TimestampTimezone::parse(timezone)
                .unwrap()
                .parse_timestamp(raw),
            Some(expected_timestamp_micros(expected))
        );
    }

    #[rstest]
    #[case::zero("+0")]
    #[case::hours("+5")]
    #[case::hours_minutes("-05:30")]
    #[case::hours_minutes_seconds("+12:45:30")]
    #[case::positive_limit("+18:00:00")]
    #[case::negative_limit("-18")]
    fn accepts_supported_fixed_offsets(#[case] timezone: &str) {
        assert!(TimestampTimezone::parse(timezone).is_ok());
    }

    #[rstest]
    #[case::empty("")]
    #[case::bare_positive_sign("+")]
    #[case::over_max_second("+18:00:01")]
    #[case::over_max_hour("+19")]
    #[case::minutes_out_of_range("+00:60")]
    #[case::seconds_out_of_range("+00:00:60")]
    #[case::extra_component("+05:30:00:00")]
    #[case::unknown_name("Not/AZone")]
    fn rejects_invalid_timezones(#[case] timezone: &str) {
        assert!(TimestampTimezone::parse(timezone).is_err());
    }

    #[test]
    fn rejects_invalid_timestamp() {
        assert_eq!(
            TimestampTimezone::default().parse_timestamp("not a timestamp"),
            None
        );
    }
}
