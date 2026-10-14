//! Validated reader timezones for partition values.

use std::str::FromStr;

use chrono::FixedOffset;
use chrono_tz::Tz;

use crate::{KernelError, KernelResult};

/// A validated timezone used to interpret an offset-less `TIMESTAMP` value.
///
/// `chrono_tz::Tz` represents named zones but not fixed offsets, so the enum carries both forms.
// Without Arrow, scan builders only validate the timezone configuration.
#[cfg_attr(not(feature = "arrow-expression"), allow(dead_code))]
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
    type Err = KernelError;

    fn from_str(value: &str) -> KernelResult<Self> {
        let timezone = if value.starts_with(['+', '-']) {
            parse_normalized_fixed_offset(value).map(Self::Fixed)
        } else {
            value.parse::<Tz>().ok().map(Self::Named)
        };
        timezone.ok_or_else(|| KernelError::generic(format!("Invalid timestamp timezone: {value}")))
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
    (tens.is_ascii_digit() && ones.is_ascii_digit())
        .then(|| i32::from(*tens - b'0') * 10 + i32::from(*ones - b'0'))
}
