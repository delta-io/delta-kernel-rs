//! Small time helpers used inside the default engine.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel::{DeltaResult, Error};

/// Returns the current time as a Duration since Unix epoch.
pub(crate) fn current_time_duration() -> DeltaResult<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("System time before Unix epoch: {e}")))
}

/// Returns the current time in milliseconds since Unix epoch.
pub(crate) fn current_time_ms() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    i64::try_from(duration.as_millis())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))
}
