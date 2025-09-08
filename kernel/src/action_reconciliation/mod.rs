use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

/// The default retention period for deleted files in seconds.
/// This is set to 7 days, which is the default in delta-spark.
pub(crate) const DEFAULT_RETENTION_SECS: u64 =
    7 * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE;

/// Provides common functionality for calculating file retention timestamps
/// and transaction expiration timestamps.
pub(crate) trait RetentionCalculator {
    /// Get the table properties for accessing retention durations
    fn table_properties(&self) -> &TableProperties;

    /// Determines the minimum timestamp before which deleted files
    /// are eligible for permanent removal during VACUUM operations. It is used
    /// during checkpointing to decide whether to include `remove` actions.
    ///
    /// If a deleted file's timestamp is older than this threshold (based on the
    /// table's `deleted_file_retention_duration`), the corresponding `remove` action
    /// is included in the checkpoint, allowing VACUUM operations to later identify
    /// and clean up those files.
    ///
    /// # Returns:
    /// The cutoff timestamp in milliseconds since epoch, matching the remove action's
    /// `deletion_timestamp` field format for comparison.
    ///
    /// # Note: The default retention period is 7 days, matching delta-spark's behavior.
    fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
        let retention_duration = self.table_properties().deleted_file_retention_duration;

        deleted_file_retention_timestamp_with_time(
            retention_duration,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::generic(format!("Failed to calculate system time: {e}")))?,
        )
    }

    /// Calculate the transaction expiration timestamp
    ///
    /// Calculates the timestamp threshold for transaction expiration based on
    /// the table's `set_transaction_retention_duration` property. Transactions that expired
    /// before this timestamp can be cleaned up.
    ///
    /// # Returns
    /// The timestamp in milliseconds since epoch before which transactions are considered expired,
    /// or `None` if transaction retention is not configured.
    ///
    /// # Errors
    /// Returns an error if the current system time cannot be obtained or if the retention
    /// duration exceeds the maximum representable value for i64.
    fn get_transaction_expiration_timestamp(&self) -> DeltaResult<Option<i64>> {
        calculate_transaction_expiration_timestamp(self.table_properties())
    }
}

/// Calculates the timestamp threshold for deleted file retention based on the provided duration.
/// This is factored out to allow testing with an injectable time and duration parameter.
///
/// # Parameters
/// - `retention_duration`: The duration to retain deleted files. The table property
///   `deleted_file_retention_duration` is passed here. If `None`, defaults to 7 days.
/// - `now_duration`: The current time as a [`Duration`]. This allows for testing with
///   a specific time instead of using `SystemTime::now()`.
///
/// # Returns: The timestamp in milliseconds since epoch
pub(crate) fn deleted_file_retention_timestamp_with_time(
    retention_duration: Option<Duration>,
    now_duration: Duration,
) -> DeltaResult<i64> {
    // Use provided retention duration or default (7 days)
    let retention_duration =
        retention_duration.unwrap_or_else(|| Duration::from_secs(DEFAULT_RETENTION_SECS));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms = i64::try_from(now_duration.as_millis())
        .map_err(|_| Error::checkpoint_write("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms = i64::try_from(retention_duration.as_millis())
        .map_err(|_| Error::checkpoint_write("Retention duration exceeds i64 millisecond range"))?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
}

/// Calculates the transaction expiration timestamp based on table properties.
/// Returns None if set_transaction_retention_duration is not set.
pub(crate) fn calculate_transaction_expiration_timestamp(
    table_properties: &TableProperties,
) -> DeltaResult<Option<i64>> {
    table_properties
        .set_transaction_retention_duration
        .map(|duration| -> DeltaResult<i64> {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::generic(format!("Failed to get current time: {e}")))?;

            let now_ms = i64::try_from(now.as_millis())
                .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))?;

            let expiration_ms = i64::try_from(duration.as_millis())
                .map_err(|_| Error::generic("Retention duration exceeds i64 millisecond range"))?;

            Ok(now_ms - expiration_ms)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_deleted_file_retention_timestamp_with_time() -> DeltaResult<()> {
        // Test with default retention (7 days)
        let reference_time = Duration::from_secs(1_000_000_000); // Some reference time
        let result = deleted_file_retention_timestamp_with_time(None, reference_time)?;
        let expected = 1_000_000_000_000 - (7 * 24 * 60 * 60 * 1000); // 7 days in milliseconds
        assert_eq!(result, expected);

        // Test with custom retention (1 day)
        let retention = Duration::from_secs(24 * 60 * 60); // 1 day
        let result = deleted_file_retention_timestamp_with_time(Some(retention), reference_time)?;
        let expected = 1_000_000_000_000 - (24 * 60 * 60 * 1000); // 1 day in milliseconds
        assert_eq!(result, expected);

        // Test with zero retention
        let retention = Duration::from_secs(0);
        let result = deleted_file_retention_timestamp_with_time(Some(retention), reference_time)?;
        let expected = 1_000_000_000_000; // Same as reference time
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_deleted_file_retention_timestamp_edge_cases() {
        // Test with very large retention duration
        let reference_time = Duration::from_secs(1_000_000_000);
        let large_retention = Duration::from_secs(u64::MAX);
        let result =
            deleted_file_retention_timestamp_with_time(Some(large_retention), reference_time);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Retention duration exceeds i64 millisecond range"));
    }
}
