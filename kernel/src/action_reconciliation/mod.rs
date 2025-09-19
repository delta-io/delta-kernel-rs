//! # Action Reconciliation
//!
//! This module implements APIs related to action reconciliation.
//! Please see the [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation)
//! for more details about action reconciliation.
//!
//! ## Log Replay for Action Reconciliation
//!
//! The [`log_replay`] module provides specialized log replay functionality for action reconciliation,
//! including checkpoint creation. It processes log files in reverse chronological order and selects
//! the appropriate actions to include based on deduplication and retention rules.
//!
//! ## Retention and Cleanup
//!
//! This module provides utilities for calculating retention timestamps used during action reconciliation:
//!
//! - **Deleted File Retention**: Determines when `remove` actions can be excluded from checkpoints
//! - **Transaction Retention**: Calculates when expired app ids can be cleaned up
use std::time::Duration;

use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

pub(crate) mod log_replay;

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
    /// Note: The default retention period is 7 days, matching delta-spark's behavior.
    fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
        let retention_duration = self.table_properties().deleted_file_retention_duration;

        deleted_file_retention_timestamp_with_time(
            retention_duration,
            crate::utils::current_time_duration()?,
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
            let now_ms = crate::utils::current_time_ms()?;

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
        let reference_time = Duration::from_secs(1_000_000_000);
        let result = deleted_file_retention_timestamp_with_time(None, reference_time)?;
        let expected = 1_000_000_000_000 - (7 * 24 * 60 * 60 * 1000);
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

    #[test]
    fn test_deleted_file_retention_timestamp_with_large_now_time() {
        // Test with very large current time that would overflow i64 milliseconds
        let reference_time = Duration::from_secs(u64::MAX);
        let retention = Duration::from_secs(1);
        let result = deleted_file_retention_timestamp_with_time(Some(retention), reference_time);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Current timestamp exceeds i64 millisecond range"));
    }

    #[test]
    fn test_calculate_transaction_expiration_timestamp() -> DeltaResult<()> {
        // No set_transaction_retention_duration
        let properties = TableProperties::default();
        let result = calculate_transaction_expiration_timestamp(&properties)?;
        assert_eq!(result, None);

        // Test with set_transaction_retention_duration
        let properties = TableProperties {
            set_transaction_retention_duration: Some(Duration::from_secs(3600)), // 1 hour
            ..Default::default()
        };
        let result = calculate_transaction_expiration_timestamp(&properties)?;
        assert!(result.is_some());

        // The result should be current time minus 1 hour (approximately)
        // We can't test exact value due to timing, but we can verify it's reasonable
        let timestamp = result.unwrap();
        let now_ms = crate::utils::current_time_ms().unwrap();
        let one_hour_ms = 3600 * 1000;

        // Should be within a reasonable range (allowing for test execution time)
        assert!(timestamp < now_ms);
        assert!(timestamp > now_ms - one_hour_ms - 1000); // Allow 1 second buffer

        Ok(())
    }

    #[test]
    fn test_calculate_transaction_expiration_timestamp_edge_cases() {
        // Test with very large retention duration that would overflow
        let properties = TableProperties {
            set_transaction_retention_duration: Some(Duration::from_secs(u64::MAX)),
            ..Default::default()
        };
        let result = calculate_transaction_expiration_timestamp(&properties);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Retention duration exceeds i64 millisecond range"));
    }

    // Mock implementation of RetentionCalculator for testing trait methods
    struct MockRetentionCalculator {
        properties: TableProperties,
    }

    impl MockRetentionCalculator {
        fn new(properties: TableProperties) -> Self {
            Self { properties }
        }
    }

    impl RetentionCalculator for MockRetentionCalculator {
        fn table_properties(&self) -> &TableProperties {
            &self.properties
        }
    }

    #[test]
    fn test_retention_calculator_trait_deleted_file_retention_timestamp() -> DeltaResult<()> {
        // Test with default retention
        let properties = TableProperties::default();
        let calculator = MockRetentionCalculator::new(properties);
        let result = calculator.deleted_file_retention_timestamp()?;

        // Should be current time minus 7 days (approximately)
        let now_ms = crate::utils::current_time_ms().unwrap();
        let seven_days_ms = 7 * 24 * 60 * 60 * 1000;

        assert!(result < now_ms);
        assert!(result > now_ms - seven_days_ms - 1000); // Allow a small 1 second buffer

        // Test with custom retention
        let properties = TableProperties {
            deleted_file_retention_duration: Some(Duration::from_secs(1800)), // 30 minutes
            ..Default::default()
        };
        let calculator = MockRetentionCalculator::new(properties);
        let result = calculator.deleted_file_retention_timestamp()?;

        let thirty_minutes_ms = 30 * 60 * 1000;
        assert!(result < now_ms);
        assert!(result > now_ms - thirty_minutes_ms - 1000); // Allow 1 second buffer

        Ok(())
    }

    #[test]
    fn test_retention_calculator_trait_get_transaction_expiration_timestamp() -> DeltaResult<()> {
        // Test with no transaction retention
        let properties = TableProperties::default();
        let calculator = MockRetentionCalculator::new(properties);
        let result = calculator.get_transaction_expiration_timestamp()?;
        assert_eq!(result, None);

        // Test with transaction retention
        let properties = TableProperties {
            set_transaction_retention_duration: Some(Duration::from_secs(7200)), // 2 hours
            ..Default::default()
        };
        let calculator = MockRetentionCalculator::new(properties);
        let result = calculator.get_transaction_expiration_timestamp()?;
        assert!(result.is_some());

        let timestamp = result.unwrap();
        let now_ms = crate::utils::current_time_ms().unwrap();
        let two_hours_ms = 2 * 60 * 60 * 1000;

        assert!(timestamp < now_ms);
        assert!(timestamp > now_ms - two_hours_ms - 1000); // Allow 1 second buffer

        Ok(())
    }
}
