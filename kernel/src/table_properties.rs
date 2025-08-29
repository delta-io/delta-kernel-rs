//! Delta Table properties. Note this module implements per-table configuration which governs how
//! table-level capabilities/properties are configured (turned on/off etc.). This is orthogonal to
//! protocol-level 'table features' which enable or disable reader/writer features (which then
//! usually must be enabled/configured by table properties).
//!
//! For example (from delta's protocol.md): A feature being supported does not imply that it is
//! active. For example, a table may have the `appendOnly` feature listed in writerFeatures, but it
//! does not have a table property delta.appendOnly that is set to `true`. In such a case the table
//! is not append-only, and writers are allowed to change, remove, and rearrange data. However,
//! writers must know that the table property delta.appendOnly should be checked before writing the
//! table.

use std::collections::HashMap;
use std::num::NonZero;
use std::time::Duration;

use crate::expressions::ColumnName;
use crate::table_features::ColumnMappingMode;
use crate::{Error, Version};

use strum::EnumString;

mod deserialize;
pub use deserialize::ParseIntervalError;

/// Delta table properties. These are parsed from the 'configuration' map in the most recent
/// 'Metadata' action of a table.
///
/// Reference: <https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala>
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct TableProperties {
    /// true for this Delta table to be append-only. If append-only, existing records cannot be
    /// deleted, and existing values cannot be updated. See [append-only tables] in the protocol.
    ///
    /// [append-only tables]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
    pub append_only: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    pub auto_compact: Option<bool>,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table
    /// during writes.
    pub optimize_write: Option<bool>,

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    pub checkpoint_interval: Option<NonZero<u64>>,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    pub checkpoint_write_stats_as_json: Option<bool>,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    pub checkpoint_write_stats_as_struct: Option<bool>,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    pub column_mapping_mode: Option<ColumnMappingMode>,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    pub data_skipping_num_indexed_cols: Option<DataSkippingNumIndexedCols>,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// `delta.dataSkippingNumIndexedCols`.
    pub data_skipping_stats_columns: Option<Vec<ColumnName>>,

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition
    /// overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for
    ///   longer than this value. Otherwise, the query may not be able to restart, as it must still
    ///   read old files.
    pub deleted_file_retention_duration: Option<Duration>,

    /// true to enable change data feed.
    pub enable_change_data_feed: Option<bool>,

    /// true to enable deletion vectors and predictive I/O for updates.
    pub enable_deletion_vectors: Option<bool>,

    /// The degree to which a transaction must be isolated from modifications made by concurrent
    /// transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    pub isolation_level: Option<IsolationLevel>,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log
    /// size increases.
    pub log_retention_duration: Option<Duration>,

    /// Whether to clean up expired checkpoints/commits in the delta log.
    pub enable_expired_log_cleanup: Option<bool>,

    /// true for Delta to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this may improve Amazon S3 performance when Delta Lake needs to send very high
    /// volumes of Amazon S3 calls to better partition across S3 servers.
    pub randomize_file_prefixes: Option<bool>,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta
    /// generates for random prefixes.
    pub random_prefix_length: Option<NonZero<u64>>,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for
    /// example, SetTransactions). When a new snapshot sees a transaction identifier older than or
    /// equal to the duration specified by this property, the snapshot considers it expired and
    /// ignores it. The SetTransaction identifier is used when making the writes idempotent.
    pub set_transaction_retention_duration: Option<Duration>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    pub target_file_size: Option<NonZero<u64>>,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600
    /// (bytes) or 100mb.
    pub tune_file_sizes_for_rewrites: Option<bool>,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    pub checkpoint_policy: Option<CheckpointPolicy>,

    /// Whether to enable row tracking for the table.
    ///
    /// When row tracking is enabled, all rows are guaranteed to have a row ID and commit version.
    pub enable_row_tracking: Option<bool>,

    /// Whether to explicitly suspend generating row tracking metadata during writes even if
    /// row tracking is supported.
    pub row_tracking_suspended: Option<bool>,

    /// The name of the internal column that contains the materialized row ID.
    pub materialized_row_id_column_name: Option<String>,

    /// The name of the internal column that contains the materialized row commit version.
    pub materialized_row_commit_version_column_name: Option<String>,

    /// Whether to enable [In-Commit Timestamps]. The in-commit timestamps writer feature strongly
    /// associates a monotonically increasing timestamp with each commit by storing it in the
    /// commit's metadata.
    ///
    /// [In-Commit Timestamps]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps
    pub enable_in_commit_timestamps: Option<bool>,

    /// The version of the table at which in-commit timestamps were enabled.
    pub in_commit_timestamp_enablement_version: Option<Version>,

    /// The timestamp of the table at which in-commit timestamps were enabled. This must be the same
    /// as the inCommitTimestamp of the commit when this feature was enabled.
    pub in_commit_timestamp_enablement_timestamp: Option<i64>,

    /// any unrecognized properties are passed through and ignored by the parser
    pub unknown_properties: HashMap<String, String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataSkippingNumIndexedCols {
    AllColumns,
    NumColumns(u64),
}

impl TryFrom<&str> for DataSkippingNumIndexedCols {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let num: i64 = value.parse().map_err(|_| {
            Error::generic("couldn't parse DataSkippingNumIndexedCols to an integer")
        })?;
        match num {
            -1 => Ok(DataSkippingNumIndexedCols::AllColumns),
            x => Ok(DataSkippingNumIndexedCols::NumColumns(
                x.try_into().map_err(|_| {
                    Error::generic("couldn't parse DataSkippingNumIndexedCols to positive integer")
                })?,
            )),
        }
    }
}

/// The isolation level applied during transaction
#[derive(Debug, EnumString, Default, Copy, Clone, PartialEq, Eq)]
#[strum(serialize_all = "camelCase")]
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    #[default]
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    SnapshotIsolation,
}

/// The checkpoint policy applied when writing checkpoints
#[derive(Debug, EnumString, Default, Clone, PartialEq, Eq)]
#[strum(serialize_all = "camelCase")]
pub enum CheckpointPolicy {
    /// classic Delta Lake checkpoints
    #[default]
    Classic,
    /// v2 checkpoints
    V2,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::column_name;
    use std::collections::HashMap;

    #[test]
    fn known_key_unknown_val() {
        let properties = HashMap::from([("delta.appendOnly".to_string(), "wack".to_string())]);
        let table_properties = TableProperties::from(properties.iter());
        let unknown_properties =
            HashMap::from([("delta.appendOnly".to_string(), "wack".to_string())]);
        let expected = TableProperties {
            unknown_properties,
            ..Default::default()
        };
        assert_eq!(table_properties, expected);
    }

    #[test]
    fn allow_unknown_keys() {
        let properties = [("unknown_properties".to_string(), "two words".to_string())];
        let actual = TableProperties::from(properties.clone().into_iter());
        let expected = TableProperties {
            unknown_properties: HashMap::from(properties),
            ..Default::default()
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_empty_table_properties() {
        let map: HashMap<String, String> = HashMap::new();
        let actual = TableProperties::from(map.iter());
        let default_table_properties = TableProperties::default();
        assert_eq!(actual, default_table_properties);
    }

    #[test]
    fn test_parse_table_properties() {
        let properties = [
            ("delta.appendOnly", "true"),
            ("delta.autoOptimize.optimizeWrite", "true"),
            ("delta.autoOptimize.autoCompact", "true"),
            ("delta.checkpointInterval", "101"),
            ("delta.checkpoint.writeStatsAsJson", "true"),
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.columnMapping.mode", "id"),
            ("delta.dataSkippingNumIndexedCols", "-1"),
            ("delta.dataSkippingStatsColumns", "col1,col2"),
            ("delta.deletedFileRetentionDuration", "interval 1 second"),
            ("delta.enableChangeDataFeed", "true"),
            ("delta.enableDeletionVectors", "true"),
            ("delta.isolationLevel", "snapshotIsolation"),
            ("delta.logRetentionDuration", "interval 2 seconds"),
            ("delta.enableExpiredLogCleanup", "true"),
            ("delta.randomizeFilePrefixes", "true"),
            ("delta.randomPrefixLength", "1001"),
            (
                "delta.setTransactionRetentionDuration",
                "interval 60 seconds",
            ),
            ("delta.targetFileSize", "1000000000"),
            ("delta.tuneFileSizesForRewrites", "true"),
            ("delta.checkpointPolicy", "v2"),
            ("delta.enableRowTracking", "true"),
            (
                "delta.rowTracking.materializedRowIdColumnName",
                "_row-id-col-some_uuid",
            ),
            (
                "delta.rowTracking.materializedRowCommitVersionColumnName",
                "_row-commit-version-col-some_uuid",
            ),
            ("delta.rowTrackingSuspended", "false"),
            ("delta.enableInCommitTimestamps", "true"),
            ("delta.inCommitTimestampEnablementVersion", "15"),
            ("delta.inCommitTimestampEnablementTimestamp", "1612345678"),
        ];
        let actual = TableProperties::from(properties.into_iter());
        let expected = TableProperties {
            append_only: Some(true),
            optimize_write: Some(true),
            auto_compact: Some(true),
            checkpoint_interval: Some(NonZero::new(101).unwrap()),
            checkpoint_write_stats_as_json: Some(true),
            checkpoint_write_stats_as_struct: Some(true),
            column_mapping_mode: Some(ColumnMappingMode::Id),
            data_skipping_num_indexed_cols: Some(DataSkippingNumIndexedCols::AllColumns),
            data_skipping_stats_columns: Some(vec![column_name!("col1"), column_name!("col2")]),
            deleted_file_retention_duration: Some(Duration::new(1, 0)),
            enable_change_data_feed: Some(true),
            enable_deletion_vectors: Some(true),
            isolation_level: Some(IsolationLevel::SnapshotIsolation),
            log_retention_duration: Some(Duration::new(2, 0)),
            enable_expired_log_cleanup: Some(true),
            randomize_file_prefixes: Some(true),
            random_prefix_length: Some(NonZero::new(1001).unwrap()),
            set_transaction_retention_duration: Some(Duration::new(60, 0)),
            target_file_size: Some(NonZero::new(1_000_000_000).unwrap()),
            tune_file_sizes_for_rewrites: Some(true),
            checkpoint_policy: Some(CheckpointPolicy::V2),
            enable_row_tracking: Some(true),
            materialized_row_id_column_name: Some("_row-id-col-some_uuid".to_string()),
            materialized_row_commit_version_column_name: Some(
                "_row-commit-version-col-some_uuid".to_string(),
            ),
            row_tracking_suspended: Some(false),
            enable_in_commit_timestamps: Some(true),
            in_commit_timestamp_enablement_version: Some(15),
            in_commit_timestamp_enablement_timestamp: Some(1_612_345_678),
            unknown_properties: HashMap::new(),
        };
        assert_eq!(actual, expected);
    }
}
