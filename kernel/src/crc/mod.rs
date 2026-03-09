//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

// Allow unreachable_pub because this module is pub when test-utils is enabled
// but pub(crate) otherwise. The items need to be pub for integration tests.
#![allow(unreachable_pub)]

mod delta;
mod file_stats;
mod lazy;
mod reader;
mod writer;

#[allow(unused)]
pub(crate) use delta::CrcDelta;
pub(crate) use file_stats::FileStats;
pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use std::collections::HashMap;

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};

/// Tracks whether file stats (`num_files`, `table_size_bytes`) are trustworthy.
///
/// Defaults to [`Valid`](Self::Valid), which is the correct state when deserializing a CRC file
/// from disk (a CRC file's stats are correct by definition).
#[allow(dead_code)] // Variants used in follow-up PRs (forward replay, transaction delta).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStatsValidity {
    /// File stats are known-correct absolute totals. This is the case when seeded from a CRC
    /// file (which contains `num_files` and `table_size_bytes`) or when replay starts from
    /// version zero (where the initial state is trivially zero).
    #[default]
    Valid,
    /// File stats are relative deltas, not absolute totals. This happens when seeding from a
    /// checkpoint: we extract metadata fields but not file counts (reading all add actions from
    /// a checkpoint just for counts is too expensive). The accumulated deltas are correct, but
    /// without a baseline they cannot produce final totals.
    RequiresCheckpointRead,
    /// A non-incremental operation was seen: file stats cannot be determined incrementally.
    /// For example, ANALYZE STATS re-adds existing files with updated statistics but no
    /// corresponding removes, so naively counting adds would double-count.
    /// A full log replay from scratch could recover correct file stats.
    Indeterminate,
    /// A file action had a missing size field: correct file stats are impossible to compute.
    /// For example, the Delta protocol allows `remove.size` to be null -- when encountered,
    /// we can no longer track byte totals. Unlike [`Indeterminate`](Self::Indeterminate), no
    /// amount of replay can recover the missing data.
    Untrackable,
}

/// Parsed content of a CRC (version checksum) file.
///
/// A CRC file must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema of this struct.
///
/// This struct and its fields are marked `pub`, but the `crc` module is only re-exported as `pub`
/// when the `test-utils` feature is enabled (otherwise `pub(crate)`). See `kernel/src/lib.rs`.
// Deserialized directly from JSON via serde. See `reader::try_read_crc_file`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Crc {
    // ===== Required fields =====
    /// Total size of the table in bytes, calculated as the sum of the `size` field of all live
    /// [`Add`] actions. Private -- use [`Crc::file_stats()`] to access safely.
    table_size_bytes: i64,
    /// Number of live [`Add`] actions in this table version after action reconciliation.
    /// Private -- use [`Crc::file_stats()`] to access safely.
    num_files: i64,
    /// Number of [`Metadata`] actions. Must be 1.
    pub num_metadata: i64,
    /// Number of [`Protocol`] actions. Must be 1.
    pub num_protocol: i64,
    /// The table [`Metadata`] at this version.
    pub metadata: Metadata,
    /// The table [`Protocol`] at this version.
    pub protocol: Protocol,
    /// Whether the file stats (`num_files`, `table_size_bytes`) in this CRC are trustworthy.
    /// Not serialized -- this is an in-memory replay concern only. When deserialized from a CRC
    /// file on disk, defaults to [`FileStatsValidity::Valid`] (a CRC file's stats are correct
    /// by definition).
    #[serde(skip)]
    pub validity: FileStatsValidity,

    // ===== Optional fields =====
    /// A unique identifier for the transaction that produced this commit.
    #[serde(skip)]
    pub txn_id: Option<String>,
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub in_commit_timestamp_opt: Option<i64>,
    /// Live transaction identifier ([`SetTransaction`]) actions at this version.
    ///
    /// Stored as a HashMap keyed by `app_id` for efficient lookup. The CRC JSON format uses
    /// a Vec, which is converted via custom serde deserialization.
    #[serde(
        default,
        deserialize_with = "de_opt_vec_to_opt_map",
        serialize_with = "ser_opt_map_to_opt_vec"
    )]
    pub(crate) set_transactions: Option<HashMap<String, SetTransaction>>,
    /// Active (non-removed) [`DomainMetadata`] actions at this version. Tombstones
    /// (`removed=true`) are never stored.
    ///
    /// Stored as a HashMap keyed by domain name for efficient lookup. The CRC JSON format uses
    /// a Vec, which is converted via custom serde deserialization.
    #[serde(
        default,
        deserialize_with = "de_opt_vec_to_opt_map",
        serialize_with = "ser_opt_map_to_opt_vec"
    )]
    pub domain_metadata: Option<HashMap<String, DomainMetadata>>,
    /// Size distribution information of files remaining after action reconciliation.
    #[serde(skip)]
    pub file_size_histogram: Option<FileSizeHistogram>,
    /// All live [`Add`] file actions at this version.
    #[serde(skip)]
    pub all_files: Option<Vec<Add>>,
    /// Number of records deleted through Deletion Vectors in this table version.
    #[serde(skip)]
    pub num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    #[serde(skip)]
    pub num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files. See this section for more details.
    #[serde(skip)]
    pub deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
}

impl Crc {
    /// Returns file-level statistics only if they are known to be valid.
    ///
    /// Returns `None` when file stats cannot be trusted -- for example, when the CRC was
    /// built from incremental replay that encountered a non-incremental operation or a
    /// missing file size.
    pub fn file_stats(&self) -> Option<FileStats> {
        match self.validity {
            FileStatsValidity::Valid => Some(FileStats {
                num_files: self.num_files,
                table_size_bytes: self.table_size_bytes,
            }),
            _ => None,
        }
    }
}

/// Trait for types that can be stored in a HashMap keyed by a string identifier.
/// Used by CRC serde helpers to convert between Vec (JSON format) and HashMap (in-memory).
trait MapKey {
    fn map_key(&self) -> &str;
}

impl MapKey for DomainMetadata {
    fn map_key(&self) -> &str {
        self.domain()
    }
}

impl MapKey for SetTransaction {
    fn map_key(&self) -> &str {
        &self.app_id
    }
}

/// Deserialize an `Option<Vec<T>>` from JSON into `Option<HashMap<String, T>>`, using
/// [`MapKey::map_key`] to derive the HashMap key for each element.
fn de_opt_vec_to_opt_map<'de, D, T>(deserializer: D) -> Result<Option<HashMap<String, T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + MapKey,
{
    let opt_vec: Option<Vec<T>> = Option::deserialize(deserializer)?;
    Ok(opt_vec.map(|vec| {
        vec.into_iter()
            .map(|item| (item.map_key().to_string(), item))
            .collect()
    }))
}

/// Serialize `Option<HashMap<String, T>>` back to `Option<Vec<T>>` so the CRC JSON format
/// uses an array (matching the Delta protocol spec).
fn ser_opt_map_to_opt_vec<S, T>(
    map: &Option<HashMap<String, T>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    match map {
        None => serializer.serialize_none(),
        Some(m) => m.values().collect::<Vec<_>>().serialize(serializer),
    }
}

/// The [FileSizeHistogram] object represents a histogram tracking file counts and total bytes
/// across different size ranges.
///
/// [FileSizeHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSizeHistogram {
    /// A sorted array of bin boundaries where each element represents the start of a bin
    /// (inclusive) and the next element represents the end of the bin (exclusive). The first
    /// element must be 0.
    pub(crate) sorted_bin_boundaries: Vec<i64>,
    /// Count of files in each bin. Length must match `sorted_bin_boundaries`.
    pub(crate) file_counts: Vec<i64>,
    /// Total bytes of files in each bin. Length must match `sorted_bin_boundaries`.
    pub(crate) total_bytes: Vec<i64>,
}

/// The [DeletedRecordCountsHistogram] object represents a histogram tracking the distribution of
/// deleted record counts across files in the table. Each bin in the histogram represents a range
/// of deletion counts and stores the number of files having that many deleted records.
///
/// The histogram bins correspond to the following ranges:
/// Bin 0: [0, 0] (files with no deletions)
/// Bin 1: [1, 9] (files with 1-9 deleted records)
/// Bin 2: [10, 99] (files with 10-99 deleted records)
/// Bin 3: [100, 999] (files with 100-999 deleted records)
/// Bin 4: [1000, 9999] (files with 1,000-9,999 deleted records)
/// Bin 5: [10000, 99999] (files with 10,000-99,999 deleted records)
/// Bin 6: [100000, 999999] (files with 100,000-999,999 deleted records)
/// Bin 7: [1000000, 9999999] (files with 1,000,000-9,999,999 deleted records)
/// Bin 8: [10000000, 2147483646] (files with 10,000,000 to 2,147,483,646 deleted records)
/// Bin 9: [2147483647, inf) (files with 2,147,483,647 or more deleted records)
///
/// [DeletedRecordCountsHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deleted-record-counts-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedRecordCountsHistogram {
    /// Array of size 10 where each element represents the count of files falling into a specific
    /// deletion count range.
    pub(crate) deleted_record_counts: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Crc;
    use crate::actions::{DomainMetadata, SetTransaction};

    /// Helper to create a minimal `Crc` with only set_transactions and domain_metadata populated.
    fn crc_with(
        txns: Option<HashMap<String, SetTransaction>>,
        domains: Option<HashMap<String, DomainMetadata>>,
    ) -> Crc {
        Crc {
            set_transactions: txns,
            domain_metadata: domains,
            ..Default::default()
        }
    }

    #[test]
    fn de_vec_to_map_produces_correct_keys_and_values() {
        let json = r#"{
            "tableSizeBytes": 0,
            "numFiles": 0,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": "test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            },
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 1},
            "setTransactions": [
                {"appId": "app-1", "version": 3, "lastUpdated": 1000},
                {"appId": "app-2", "version": 7}
            ],
            "domainMetadata": [
                {"domain": "delta.rowTracking", "configuration": "{\"rowIdHighWaterMark\":1}", "removed": false},
                {"domain": "delta.clustering", "configuration": "{}", "removed": false}
            ]
        }"#;

        let crc: Crc = serde_json::from_str(json).unwrap();

        let txns = crc.set_transactions.as_ref().unwrap();
        assert_eq!(txns.len(), 2);

        let txn1 = &txns["app-1"];
        assert_eq!(txn1.app_id, "app-1");
        assert_eq!(txn1.version, 3);
        assert_eq!(txn1.last_updated, Some(1000));

        let txn2 = &txns["app-2"];
        assert_eq!(txn2.app_id, "app-2");
        assert_eq!(txn2.version, 7);
        assert_eq!(txn2.last_updated, None);

        let domains = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(domains.len(), 2);
        assert!(domains.contains_key("delta.rowTracking"));
        assert!(domains.contains_key("delta.clustering"));
    }

    #[test]
    fn de_null_deserializes_to_none() {
        let json = r#"{
            "tableSizeBytes": 0,
            "numFiles": 0,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": "test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            },
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 1},
            "setTransactions": null,
            "domainMetadata": null
        }"#;
        let crc: Crc = serde_json::from_str(json).unwrap();
        assert!(crc.set_transactions.is_none());
        assert!(crc.domain_metadata.is_none());
    }

    #[test]
    fn de_missing_field_deserializes_to_none() {
        let json = r#"{
            "tableSizeBytes": 0,
            "numFiles": 0,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": "test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            },
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 1}
        }"#;
        let crc: Crc = serde_json::from_str(json).unwrap();
        assert!(crc.set_transactions.is_none());
        assert!(crc.domain_metadata.is_none());
    }

    #[test]
    fn ser_none_serializes_to_null() {
        let crc = crc_with(None, None);
        let json = serde_json::to_value(&crc).unwrap();
        assert!(json["setTransactions"].is_null());
        assert!(json["domainMetadata"].is_null());
    }

    #[test]
    fn ser_map_round_trips_through_vec() {
        let mut txns = HashMap::new();
        txns.insert(
            "app-1".to_string(),
            SetTransaction::new("app-1".to_string(), 5, Some(2000)),
        );
        txns.insert(
            "app-2".to_string(),
            SetTransaction::new("app-2".to_string(), 10, None),
        );

        let mut domains = HashMap::new();
        domains.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new("delta.rowTracking".to_string(), "{}".to_string()),
        );

        let original = crc_with(Some(txns), Some(domains));

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn round_trip_empty_maps() {
        let original = crc_with(Some(HashMap::new()), Some(HashMap::new()));

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);

        // Verify the JSON has empty arrays (not null)
        let json_value = serde_json::to_value(&original).unwrap();
        assert_eq!(json_value["setTransactions"], serde_json::json!([]));
        assert_eq!(json_value["domainMetadata"], serde_json::json!([]));
    }

    #[test]
    fn test_crc_with_multiple_domain_metadatas_and_set_transactions() {
        let mut txns = HashMap::new();
        txns.insert(
            "streaming-app".to_string(),
            SetTransaction::new("streaming-app".to_string(), 42, Some(1700000000)),
        );
        txns.insert(
            "batch-job".to_string(),
            SetTransaction::new("batch-job".to_string(), 100, None),
        );
        txns.insert(
            "etl-pipeline".to_string(),
            SetTransaction::new("etl-pipeline".to_string(), 7, Some(1700001000)),
        );

        let mut domains = HashMap::new();
        domains.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new(
                "delta.rowTracking".to_string(),
                r#"{"rowIdHighWaterMark":500}"#.to_string(),
            ),
        );
        domains.insert(
            "delta.clustering".to_string(),
            DomainMetadata::new("delta.clustering".to_string(), "{}".to_string()),
        );
        domains.insert(
            "custom.app".to_string(),
            DomainMetadata::new("custom.app".to_string(), r#"{"version":"2.0"}"#.to_string()),
        );

        let crc = Crc {
            table_size_bytes: 1024 * 1024,
            num_files: 10,
            num_metadata: 1,
            num_protocol: 1,
            set_transactions: Some(txns),
            domain_metadata: Some(domains),
            ..Default::default()
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&crc).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        // Verify scalar fields survive the round-trip
        assert_eq!(deserialized.table_size_bytes, 1024 * 1024);
        assert_eq!(deserialized.num_files, 10);

        // Verify all set transactions
        let txns = deserialized.set_transactions.as_ref().unwrap();
        assert_eq!(txns.len(), 3);
        assert_eq!(txns["streaming-app"].version, 42);
        assert_eq!(txns["streaming-app"].last_updated, Some(1700000000));
        assert_eq!(txns["batch-job"].version, 100);
        assert_eq!(txns["batch-job"].last_updated, None);
        assert_eq!(txns["etl-pipeline"].version, 7);

        // Verify all domain metadatas
        let domains = deserialized.domain_metadata.as_ref().unwrap();
        assert_eq!(domains.len(), 3);
        assert!(domains.contains_key("delta.rowTracking"));
        assert!(domains.contains_key("delta.clustering"));
        assert!(domains.contains_key("custom.app"));
        assert_eq!(
            domains["custom.app"].configuration(),
            r#"{"version":"2.0"}"#
        );

        // Verify the original and deserialized are equal
        assert_eq!(crc, deserialized);
    }
}
