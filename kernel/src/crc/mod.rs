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
mod file_size_histogram;
mod file_stats;
mod file_stats_state;
mod lazy;
mod reader;
mod writer;

#[allow(unused)]
pub(crate) use delta::CrcUpdate;
pub(crate) use file_size_histogram::FileSizeHistogram;
pub(crate) use file_stats::FileStats;
#[allow(unused)]
pub(crate) use file_stats::FileStatsDelta;
#[allow(unused)]
pub use file_stats_state::{
    DomainMetadataState, FileStatsValidity, SetTransactionState,
};
pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};

/// Parsed content of a CRC (version checksum) file.
///
/// A `Crc` is either (a) loaded from disk (deserialized from a `.crc` JSON file) or (b) computed
/// in memory (built incrementally via `Crc::apply`).
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
    /// [`Add`] actions. Private; use [`Crc::file_stats()`] to access safely (checks validity).
    table_size_bytes: i64,
    /// Number of live [`Add`] actions in this table version after action reconciliation.
    /// Private; use [`Crc::file_stats()`] to access safely (checks validity).
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
    /// by definition). A CRC is only safe to write to disk when validity is `Valid`.
    #[serde(skip)]
    pub file_stats_validity: FileStatsValidity,

    // ===== Optional fields =====
    /// A unique identifier for the transaction that produced this commit.
    #[serde(skip)]
    pub txn_id: Option<String>,
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub in_commit_timestamp_opt: Option<i64>,
    /// Set transaction state. On disk: `"setTransactions": [...]` for Complete, `null`/absent
    /// for Untracked. Partial is in-memory only and serializes as `null`.
    #[serde(
        default,
        deserialize_with = "de_set_transaction_state",
        serialize_with = "ser_set_transaction_state"
    )]
    pub set_transactions: SetTransactionState,
    /// Domain metadata state. On disk: `"domainMetadata": [...]` for Complete, `null`/absent
    /// for Untracked. Partial is in-memory only and serializes as `null`.
    #[serde(
        default,
        deserialize_with = "de_domain_metadata_state",
        serialize_with = "ser_domain_metadata_state"
    )]
    pub domain_metadata: DomainMetadataState,
    /// Size distribution information of files remaining after action reconciliation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
        match self.file_stats_validity {
            FileStatsValidity::Valid => Some(FileStats {
                num_files: self.num_files,
                table_size_bytes: self.table_size_bytes,
                file_size_histogram: self.file_size_histogram.clone(),
            }),
            _ => None,
        }
    }

    /// Build a minimal CRC from Protocol and Metadata only. File stats are
    /// `RequiresCheckpointRead`, DM/txns are `Untracked`, histogram is `None`.
    ///
    /// Used when no CRC file is available and full log replay was not performed
    /// (e.g. the incremental snapshot update fallback path).
    pub(crate) fn minimal_from_pm(protocol: Protocol, metadata: Metadata) -> Self {
        Crc {
            metadata,
            protocol,
            num_metadata: 1,
            num_protocol: 1,
            file_stats_validity: FileStatsValidity::RequiresCheckpointRead,
            ..Default::default()
        }
    }

    /// Returns the domain metadata state for querying.
    pub fn domain_metadata_state(&self) -> &DomainMetadataState {
        &self.domain_metadata
    }

    /// Returns the set transaction state for querying.
    pub fn set_transaction_state(&self) -> &SetTransactionState {
        &self.set_transactions
    }
}

// ============================================================================
// Serde helpers for DomainMetadataState / SetTransactionState
// ============================================================================
// CRC JSON format uses arrays for DM/txns: `"domainMetadata": [...]` or `null`.
// Deserialization: `[...]` -> Complete(HashMap), `null`/absent -> Untracked.
// Serialization: Complete -> `[...]`, Partial/Untracked -> `null`.

fn de_domain_metadata_state<'de, D>(deserializer: D) -> Result<DomainMetadataState, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_vec: Option<Vec<DomainMetadata>> = Option::deserialize(deserializer)?;
    Ok(match opt_vec {
        Some(vec) => DomainMetadataState::Complete(
            vec.into_iter()
                .map(|dm| (dm.domain().to_string(), dm))
                .collect(),
        ),
        None => DomainMetadataState::Untracked,
    })
}

fn ser_domain_metadata_state<S>(
    state: &DomainMetadataState,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match state {
        DomainMetadataState::Complete(map) => {
            map.values().collect::<Vec<_>>().serialize(serializer)
        }
        _ => serializer.serialize_none(),
    }
}

fn de_set_transaction_state<'de, D>(deserializer: D) -> Result<SetTransactionState, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_vec: Option<Vec<SetTransaction>> = Option::deserialize(deserializer)?;
    Ok(match opt_vec {
        Some(vec) => SetTransactionState::Complete(
            vec.into_iter()
                .map(|txn| (txn.app_id.clone(), txn))
                .collect(),
        ),
        None => SetTransactionState::Untracked,
    })
}

fn ser_set_transaction_state<S>(
    state: &SetTransactionState,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match state {
        SetTransactionState::Complete(map) => {
            map.values().collect::<Vec<_>>().serialize(serializer)
        }
        _ => serializer.serialize_none(),
    }
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
    use super::{DomainMetadataState, SetTransactionState};
    use crate::actions::{DomainMetadata, SetTransaction};

    /// Helper to create a minimal `Crc` with only set_transactions and domain_metadata populated.
    fn crc_with(txns: SetTransactionState, domains: DomainMetadataState) -> Crc {
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

        let txns = crc.set_transactions.map().unwrap();
        assert_eq!(txns.len(), 2);

        let txn1 = &txns["app-1"];
        assert_eq!(txn1.app_id, "app-1");
        assert_eq!(txn1.version, 3);
        assert_eq!(txn1.last_updated, Some(1000));

        let txn2 = &txns["app-2"];
        assert_eq!(txn2.app_id, "app-2");
        assert_eq!(txn2.version, 7);
        assert_eq!(txn2.last_updated, None);

        let domains = crc.domain_metadata.map().unwrap();
        assert_eq!(domains.len(), 2);
        assert!(domains.contains_key("delta.rowTracking"));
        assert!(domains.contains_key("delta.clustering"));
    }

    #[test]
    fn de_null_deserializes_to_untracked() {
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
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Untracked
        ));
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
    }

    #[test]
    fn de_missing_field_deserializes_to_untracked() {
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
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Untracked
        ));
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
    }

    #[test]
    fn ser_untracked_serializes_to_null() {
        let crc = crc_with(
            SetTransactionState::Untracked,
            DomainMetadataState::Untracked,
        );
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

        let original = crc_with(
            SetTransactionState::Complete(txns),
            DomainMetadataState::Complete(domains),
        );

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn round_trip_empty_maps() {
        let original = crc_with(
            SetTransactionState::Complete(HashMap::new()),
            DomainMetadataState::Complete(HashMap::new()),
        );

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
            set_transactions: SetTransactionState::Complete(txns),
            domain_metadata: DomainMetadataState::Complete(domains),
            ..Default::default()
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&crc).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        // Verify scalar fields survive the round-trip
        assert_eq!(deserialized.table_size_bytes, 1024 * 1024);
        assert_eq!(deserialized.num_files, 10);

        // Verify all set transactions
        let txns = deserialized.set_transactions.map().unwrap();
        assert_eq!(txns.len(), 3);
        assert_eq!(txns["streaming-app"].version, 42);
        assert_eq!(txns["streaming-app"].last_updated, Some(1700000000));
        assert_eq!(txns["batch-job"].version, 100);
        assert_eq!(txns["batch-job"].last_updated, None);
        assert_eq!(txns["etl-pipeline"].version, 7);

        // Verify all domain metadatas
        let domains = deserialized.domain_metadata.map().unwrap();
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
