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
pub use file_stats_state::{
    DomainMetadataState, FileStatsState, FileStatsValidity, SetTransactionState,
};
pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use serde::{Deserialize, Serialize};

use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};

/// In-memory CRC state for a snapshot. Always present on Snapshot.
///
/// Serde goes through `CrcRaw` as an intermediate representation via `#[serde(from, into)]`.
/// `CrcRaw` maps 1:1 to the on-disk `.crc` JSON format (flat `tableSizeBytes`, `numFiles`, etc.).
/// This struct uses typed state enums (`FileStatsState`, `DomainMetadataState`,
/// `SetTransactionState`) that make it impossible to misuse file stats values.
///
/// This struct and its fields are marked `pub`, but the `crc` module is only re-exported as `pub`
/// when the `test-utils` feature is enabled (otherwise `pub(crate)`). See `kernel/src/lib.rs`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(from = "CrcRaw", into = "CrcRaw")]
pub struct Crc {
    /// The table Protocol at this version.
    pub protocol: Protocol,
    /// The table Metadata at this version.
    pub metadata: Metadata,
    /// File-level statistics with typed validity. Encodes both values and their trustworthiness.
    pub file_stats: FileStatsState,
    /// Domain metadata completeness state.
    pub domain_metadata: DomainMetadataState,
    /// Set transaction completeness state.
    pub set_transactions: SetTransactionState,
    /// In-commit timestamp, if ICT is enabled.
    pub in_commit_timestamp_opt: Option<i64>,
}

impl Default for Crc {
    /// Default CRC: Valid with zero counts, Untracked DM/txns, default Protocol and Metadata.
    fn default() -> Self {
        Crc {
            protocol: Protocol::default(),
            metadata: Metadata::default(),
            file_stats: FileStatsState::default(),
            domain_metadata: DomainMetadataState::default(),
            set_transactions: SetTransactionState::default(),
            in_commit_timestamp_opt: None,
        }
    }
}

impl Crc {
    /// Returns file-level statistics only if they are known to be valid.
    ///
    /// Returns `None` when file stats cannot be trusted -- for example, when the CRC was
    /// built from incremental replay that encountered a non-incremental operation or a
    /// missing file size.
    pub fn file_stats(&self) -> Option<FileStats> {
        self.file_stats.file_stats()
    }

    /// Returns the simplified validity classification for cost inspection by connectors.
    ///
    /// Connectors use this to decide whether to write CRC synchronously, asynchronously,
    /// or call `load_file_stats()` first.
    pub fn file_stats_validity(&self) -> FileStatsValidity {
        self.file_stats.validity()
    }

    /// Build a minimal CRC from Protocol and Metadata only. File stats are
    /// `RequiresCheckpointRead`, DM/txns are `Untracked`, histogram is `None`.
    ///
    /// Used when no CRC file is available and full log replay was not performed
    /// (e.g. the incremental snapshot update fallback path).
    pub(crate) fn minimal_from_pm(protocol: Protocol, metadata: Metadata) -> Self {
        Crc {
            protocol,
            metadata,
            file_stats: FileStatsState::RequiresCheckpointRead {
                commit_delta_files: 0,
                commit_delta_bytes: 0,
                commit_delta_histogram: None,
            },
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
// CrcRaw: serde intermediate representation
// ============================================================================

/// On-disk CRC format. Maps 1:1 to the Delta protocol spec's JSON schema.
/// Used only as a serde intermediate for [`Crc`].
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CrcRaw {
    table_size_bytes: i64,
    num_files: i64,
    num_metadata: i64,
    num_protocol: i64,
    metadata: Metadata,
    protocol: Protocol,
    in_commit_timestamp_opt: Option<i64>,
    #[serde(default)]
    set_transactions: Option<Vec<SetTransaction>>,
    #[serde(default)]
    domain_metadata: Option<Vec<DomainMetadata>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_size_histogram: Option<FileSizeHistogram>,
}

impl From<CrcRaw> for Crc {
    fn from(raw: CrcRaw) -> Self {
        let file_stats = FileStatsState::Valid {
            num_files: raw.num_files,
            table_size_bytes: raw.table_size_bytes,
            histogram: raw.file_size_histogram,
        };
        let domain_metadata = match raw.domain_metadata {
            Some(vec) => DomainMetadataState::Complete(
                vec.into_iter()
                    .map(|dm| (dm.domain().to_string(), dm))
                    .collect(),
            ),
            None => DomainMetadataState::Untracked,
        };
        let set_transactions = match raw.set_transactions {
            Some(vec) => SetTransactionState::Complete(
                vec.into_iter()
                    .map(|txn| (txn.app_id.clone(), txn))
                    .collect(),
            ),
            None => SetTransactionState::Untracked,
        };
        Crc {
            protocol: raw.protocol,
            metadata: raw.metadata,
            file_stats,
            domain_metadata,
            set_transactions,
            in_commit_timestamp_opt: raw.in_commit_timestamp_opt,
        }
    }
}

impl From<Crc> for CrcRaw {
    fn from(crc: Crc) -> Self {
        let (num_files, table_size_bytes, file_size_histogram) = match crc.file_stats {
            FileStatsState::Valid {
                num_files,
                table_size_bytes,
                histogram,
            } => (num_files, table_size_bytes, histogram),
            // For non-Valid states, use 0/0/None. The writer already guards against writing
            // non-Valid CRCs, so this path is only hit for in-memory serialization.
            _ => (0, 0, None),
        };
        let domain_metadata = match crc.domain_metadata {
            DomainMetadataState::Complete(map) => Some(map.into_values().collect()),
            _ => None,
        };
        let set_transactions = match crc.set_transactions {
            SetTransactionState::Complete(map) => Some(map.into_values().collect()),
            _ => None,
        };
        CrcRaw {
            table_size_bytes,
            num_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata: crc.metadata,
            protocol: crc.protocol,
            in_commit_timestamp_opt: crc.in_commit_timestamp_opt,
            set_transactions,
            domain_metadata,
            file_size_histogram,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Crc;
    use super::{DomainMetadataState, SetTransactionState};
    use crate::actions::{DomainMetadata, SetTransaction};
    use crate::crc::FileStatsState;

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
            file_stats: FileStatsState::Valid {
                num_files: 10,
                table_size_bytes: 1024 * 1024,
                histogram: None,
            },
            set_transactions: SetTransactionState::Complete(txns),
            domain_metadata: DomainMetadataState::Complete(domains),
            ..Default::default()
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&crc).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        // Verify scalar fields survive the round-trip via file_stats()
        let stats = deserialized.file_stats().unwrap();
        assert_eq!(stats.table_size_bytes, 1024 * 1024);
        assert_eq!(stats.num_files, 10);

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
