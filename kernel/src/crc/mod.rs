//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [`Crc`] holds the in-memory state using shapes that make kernel queries easy: typed
//! validity enums and `HashMap`s keyed by id, instead of the flat scalars and arrays of the
//! on-disk format. It (de)serializes to/from JSON via the private [`CrcRaw`] serde
//! intermediate, which mirrors the wire format exactly.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

// Allow unreachable_pub because this module is pub when test-utils is enabled
// but pub(crate) otherwise. The items need to be pub for integration tests.
#![allow(unreachable_pub)]

mod delta;
mod file_size_histogram;
mod file_stats;
mod lazy;
mod reader;
mod writer;

use std::collections::HashMap;

#[allow(unused)]
pub(crate) use delta::CrcDelta;
pub use file_size_histogram::FileSizeHistogram;
pub use file_stats::FileStats;
#[allow(unused)]
pub(crate) use file_stats::FileStatsDelta;
pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::Error;

/// Tracks whether file stats (`num_files`, `table_size_bytes`) are trustworthy.
///
/// Defaults to [`Valid`](Self::Valid), which is the correct state when deserializing a CRC file
/// from disk (a CRC file's stats are correct by definition).
// TODO: replace `FileStatsValidity` with a typed `FileStatsState` enum that carries the
//       data per variant so reads from a degraded state are a compile error.
#[allow(dead_code)] // Variants used in follow-up PRs (forward replay, transaction delta).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStatsValidity {
    /// File stats are known-correct absolute totals. This is the case when seeded from a CRC
    /// file (which contains `num_files` and `table_size_bytes`) or when replay starts from
    /// version zero (where the initial state is trivially zero). Safe to write to disk.
    #[default]
    Valid,
    /// File stats are relative deltas, not absolute totals. This happens when seeding from a
    /// checkpoint: we extract metadata fields but not file counts (reading all add actions from
    /// a checkpoint just for counts is too expensive). The accumulated deltas are correct, but
    /// without a baseline they cannot produce final totals. Not safe to write to disk.
    RequiresCheckpointRead,
    /// A non-incremental operation was seen: file stats cannot be determined incrementally.
    /// For example, ANALYZE STATS re-adds existing files with updated statistics but no
    /// corresponding removes, so naively counting adds would double-count.
    /// A full log replay from scratch could recover correct file stats. Not safe to write to disk.
    Indeterminate,
    /// A file action had a missing size field: correct file stats are impossible to compute.
    /// For example, the Delta protocol allows `remove.size` to be null -- when encountered,
    /// we can no longer track byte totals. Unlike [`Indeterminate`](Self::Indeterminate), no
    /// amount of replay can recover the missing data. Not safe to write to disk.
    Untrackable,
}

// ============================================================================
// Crc: in-memory representation
// ============================================================================

/// Parsed content of a CRC (version checksum) file.
///
/// A `Crc` is either (a) loaded from disk (deserialized from a `.crc` JSON file via
/// the private [`CrcRaw`] intermediate) or (b) computed in memory (built incrementally via
/// [`Crc::apply`]).
///
/// A CRC file must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema mirrored by [`CrcRaw`].
///
/// This struct and its fields are marked `pub`, but the `crc` module is only re-exported as `pub`
/// when the `test-utils` feature is enabled (otherwise `pub(crate)`). See `kernel/src/lib.rs`.
// TODO: rename `Crc` to `CrcState` to align with `FileStatsState`, `SetTransactionState`, etc.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "CrcRaw", into = "CrcRaw")]
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
    /// by definition). A CRC is only safe to write to disk when validity is `Valid`.
    pub file_stats_validity: FileStatsValidity,

    // ===== Optional fields =====
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub in_commit_timestamp_opt: Option<i64>,
    // TODO: introduce `SetTransactionState` (Complete / Partial) to disambiguate "no
    //       observations" from "fully tracked but empty".
    /// Live transaction identifier ([`SetTransaction`]) actions at this version. `None` = not
    /// tracked (field absent in CRC JSON or not computed). `Some(empty_map)` = tracked, no
    /// active set transactions. [`Crc::apply`] skips updates when `None`.
    ///
    /// Stored as a HashMap keyed by `app_id` for efficient lookup. The CRC JSON format uses
    /// a Vec, which is converted via the [`CrcRaw`] serde intermediate.
    pub set_transactions: Option<HashMap<String, SetTransaction>>,
    // TODO: introduce `DomainMetadataState` (Complete / Partial) to disambiguate "no
    //       observations" from "fully tracked but empty".
    /// Active (non-removed) [`DomainMetadata`] actions at this version. Tombstones
    /// (`removed=true`) are never stored. `None` = not tracked (field absent in CRC JSON or not
    /// computed). `Some(empty_map)` = tracked, no active domain metadata. [`Crc::apply`]
    /// skips updates when `None`.
    ///
    /// Stored as a HashMap keyed by domain name for efficient lookup. The CRC JSON format uses
    /// a Vec, which is converted via the [`CrcRaw`] serde intermediate.
    pub domain_metadata: Option<HashMap<String, DomainMetadata>>,
    /// Size distribution information of files remaining after action reconciliation.
    pub file_size_histogram: Option<FileSizeHistogram>,

    // ===== Not yet supported fields =====
    /// A unique identifier for the transaction that produced this commit.
    pub txn_id: Option<String>,
    /// All live [`Add`] file actions at this version.
    pub all_files: Option<Vec<Add>>,
    /// Number of records deleted through Deletion Vectors in this table version.
    pub num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    pub num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files.
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
}

// ============================================================================
// CrcRaw: serde intermediate
// ============================================================================

/// The on-disk JSON shape of a CRC file. Serves as the serde intermediate for [`Crc`].
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CrcRaw {
    table_size_bytes: i64,
    num_files: i64,
    num_metadata: i64,
    num_protocol: i64,
    metadata: Metadata,
    protocol: Protocol,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    in_commit_timestamp_opt: Option<i64>,
    #[serde(default)]
    set_transactions: Option<Vec<SetTransaction>>,
    #[serde(default)]
    domain_metadata: Option<Vec<DomainMetadata>>,
    /// The Delta protocol spec names this field `fileSizeHistogram`, but Delta-Spark writers
    /// historically emit it as `histogramOpt`. To remain compatible with CRC files written by
    /// those tools, deserialization accepts either name, but not both. If both are present
    /// deserialization will throw an error. Serialization always emits the spec-correct
    /// `fileSizeHistogram`. Mirrors the kernel-java fix in
    /// <https://github.com/delta-io/delta/pull/6281>.
    #[serde(
        default,
        alias = "histogramOpt",
        deserialize_with = "de_validated_file_size_histogram",
        skip_serializing_if = "Option::is_none"
    )]
    file_size_histogram: Option<FileSizeHistogram>,
}

impl TryFrom<CrcRaw> for Crc {
    type Error = Error;

    fn try_from(raw: CrcRaw) -> Result<Self, Self::Error> {
        // Per the Delta protocol spec, numMetadata and numProtocol MUST be 1 in any CRC file.
        // Reject malformed files at the deserialization boundary so callers can trust the value.
        if raw.num_metadata != 1 {
            return Err(Error::generic(format!(
                "CRC file has invalid numMetadata: expected 1, got {}",
                raw.num_metadata
            )));
        }
        if raw.num_protocol != 1 {
            return Err(Error::generic(format!(
                "CRC file has invalid numProtocol: expected 1, got {}",
                raw.num_protocol
            )));
        }
        let domain_metadata = raw.domain_metadata.map(|vec| {
            vec.into_iter()
                .map(|dm| (dm.domain().to_string(), dm))
                .collect()
        });
        let set_transactions = raw.set_transactions.map(|vec| {
            vec.into_iter()
                .map(|txn| (txn.app_id.clone(), txn))
                .collect()
        });
        Ok(Crc {
            table_size_bytes: raw.table_size_bytes,
            num_files: raw.num_files,
            num_metadata: raw.num_metadata,
            num_protocol: raw.num_protocol,
            metadata: raw.metadata,
            protocol: raw.protocol,
            // A CRC file on disk is by definition valid; we never deserialize a degraded state.
            file_stats_validity: FileStatsValidity::Valid,
            in_commit_timestamp_opt: raw.in_commit_timestamp_opt,
            set_transactions,
            domain_metadata,
            file_size_histogram: raw.file_size_histogram,
            // Not yet round-tripped through CrcRaw; see the "not yet supported" fields on Crc.
            txn_id: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        })
    }
}

impl From<Crc> for CrcRaw {
    fn from(crc: Crc) -> Self {
        CrcRaw {
            table_size_bytes: crc.table_size_bytes,
            num_files: crc.num_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata: crc.metadata,
            protocol: crc.protocol,
            in_commit_timestamp_opt: crc.in_commit_timestamp_opt,
            set_transactions: crc
                .set_transactions
                .map(|map| map.into_values().collect::<Vec<_>>()),
            domain_metadata: crc
                .domain_metadata
                .map(|map| map.into_values().collect::<Vec<_>>()),
            file_size_histogram: crc.file_size_histogram,
        }
    }
}

/// Deserializes an `Option<FileSizeHistogram>` from a CRC JSON file with validation.
///
/// After serde deserializes the raw JSON fields, this validates the histogram invariants
/// (sorted boundaries, matching array lengths, etc.) via [`FileSizeHistogram::try_new`],
/// ensuring malformed CRC files are rejected rather than causing panics later.
fn de_validated_file_size_histogram<'de, D>(
    deserializer: D,
) -> Result<Option<FileSizeHistogram>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<FileSizeHistogram> = Option::deserialize(deserializer)?;
    match opt {
        Some(hist) => FileSizeHistogram::try_new(
            hist.sorted_bin_boundaries,
            hist.file_counts,
            hist.total_bytes,
        )
        .map(Some)
        .map_err(serde::de::Error::custom),
        None => Ok(None),
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
    use crate::actions::{DomainMetadata, SetTransaction};

    /// Helper to create a minimal `Crc` with only set_transactions and domain_metadata populated.
    fn crc_with(
        txns: Option<HashMap<String, SetTransaction>>,
        domains: Option<HashMap<String, DomainMetadata>>,
    ) -> Crc {
        Crc {
            num_metadata: 1,
            num_protocol: 1,
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

    // ===== numMetadata / numProtocol rejection =====

    /// Minimal CRC JSON with a given numMetadata / numProtocol pair. Both fields are required.
    fn crc_json_with_counts(num_metadata: i64, num_protocol: i64) -> String {
        format!(
            r#"{{
                "tableSizeBytes": 0,
                "numFiles": 0,
                "numMetadata": {num_metadata},
                "numProtocol": {num_protocol},
                "metadata": {{
                    "id": "test",
                    "format": {{"provider": "parquet", "options": {{}}}},
                    "schemaString": "{{\"type\":\"struct\",\"fields\":[]}}",
                    "partitionColumns": [],
                    "configuration": {{}},
                    "createdTime": 0
                }},
                "protocol": {{"minReaderVersion": 1, "minWriterVersion": 1}}
            }}"#
        )
    }

    /// Per the Delta protocol spec, `numMetadata` MUST be 1.
    #[test]
    fn de_invalid_num_metadata_is_rejected() {
        for bad in [0i64, 2, 3, -1] {
            let json = crc_json_with_counts(bad, 1);
            let err = serde_json::from_str::<Crc>(&json).unwrap_err().to_string();
            assert!(
                err.contains("numMetadata"),
                "expected error to mention numMetadata for value {bad}, got: {err}"
            );
        }
    }

    /// Per the Delta protocol spec, `numProtocol` MUST be 1.
    #[test]
    fn de_invalid_num_protocol_is_rejected() {
        for bad in [0i64, 2, 3, -1] {
            let json = crc_json_with_counts(1, bad);
            let err = serde_json::from_str::<Crc>(&json).unwrap_err().to_string();
            assert!(
                err.contains("numProtocol"),
                "expected error to mention numProtocol for value {bad}, got: {err}"
            );
        }
    }

    // ===== File size histogram validation =====

    /// Minimal CRC JSON with a file size histogram field spliced in under the given field name
    /// (`fileSizeHistogram` per the Delta spec, or `histogramOpt` for legacy Delta-Spark
    /// compatibility).
    fn crc_json_with_histogram(field_name: &str, histogram_json: &str) -> String {
        format!(
            r#"{{
                "tableSizeBytes": 0,
                "numFiles": 0,
                "numMetadata": 1,
                "numProtocol": 1,
                "metadata": {{
                    "id": "test",
                    "format": {{"provider": "parquet", "options": {{}}}},
                    "schemaString": "{{\"type\":\"struct\",\"fields\":[]}}",
                    "partitionColumns": [],
                    "configuration": {{}},
                    "createdTime": 0
                }},
                "protocol": {{"minReaderVersion": 1, "minWriterVersion": 1}},
                "{field_name}": {histogram_json}
            }}"#
        )
    }

    use rstest::rstest;

    /// Both the Delta spec field name and the legacy Delta-Spark name must deserialize.
    #[rstest]
    #[case::spec_name("fileSizeHistogram")]
    #[case::legacy_name("histogramOpt")]
    fn de_valid_file_size_histogram_succeeds(#[case] field_name: &str) {
        let json = crc_json_with_histogram(
            field_name,
            r#"{"sortedBinBoundaries": [0, 100, 200], "fileCounts": [1, 2, 3], "totalBytes": [10, 200, 300]}"#,
        );
        let crc: Crc = serde_json::from_str(&json).unwrap();
        assert!(crc.file_size_histogram.is_some());
    }

    #[rstest]
    #[case::spec_name("fileSizeHistogram")]
    #[case::legacy_name("histogramOpt")]
    fn de_null_file_size_histogram_deserializes_to_none(#[case] field_name: &str) {
        let json = crc_json_with_histogram(field_name, "null");
        let crc: Crc = serde_json::from_str(&json).unwrap();
        assert!(crc.file_size_histogram.is_none());
    }

    /// Validation must reject malformed histograms regardless of which field name they arrived
    /// under. Cartesian product across malformed payloads x both accepted field names.
    #[rstest]
    #[case::unsorted_boundaries(
        r#"{"sortedBinBoundaries": [0, 200, 100], "fileCounts": [0, 0, 0], "totalBytes": [0, 0, 0]}"#
    )]
    #[case::nonzero_first_boundary(
        r#"{"sortedBinBoundaries": [1, 100], "fileCounts": [0, 0], "totalBytes": [0, 0]}"#
    )]
    #[case::mismatched_lengths(
        r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [0], "totalBytes": [0, 0]}"#
    )]
    #[case::single_boundary(
        r#"{"sortedBinBoundaries": [0], "fileCounts": [0], "totalBytes": [0]}"#
    )]
    fn de_malformed_file_size_histogram_returns_error(
        #[case] histogram_json: &str,
        #[values("fileSizeHistogram", "histogramOpt")] field_name: &str,
    ) {
        let json = crc_json_with_histogram(field_name, histogram_json);
        assert!(serde_json::from_str::<Crc>(&json).is_err());
    }

    /// CRC files written by kernel always use the spec-correct field name `fileSizeHistogram`,
    /// even when the input JSON used the legacy `histogramOpt` alias. This matches kernel-java
    /// (delta-io/delta#6281) and ensures kernel-written CRCs are protocol-compliant.
    #[test]
    fn ser_uses_spec_field_name_after_deserializing_legacy_alias() {
        let legacy_json = crc_json_with_histogram(
            "histogramOpt",
            r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [1, 0], "totalBytes": [50, 0]}"#,
        );
        let crc: Crc = serde_json::from_str(&legacy_json).unwrap();

        let serialized = serde_json::to_value(&crc).unwrap();
        assert!(serialized.get("fileSizeHistogram").is_some());
        assert!(serialized.get("histogramOpt").is_none());
    }

    /// A CRC that contains both `fileSizeHistogram` and `histogramOpt` is rejected with a
    /// "duplicate field" error -- serde's `#[serde(alias)]` treats both names as the same
    /// logical field and refuses to deserialize repeated sets. No real producer emits both
    /// fields today (Delta-Spark writes only `histogramOpt` or only `fileSizeHistogram`,
    /// kernel-java / kernel-rust write only `fileSizeHistogram`), so this is a defensive guard
    /// against malformed CRCs. The error fires regardless of the data carried under each name.
    /// The cases below exercise both matching and mismatched payloads, in both JSON orderings.
    #[rstest]
    #[case::matching_payloads(
        r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [1, 0], "totalBytes": [50, 0]}"#,
        r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [1, 0], "totalBytes": [50, 0]}"#
    )]
    #[case::mismatched_payloads(
        r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [1, 0], "totalBytes": [50, 0]}"#,
        r#"{"sortedBinBoundaries": [0, 200], "fileCounts": [9, 9], "totalBytes": [99, 99]}"#
    )]
    fn de_both_field_names_present_returns_duplicate_field_error(
        #[case] histogram_opt_payload: &str,
        #[case] file_size_histogram_payload: &str,
        #[values(true, false)] spec_listed_last: bool,
    ) {
        let (first_name, first_payload, second_name, second_payload) = if spec_listed_last {
            (
                "histogramOpt",
                histogram_opt_payload,
                "fileSizeHistogram",
                file_size_histogram_payload,
            )
        } else {
            (
                "fileSizeHistogram",
                file_size_histogram_payload,
                "histogramOpt",
                histogram_opt_payload,
            )
        };
        let json = format!(
            r#"{{
                "tableSizeBytes": 0,
                "numFiles": 0,
                "numMetadata": 1,
                "numProtocol": 1,
                "metadata": {{
                    "id": "test",
                    "format": {{"provider": "parquet", "options": {{}}}},
                    "schemaString": "{{\"type\":\"struct\",\"fields\":[]}}",
                    "partitionColumns": [],
                    "configuration": {{}},
                    "createdTime": 0
                }},
                "protocol": {{"minReaderVersion": 1, "minWriterVersion": 1}},
                "{first_name}": {first_payload},
                "{second_name}": {second_payload}
            }}"#
        );
        let err = serde_json::from_str::<Crc>(&json).unwrap_err();
        assert!(
            err.to_string().contains("duplicate field"),
            "expected duplicate-field error, got: {err}"
        );
    }
}
