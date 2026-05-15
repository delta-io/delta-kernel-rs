//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [`Crc`] holds the in-memory state using shapes that make kernel queries easy: typed
//! state enums (`FileStatsState`, `DomainMetadataState`) and `HashMap`s keyed by id, instead
//! of the flat scalars and arrays of the on-disk format. It (de)serializes to/from JSON via
//! the private `CrcRaw` serde intermediate, which mirrors the wire format exactly.
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
mod state;
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
pub use state::{DomainMetadataState, FileStatsState};
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::Error;

// ============================================================================
// Crc: in-memory representation
// ============================================================================

/// Parsed content of a CRC (version checksum) file.
///
/// A `Crc` is either (a) loaded from disk (deserialized from a `.crc` JSON file via
/// the private `CrcRaw` intermediate) or (b) computed in memory (built incrementally via
/// `Crc::apply`).
///
/// A CRC file must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema mirrored by `CrcRaw`.
///
/// This struct and its fields are marked `pub`, but the `crc` module is only re-exported as `pub`
/// when the `test-utils` feature is enabled (otherwise `pub(crate)`). See `kernel/src/lib.rs`.
// TODO: rename `Crc` to `CrcState` to align with `FileStatsState`, `SetTransactionState`, etc.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(try_from = "CrcRaw")]
pub struct Crc {
    // ===== Required fields =====
    /// The table [`Metadata`] at this version.
    pub metadata: Metadata,
    /// The table [`Protocol`] at this version.
    pub protocol: Protocol,
    /// File-level statistics as a typed state. See [`FileStatsState`].
    pub(crate) file_stats_state: FileStatsState,

    // ===== Optional fields =====
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub in_commit_timestamp_opt: Option<i64>,
    // TODO: introduce `SetTransactionState` (Complete / Partial) to disambiguate "no
    //       observations" from "fully tracked but empty".
    /// Live transaction identifier ([`SetTransaction`]) actions at this version. `None` = not
    /// tracked (field absent in CRC JSON or not computed). `Some(empty_map)` = tracked, no
    /// active set transactions. `Crc::apply` skips updates when `None`.
    ///
    /// Stored as a HashMap keyed by `app_id` for efficient lookup. The CRC JSON format uses
    /// a Vec, which is converted via the `CrcRaw` serde intermediate.
    pub set_transactions: Option<HashMap<String, SetTransaction>>,
    /// Active (non-removed) [`DomainMetadata`] actions at this version, as a typed
    /// [`DomainMetadataState`]. Tombstones (`removed=true`) are never stored. `Complete(map)`
    /// is authoritative for misses; `Partial(map)` carries known-correct entries but requires
    /// log replay for misses. Only the `Complete` variant is persisted to the CRC file.
    ///
    /// TODO: when the table protocol does not enable the `domainMetadata` feature, no DM
    ///       action can exist, so `Partial(_)` is semantically equivalent to
    ///       `Complete(empty)` and both serde paths could collapse the distinction.
    pub domain_metadata_state: DomainMetadataState,

    // ===== Not yet supported fields =====
    /// A unique identifier for the transaction that produced this commit.
    pub(crate) txn_id: Option<String>,
    /// All live [`Add`] file actions at this version.
    pub(crate) all_files: Option<Vec<Add>>,
    /// Number of records deleted through Deletion Vectors in this table version.
    pub(crate) num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    pub(crate) num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files.
    pub(crate) deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
}

impl Crc {
    /// Returns absolute file-level statistics only if `file_stats_state` is `Complete`.
    ///
    /// Returns `None` when file stats cannot be trusted -- for example, when the CRC was
    /// built from incremental replay that encountered a non-incremental operation or a
    /// missing file size.
    pub fn file_stats(&self) -> Option<&FileStats> {
        self.file_stats_state.file_stats()
    }

    /// Returns the typed file-stats state. Useful for callers that want to inspect the
    /// variant directly (via `matches!` or the `is_*` predicates).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn file_stats_state(&self) -> &FileStatsState {
        &self.file_stats_state
    }
}

/// Refuses to serialize a degraded (non-`Complete`) CRC, so an invalid state can never
/// round-trip through disk.
impl Serialize for Crc {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CrcRaw::try_from(self)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
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
        for (name, value) in [
            ("numMetadata", raw.num_metadata),
            ("numProtocol", raw.num_protocol),
        ] {
            if value != 1 {
                return Err(Error::generic(format!(
                    "CRC file has invalid {name}: expected 1, got {value}"
                )));
            }
        }
        // A CRC file on disk is by definition complete; we never deserialize a degraded state.
        let file_stats_state = FileStatsState::Complete(FileStats {
            num_files: raw.num_files,
            table_size_bytes: raw.table_size_bytes,
            file_size_histogram: raw.file_size_histogram,
        });
        Ok(Crc {
            metadata: raw.metadata,
            protocol: raw.protocol,
            file_stats_state,
            in_commit_timestamp_opt: raw.in_commit_timestamp_opt,
            set_transactions: raw
                .set_transactions
                .map(|v| v.into_iter().map(|t| (t.app_id.clone(), t)).collect()),
            // Present array (including empty `[]`) deserializes as Complete; absent or null
            // deserializes as Partial(empty).
            domain_metadata_state: match raw.domain_metadata {
                Some(v) => DomainMetadataState::Complete(
                    v.into_iter().map(|d| (d.domain().to_string(), d)).collect(),
                ),
                None => DomainMetadataState::Partial(HashMap::new()),
            },
            // Not yet round-tripped through CrcRaw; see the "not yet supported" fields on Crc.
            txn_id: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        })
    }
}

/// Fails for non-`Complete` file stats: a degraded CRC has no well-defined on-disk shape.
impl TryFrom<&Crc> for CrcRaw {
    type Error = Error;
    fn try_from(crc: &Crc) -> Result<Self, Self::Error> {
        let FileStatsState::Complete(stats) = &crc.file_stats_state else {
            return Err(Error::ChecksumWriteUnsupported(format!(
                "Cannot serialize CRC with {:?} file stats",
                crc.file_stats_state
            )));
        };
        Ok(CrcRaw {
            table_size_bytes: stats.table_size_bytes,
            num_files: stats.num_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata: crc.metadata.clone(),
            protocol: crc.protocol.clone(),
            in_commit_timestamp_opt: crc.in_commit_timestamp_opt,
            set_transactions: crc
                .set_transactions
                .as_ref()
                .map(|m| m.values().cloned().collect()),
            // Only `Complete` is written; `Partial` is dropped.
            domain_metadata: match &crc.domain_metadata_state {
                DomainMetadataState::Complete(m) => Some(m.values().cloned().collect()),
                DomainMetadataState::Partial(_) => None,
            },
            file_size_histogram: stats.file_size_histogram.clone(),
        })
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

    use rstest::rstest;

    use super::{Crc, CrcRaw, DomainMetadataState, FileStats, FileStatsState};
    use crate::actions::{DomainMetadata, SetTransaction};

    /// Helper to create a minimal `Crc` with only set_transactions and domain_metadata_state
    /// populated.
    fn crc_with(
        txns: Option<HashMap<String, SetTransaction>>,
        domain_metadata_state: DomainMetadataState,
    ) -> Crc {
        Crc {
            set_transactions: txns,
            domain_metadata_state,
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

        // A present `domainMetadata` array deserializes as `Complete` (authoritative).
        let domains = crc.domain_metadata_state.expect_complete();
        assert_eq!(domains.len(), 2);
        assert!(domains.contains_key("delta.rowTracking"));
        assert!(domains.contains_key("delta.clustering"));
    }

    #[test]
    fn de_null_dm_deserializes_to_partial_empty_and_null_txns_deserializes_to_none() {
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
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
    }

    #[test]
    fn de_missing_dm_field_deserializes_to_partial_empty_and_missing_txns_to_none() {
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
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
    }

    #[test]
    fn ser_partial_dm_and_none_txns_serialize_to_null() {
        let crc = crc_with(None, DomainMetadataState::default());
        let json = serde_json::to_value(&crc).unwrap();
        assert!(json["setTransactions"].is_null());
        // Partial is not authoritative for misses; persisting it would falsely promote
        // to `Complete(empty)` on the next read.
        assert!(json["domainMetadata"].is_null());
    }

    #[test]
    fn ser_non_empty_partial_dm_still_serializes_to_null() {
        let mut partial = HashMap::new();
        partial.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new("delta.rowTracking".to_string(), "{}".to_string()),
        );
        let crc = crc_with(None, DomainMetadataState::Partial(partial));
        let json = serde_json::to_value(&crc).unwrap();
        // Even non-empty Partial maps drop on serialize.
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

        let original = crc_with(Some(txns), DomainMetadataState::Complete(domains));

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn round_trip_empty_complete_dm_and_empty_txns() {
        let original = crc_with(
            Some(HashMap::new()),
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
    fn round_trip_partial_dm_becomes_empty_partial() {
        let mut partial = HashMap::new();
        partial.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new("delta.rowTracking".to_string(), "{}".to_string()),
        );
        let original = crc_with(None, DomainMetadataState::Partial(partial));

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            deserialized.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
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
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 10,
                table_size_bytes: 1024 * 1024,
                file_size_histogram: None,
            }),
            set_transactions: Some(txns),
            domain_metadata_state: DomainMetadataState::Complete(domains),
            ..Default::default()
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&crc).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        // Verify scalar fields survive the round-trip
        let stats = deserialized.file_stats().unwrap();
        assert_eq!(stats.table_size_bytes(), 1024 * 1024);
        assert_eq!(stats.num_files(), 10);

        // Verify all set transactions
        let txns = deserialized.set_transactions.as_ref().unwrap();
        assert_eq!(txns.len(), 3);
        assert_eq!(txns["streaming-app"].version, 42);
        assert_eq!(txns["streaming-app"].last_updated, Some(1700000000));
        assert_eq!(txns["batch-job"].version, 100);
        assert_eq!(txns["batch-job"].last_updated, None);
        assert_eq!(txns["etl-pipeline"].version, 7);

        // Verify all domain metadatas
        let domains = deserialized.domain_metadata_state.expect_complete();
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

    /// Minimal CRC JSON with the supplied numMetadata / numProtocol values; used to construct
    /// invalid CRCs and verify rejection.
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

    /// Per the Delta protocol spec, both `numMetadata` and `numProtocol` MUST be 1; any other
    /// value (zero, two, negative) is rejected, and the error names the offending field.
    #[rstest]
    #[case::num_metadata("numMetadata", |b| (b, 1))]
    #[case::num_protocol("numProtocol", |b| (1, b))]
    fn de_invalid_count_is_rejected(
        #[case] field: &str,
        #[case] counts: fn(i64) -> (i64, i64),
        #[values(0i64, 2, 3, -1)] bad: i64,
    ) {
        let (m, p) = counts(bad);
        let json = crc_json_with_counts(m, p);
        let err = serde_json::from_str::<Crc>(&json).unwrap_err().to_string();
        assert!(
            err.contains(field),
            "expected error to mention {field} for value {bad}, got: {err}"
        );
    }

    #[test]
    fn ser_indeterminate_file_stats_returns_error() {
        let crc = Crc {
            file_stats_state: FileStatsState::Indeterminate,
            ..Default::default()
        };
        let err = serde_json::to_string(&crc).unwrap_err().to_string();
        assert!(
            err.contains("Cannot serialize CRC"),
            "expected serialize-rejection error, got: {err}"
        );
    }

    #[test]
    fn try_from_ref_indeterminate_returns_checksum_write_unsupported() {
        let crc = Crc {
            file_stats_state: FileStatsState::Indeterminate,
            ..Default::default()
        };
        let err = CrcRaw::try_from(&crc).unwrap_err();
        assert!(
            matches!(err, crate::Error::ChecksumWriteUnsupported(_)),
            "expected ChecksumWriteUnsupported, got: {err:?}"
        );
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
        assert!(crc.file_stats().unwrap().file_size_histogram().is_some());
    }

    #[rstest]
    #[case::spec_name("fileSizeHistogram")]
    #[case::legacy_name("histogramOpt")]
    fn de_null_file_size_histogram_deserializes_to_none(#[case] field_name: &str) {
        let json = crc_json_with_histogram(field_name, "null");
        let crc: Crc = serde_json::from_str(&json).unwrap();
        assert!(crc.file_stats().unwrap().file_size_histogram().is_none());
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
