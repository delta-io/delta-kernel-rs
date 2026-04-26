//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! # Architecture
//!
//! The in-memory representation [`Crc`] uses typed state enums that encode both the value and
//! the trustworthiness of every field. This makes invalid states unrepresentable: you cannot
//! accidentally read an absolute file count from a relative delta because they live in
//! different variants of [`FileStatsState`].
//!
//! On-disk JSON serialization goes through [`CrcRaw`], a flat 1:1 mirror of the Delta protocol
//! [CRC file] format. The mapping is mediated by `#[serde(from, into)]` on [`Crc`].
//!
//! [`CrcUpdate`] is the universal delta type. Every CRC mutation flows through
//! [`Crc::apply`]. There are four ways to produce a `CrcUpdate`:
//!
//! 1. **Commit-time** ([`Transaction::commit`]): build from staged add/remove metadata + the
//!    transaction's own commitInfo / DM / txn changes. Apply once to the read snapshot's CRC.
//! 2. **Stale-CRC catch-up**: reverse-replay commits after the CRC version, accumulate, apply once.
//!    The accumulator does first-seen-wins for per-key fields (DM/txn/P/M).
//! 3. **No-CRC bootstrap**: same shape as (2) but the segment includes a checkpoint and the base is
//!    built via [`CrcUpdate::into_fresh_crc`].
//! 4. **Future: forward rebase / action reconciliation** (priorities 4 and 5 in the design plan):
//!    different production strategies, same `Crc::apply`.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
//! [`Transaction::commit`]: crate::transaction::Transaction::commit

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

#[allow(unused)]
pub(crate) use delta::CrcUpdate;
pub use file_size_histogram::FileSizeHistogram;
pub use file_stats::FileStats;
#[allow(unused)]
pub(crate) use file_stats::FileStatsDelta;
pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
use serde::{Deserialize, Serialize};
pub use state::{DomainMetadataState, FileStatsState, FileStatsValidity, SetTransactionState};
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};

/// In-memory CRC state for a snapshot. Always present on
/// [`Snapshot`](crate::snapshot::Snapshot).
///
/// `Crc` carries P&M (always), file statistics with typed validity ([`FileStatsState`]),
/// completeness-tracked maps for domain metadata and set transactions
/// ([`DomainMetadataState`] / [`SetTransactionState`]), and an optional in-commit timestamp.
///
/// Mutated only via [`Crc::apply`]. Constructed either by deserializing from disk (via the
/// [`CrcRaw`] serde intermediate), by [`Crc::minimal_from_pm`], or by
/// [`CrcUpdate::into_fresh_crc`] (when there is no base CRC to apply onto).
///
/// This struct and its fields are `pub` so integration tests under the `test-utils` feature
/// can inspect them; without `test-utils` the `crc` module is `pub(crate)`. See
/// `kernel/src/lib.rs`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(from = "CrcRaw", into = "CrcRaw")]
pub struct Crc {
    /// The table [`Protocol`] at this version.
    pub protocol: Protocol,
    /// The table [`Metadata`] at this version.
    pub metadata: Metadata,
    /// File-level statistics with typed validity. Encodes both values and their
    /// trustworthiness.
    pub file_stats: FileStatsState,
    /// Domain metadata completeness state.
    pub domain_metadata: DomainMetadataState,
    /// Set transaction completeness state.
    pub set_transactions: SetTransactionState,
    /// In-commit timestamp from the latest commit, if ICT is enabled at this version.
    pub in_commit_timestamp_opt: Option<i64>,
}

impl Default for Crc {
    /// Default CRC: [`Valid`](FileStatsState::Valid) with zero counts and no histogram,
    /// [`Untracked`](DomainMetadataState::Untracked) DM and txns, default P&M.
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
    /// Returns file-level statistics if [`FileStatsState`] is `Valid`, else `None`.
    pub fn file_stats(&self) -> Option<FileStats> {
        self.file_stats.file_stats()
    }

    /// Returns the simplified validity classification of file stats. Useful for cost
    /// inspection by connectors.
    pub fn file_stats_validity(&self) -> FileStatsValidity {
        self.file_stats.validity()
    }

    /// Build a minimal CRC from Protocol and Metadata only. File stats are
    /// [`RequiresCheckpointRead`](FileStatsState::RequiresCheckpointRead), DM/txns are
    /// [`Untracked`](DomainMetadataState::Untracked), histogram is `None`, ICT is `None`.
    ///
    /// Used when no CRC file is available and a full log replay was not performed -- e.g.
    /// the incremental snapshot update fallback path.
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
}

// ============================================================================
// CrcRaw: serde intermediate representation
// ============================================================================

/// Flat 1:1 mirror of the on-disk CRC JSON format from the Delta protocol spec.
///
/// Used only as a serde intermediate via `#[serde(from, into)]` on [`Crc`]. Maps the typed
/// in-memory enums to/from the flat JSON shape.
///
/// The Delta protocol always writes complete data (or omits the field entirely): a CRC file
/// on disk is `Valid` for file stats, and `Complete` (or absent) for DM and txns. The serde
/// intermediate enforces this via deserialization (always to `Valid` / `Complete`) and via
/// [`try_write_crc_file`] (which gates on [`FileStatsState::is_writable`]).
///
/// Fields with `#[serde(default)]` produce the appropriate "absent" state when missing or
/// `null`: `set_transactions` and `domain_metadata` map to `Untracked`, `file_size_histogram`
/// stays `None`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CrcRaw {
    /// Total size of the table in bytes (sum of all active Add file sizes).
    table_size_bytes: i64,
    /// Number of active Add file actions.
    num_files: i64,
    /// Number of Metadata actions. Always 1.
    num_metadata: i64,
    /// Number of Protocol actions. Always 1.
    num_protocol: i64,
    metadata: Metadata,
    protocol: Protocol,
    /// In-commit timestamp from the latest commit, when ICT is enabled. Skipped on
    /// serialize when `None` to avoid emitting `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    in_commit_timestamp_opt: Option<i64>,
    /// Active set transactions, when tracked. `None` (absent or `null` in JSON) maps to
    /// [`SetTransactionState::Untracked`].
    #[serde(default)]
    set_transactions: Option<Vec<SetTransaction>>,
    /// Active (non-removed) domain metadata, when tracked. `None` maps to
    /// [`DomainMetadataState::Untracked`].
    #[serde(default)]
    domain_metadata: Option<Vec<DomainMetadata>>,
    /// File size histogram, when present. Validated on deserialize via
    /// [`FileSizeHistogram::try_new`].
    #[serde(
        default,
        deserialize_with = "deserialize_validated_histogram",
        skip_serializing_if = "Option::is_none"
    )]
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
        // For non-Valid states, fall back to zeros and no histogram. The writer guards
        // against writing non-Valid CRCs via FileStatsState::is_writable, so this branch
        // is only hit for in-memory serde round-trips of degraded states (mostly tests).
        let (num_files, table_size_bytes, file_size_histogram) = match crc.file_stats {
            FileStatsState::Valid {
                num_files,
                table_size_bytes,
                histogram,
            } => (num_files, table_size_bytes, histogram),
            _ => (0, 0, None),
        };
        // Only Complete maps serialize to arrays; Partial and Untracked omit the field
        // (writers can only write Complete, enforced upstream).
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

/// Deserializes an `Option<FileSizeHistogram>` and validates structural invariants via
/// [`FileSizeHistogram::try_new`] (sorted boundaries, matching array lengths, etc.). Rejects
/// malformed CRC files instead of producing a panicky histogram later.
fn deserialize_validated_histogram<'de, D>(
    deserializer: D,
) -> Result<Option<FileSizeHistogram>, D::Error>
where
    D: serde::Deserializer<'de>,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Helper: build a minimal `Crc` overriding only the DM and txn states.
    fn crc_with(dm: DomainMetadataState, txns: SetTransactionState) -> Crc {
        Crc {
            domain_metadata: dm,
            set_transactions: txns,
            ..Default::default()
        }
    }

    #[test]
    fn de_vec_to_complete_state_with_correct_keys() {
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
        assert_eq!(txns["app-1"].version, 3);
        assert_eq!(txns["app-1"].last_updated, Some(1000));
        assert_eq!(txns["app-2"].version, 7);
        assert_eq!(txns["app-2"].last_updated, None);

        let domains = crc.domain_metadata.map().unwrap();
        assert_eq!(domains.len(), 2);
        assert!(domains.contains_key("delta.rowTracking"));
        assert!(domains.contains_key("delta.clustering"));

        // File stats from disk are always Valid.
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
    }

    #[test]
    fn de_null_dm_and_txns_become_untracked() {
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
    fn de_missing_dm_and_txn_fields_become_untracked() {
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
    fn ser_untracked_omits_dm_and_txn_fields() {
        // Untracked produces None in CrcRaw; with default serde, Option::None becomes null.
        let crc = crc_with(
            DomainMetadataState::Untracked,
            SetTransactionState::Untracked,
        );
        let json = serde_json::to_value(&crc).unwrap();
        assert!(json["setTransactions"].is_null());
        assert!(json["domainMetadata"].is_null());
    }

    #[test]
    fn ser_de_round_trip_complete_state() {
        let mut txns = HashMap::new();
        txns.insert(
            "app-1".to_string(),
            SetTransaction::new("app-1".to_string(), 5, Some(2000)),
        );
        let mut domains = HashMap::new();
        domains.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new("delta.rowTracking".to_string(), "{}".to_string()),
        );

        let original = crc_with(
            DomainMetadataState::Complete(domains),
            SetTransactionState::Complete(txns),
        );

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn ser_complete_empty_map_produces_empty_array() {
        let original = crc_with(
            DomainMetadataState::Complete(HashMap::new()),
            SetTransactionState::Complete(HashMap::new()),
        );

        let json_value = serde_json::to_value(&original).unwrap();
        assert_eq!(json_value["setTransactions"], serde_json::json!([]));
        assert_eq!(json_value["domainMetadata"], serde_json::json!([]));

        // Empty Complete round-trips to Complete (not Untracked).
        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized: Crc = serde_json::from_str(&json_str).unwrap();
        assert!(deserialized.domain_metadata.is_complete());
        assert!(deserialized.set_transactions.is_complete());
    }

    #[test]
    fn ser_partial_state_omits_fields() {
        // Partial DM/txns shouldn't be persisted (writer guards via is_writable, but the
        // serde mapping for Partial drops the field anyway).
        let mut dm_map = HashMap::new();
        dm_map.insert(
            "d".to_string(),
            DomainMetadata::new("d".to_string(), "{}".to_string()),
        );
        let crc = crc_with(
            DomainMetadataState::Partial(dm_map),
            SetTransactionState::Untracked,
        );
        let json = serde_json::to_value(&crc).unwrap();
        assert!(json["domainMetadata"].is_null());
        assert!(json["setTransactions"].is_null());
    }

    #[test]
    fn ser_non_valid_file_stats_becomes_zeros() {
        // Non-Valid CRCs should not normally be serialized (writer guards), but if forced
        // (e.g. for in-memory debugging), file stats fields fall back to zero/None.
        let crc = Crc {
            file_stats: FileStatsState::Indeterminate,
            ..Default::default()
        };
        let json = serde_json::to_value(&crc).unwrap();
        assert_eq!(json["numFiles"], 0);
        assert_eq!(json["tableSizeBytes"], 0);
        assert!(json.get("fileSizeHistogram").is_none());
    }

    // ===== File size histogram validation =====

    fn crc_json_with_histogram(histogram_json: &str) -> String {
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
                "fileSizeHistogram": {histogram_json}
            }}"#
        )
    }

    #[test]
    fn de_valid_histogram_lands_in_file_stats() {
        let json = crc_json_with_histogram(
            r#"{"sortedBinBoundaries": [0, 100, 200], "fileCounts": [1, 2, 3], "totalBytes": [10, 200, 300]}"#,
        );
        let crc: Crc = serde_json::from_str(&json).unwrap();
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.sorted_bin_boundaries(), &[0, 100, 200]);
    }

    #[test]
    fn de_null_histogram_is_none() {
        let json = crc_json_with_histogram("null");
        let crc: Crc = serde_json::from_str(&json).unwrap();
        assert!(crc.file_stats().unwrap().file_size_histogram().is_none());
    }

    use rstest::rstest;

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
    fn de_malformed_histogram_returns_error(#[case] histogram_json: &str) {
        let json = crc_json_with_histogram(histogram_json);
        assert!(serde_json::from_str::<Crc>(&json).is_err());
    }

    #[test]
    fn minimal_from_pm_produces_requires_checkpoint_read() {
        let crc = Crc::minimal_from_pm(Protocol::default(), Metadata::default());
        assert_eq!(
            crc.file_stats_validity(),
            FileStatsValidity::RequiresCheckpointRead
        );
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Untracked
        ));
    }
}
