//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, set
//! transactions, and ICT.
//!
//! [`Crc`] holds the in-memory state using shapes that make kernel queries easy: typed
//! state enums (`FileStatsState`, `DomainMetadataState`, `SetTransactionState`) and `HashMap`s
//! keyed by id, instead of the flat scalars and arrays of the on-disk format. It deserializes the
//! on-disk JSON through the private `CrcRaw` serde intermediate. Serialization omits
//! version-specific fields that incremental replay does not reconstruct.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

// Allow unreachable_pub because this module is pub when internal-api is enabled
// but pub(crate) otherwise.
#![allow(unreachable_pub)]

use std::collections::HashSet;

mod delta;
mod file_size_histogram;
mod file_stats;
mod reader;
mod state;
mod writer;

#[allow(unused)]
pub(crate) use delta::{merge_domain_metadata, CrcDelta};
use delta_kernel_derive::internal_api;
pub use file_size_histogram::FileSizeHistogram;
pub use file_stats::FileStats;
#[allow(unused)]
pub(crate) use file_stats::{is_incremental_safe_operation, size_to_u64, FileStatsDelta};
pub(crate) use reader::read_crc_file_or_none;
#[cfg(test)]
pub(crate) use reader::try_read_crc_file;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
pub use state::{DomainMetadataState, FileStatsState, SetTransactionState};
#[allow(unused)]
pub(crate) use writer::try_write_crc_file_with_channel;

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::table_properties::ENABLE_IN_COMMIT_TIMESTAMPS;
use crate::{DeltaResult, Error, Version};

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
/// when the `internal-api` feature is enabled (otherwise `pub(crate)`). See `kernel/src/lib.rs`.
// TODO: rename `Crc` to `CrcState` to align with `FileStatsState`, `SetTransactionState`, etc.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Crc {
    // ===== Required fields =====
    /// The table version this CRC describes.
    pub version: Version,
    /// The table [`Metadata`] at this version.
    pub metadata: Metadata,
    /// The table [`Protocol`] at this version.
    pub protocol: Protocol,
    /// File-level statistics as a typed state. See [`FileStatsState`].
    pub(crate) file_stats_state: FileStatsState,

    // ===== Optional fields =====
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub in_commit_timestamp_opt: Option<i64>,
    /// Active [`SetTransaction`] actions at this version, as a typed [`SetTransactionState`].
    /// `Complete(map)` is authoritative for misses; `Partial(map)` carries known-correct entries
    /// but requires log replay for misses. Only the `Complete` variant is persisted to the CRC
    /// file.
    pub set_transaction_state: SetTransactionState,
    /// Active (non-removed) [`DomainMetadata`] actions at this version, as a typed
    /// [`DomainMetadataState`]. Tombstones (`removed=true`) are never stored. `Complete(map)`
    /// is authoritative for misses; `Partial(map)` carries known-correct entries but requires
    /// log replay for misses. Only the `Complete` variant is persisted to the CRC file.
    ///
    /// TODO: when the table protocol does not enable the `domainMetadata` feature, no DM
    ///       action can exist, so `Partial(_)` is semantically equivalent to
    ///       `Complete(empty)` and both serde paths could collapse the distinction.
    pub domain_metadata_state: DomainMetadataState,
    /// All live [`Add`] file actions at this version. Present only when read whole from an
    /// at-version CRC; `Crc::apply` drops it, since incremental advance does not reconstruct the
    /// set.
    // TODO(#3361): read-only for now; advancing, writing, and data-skipping use are unsupported.
    pub(crate) all_files: Option<Vec<Add>>,

    // ===== Extended optional fields =====
    /// A unique identifier for the transaction that produced this commit.
    pub(crate) txn_id: Option<String>,
    /// Number of records deleted through Deletion Vectors in this table version.
    pub(crate) num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    pub(crate) num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files.
    pub(crate) deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
}

impl Crc {
    /// Reconstructs CRC state from its in-memory fields.
    ///
    /// Returns an error when fields violate CRC invariants or contain inconsistent file,
    /// deletion-vector, histogram, or in-commit-timestamp aggregates.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_from_parts(
        version: Version,
        metadata: Metadata,
        protocol: Protocol,
        file_stats_state: FileStatsState,
        in_commit_timestamp_opt: Option<i64>,
        set_transaction_state: SetTransactionState,
        domain_metadata_state: DomainMetadataState,
        txn_id: Option<String>,
        all_files: Option<Vec<Add>>,
        num_deleted_records_opt: Option<i64>,
        num_deletion_vectors_opt: Option<i64>,
        deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
    ) -> DeltaResult<Self> {
        let crc = Self {
            version,
            metadata,
            protocol,
            file_stats_state,
            in_commit_timestamp_opt,
            set_transaction_state,
            domain_metadata_state,
            txn_id,
            all_files,
            num_deleted_records_opt,
            num_deletion_vectors_opt,
            deleted_record_counts_histogram_opt,
        };
        crc.validate()?;
        Ok(crc)
    }

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

    /// Returns the complete set of live [`Add`] file actions, if this CRC carries one.
    ///
    /// `allFiles` is present only when the writer chose to include it (e.g. the table was small
    /// enough); otherwise this returns `None` and callers reconstruct the file list through log
    /// replay.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub fn all_files(&self) -> Option<&[Add]> {
        self.all_files.as_deref()
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
///
/// Fields marked `skip_serializing` are read for validation but omitted because incremental replay
/// does not reconstruct them for the resulting table version.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CrcRaw {
    #[serde(default, skip_serializing)]
    txn_id: Option<String>,
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
    #[serde(default, skip_serializing)]
    all_files: Option<Vec<Add>>,
    #[serde(default, skip_serializing)]
    num_deleted_records_opt: Option<i64>,
    #[serde(default, skip_serializing)]
    num_deletion_vectors_opt: Option<i64>,
    #[serde(default, skip_serializing)]
    deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogramRaw>,
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

impl Crc {
    /// Parses a `.crc` file body for `version`, which comes from the filename because the body does
    /// not carry it.
    ///
    /// Returns parsed CRC state after validating its JSON shape, required action counts,
    /// non-negative aggregate statistics, and histogram structure. This does not compare the state
    /// with log replay.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or invalid counts, statistics, or histogram fields.
    #[internal_api]
    pub(crate) fn try_from_json_bytes(bytes: &[u8], version: Version) -> DeltaResult<Self> {
        let raw: CrcRaw = serde_json::from_slice(bytes)?;
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
        for (name, value) in [
            ("numFiles", raw.num_files),
            ("tableSizeBytes", raw.table_size_bytes),
        ] {
            if value < 0 {
                return Err(Error::generic(format!(
                    "CRC file has invalid {name}: expected a non-negative value, got {value}"
                )));
            }
        }
        let file_stats_state = FileStatsState::Complete(FileStats::try_new(
            raw.num_files,
            raw.table_size_bytes,
            raw.file_size_histogram,
        )?);
        Crc::try_from_parts(
            version,
            raw.metadata,
            raw.protocol,
            file_stats_state,
            raw.in_commit_timestamp_opt,
            match raw.set_transactions {
                Some(values) => SetTransactionState::try_complete(values)?,
                None => SetTransactionState::try_partial(Vec::new())?,
            },
            match raw.domain_metadata {
                Some(values) => DomainMetadataState::try_complete(values)?,
                None => DomainMetadataState::try_partial(Vec::new())?,
            },
            raw.txn_id,
            raw.all_files,
            raw.num_deleted_records_opt,
            raw.num_deletion_vectors_opt,
            raw.deleted_record_counts_histogram_opt
                .map(TryInto::try_into)
                .transpose()?,
        )
    }
}

/// Fails for non-`Complete` file stats: a degraded CRC has no well-defined on-disk shape.
impl TryFrom<&Crc> for CrcRaw {
    type Error = Error;
    fn try_from(crc: &Crc) -> Result<Self, Self::Error> {
        crc.validate()?;
        let FileStatsState::Complete(stats) = &crc.file_stats_state else {
            return Err(Error::ChecksumWriteUnsupported(format!(
                "Cannot serialize CRC with {:?} file stats",
                crc.file_stats_state
            )));
        };
        Ok(CrcRaw {
            txn_id: None,
            table_size_bytes: stats.table_size_bytes,
            num_files: stats.num_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata: crc.metadata.clone(),
            protocol: crc.protocol.clone(),
            in_commit_timestamp_opt: crc.in_commit_timestamp_opt,
            // Only `Complete` is written; `Partial` is dropped.
            set_transactions: match &crc.set_transaction_state {
                SetTransactionState::Complete(m) => Some(m.values().cloned().collect()),
                SetTransactionState::Partial(_) => None,
            },
            // Only `Complete` is written; `Partial` is dropped.
            domain_metadata: match &crc.domain_metadata_state {
                DomainMetadataState::Complete(m) => Some(m.values().cloned().collect()),
                DomainMetadataState::Partial(_) => None,
            },
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
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
        Some(hist) => {
            if let Some(bin) = hist
                .file_counts
                .iter()
                .zip(&hist.total_bytes)
                .position(|(count, bytes)| *count < 0 || *bytes < 0)
            {
                return Err(serde::de::Error::custom(format!(
                    "CRC fileSizeHistogram has negative counts or bytes at bin {bin}"
                )));
            }
            FileSizeHistogram::try_new(
                hist.sorted_bin_boundaries,
                hist.file_counts,
                hist.total_bytes,
            )
            .map(Some)
            .map_err(serde::de::Error::custom)
        }
        None => Ok(None),
    }
}

impl Crc {
    fn validate(&self) -> DeltaResult<()> {
        for (name, value) in [
            ("numDeletedRecordsOpt", self.num_deleted_records_opt),
            ("numDeletionVectorsOpt", self.num_deletion_vectors_opt),
        ] {
            if value.is_some_and(|value| value < 0) {
                return Err(Error::generic(format!(
                    "CRC file has invalid {name}: expected a non-negative value"
                )));
            }
        }
        if self
            .metadata
            .configuration()
            .get(ENABLE_IN_COMMIT_TIMESTAMPS)
            .is_some_and(|value| value == "true")
            && self.in_commit_timestamp_opt.is_none()
        {
            return Err(Error::generic(
                "CRC file is missing inCommitTimestampOpt for an ICT-enabled table",
            ));
        }

        if let Some(files) = &self.all_files {
            let mut paths = HashSet::with_capacity(files.len());
            if let Some(add) = files.iter().find(|add| !paths.insert(add.path.as_str())) {
                return Err(Error::generic(format!(
                    "allFiles contains duplicate path {}",
                    add.path
                )));
            }
            if let Some(add) = files.iter().find(|add| add.size < 0) {
                return Err(Error::generic(format!(
                    "allFiles contains negative file size {} for {}",
                    add.size, add.path
                )));
            }
        }

        if let FileStatsState::Complete(stats) = &self.file_stats_state {
            if let Some(histogram) = &stats.file_size_histogram {
                validate_sum(
                    "fileSizeHistogram file count",
                    &histogram.file_counts,
                    stats.num_files,
                )?;
                validate_sum(
                    "fileSizeHistogram total bytes",
                    &histogram.total_bytes,
                    stats.table_size_bytes,
                )?;
            }
            if let Some(files) = &self.all_files {
                let file_count = i64::try_from(files.len())
                    .map_err(|_| Error::generic("allFiles length exceeds i64"))?;
                if file_count != stats.num_files {
                    return Err(Error::generic(format!(
                        "allFiles/numFiles mismatch: {file_count} != {}",
                        stats.num_files
                    )));
                }
                let table_size =
                    checked_sum("allFiles table size", files.iter().map(|add| add.size))?;
                if table_size != stats.table_size_bytes {
                    return Err(Error::generic(format!(
                        "allFiles/tableSizeBytes mismatch: {table_size} != {}",
                        stats.table_size_bytes
                    )));
                }
                if let Some(histogram) = &stats.file_size_histogram {
                    let mut derived = FileSizeHistogram::create_empty_with_boundaries(
                        histogram.sorted_bin_boundaries.clone(),
                    )?;
                    for add in files {
                        derived.insert(add.size)?;
                    }
                    if &derived != histogram {
                        return Err(Error::generic(
                            "allFiles/fileSizeHistogram bins do not match",
                        ));
                    }
                }
            }
        }

        if let Some(histogram) = &self.deleted_record_counts_histogram_opt {
            let expected_files = self
                .file_stats_state
                .file_stats()
                .map(|stats| stats.num_files);
            if let Some(expected) = expected_files {
                validate_sum(
                    "deletedRecordCountsHistogram file count",
                    &histogram.deleted_record_counts,
                    expected,
                )?;
            }
        }

        if let Some(files) = &self.all_files {
            let derived = DerivedDeletionStats::try_from(files.as_slice())?;
            for (name, actual, expected) in [
                (
                    "numDeletedRecordsOpt",
                    derived.deleted_records,
                    self.num_deleted_records_opt,
                ),
                (
                    "numDeletionVectorsOpt",
                    derived.deletion_vectors,
                    self.num_deletion_vectors_opt,
                ),
            ] {
                if expected.is_some_and(|expected| expected != actual) {
                    return Err(Error::generic(format!(
                        "allFiles/{name} mismatch: derived {actual}"
                    )));
                }
            }
            if self
                .deleted_record_counts_histogram_opt
                .as_ref()
                .is_some_and(|histogram| histogram != &derived.histogram)
            {
                return Err(Error::generic(
                    "allFiles/deletedRecordCountsHistogramOpt bins do not match",
                ));
            }
        }
        Ok(())
    }
}

struct DerivedDeletionStats {
    deleted_records: i64,
    deletion_vectors: i64,
    histogram: DeletedRecordCountsHistogram,
}

impl TryFrom<&[Add]> for DerivedDeletionStats {
    type Error = Error;

    fn try_from(files: &[Add]) -> DeltaResult<Self> {
        let cardinalities = || {
            files.iter().map(|add| {
                add.deletion_vector
                    .as_ref()
                    .map_or(0, |deletion_vector| deletion_vector.cardinality)
            })
        };
        let histogram = DeletedRecordCountsHistogram::try_from_cardinalities(cardinalities())?;
        let deleted_records = checked_sum("allFiles deleted-record total", cardinalities())?;
        let deletion_vectors = i64::try_from(
            files
                .iter()
                .filter(|add| add.deletion_vector.is_some())
                .count(),
        )
        .map_err(|_| Error::generic("allFiles deletion-vector count exceeds i64"))?;
        Ok(Self {
            deleted_records,
            deletion_vectors,
            histogram,
        })
    }
}

fn validate_sum(name: &str, values: &[i64], expected: i64) -> DeltaResult<()> {
    let actual = checked_sum(name, values.iter().copied())?;
    if actual != expected {
        return Err(Error::generic(format!(
            "CRC {name} mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn checked_sum(name: &str, mut values: impl Iterator<Item = i64>) -> DeltaResult<i64> {
    values.try_fold(0_i64, |sum, value| {
        sum.checked_add(value)
            .ok_or_else(|| Error::generic(format!("CRC {name} overflow")))
    })
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeletedRecordCountsHistogramRaw {
    deleted_record_counts: [i64; 10],
}

impl TryFrom<DeletedRecordCountsHistogramRaw> for DeletedRecordCountsHistogram {
    type Error = Error;

    fn try_from(value: DeletedRecordCountsHistogramRaw) -> DeltaResult<Self> {
        Self::try_new(value.deleted_record_counts.into())
    }
}

impl DeletedRecordCountsHistogram {
    /// Reconstructs a deleted-record-count histogram from its serialized bins.
    ///
    /// Returns an error unless exactly ten non-negative bin counts are provided.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn try_new(deleted_record_counts: Vec<i64>) -> DeltaResult<Self> {
        Self::validate(&deleted_record_counts)?;
        Ok(Self {
            deleted_record_counts,
        })
    }

    fn try_from_cardinalities(cardinalities: impl IntoIterator<Item = i64>) -> DeltaResult<Self> {
        let mut bins = vec![0; 10];
        for cardinality in cardinalities {
            if cardinality < 0 {
                return Err(Error::generic(format!(
                    "allFiles contains negative deletion-vector cardinality {cardinality}"
                )));
            }
            let bin = match cardinality {
                0 => 0,
                1..=9 => 1,
                10..=99 => 2,
                100..=999 => 3,
                1_000..=9_999 => 4,
                10_000..=99_999 => 5,
                100_000..=999_999 => 6,
                1_000_000..=9_999_999 => 7,
                10_000_000..=2_147_483_646 => 8,
                _ => 9,
            };
            bins[bin] += 1;
        }
        Self::try_new(bins)
    }

    fn validate(deleted_record_counts: &[i64]) -> DeltaResult<()> {
        if deleted_record_counts.len() != 10 {
            return Err(Error::generic(format!(
                "deleted-record-count histogram must contain exactly 10 bins, got {}",
                deleted_record_counts.len()
            )));
        }
        if let Some((bin, count)) = deleted_record_counts
            .iter()
            .copied()
            .enumerate()
            .find(|(_, count)| *count < 0)
        {
            return Err(Error::generic(format!(
                "deleted-record-count histogram has negative file count {count} at bin {bin}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::{
        Crc, CrcRaw, DeletedRecordCountsHistogram, DomainMetadataState, FileStats, FileStatsState,
        SetTransactionState, ENABLE_IN_COMMIT_TIMESTAMPS,
    };
    use crate::actions::{Add, DomainMetadata, Protocol, SetTransaction};
    use crate::table_features::TableFeature;

    /// A minimal valid protocol for round-trip tests. `Protocol::default()` is `(0, 0)`, which
    /// `try_new` rejects, so a default protocol can't round-trip through serde (deserialization
    /// validates via `try_new`).
    fn valid_protocol() -> Protocol {
        Protocol::try_new(1, 1, TableFeature::NO_LIST, TableFeature::NO_LIST).unwrap()
    }

    /// Helper to create a minimal `Crc` with only `set_transaction_state` and
    /// `domain_metadata_state` populated.
    fn crc_with(
        set_transaction_state: SetTransactionState,
        domain_metadata_state: DomainMetadataState,
    ) -> Crc {
        Crc {
            protocol: valid_protocol(),
            set_transaction_state,
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

        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();

        // A present `setTransactions` array deserializes as `Complete` (authoritative).
        let txns = crc.set_transaction_state.expect_complete();
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
    fn de_null_dm_and_txns_deserialize_to_partial_empty() {
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
        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();
        assert_eq!(
            crc.set_transaction_state,
            SetTransactionState::Partial(HashMap::new())
        );
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
    }

    #[test]
    fn de_missing_dm_and_txns_fields_deserialize_to_partial_empty() {
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
        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();
        assert_eq!(
            crc.set_transaction_state,
            SetTransactionState::Partial(HashMap::new())
        );
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
    }

    #[test]
    fn ser_partial_dm_and_partial_txns_serialize_to_null() {
        let crc = crc_with(
            SetTransactionState::Partial(HashMap::new()),
            DomainMetadataState::Partial(HashMap::new()),
        );
        let json = serde_json::to_value(&crc).unwrap();
        // Partial is not authoritative for misses; persisting it would falsely promote
        // to `Complete(empty)` on the next read.
        assert!(json["setTransactions"].is_null());
        assert!(json["domainMetadata"].is_null());
    }

    #[test]
    fn ser_non_empty_partial_dm_still_serializes_to_null() {
        let mut partial = HashMap::new();
        partial.insert(
            "delta.rowTracking".to_string(),
            DomainMetadata::new("delta.rowTracking".to_string(), "{}".to_string()),
        );
        let crc = crc_with(
            SetTransactionState::Partial(HashMap::new()),
            DomainMetadataState::Partial(partial),
        );
        let json = serde_json::to_value(&crc).unwrap();
        // Even non-empty Partial maps drop on serialize.
        assert!(json["domainMetadata"].is_null());
    }

    #[test]
    fn ser_non_empty_partial_txns_still_serializes_to_null() {
        let mut partial = HashMap::new();
        partial.insert(
            "my-app".to_string(),
            SetTransaction::new("my-app".to_string(), 1, None),
        );
        let crc = crc_with(
            SetTransactionState::Partial(partial),
            DomainMetadataState::Partial(HashMap::new()),
        );
        let json = serde_json::to_value(&crc).unwrap();
        // Even non-empty Partial maps drop on serialize.
        assert!(json["setTransactions"].is_null());
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
        let deserialized = Crc::try_from_json_bytes(json_str.as_bytes(), 0).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn round_trip_empty_complete_dm_and_empty_txns() {
        let original = crc_with(
            SetTransactionState::Complete(HashMap::new()),
            DomainMetadataState::Complete(HashMap::new()),
        );

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized = Crc::try_from_json_bytes(json_str.as_bytes(), 0).unwrap();

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
        let original = crc_with(
            SetTransactionState::Partial(HashMap::new()),
            DomainMetadataState::Partial(partial),
        );

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized = Crc::try_from_json_bytes(json_str.as_bytes(), 0).unwrap();

        assert_eq!(
            deserialized.domain_metadata_state,
            DomainMetadataState::Partial(HashMap::new())
        );
    }

    #[test]
    fn partial_txns_written_as_null_reads_back_as_empty_partial() {
        let mut partial = HashMap::new();
        partial.insert(
            "my-app".to_string(),
            SetTransaction::new("my-app".to_string(), 7, Some(1000)),
        );
        let original = crc_with(
            SetTransactionState::Partial(partial),
            DomainMetadataState::Partial(HashMap::new()),
        );

        let json_str = serde_json::to_string(&original).unwrap();
        let deserialized = Crc::try_from_json_bytes(json_str.as_bytes(), 0).unwrap();

        assert_eq!(
            deserialized.set_transaction_state,
            SetTransactionState::Partial(HashMap::new())
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
            protocol: valid_protocol(),
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 10,
                table_size_bytes: 1024 * 1024,
                file_size_histogram: None,
            }),
            set_transaction_state: SetTransactionState::Complete(txns),
            domain_metadata_state: DomainMetadataState::Complete(domains),
            ..Default::default()
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&crc).unwrap();
        let deserialized = Crc::try_from_json_bytes(json_str.as_bytes(), 0).unwrap();

        // Verify scalar fields survive the round-trip
        let stats = deserialized.file_stats().unwrap();
        assert_eq!(stats.table_size_bytes(), 1024 * 1024);
        assert_eq!(stats.num_files(), 10);

        // Verify all set transactions
        let txns = deserialized.set_transaction_state.expect_complete();
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
    fn crc_json_with_counts(
        table_size_bytes: i64,
        num_files: i64,
        num_metadata: i64,
        num_protocol: i64,
    ) -> String {
        format!(
            r#"{{
                "tableSizeBytes": {table_size_bytes},
                "numFiles": {num_files},
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
        let json = crc_json_with_counts(0, 0, m, p);
        let err = Crc::try_from_json_bytes(json.as_bytes(), 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(field),
            "expected error to mention {field} for value {bad}, got: {err}"
        );
    }

    #[rstest]
    #[case::num_files("numFiles", 0, -1)]
    #[case::table_size_bytes("tableSizeBytes", -1, 0)]
    fn de_negative_file_stat_is_rejected(
        #[case] field: &str,
        #[case] table_size_bytes: i64,
        #[case] num_files: i64,
    ) {
        let json = crc_json_with_counts(table_size_bytes, num_files, 1, 1);
        let err = Crc::try_from_json_bytes(json.as_bytes(), 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(field),
            "expected error to mention {field}: {err}"
        );
    }

    #[rstest]
    #[case::file_count("fileCounts", vec![-1, 0], vec![0, 0])]
    #[case::total_bytes("totalBytes", vec![0, 0], vec![0, -1])]
    fn de_negative_file_size_histogram_stat_is_rejected(
        #[case] field: &str,
        #[case] file_counts: Vec<i64>,
        #[case] total_bytes: Vec<i64>,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc["fileSizeHistogram"] = serde_json::json!({
            "sortedBinBoundaries": [0, 1],
            "fileCounts": file_counts,
            "totalBytes": total_bytes,
        });
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(
            error.to_string().contains("negative counts or bytes"),
            "expected invalid {field}, got {error}"
        );
        assert!(
            !error.to_string().contains("kernel bug"),
            "malformed external data must not be reported as a kernel bug: {error}"
        );
    }

    #[rstest]
    #[case::transactions(
        "setTransactions",
        serde_json::json!([
            {"appId": "orders", "version": 1},
            {"appId": "orders", "version": 2}
        ]),
        "duplicate transaction application id"
    )]
    #[case::domains(
        "domainMetadata",
        serde_json::json!([
            {"domain": "example", "configuration": "{}", "removed": false},
            {"domain": "example", "configuration": "{}", "removed": false}
        ]),
        "duplicate domain"
    )]
    fn de_duplicate_complete_state_is_rejected(
        #[case] field: &str,
        #[case] value: serde_json::Value,
        #[case] message: &str,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc[field] = value;
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn de_complete_domain_tombstone_is_rejected() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc["domainMetadata"] = serde_json::json!([
            {"domain": "example", "configuration": "{}", "removed": true}
        ]);
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("tombstone"), "{error}");
    }

    #[rstest]
    #[case::file_count(vec![0, 2], vec![0, 5], "file count")]
    #[case::total_bytes(vec![1, 0], vec![0, 4], "total bytes")]
    #[case::file_count_overflow(vec![i64::MAX, 1], vec![5, 0], "overflow")]
    fn de_file_size_histogram_aggregate_is_validated(
        #[case] file_counts: Vec<i64>,
        #[case] total_bytes: Vec<i64>,
        #[case] message: &str,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(5, 1, 1, 1)).unwrap();
        crc["fileSizeHistogram"] = serde_json::json!({
            "sortedBinBoundaries": [0, 10],
            "fileCounts": file_counts,
            "totalBytes": total_bytes,
        });
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[rstest]
    #[case::too_few(vec![0; 9], "invalid length")]
    #[case::too_many(vec![0; 11], "trailing characters")]
    #[case::negative(vec![0, 0, -1, 0, 0, 0, 0, 0, 0, 0], "negative file count")]
    fn de_deleted_record_histogram_shape_is_validated(
        #[case] bins: Vec<i64>,
        #[case] message: &str,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc["deletedRecordCountsHistogramOpt"] = serde_json::json!({"deletedRecordCounts": bins});
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[rstest]
    #[case::deleted_records("numDeletedRecordsOpt")]
    #[case::deletion_vectors("numDeletionVectorsOpt")]
    fn de_negative_deletion_total_is_rejected(#[case] field: &str) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc[field] = (-1).into();
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(field), "{error}");
    }

    #[rstest]
    #[case::deleted_records("numDeletedRecordsOpt", Some(0), None)]
    #[case::deletion_vectors("numDeletionVectorsOpt", None, Some(0))]
    fn de_unpaired_deletion_totals_are_accepted(
        #[case] field: &str,
        #[case] expected_deleted_records: Option<i64>,
        #[case] expected_deletion_vectors: Option<i64>,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc[field] = 0.into();

        let crc = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
        assert_eq!(crc.num_deleted_records_opt, expected_deleted_records);
        assert_eq!(crc.num_deletion_vectors_opt, expected_deletion_vectors);
    }

    #[test]
    fn de_deleted_record_histogram_sum_overflow_is_rejected() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, i64::MAX, 1, 1)).unwrap();
        crc["deletedRecordCountsHistogramOpt"] = serde_json::json!({
            "deletedRecordCounts": [i64::MAX, 1, 0, 0, 0, 0, 0, 0, 0, 0]
        });
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("overflow"), "{error}");
    }

    #[test]
    fn de_deleted_record_histogram_file_count_mismatch_is_rejected() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 2, 1, 1)).unwrap();
        crc["deletedRecordCountsHistogramOpt"] = serde_json::json!({
            "deletedRecordCounts": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        });
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("file count"), "{error}");
    }

    #[rstest]
    #[case::file_count("numFiles", 2, 5, None, None)]
    #[case::table_size("tableSizeBytes", 1, 6, None, None)]
    #[case::deleted_records("numDeletedRecordsOpt", 1, 5, Some(1), Some(0))]
    #[case::deletion_vectors("numDeletionVectorsOpt", 1, 5, Some(0), Some(1))]
    fn de_all_files_aggregates_are_validated(
        #[case] message: &str,
        #[case] num_files: i64,
        #[case] table_size: i64,
        #[case] num_deleted_records: Option<i64>,
        #[case] num_deletion_vectors: Option<i64>,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(table_size, num_files, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet",
            "partitionValues": {},
            "size": 5,
            "modificationTime": 0,
            "dataChange": false
        }]);
        if let Some(value) = num_deleted_records {
            crc["numDeletedRecordsOpt"] = value.into();
        }
        if let Some(value) = num_deletion_vectors {
            crc["numDeletionVectorsOpt"] = value.into();
        }
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn de_all_files_rejects_duplicate_paths() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(10, 2, 1, 1)).unwrap();
        let add = serde_json::json!({
            "path": "part.parquet", "partitionValues": {}, "size": 5,
            "modificationTime": 0, "dataChange": false
        });
        crc["allFiles"] = serde_json::json!([add.clone(), add]);
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("duplicate path"), "{error}");
    }

    #[rstest]
    #[case::duplicate(
        vec![
            Add { path: "part.parquet".to_string(), size: 1, ..Default::default() },
            Add { path: "part.parquet".to_string(), size: 1, ..Default::default() },
        ],
        "duplicate path"
    )]
    #[case::negative_size(
        vec![Add { path: "part.parquet".to_string(), size: -1, ..Default::default() }],
        "negative file size"
    )]
    fn all_files_intrinsic_validation_does_not_require_complete_file_stats(
        #[case] all_files: Vec<Add>,
        #[case] message: &str,
    ) {
        let error = Crc::try_from_parts(
            0,
            Default::default(),
            valid_protocol(),
            FileStatsState::Indeterminate,
            None,
            Default::default(),
            Default::default(),
            None,
            Some(all_files),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn de_all_files_file_size_histogram_bins_are_validated() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(5, 1, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet", "partitionValues": {}, "size": 5,
            "modificationTime": 0, "dataChange": false
        }]);
        crc["fileSizeHistogram"] = serde_json::json!({
            "sortedBinBoundaries": [0, 10], "fileCounts": [0, 1], "totalBytes": [0, 5]
        });
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("bins do not match"), "{error}");
    }

    #[test]
    fn de_all_files_matching_file_size_histogram_is_accepted() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(5, 1, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet", "partitionValues": {}, "size": 5,
            "modificationTime": 0, "dataChange": false
        }]);
        crc["fileSizeHistogram"] = serde_json::json!({
            "sortedBinBoundaries": [0, 10], "fileCounts": [1, 0], "totalBytes": [5, 0]
        });
        Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
    }

    #[test]
    fn de_all_files_drops_null_partition_values() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(5, 1, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet",
            "partitionValues": {"null_part": null, "value_part": "x"},
            "size": 5,
            "modificationTime": 0,
            "dataChange": false
        }]);

        let crc = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
        assert_eq!(
            crc.all_files.unwrap()[0].partition_values,
            HashMap::from([("value_part".to_string(), "x".to_string())])
        );
    }

    #[rstest]
    #[case::absent(None, false)]
    #[case::present_empty(Some(serde_json::json!([])), true)]
    fn de_all_files_preserves_absent_vs_empty(
        #[case] value: Option<serde_json::Value>,
        #[case] present: bool,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        if let Some(value) = value {
            crc["allFiles"] = value;
        }

        let crc = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
        assert_eq!(crc.all_files.is_some(), present);
    }

    #[rstest]
    #[case::inline_offset(
        serde_json::json!({
            "storageType": "i", "pathOrInlineDv": "", "offset": 0,
            "sizeInBytes": 0, "cardinality": 0
        }),
        "inline deletion vectors must not carry an offset"
    )]
    #[case::negative_size(
        serde_json::json!({
            "storageType": "i", "pathOrInlineDv": "", "sizeInBytes": -1,
            "cardinality": 0
        }),
        "size_in_bytes must be non-negative"
    )]
    #[case::negative_cardinality(
        serde_json::json!({
            "storageType": "i", "pathOrInlineDv": "", "sizeInBytes": 0,
            "cardinality": -1
        }),
        "cardinality must be non-negative"
    )]
    #[case::unknown_storage_type(
        serde_json::json!({
            "storageType": "x", "pathOrInlineDv": "", "sizeInBytes": 0,
            "cardinality": 0
        }),
        "Unsupported deletion vector format option"
    )]
    fn de_all_files_validates_deletion_vector(
        #[case] deletion_vector: serde_json::Value,
        #[case] message: &str,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(5, 1, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet", "partitionValues": {}, "size": 5,
            "modificationTime": 0, "dataChange": false,
            "deletionVector": deletion_vector
        }]);

        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn ser_does_not_emit_newly_read_extended_fields() {
        let crc = Crc {
            protocol: valid_protocol(),
            file_stats_state: FileStatsState::Complete(FileStats::try_new(1, 5, None).unwrap()),
            txn_id: Some("txn".to_string()),
            all_files: Some(vec![Add {
                path: "part.parquet".to_string(),
                size: 5,
                ..Default::default()
            }]),
            num_deleted_records_opt: Some(0),
            num_deletion_vectors_opt: Some(0),
            deleted_record_counts_histogram_opt: Some(
                DeletedRecordCountsHistogram::try_new(vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            ),
            ..Default::default()
        };

        let json = serde_json::to_value(crc).unwrap();
        for field in [
            "txnId",
            "allFiles",
            "numDeletedRecordsOpt",
            "numDeletionVectorsOpt",
            "deletedRecordCountsHistogramOpt",
        ] {
            assert!(
                json.get(field).is_none(),
                "unexpected field {field}: {json}"
            );
        }
    }

    #[rstest]
    #[case::negative(vec![-1], "negative file size")]
    #[case::overflow(vec![i64::MAX, 1], "allFiles table size overflow")]
    fn de_invalid_all_file_sizes_are_rejected(#[case] sizes: Vec<i64>, #[case] message: &str) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, sizes.len() as i64, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::Value::Array(
            sizes
                .into_iter()
                .enumerate()
                .map(|(index, size)| {
                    serde_json::json!({
                        "path": format!("part-{index}.parquet"), "partitionValues": {},
                        "size": size, "modificationTime": 0, "dataChange": false
                    })
                })
                .collect(),
        );
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    #[test]
    fn de_all_files_deleted_record_total_overflow_is_rejected() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(2, 2, 1, 1)).unwrap();
        crc["allFiles"] = serde_json::json!([
            {
                "path": "part-0.parquet", "partitionValues": {}, "size": 1,
                "modificationTime": 0, "dataChange": false,
                "deletionVector": {
                    "storageType": "i", "pathOrInlineDv": "", "sizeInBytes": 0,
                    "cardinality": i64::MAX
                }
            },
            {
                "path": "part-1.parquet", "partitionValues": {}, "size": 1,
                "modificationTime": 0, "dataChange": false,
                "deletionVector": {
                    "storageType": "i", "pathOrInlineDv": "", "sizeInBytes": 0,
                    "cardinality": 1
                }
            }
        ]);

        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(error.to_string().contains("overflow"), "{error}");
    }

    #[rstest]
    #[case::bin_one(9, 1)]
    #[case::bin_two(10, 2)]
    #[case::bin_eight(2_147_483_646, 8)]
    #[case::bin_nine(2_147_483_647, 9)]
    fn de_deleted_record_histogram_uses_protocol_bins(
        #[case] cardinality: i64,
        #[case] bin: usize,
    ) {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 1, 1, 1)).unwrap();
        crc["numDeletedRecordsOpt"] = cardinality.into();
        crc["numDeletionVectorsOpt"] = 1.into();
        crc["allFiles"] = serde_json::json!([{
            "path": "part.parquet", "partitionValues": {}, "size": 0,
            "modificationTime": 0, "dataChange": false,
            "deletionVector": {
                "storageType": "i", "pathOrInlineDv": "", "sizeInBytes": 0,
                "cardinality": cardinality
            }
        }]);
        let mut bins = vec![0_i64; 10];
        bins[bin] = 1;
        crc["deletedRecordCountsHistogramOpt"] = serde_json::json!({"deletedRecordCounts": bins});
        Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
    }

    #[test]
    fn de_ict_enabled_metadata_requires_crc_timestamp() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc["metadata"]["configuration"]["delta.enableInCommitTimestamps"] =
            serde_json::json!("true");
        let error = Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap_err();
        assert!(
            error.to_string().contains("inCommitTimestampOpt"),
            "{error}"
        );
    }

    #[test]
    fn de_ict_enablement_value_is_case_sensitive() {
        let mut crc: serde_json::Value =
            serde_json::from_str(&crc_json_with_counts(0, 0, 1, 1)).unwrap();
        crc["metadata"]["configuration"][ENABLE_IN_COMMIT_TIMESTAMPS] = serde_json::json!("True");
        Crc::try_from_json_bytes(crc.to_string().as_bytes(), 0).unwrap();
    }

    // ===== protocol validation on the CRC deserialization path =====

    /// Minimal CRC JSON whose `protocol` is the supplied fragment. Proves CRC deserialization
    /// runs the protocol through `Protocol::try_new` instead of building an unchecked one.
    fn crc_json_with_protocol(protocol: &str) -> String {
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
                "protocol": {protocol}
            }}"#
        )
    }

    #[test]
    fn deserialize_crc_accepts_orphaned_column_mapping() {
        let json = crc_json_with_protocol(
            r#"{"minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": [], "writerFeatures": ["columnMapping"]}"#,
        );
        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();
        assert_eq!(crc.protocol.min_reader_version(), 3);
    }

    #[test]
    fn deserialize_crc_rejects_orphaned_non_legacy_reader_writer_feature() {
        let json = crc_json_with_protocol(
            r#"{"minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": [], "writerFeatures": ["columnMapping", "deletionVectors"]}"#,
        );
        assert!(Crc::try_from_json_bytes(json.as_bytes(), 0).is_err());
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
                "tableSizeBytes": 510,
                "numFiles": 6,
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
        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();
        assert!(crc.file_stats().unwrap().file_size_histogram().is_some());
    }

    #[rstest]
    #[case::spec_name("fileSizeHistogram")]
    #[case::legacy_name("histogramOpt")]
    fn de_null_file_size_histogram_deserializes_to_none(#[case] field_name: &str) {
        let json = crc_json_with_histogram(field_name, "null");
        let crc = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap();
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
        assert!(Crc::try_from_json_bytes(json.as_bytes(), 0).is_err());
    }

    /// CRC files written by kernel always use the spec-correct field name `fileSizeHistogram`,
    /// even when the input JSON used the legacy `histogramOpt` alias. This matches kernel-java
    /// (delta-io/delta#6281) and ensures kernel-written CRCs are protocol-compliant.
    #[test]
    fn ser_uses_spec_field_name_after_deserializing_legacy_alias() {
        let legacy_json = crc_json_with_histogram(
            "histogramOpt",
            r#"{"sortedBinBoundaries": [0, 100], "fileCounts": [6, 0], "totalBytes": [510, 0]}"#,
        );
        let crc = Crc::try_from_json_bytes(legacy_json.as_bytes(), 0).unwrap();

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
        let err = Crc::try_from_json_bytes(json.as_bytes(), 0).unwrap_err();
        assert!(
            err.to_string().contains("duplicate field"),
            "expected duplicate-field error, got: {err}"
        );
    }
}
