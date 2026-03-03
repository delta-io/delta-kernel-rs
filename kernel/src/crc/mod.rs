//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

mod lazy;
mod reader;
mod writer;

pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;
#[allow(unused)]
pub(crate) use writer::try_write_crc_file;

use serde::{Deserialize, Serialize};

use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};

/// Parsed content of a CRC (version checksum) file.
///
/// A CRC file must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema of this struct.
// Deserialized directly from JSON via serde. See `reader::try_read_crc_file`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Crc {
    // ===== Required fields =====
    /// Total size of the table in bytes, calculated as the sum of the `size` field of all live
    /// [`Add`] actions.
    pub(crate) table_size_bytes: i64,
    /// Number of live [`Add`] actions in this table version after action reconciliation.
    pub(crate) num_files: i64,
    /// Number of [`Metadata`] actions. Must be 1.
    pub(crate) num_metadata: i64,
    /// Number of [`Protocol`] actions. Must be 1.
    pub(crate) num_protocol: i64,
    /// The table [`Metadata`] at this version.
    pub(crate) metadata: Metadata,
    /// The table [`Protocol`] at this version.
    pub(crate) protocol: Protocol,

    // ===== Optional fields =====
    /// A unique identifier for the transaction that produced this commit.
    #[serde(skip)]
    pub(crate) txn_id: Option<String>,
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub(crate) in_commit_timestamp_opt: Option<i64>,
    /// Live transaction identifier ([`SetTransaction`]) actions at this version.
    #[serde(skip)]
    pub(crate) set_transactions: Option<Vec<SetTransaction>>,
    /// Live [`DomainMetadata`] actions at this version, excluding tombstones.
    pub(crate) domain_metadata: Option<Vec<DomainMetadata>>,
    /// Size distribution information of files remaining after action reconciliation.
    #[serde(skip)]
    pub(crate) file_size_histogram: Option<FileSizeHistogram>,
    /// All live [`Add`] file actions at this version.
    #[serde(skip)]
    pub(crate) all_files: Option<Vec<Add>>,
    /// Number of records deleted through Deletion Vectors in this table version.
    #[serde(skip)]
    pub(crate) num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    #[serde(skip)]
    pub(crate) num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files. See this section for more details.
    #[serde(skip)]
    pub(crate) deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
}

/// The [FileSizeHistogram] object represents a histogram tracking file counts and total bytes
/// across different size ranges.
///
/// [FileSizeHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileSizeHistogram {
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
pub(crate) struct DeletedRecordCountsHistogram {
    /// Array of size 10 where each element represents the count of files falling into a specific
    /// deletion count range.
    pub(crate) deleted_record_counts: Vec<i64>,
}
