//! CRC (version checksum) file

use super::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};
use delta_kernel_derive::Schema;

/// Though technically not an action, we include the CRC (version checksum) file here. A [CRC file]
/// must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema of this [`Crc`] struct.
///
/// [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
#[allow(unused)] // TODO: remove after we complete CRC support
#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub(crate) struct Crc {
    /// A unique identifier for the transaction that produced this commit.
    pub(crate) txn_id: Option<String>,
    /// Total size of the table in bytes, calculated as the sum of the `size` field of all live
    /// [`Add`] actions.
    pub(crate) table_size_bytes: i64,
    /// Number of live [`Add`] actions in this table version after action reconciliation.
    pub(crate) num_files: i64,
    /// Number of [`Metadata`] actions. Must be 1.
    pub(crate) num_metadata: i64,
    /// Number of [`Protocol`] actions. Must be 1.
    pub(crate) num_protocol: i64,
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub(crate) in_commit_timestamp_opt: Option<i64>,
    /// Live transaction identifier ([`SetTransaction`]) actions at this version.
    pub(crate) set_transactions: Option<Vec<SetTransaction>>,
    /// Live [`DomainMetadata`] actions at this version, excluding tombstones.
    pub(crate) domain_metadata: Option<Vec<DomainMetadata>>,
    /// The table [`Metadata`] at this version.
    pub(crate) metadata: Metadata,
    /// The table [`Protocol`] at this version.
    pub(crate) protocol: Protocol,
    /// Size distribution information of files remaining after action reconciliation.
    pub(crate) file_size_histogram: Option<FileSizeHistogram>,
    /// All live [`Add`] file actions at this version.
    pub(crate) all_files: Option<Vec<Add>>,
    /// Number of records deleted through Deletion Vectors in this table version.
    pub(crate) num_deleted_records_opt: Option<i64>,
    /// Number of Deletion Vectors active in this table version.
    pub(crate) num_deletion_vectors_opt: Option<i64>,
    /// Distribution of deleted record counts across files. See this section for more details.
    pub(crate) deleted_record_counts_histogram_opt: Option<DeletedRecordCountsHistogram>,
}

/// The [FileSizeHistogram] object represents a histogram tracking file counts and total bytes
/// across different size ranges.
///
/// [FileSizeHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq, Schema)]
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
/// Bin 9: [2147483647, ∞) (files with 2,147,483,647 or more deleted records)
///
/// [DeletedRecordCountsHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deleted-record-counts-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub(crate) struct DeletedRecordCountsHistogram {
    /// Array of size 10 where each element represents the count of files falling into a specific
    /// deletion count range.
    pub(crate) deleted_record_counts: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::schemas::{ToDataType as _, ToSchema as _};
    use crate::schema::{ArrayType, DataType, StructField, StructType};

    #[test]
    fn test_file_size_histogram_schema() {
        let schema = FileSizeHistogram::to_schema();
        let expected = StructType::new([
            StructField::not_null("sortedBinBoundaries", ArrayType::new(DataType::LONG, false)),
            StructField::not_null("fileCounts", ArrayType::new(DataType::LONG, false)),
            StructField::not_null("totalBytes", ArrayType::new(DataType::LONG, false)),
        ]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_deleted_record_counts_histogram_schema() {
        let schema = DeletedRecordCountsHistogram::to_schema();
        let expected = StructType::new([StructField::not_null(
            "deletedRecordCounts",
            ArrayType::new(DataType::LONG, false),
        )]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_crc_schema() {
        let schema = Crc::to_schema();
        let expected = StructType::new([
            StructField::nullable("txnId", DataType::STRING),
            StructField::not_null("tableSizeBytes", DataType::LONG),
            StructField::not_null("numFiles", DataType::LONG),
            StructField::not_null("numMetadata", DataType::LONG),
            StructField::not_null("numProtocol", DataType::LONG),
            StructField::nullable("inCommitTimestampOpt", DataType::LONG),
            StructField::nullable(
                "setTransactions",
                ArrayType::new(SetTransaction::to_data_type(), false),
            ),
            StructField::nullable(
                "domainMetadata",
                ArrayType::new(DomainMetadata::to_data_type(), false),
            ),
            StructField::not_null("metadata", Metadata::to_data_type()),
            StructField::not_null("protocol", Protocol::to_data_type()),
            StructField::nullable("fileSizeHistogram", FileSizeHistogram::to_data_type()),
            StructField::nullable("allFiles", ArrayType::new(Add::to_data_type(), false)),
            StructField::nullable("numDeletedRecordsOpt", DataType::LONG),
            StructField::nullable("numDeletionVectorsOpt", DataType::LONG),
            StructField::nullable(
                "deletedRecordCountsHistogramOpt",
                DeletedRecordCountsHistogram::to_data_type(),
            ),
        ]);
        assert_eq!(schema, expected);
    }
}
