//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

mod lazy;
mod reader;
pub(crate) mod writer;
#[cfg(test)]
mod writer_tests;

pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;

use std::sync::LazyLock;

use crate::actions::visitors::{visit_metadata_at, visit_protocol_at};
use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction, PROTOCOL_NAME};
use crate::engine_data::{GetData, TypedGetData};
use crate::schema::ToSchema as _;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};
use delta_kernel_derive::ToSchema;

/// Parsed content of a CRC (version checksum) file.
///
/// A CRC file must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema of this struct.
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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
    pub(crate) txn_id: Option<String>,
    /// The in-commit timestamp of this version. Present iff In-Commit Timestamps are enabled.
    pub(crate) in_commit_timestamp_opt: Option<i64>,
    /// Live transaction identifier ([`SetTransaction`]) actions at this version.
    pub(crate) set_transactions: Option<Vec<SetTransaction>>,
    /// Live [`DomainMetadata`] actions at this version, excluding tombstones.
    pub(crate) domain_metadata: Option<Vec<DomainMetadata>>,
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
/// TODO: This struct is defined for schema generation but not yet parsed from CRC files.
///
/// [FileSizeHistogram]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-size-histogram-schema
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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
/// TODO: This struct is defined for schema generation but not yet parsed from CRC files.
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
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
pub(crate) struct DeletedRecordCountsHistogram {
    /// Array of size 10 where each element represents the count of files falling into a specific
    /// deletion count range.
    pub(crate) deleted_record_counts: Vec<i64>,
}

/// Visitor for extracting data from CRC files.
///
/// This visitor extracts Protocol, Metadata, and additional fields needed for CRC optimizations
/// (in-commit timestamp, table statistics). The visitor builds a [`Crc`] directly during visitation.
#[derive(Debug, Default)]
pub(crate) struct CrcVisitor {
    pub(crate) crc: Option<Crc>,
}

impl CrcVisitor {
    pub(crate) fn into_crc(self) -> DeltaResult<Crc> {
        self.crc
            .ok_or_else(|| Error::generic("CRC file was not visited"))
    }
}

/// Number of leaf columns for Metadata in the visitor schema.
const METADATA_LEAF_COUNT: usize = 9;
/// Number of leaf columns for Protocol in the visitor schema.
const PROTOCOL_LEAF_COUNT: usize = 4;

impl RowVisitor for CrcVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let mut cols = ColumnNamesAndTypes::default();
            cols.extend(
                (
                    vec![ColumnName::new(["tableSizeBytes"])],
                    vec![DataType::LONG],
                )
                    .into(),
            );
            cols.extend((vec![ColumnName::new(["numFiles"])], vec![DataType::LONG]).into());
            // num_metadata: hardcoded to 1
            // num_protocol: hardcoded to 1
            // NOTE: CRC uses 'metadata' not 'metaData' like in actions
            cols.extend(Metadata::to_schema().leaves("metadata"));
            cols.extend(Protocol::to_schema().leaves(PROTOCOL_NAME));
            // txn_id: not extracted yet
            cols.extend(
                (
                    vec![ColumnName::new(["inCommitTimestampOpt"])],
                    vec![DataType::LONG],
                )
                    .into(),
            );
            cols
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Getters follow Crc struct order:
        // [0]: tableSizeBytes
        // [1]: numFiles
        // [2..11]: metadata (9 leaf columns)
        // [11..15]: protocol (4 leaf columns)
        // [15]: inCommitTimestampOpt
        const EXPECTED_GETTERS: usize = 2 + METADATA_LEAF_COUNT + PROTOCOL_LEAF_COUNT + 1;
        require!(
            getters.len() == EXPECTED_GETTERS,
            Error::InternalError(format!(
                "Wrong number of CrcVisitor getters: {} (expected {})",
                getters.len(),
                EXPECTED_GETTERS
            ))
        );
        if row_count != 1 {
            return Err(Error::InternalError(format!(
                "Expected 1 row for CRC file, but got {row_count}",
            )));
        }

        let table_size_bytes: i64 = getters[0].get(0, "crc.tableSizeBytes")?;
        let num_files: i64 = getters[1].get(0, "crc.numFiles")?;
        let metadata_end = 2 + METADATA_LEAF_COUNT;
        let protocol_end = metadata_end + PROTOCOL_LEAF_COUNT;
        let metadata = visit_metadata_at(0, &getters[2..metadata_end])?
            .ok_or_else(|| Error::generic("Metadata not found in CRC file"))?;
        let protocol = visit_protocol_at(0, &getters[metadata_end..protocol_end])?
            .ok_or_else(|| Error::generic("Protocol not found in CRC file"))?;
        let in_commit_timestamp_opt: Option<i64> =
            getters[protocol_end].get_opt(0, "crc.inCommitTimestampOpt")?;

        self.crc = Some(Crc {
            table_size_bytes,
            num_files,
            num_metadata: 1, // Always 1 per protocol
            num_protocol: 1, // Always 1 per protocol
            metadata,
            protocol,
            txn_id: None, // TODO: extract this
            in_commit_timestamp_opt,
            set_transactions: None,                    // TODO: extract this
            domain_metadata: None,                     // TODO: extract this
            file_size_histogram: None,                 // TODO: extract this
            all_files: None,                           // TODO: extract this
            num_deleted_records_opt: None,             // TODO: extract this
            num_deletion_vectors_opt: None,            // TODO: extract this
            deleted_record_counts_histogram_opt: None, // TODO: extract this
        });

        Ok(())
    }
}

// See reader::tests::test_read_crc_file for the e2e test that tests CrcVisitor.
#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::derive_macro_utils::ToDataType as _;
    use crate::schema::{ArrayType, DataType, StructField, StructType};

    #[test]
    fn test_file_size_histogram_schema() {
        let schema = FileSizeHistogram::to_schema();
        let expected = StructType::new_unchecked([
            StructField::not_null("sortedBinBoundaries", ArrayType::new(DataType::LONG, false)),
            StructField::not_null("fileCounts", ArrayType::new(DataType::LONG, false)),
            StructField::not_null("totalBytes", ArrayType::new(DataType::LONG, false)),
        ]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_deleted_record_counts_histogram_schema() {
        let schema = DeletedRecordCountsHistogram::to_schema();
        let expected = StructType::new_unchecked([StructField::not_null(
            "deletedRecordCounts",
            ArrayType::new(DataType::LONG, false),
        )]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_crc_schema() {
        let schema = Crc::to_schema();
        let expected = StructType::new_unchecked([
            // Required fields
            StructField::not_null("tableSizeBytes", DataType::LONG),
            StructField::not_null("numFiles", DataType::LONG),
            StructField::not_null("numMetadata", DataType::LONG),
            StructField::not_null("numProtocol", DataType::LONG),
            StructField::not_null("metadata", Metadata::to_data_type()),
            StructField::not_null("protocol", Protocol::to_data_type()),
            // Optional fields
            StructField::nullable("txnId", DataType::STRING),
            StructField::nullable("inCommitTimestampOpt", DataType::LONG),
            StructField::nullable(
                "setTransactions",
                ArrayType::new(SetTransaction::to_data_type(), false),
            ),
            StructField::nullable(
                "domainMetadata",
                ArrayType::new(DomainMetadata::to_data_type(), false),
            ),
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
