//! CRC (version checksum) file
use std::sync::LazyLock;

use super::visitors::{visit_metadata_at, visit_protocol_at};
use super::{Add, DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::actions::PROTOCOL_NAME;
use crate::engine_data::GetData;
use crate::schema::ToSchema as _;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Error, RowVisitor};
use delta_kernel_derive::ToSchema;

/// Though technically not an action, we include the CRC (version checksum) file here. A [CRC file]
/// must:
/// 1. Be named `{version}.crc` with version zero-padded to 20 digits: `00000000000000000001.crc`
/// 2. Be stored directly in the _delta_log directory alongside Delta log files
/// 3. Contain exactly one JSON object with the schema of this [`Crc`] struct.
///
/// [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file
#[allow(unused)] // TODO: remove after we complete CRC support
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
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

/// For now we just define a visitor for Protocol and Metadata in CRC files since (for now) that's
/// the only optimization we implement. Since CRC files can contain lots of other data, we have a
/// specific visitor for only Protocol/Metadata here.
#[allow(unused)] // TODO: remove after we read CRCs
#[derive(Debug, Default)]
pub(crate) struct CrcProtocolMetadataVisitor {
    pub(crate) protocol: Protocol,
    pub(crate) metadata: Metadata,
}

impl RowVisitor for CrcProtocolMetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            // annoyingly, the 'metadata' in CRC is under the name 'metadata', not 'metaData'
            let mut cols = Metadata::to_schema().leaves("metadata");
            cols.extend(Protocol::to_schema().leaves(PROTOCOL_NAME));
            cols
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // getters = sum of Protocol + Metadata
        require!(
            getters.len() == 13,
            Error::InternalError(format!(
                "Wrong number of CrcProtocolMetadataVisitor getters: {}",
                getters.len()
            ))
        );
        if row_count != 1 {
            return Err(Error::InternalError(format!(
                "Expected 1 row for CRC file, but got {row_count}",
            )));
        }

        self.metadata = visit_metadata_at(0, &getters[..9])?
            .ok_or(Error::generic("Metadata not found in CRC file"))?;
        self.protocol = visit_protocol_at(0, &getters[9..])?
            .ok_or(Error::generic("Protocol not found in CRC file"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::arrow::array::StringArray;

    use crate::actions::{Format, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::schema::derive_macro_utils::ToDataType as _;
    use crate::schema::{ArrayType, DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::string_array_to_engine_data;
    use crate::Engine;

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

    #[test]
    fn test_crc_protocol_metadata_visitor() {
        // create CRC to visit
        let crc_json = serde_json::json!({
            "tableSizeBytes": 100,
            "numFiles": 10,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": "testId",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "none"
                },
                "createdTime": 1677811175
            },
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": ["columnMapping"]
            }
        });

        // convert JSON -> StringArray -> (string)EngineData -> actual CRC EngineData
        let json_string = crc_json.to_string();
        let json_strings = StringArray::from(vec![json_string.as_str()]);
        let engine_data = string_array_to_engine_data(json_strings);
        let engine = SyncEngine::new();
        let json_handler = engine.json_handler();
        let output_schema = Arc::new(Crc::to_schema());
        let data = json_handler.parse_json(engine_data, output_schema).unwrap();

        // run the visitor
        let mut visitor = CrcProtocolMetadataVisitor::default();
        visitor.visit_rows_of(data.as_ref()).unwrap();

        let expected_protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![TableFeature::ColumnMapping]),
            writer_features: Some(vec![TableFeature::ColumnMapping]),
        };
        let expected_metadata = Metadata {
            id: "testId".to_string(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".to_string(),
                options: std::collections::HashMap::new(),
            },
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            partition_columns: vec![],
            created_time: Some(1677811175),
            configuration: std::collections::HashMap::from([
                ("delta.columnMapping.mode".to_string(), "none".to_string()),
            ]),
        };

        assert_eq!(visitor.protocol, expected_protocol);
        assert_eq!(visitor.metadata, expected_metadata);
    }
}
