//! CRC (version checksum) file support.
//!
//! A [CRC file] contains a snapshot of table state at a specific version, which can be used to
//! optimize log replay operations like reading Protocol/Metadata, domain metadata, and ICT.
//!
//! [CRC file]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#version-checksum-file

mod lazy;
mod reader;
pub(crate) mod writer;

pub(crate) use lazy::{CrcLoadResult, LazyCrc};
pub(crate) use reader::try_read_crc_file;

use std::sync::LazyLock;

use crate::actions::visitors::{visit_metadata_at, visit_protocol_at};
use crate::actions::{Add, DomainMetadata, Metadata, Protocol, SetTransaction, PROTOCOL_NAME};
use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::{ArrayData, Scalar, StructData};
use crate::schema::derive_macro_utils::ToDataType as _;
use crate::schema::ToSchema as _;
use crate::schema::{ArrayType, ColumnName, ColumnNamesAndTypes, DataType, StructField};
use crate::utils::require;
use crate::{
    DeltaResult, Engine, Error, EvaluationHandlerExtension as _, IntoEngineData, RowVisitor,
};
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

#[allow(dead_code)] // Methods used in Phase 5 (post-commit CRC injection)
impl Crc {
    /// Compute a new CRC from a previous CRC + commit stats delta (SIMPLE path).
    ///
    /// This is an O(1) operation that combines the previous version's CRC with the changes
    /// from the current commit to produce the new version's CRC.
    pub(crate) fn compute_post_commit(
        old_crc: &Crc,
        delta: &CommitStatsDelta,
        metadata: Metadata,
        protocol: Protocol,
    ) -> Crc {
        Crc {
            table_size_bytes: old_crc.table_size_bytes + delta.total_add_file_size_bytes
                - delta.total_remove_file_size_bytes,
            num_files: old_crc.num_files + delta.num_add_files - delta.num_remove_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata,
            protocol,
            txn_id: None,
            in_commit_timestamp_opt: delta.in_commit_timestamp,
            set_transactions: None,
            domain_metadata: Some(Self::merge_domain_metadata(
                old_crc.domain_metadata.as_deref(),
                &delta.domain_metadata_actions,
            )),
            // Fields not tracked incrementally
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        }
    }

    /// Compute a CRC for a newly created table (version 0, no previous CRC).
    ///
    /// All values come directly from the commit's stats delta and the new table configuration.
    pub(crate) fn compute_from_create_table(
        delta: &CommitStatsDelta,
        metadata: Metadata,
        protocol: Protocol,
    ) -> Crc {
        // For create-table, filter domain metadata to only keep non-removed entries
        let active_domains: Vec<DomainMetadata> = delta
            .domain_metadata_actions
            .iter()
            .filter(|dm| !dm.is_removed())
            .cloned()
            .collect();

        Crc {
            table_size_bytes: delta.total_add_file_size_bytes,
            num_files: delta.num_add_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata,
            protocol,
            txn_id: None,
            in_commit_timestamp_opt: delta.in_commit_timestamp,
            set_transactions: None,
            domain_metadata: Some(active_domains),
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        }
    }

    /// Merge old domain metadata with new domain metadata actions.
    ///
    /// - Additions (removed=false): insert or replace by domain name
    /// - Removals (removed=true): remove the entry for that domain name
    /// - Result: only active (non-removed) domains
    fn merge_domain_metadata(
        old_domains: Option<&[DomainMetadata]>,
        actions: &[DomainMetadata],
    ) -> Vec<DomainMetadata> {
        use std::collections::HashMap;
        let mut domain_map: HashMap<String, DomainMetadata> = old_domains
            .unwrap_or_default()
            .iter()
            .filter(|dm| !dm.is_removed())
            .map(|dm| (dm.domain().to_owned(), dm.clone()))
            .collect();

        for action in actions {
            if action.is_removed() {
                domain_map.remove(action.domain());
            } else {
                domain_map.insert(action.domain().to_owned(), action.clone());
            }
        }

        domain_map.into_values().collect()
    }
}

/// Convert an `Option<Vec<DomainMetadata>>` to a `Scalar` value for serialization.
///
/// - `None` produces a null array scalar
/// - `Some(vec)` produces a `Scalar::Array` of `Scalar::Struct` elements
fn domain_metadata_to_scalar(dm_opt: Option<Vec<DomainMetadata>>) -> DeltaResult<Scalar> {
    let dm_data_type = DomainMetadata::to_data_type();
    let array_type = ArrayType::new(dm_data_type.clone(), false);
    match dm_opt {
        None => Ok(Scalar::Null(DataType::Array(Box::new(array_type)))),
        Some(domains) => {
            let DataType::Struct(ref struct_type) = dm_data_type else {
                return Err(Error::internal_error(
                    "DomainMetadata::to_data_type() should return a Struct",
                ));
            };
            let dm_fields: Vec<StructField> = struct_type.fields().cloned().collect();
            let elements: DeltaResult<Vec<Scalar>> = domains
                .into_iter()
                .map(|dm| {
                    Ok(Scalar::Struct(StructData::try_new(
                        dm_fields.clone(),
                        vec![
                            Scalar::String(dm.domain().to_owned()),
                            Scalar::String(dm.configuration().to_owned()),
                            Scalar::Boolean(dm.is_removed()),
                        ],
                    )?))
                })
                .collect();
            let array_data = ArrayData::try_new(array_type, elements?)?;
            Ok(Scalar::Array(array_data))
        }
    }
}

/// Helper to create a null Scalar for an array-of-struct type.
fn null_array_scalar<T: crate::schema::ToSchema>() -> Scalar {
    Scalar::Null(DataType::Array(Box::new(ArrayType::new(
        T::to_data_type(),
        false,
    ))))
}

/// Helper to create a null Scalar for an array of a primitive type.
fn null_prim_array_scalar(elem_type: DataType) -> Scalar {
    Scalar::Null(DataType::Array(Box::new(ArrayType::new(elem_type, false))))
}

// NOTE: We can't derive IntoEngineData for Crc because it contains nested Metadata and Protocol
// structs whose fields are private. We flatten them into leaf-level scalars using into_leaf_scalars.
impl IntoEngineData for Crc {
    fn into_engine_data(
        self,
        schema: crate::schema::SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn crate::EngineData>> {
        let domain_metadata_scalar = domain_metadata_to_scalar(self.domain_metadata)?;

        // Flatten all leaf values in schema traversal order. Struct fields are recursed into
        // (providing leaf scalars for each sub-field), while Array/Map fields are single leaves.
        let mut values: Vec<Scalar> = Vec::with_capacity(30);

        // Required scalars
        values.push(self.table_size_bytes.into());
        values.push(self.num_files.into());
        values.push(self.num_metadata.into());
        values.push(self.num_protocol.into());

        // Metadata (9 leaf values: id, name, description, format.provider, format.options,
        // schema_string, partition_columns, created_time, configuration)
        values.extend(self.metadata.into_leaf_scalars()?);

        // Protocol (4 leaf values: min_reader_version, min_writer_version,
        // reader_features, writer_features)
        values.extend(self.protocol.into_leaf_scalars()?);

        // Optional simple scalars
        values.push(self.txn_id.into());
        values.push(self.in_commit_timestamp_opt.into());

        // set_transactions: nullable array of struct (always None for SIMPLE path)
        values.push(null_array_scalar::<SetTransaction>());

        // domain_metadata: nullable array of struct
        values.push(domain_metadata_scalar);

        // file_size_histogram: nullable struct with 3 non-nullable array<long> fields.
        // When None, provide null scalars for each leaf -> all-null triggers null-struct logic.
        match self.file_size_histogram {
            None => {
                values.push(null_prim_array_scalar(DataType::LONG)); // sorted_bin_boundaries
                values.push(null_prim_array_scalar(DataType::LONG)); // file_counts
                values.push(null_prim_array_scalar(DataType::LONG)); // total_bytes
            }
            Some(h) => {
                values.push(h.sorted_bin_boundaries.try_into()?);
                values.push(h.file_counts.try_into()?);
                values.push(h.total_bytes.try_into()?);
            }
        }

        // all_files: nullable array of struct (always None for SIMPLE path)
        values.push(null_array_scalar::<Add>());

        // Optional DV stats
        values.push(self.num_deleted_records_opt.into());
        values.push(self.num_deletion_vectors_opt.into());

        // deleted_record_counts_histogram_opt: nullable struct with 1 non-nullable array<long>
        match self.deleted_record_counts_histogram_opt {
            None => {
                values.push(null_prim_array_scalar(DataType::LONG));
            }
            Some(h) => {
                values.push(h.deleted_record_counts.try_into()?);
            }
        }

        engine.evaluation_handler().create_one(schema, &values)
    }
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

/// Net file/byte changes from a single commit, used for incremental CRC computation.
///
/// Accumulated during `Transaction::commit()` before actions are passed to the committer.
/// Combined with a previous version's CRC to produce the new version's CRC in O(1).
#[derive(Debug, Default)]
#[allow(dead_code)] // Fields read in Phase 3+ (CRC computation)
pub(crate) struct CommitStatsDelta {
    /// Number of add file actions in this commit.
    pub(crate) num_add_files: i64,
    /// Number of remove file actions in this commit.
    pub(crate) num_remove_files: i64,
    /// Total size in bytes of all added files.
    pub(crate) total_add_file_size_bytes: i64,
    /// Total size in bytes of all removed files.
    pub(crate) total_remove_file_size_bytes: i64,
    /// All domain metadata actions in this commit (additions + removals).
    pub(crate) domain_metadata_actions: Vec<DomainMetadata>,
    /// In-commit timestamp for this version (if ICT enabled).
    pub(crate) in_commit_timestamp: Option<i64>,
}

/// Visitor that accumulates file counts and sizes from EngineData batches.
///
/// Used to count files and sum sizes from add_files_metadata (which has `size` at a
/// configurable column index) and remove_files_metadata / dv_matched_files.
pub(crate) struct FileSizeAccumulator {
    /// Index of the `size` column in the data being visited.
    #[allow(dead_code)] // Reserved for future use with different schemas
    size_col_index: usize,
    /// Running count of files visited.
    pub(crate) file_count: i64,
    /// Running sum of file sizes in bytes.
    pub(crate) total_size_bytes: i64,
}

impl FileSizeAccumulator {
    /// Create a new accumulator.
    ///
    /// `size_col_index` is the position of the `size` column among the selected columns.
    pub(crate) fn new(size_col_index: usize) -> Self {
        Self {
            size_col_index,
            file_count: 0,
            total_size_bytes: 0,
        }
    }
}

impl RowVisitor for FileSizeAccumulator {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // We only need the `size` column. The actual column name depends on the schema
        // being visited (add_files_metadata vs scan_row_schema), so we use two static
        // variants and pick the right one based on size_col_index.
        //
        // For add_files_metadata: size is the 3rd field (index 2) -> "size"
        // For scan_row_schema (removes/DV updates): size is the 2nd field (index 1) -> "size"
        //
        // We always select just "size" since that's all we need.
        static ADD_FILES_NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![ColumnName::new(["size"])], vec![DataType::LONG]).into());
        ADD_FILES_NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "FileSizeAccumulator expected 1 getter, got {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            let size: i64 = getters[0].get(i, "size")?;
            self.file_count += 1;
            self.total_size_bytes += size;
        }
        Ok(())
    }
}

// See reader::tests::test_read_crc_file for the e2e test that tests CrcVisitor.
#[cfg(test)]
mod tests {
    use super::*;

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

    // ===== Phase 3: CRC computation tests =====

    fn make_test_crc(table_size_bytes: i64, num_files: i64) -> Crc {
        Crc {
            table_size_bytes,
            num_files,
            num_metadata: 1,
            num_protocol: 1,
            metadata: Metadata::default(),
            protocol: Protocol::default(),
            txn_id: None,
            in_commit_timestamp_opt: None,
            set_transactions: None,
            domain_metadata: None,
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        }
    }

    #[test]
    fn test_compute_post_commit_basic() {
        let old_crc = make_test_crc(1000, 10);
        let delta = CommitStatsDelta {
            num_add_files: 3,
            num_remove_files: 1,
            total_add_file_size_bytes: 300,
            total_remove_file_size_bytes: 100,
            domain_metadata_actions: vec![],
            in_commit_timestamp: Some(12345),
        };
        let new_crc =
            Crc::compute_post_commit(&old_crc, &delta, Metadata::default(), Protocol::default());
        assert_eq!(new_crc.table_size_bytes, 1200); // 1000 + 300 - 100
        assert_eq!(new_crc.num_files, 12); // 10 + 3 - 1
        assert_eq!(new_crc.in_commit_timestamp_opt, Some(12345));
        assert_eq!(new_crc.num_metadata, 1);
        assert_eq!(new_crc.num_protocol, 1);
    }

    #[test]
    fn test_compute_from_create_table() {
        let delta = CommitStatsDelta {
            num_add_files: 5,
            total_add_file_size_bytes: 500,
            domain_metadata_actions: vec![DomainMetadata::new(
                "user.domain".to_string(),
                "config1".to_string(),
            )],
            ..Default::default()
        };
        let crc = Crc::compute_from_create_table(&delta, Metadata::default(), Protocol::default());
        assert_eq!(crc.table_size_bytes, 500);
        assert_eq!(crc.num_files, 5);
        let domains = crc.domain_metadata.unwrap();
        assert_eq!(domains.len(), 1);
        assert_eq!(domains[0].domain(), "user.domain");
    }

    #[test]
    fn test_merge_domain_metadata_additions() {
        let old_domains = vec![DomainMetadata::new(
            "domain.a".to_string(),
            "old_config".to_string(),
        )];
        let actions = vec![
            DomainMetadata::new("domain.b".to_string(), "new_config".to_string()),
            DomainMetadata::new("domain.a".to_string(), "updated_config".to_string()),
        ];
        let merged = Crc::merge_domain_metadata(Some(&old_domains), &actions);
        assert_eq!(merged.len(), 2);
        let map: std::collections::HashMap<_, _> = merged
            .iter()
            .map(|dm| (dm.domain(), dm.configuration()))
            .collect();
        assert_eq!(map["domain.a"], "updated_config");
        assert_eq!(map["domain.b"], "new_config");
    }

    #[test]
    fn test_merge_domain_metadata_removals() {
        let old_domains = vec![
            DomainMetadata::new("domain.a".to_string(), "config_a".to_string()),
            DomainMetadata::new("domain.b".to_string(), "config_b".to_string()),
        ];
        let actions = vec![DomainMetadata::remove(
            "domain.a".to_string(),
            String::new(),
        )];
        let merged = Crc::merge_domain_metadata(Some(&old_domains), &actions);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].domain(), "domain.b");
    }

    #[test]
    fn test_merge_domain_metadata_none_old() {
        let actions = vec![DomainMetadata::new(
            "domain.a".to_string(),
            "config".to_string(),
        )];
        let merged = Crc::merge_domain_metadata(None, &actions);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].domain(), "domain.a");
    }

    // ===== Phase 4: IntoEngineData serialization tests =====

    #[test]
    fn test_crc_into_engine_data_basic() {
        use crate::engine::sync::SyncEngine;
        use crate::IntoEngineData as _;
        let crc = make_test_crc(1000, 10);
        let schema = std::sync::Arc::new(Crc::to_schema());
        let engine = SyncEngine::new();
        let engine_data = crc.into_engine_data(schema, &engine);
        assert!(
            engine_data.is_ok(),
            "Failed to serialize CRC: {:?}",
            engine_data.err()
        );
        assert_eq!(engine_data.unwrap().len(), 1); // single row
    }

    #[test]
    fn test_crc_into_engine_data_with_domain_metadata() {
        use crate::engine::sync::SyncEngine;
        use crate::IntoEngineData as _;
        let mut crc = make_test_crc(500, 5);
        crc.domain_metadata = Some(vec![
            DomainMetadata::new("delta.rowTracking".to_string(), "config1".to_string()),
            DomainMetadata::new("user.domain".to_string(), "config2".to_string()),
        ]);
        crc.in_commit_timestamp_opt = Some(99999);
        let schema = std::sync::Arc::new(Crc::to_schema());
        let engine = SyncEngine::new();
        let engine_data = crc.into_engine_data(schema, &engine);
        assert!(
            engine_data.is_ok(),
            "Failed to serialize CRC with DM: {:?}",
            engine_data.err()
        );
        assert_eq!(engine_data.unwrap().len(), 1);
    }

    #[test]
    fn test_crc_into_engine_data_empty_domain_metadata() {
        use crate::engine::sync::SyncEngine;
        use crate::IntoEngineData as _;
        let mut crc = make_test_crc(0, 0);
        crc.domain_metadata = Some(vec![]); // empty but present
        let schema = std::sync::Arc::new(Crc::to_schema());
        let engine = SyncEngine::new();
        let engine_data = crc.into_engine_data(schema, &engine);
        assert!(
            engine_data.is_ok(),
            "Failed to serialize CRC with empty DM: {:?}",
            engine_data.err()
        );
    }
}
