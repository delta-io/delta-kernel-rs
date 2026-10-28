//! Adaptive Metadata Tree (AMT) support.
// TODO: remove these allows once the AMT read/write paths consume these types and the public
// types are re-exported from the crate root.
#![allow(dead_code)]
#![allow(unreachable_pub)]

mod builder;
mod dv_conversion;
mod mumbling;
mod reader;
pub(crate) mod stats;
#[cfg(test)]
pub(crate) mod test_utils;

use std::collections::HashMap;
use std::sync::LazyLock;

use bytes::Bytes;
use delta_kernel_derive::ToSchema;
use url::Url;

use crate::engine_data::{EngineData, GetData, TypedGetData as _};
use crate::expressions::{Expression, Scalar, StructData, null_lit};
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::{ColumnNamesAndTypes, DataType, StructType, ToSchema as _};
use crate::{KernelError, KernelResult, Version};

/// Field names in the [`ContentTreeNodeEntry`] schema.
pub(crate) const CONTENT_TYPE: &str = "contentType";
pub(crate) const LOCATION: &str = "location";
pub(crate) const FILE_FORMAT: &str = "fileFormat";
pub(crate) const TRACKING: &str = "tracking";
pub(crate) const DV_INFO: &str = "deletionVector";
pub(crate) const PARTITION_SPEC_ID: &str = "specId";
pub(crate) const PARTITION: &str = "partition";
pub(crate) const SORT_ORDER_ID: &str = "sortOrderId";
pub(crate) const RECORD_COUNT: &str = "recordCount";
pub(crate) const FILE_SIZE_IN_BYTES: &str = "fileSizeInBytes";
pub(crate) const CONTENT_STATS_FIELD_NAME: &str = "content_stats";
pub(crate) const MANIFEST_INFO: &str = "manifestInfo";
pub(crate) const KEY_METADATA: &str = "keyMetadata";
pub(crate) const SPLIT_OFFSETS: &str = "splitOffsets";
pub(crate) const COLUMN_FILES: &str = "columnFiles";
pub(crate) const TAGS: &str = "tags";

/// Field names within the [`ManifestInfo`] sub-struct.
pub(crate) const FORMAT_VERSION: &str = "formatVersion";
pub(crate) const MANIFEST_DV: &str = "dv";
pub(crate) const ADDED_FILES_COUNT: &str = "addedFilesCount";
pub(crate) const EXISTING_FILES_COUNT: &str = "existingFilesCount";
pub(crate) const DELETED_FILES_COUNT: &str = "deletedFilesCount";
pub(crate) const REPLACED_FILES_COUNT: &str = "replacedFilesCount";
pub(crate) const MODIFIED_FILES_COUNT: &str = "modifiedFilesCount";
pub(crate) const ADDED_ROWS_COUNT: &str = "addedRowsCount";
pub(crate) const EXISTING_ROWS_COUNT: &str = "existingRowsCount";
pub(crate) const DELETED_ROWS_COUNT: &str = "deletedRowsCount";
pub(crate) const REPLACED_ROWS_COUNT: &str = "replacedRowsCount";
pub(crate) const MODIFIED_ROWS_COUNT: &str = "modifiedRowsCount";
pub(crate) const MIN_SEQUENCE_NUMBER: &str = "minSequenceNumber";

/// The `manifestInfo.formatVersion` of a manifest written in Iceberg v4. Manifests written by
/// earlier versions record 0.
pub(crate) const AMT_FORMAT_VERSION: i32 = 4;

/// Field names within the [`TrackingInfo`] sub-struct, shared by the AMT write and read paths.
pub(crate) const TRACKING_STATUS: &str = "status";
pub(crate) const TRACKING_SNAPSHOT_ID: &str = "snapshotId";
pub(crate) const MODIFIED_SNAPSHOT_ID: &str = "modifiedSnapshotId";
pub(crate) const SEQUENCE_NUMBER: &str = "sequenceNumber";
pub(crate) const FILE_SEQUENCE_NUMBER: &str = "fileSequenceNumber";
pub(crate) const FIRST_ROW_ID: &str = "firstRowId";
pub(crate) const DELETED_POSITIONS: &str = "deletedPositions";
pub(crate) const REPLACED_POSITIONS: &str = "replacedPositions";

/// Field names within the [`DeletionVectorInfo`] sub-struct.
pub(crate) const DV_INFO_OFFSET: &str = "offset";
pub(crate) const DV_INFO_SIZE_IN_BYTES: &str = "sizeInBytes";
pub(crate) const DV_INFO_CARDINALITY: &str = "cardinality";

/// Field names within the [`ColumnFile`] sub-struct.
pub(crate) const FIELD_IDS: &str = "fieldIds";

/// Field names for the different fields within content_stats.
pub(crate) const LOWER_BOUND: &str = "lower_bound";
pub(crate) const UPPER_BOUND: &str = "upper_bound";
pub(crate) const TIGHT_BOUNDS: &str = "tight_bounds";
pub(crate) const VALUE_COUNT: &str = "value_count";
pub(crate) const NULL_VALUE_COUNT: &str = "null_value_count";
pub(crate) const NAN_VALUE_COUNT: &str = "nan_value_count";
pub(crate) const TOTAL_BYTES: &str = "total_bytes";

/// In memory reprsentation of a node in the Adaptive Metadata Tree (AMT) format.
///
/// This used as an intermediate structure for both reading and writing AMT nodes.
pub(super) struct ContentTreeNode {
    // Collection of EngineData in the node. For root nodes this can contain
    // both data files and manifest references.
    data: Vec<Box<dyn EngineData>>,
    // The delta table version this tree was constructed at.
    version: Version,
    /// URL that paths stored in this node are relative to.
    table_root: Url,
    /// The exact path string as it appears in the Delta log (from contentRoot action) or manifest
    /// location field in the AMT root. This is NOT normalized or converted - it flows through
    /// exactly as stored in the log. Empty string for newly built metadata that hasn't been
    /// written yet.
    path_in_log: String,
}

/// Sub-struct of ContentTreeNodeEntry that hold information about
/// deletion vector applied to data files.
#[derive(Debug, Clone, ToSchema)]
#[cfg_attr(test, derive(delta_kernel_derive::IntoStructData))]
pub(crate) struct DeletionVectorInfo {
    /// Path to location that DV is stored in.
    #[field_id = 155]
    pub(crate) location: String,

    /// The offset in the file where the content starts.
    #[field_id = 144]
    pub(crate) offset: i64,

    /// The length of the referenced content stored in the file;
    /// required if content_offset is present.
    #[field_id = 145]
    pub(crate) size_in_bytes: i64,

    /// Number of set bits (deleted rows) in the deletion vector.
    #[field_id = 156]
    pub(crate) cardinality: i64,

    /// Implementation-specific key metadata for encryption.
    #[field_id = 149]
    pub(crate) key_metadata: Option<Bytes>,
}

/// Sub-struct of ContentTreeNodeEntry that tracks details
/// of the history of a file in the AMT (its current state,
/// a sequence number for when it was added, etc).
#[derive(Debug, Clone, ToSchema)]
#[cfg_attr(test, derive(PartialEq, delta_kernel_derive::IntoStructData))]
pub struct TrackingInfo {
    /// Whether this entry is added, existing, or deleted.
    #[field_id = 0]
    pub(crate) status: TrackingStatus,

    /// Snapshot ID where the file was added, or deleted if status is 2. Inherited when `None`.
    /// Must be written in the root file.
    #[field_id = 1]
    pub snapshot_id: Option<i64>,

    /// Snapshot ID in which this entry was last modified (its deletion vector or column files
    /// changed). Preserved when the entry is carried forward, so it can be set on any status, not
    /// just `Modified`.
    #[field_id = 5]
    pub(crate) modified_snapshot_id: Option<i64>,

    /// Data sequence number of the file. Inherited when `None` and status is 1 (added).
    /// Must be equal to file_sequence_number if content_type is {Data,Delete}Manifest.
    /// Must be written in the root file.
    #[field_id = 3]
    pub(crate) sequence_number: Option<i64>,

    /// File sequence number indicating when the file was added. Inherited when `None` and status
    /// is added. Must be equal to sequence_number if content_type is {Data,Delete}Manifest.
    #[field_id = 4]
    pub(crate) file_sequence_number: Option<i64>,

    /// The _row_id for the first row in the data file if content_type is Data.
    /// If content_type is DataManifest, this is the starting _row_id to assign to rows added by
    /// ADDED data files.
    #[field_id = 142]
    pub(crate) first_row_id: Option<i64>,

    /// Positions deleted from this manifest in the [`Self::modified_snapshot_id`] snapshot. Only
    /// set on manifest entries. Encoded as a
    /// [Mumbling bitmap](https://iceberg.apache.org/mumbling-spec/).
    #[field_id = 6]
    pub(crate) deleted_positions: Option<Bytes>,

    /// Positions replaced in this manifest in the [`Self::modified_snapshot_id`] snapshot,
    /// disjoint from `deleted_positions`. Only set on manifest entries. Encoded as a Mumbling
    /// bitmap, matching `deleted_positions`.
    // TODO: always `None` until DvCache tracks replaced positions in a later change.
    #[field_id = 7]
    pub(crate) replaced_positions: Option<Bytes>,
}

/// The `tracking.*` leaf columns [`TrackingInfo::try_from_getters`] reads, in getter order.
pub(crate) static TRACKING_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| TrackingInfo::to_schema().leaves(TRACKING));

impl TrackingInfo {
    /// Parses one row's `tracking` struct from `getters`, which must follow [`TRACKING_COLUMNS`]
    /// order.
    ///
    /// # Errors
    /// Returns an error if the getter count differs from [`TRACKING_COLUMNS`], if `status` is null
    /// or not a known [`TrackingStatus`], or if a getter yields a value of the wrong type.
    pub(crate) fn try_from_getters<'a>(
        row: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> KernelResult<Self> {
        let expected = TRACKING_COLUMNS.as_ref().0.len();
        if getters.len() != expected {
            return Err(KernelError::internal_error(format!(
                "Expected {expected} AMT tracking getters, got {}",
                getters.len()
            )));
        }
        let positions = |getter: &'a dyn GetData<'a>, field: &str| -> KernelResult<_> {
            let bytes: Option<&[u8]> = getter.get_opt(row, field)?;
            Ok(bytes.map(Bytes::copy_from_slice))
        };
        Ok(Self {
            status: TrackingStatus::try_from_repr(getters[0].get(row, TRACKING_STATUS)?)?,
            snapshot_id: getters[1].get_opt(row, TRACKING_SNAPSHOT_ID)?,
            modified_snapshot_id: getters[2].get_opt(row, MODIFIED_SNAPSHOT_ID)?,
            sequence_number: getters[3].get_opt(row, SEQUENCE_NUMBER)?,
            file_sequence_number: getters[4].get_opt(row, FILE_SEQUENCE_NUMBER)?,
            first_row_id: getters[5].get_opt(row, FIRST_ROW_ID)?,
            deleted_positions: positions(getters[6], DELETED_POSITIONS)?,
            replaced_positions: positions(getters[7], REPLACED_POSITIONS)?,
        })
    }
}

/// Represents an entry/row in a ContentTree node.
#[derive(Debug, Clone, ToSchema)]
#[cfg_attr(test, derive(delta_kernel_derive::IntoStructData))]
pub(super) struct ContentTreeNodeEntry {
    /// Type of content stored by the entry.
    /// DataManifest and DeleteManifest can only be defined in the root manifest.
    #[field_id = 134]
    pub content_type: DataContentType,

    /// Location of the file.
    #[field_id = 100]
    pub location: String,

    /// File format of the entry. Kernel only supports `parquet`; see [`DataFileFormat`].
    #[field_id = 101]
    pub(crate) file_format: DataFileFormat,

    #[field_id = 147]
    pub tracking: TrackingInfo,

    #[field_id = 148]
    pub(crate) deletion_vector: Option<DeletionVectorInfo>,

    /// ID of partition spec used to write manifest or data/delete files. Written as `None` by
    /// kernel, which does not yet track partition specs.
    #[field_id = 141]
    pub(crate) spec_id: Option<i32>,

    /// Partition data tuple, schema based on the partition spec. Required (non-nullable)
    /// when present in the schema. The schema is dynamically generated based on the
    /// partition spec via [`Self::to_schema_with_content_stats`]. When `None` in Rust,
    /// a struct with null-valued fields is produced during serialization.
    #[skip_schema]
    #[field_id = 102]
    pub(crate) partition: Option<StructData>,

    /// ID representing sort order for this file. Can only be set if content_type is Data.
    #[field_id = 140]
    pub(crate) sort_order_id: Option<i32>,

    /// Number of records in this file, or the cardinality of a deletion vector
    #[field_id = 103]
    pub(crate) record_count: i64,

    /// Total file size in bytes.
    #[field_id = 104]
    pub(crate) file_size_in_bytes: i64,

    /// Column-level statistics for the data file.
    /// The schema of this struct is dynamically generated based on the table schema
    /// using [`stats::stats_schema`]. When `None`, no statistics are available.
    /// See: <https://docs.google.com/document/d/1uvbrwwAJW2TgsnoaIcwAFpjbhHkBUL5wY_24nKgtt9I/>
    // Skip the schema since we don't know the type here, use to_schema_with_content_stats instead
    #[skip_schema]
    #[field_id = 146]
    pub(crate) content_stats: Option<StructData>,

    /// Must be set if content_type is {Data,Delete}Manifest, otherwise `None`.
    #[field_id = 150]
    pub(crate) manifest_info: Option<ManifestInfo>,

    /// Location of affiliated data manifest if content_type is or DeleteManifest or `None` if
    /// delete manifest is unaffiliated. TODO: place holder for referenced file which is no longer
    /// necessary. #[field_id = 143]
    /// pub referenced_file: `Option<String>`,

    /// Implementation-specific key metadata for encryption
    #[field_id = 131]
    pub(crate) key_metadata: Option<Bytes>,

    /// Split offsets for the data file. For example, all row group offsets in a Parquet file. Must
    /// be sorted ascending
    #[field_id = 132]
    #[nested_field_id = 133]
    pub(crate) split_offsets: Option<Vec<i64>>,

    /// Column files storing updated values of some of this data file's fields. Only set on data
    /// entries and v4 leaf manifests.
    #[field_id = 158]
    #[nested_field_id = 159]
    pub(crate) column_files: Option<Vec<ColumnFile>>,

    /// Metadata tags for this file, propagated from the Add action. Map values can be null.
    ///
    /// Unlike other entry fields, `tags` carries no Parquet field ID. It was added after the
    /// initial Iceberg AMF schema was fixed, so it is matched by column name when reading.
    /// TODO: assign a Parquet field ID dynamically based on Iceberg expressions once they are
    /// supported in the AMF spec.
    pub(crate) tags: Option<HashMap<String, Option<String>>>,
}

/// A file storing the values of some fields for each row of another file, used by column updates.
#[derive(Debug, Clone, ToSchema)]
#[cfg_attr(test, derive(delta_kernel_derive::IntoStructData))]
pub(crate) struct ColumnFile {
    /// Location of the column file.
    #[field_id = 160]
    pub(crate) location: String,

    /// IDs of the top-level fields whose values this column file stores.
    #[field_id = 161]
    #[nested_field_id = 162]
    pub(crate) field_ids: Vec<i32>,

    /// File format of the column file.
    #[field_id = 163]
    pub(crate) file_format: DataFileFormat,

    /// Total column file size in bytes.
    #[field_id = 164]
    pub(crate) file_size_in_bytes: i64,

    /// Implementation-specific key metadata for encryption.
    #[field_id = 165]
    pub(crate) key_metadata: Option<Bytes>,
}

/// Type of content stored by the manifest entry
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    Data = 0,
    // Types below are only allowed in the root
    DataManifest = 3,   // manifest of data files with inline DV info
    DeleteManifest = 4, // kept for backwards compat reading only
}

impl DataContentType {
    /// Maps the on-disk integer representation to the enum, erroring on unknown values.
    pub(crate) fn try_from_repr(value: i32) -> KernelResult<Self> {
        match value {
            0 => Ok(Self::Data),
            3 => Ok(Self::DataManifest),
            4 => Ok(Self::DeleteManifest),
            other => Err(KernelError::generic(format!(
                "Invalid AMT content type value: {other}"
            ))),
        }
    }
}

impl ToDataType for DataContentType {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<DataContentType> for Scalar {
    fn from(value: DataContentType) -> Self {
        Scalar::Integer(value as i32)
    }
}

/// Format of this data.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum DataFileFormat {
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
}

impl ToDataType for DataFileFormat {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl DataFileFormat {
    /// The on-disk name of this format.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

impl From<DataFileFormat> for Scalar {
    fn from(value: DataFileFormat) -> Self {
        Scalar::String(value.name().to_string())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TrackingStatus {
    Existing = 0,
    Added = 1,
    Deleted = 2,
    Replaced = 3,
    Modified = 4,
}

impl TrackingStatus {
    /// Maps the on-disk integer representation to the enum, erroring on unknown values.
    pub(crate) fn try_from_repr(value: i32) -> KernelResult<Self> {
        match value {
            0 => Ok(Self::Existing),
            1 => Ok(Self::Added),
            2 => Ok(Self::Deleted),
            3 => Ok(Self::Replaced),
            4 => Ok(Self::Modified),
            other => Err(KernelError::generic(format!(
                "Invalid AMT tracking status value: {other}"
            ))),
        }
    }

    /// Whether this entry contributes rows to reads. Live entries (`Existing`, `Added`,
    /// `Modified`) are surfaced as `Add` actions; not-live entries (`Deleted`, `Replaced`) are not.
    pub(crate) fn is_live(self) -> bool {
        self == Self::Existing || self == Self::Added || self == Self::Modified
    }
}

impl ToDataType for TrackingStatus {
    fn to_data_type() -> DataType {
        DataType::INTEGER
    }
}

impl From<TrackingStatus> for Scalar {
    fn from(value: TrackingStatus) -> Self {
        Scalar::Integer(value as i32)
    }
}

/// Sub-struct of ContentTreeNodeEntry summarizing a referenced manifest.
#[derive(Debug, Clone, Default, PartialEq, ToSchema)]
#[cfg_attr(test, derive(delta_kernel_derive::IntoStructData))]
pub(crate) struct ManifestInfo {
    /// Format version the manifest was written with: [`AMT_FORMAT_VERSION`] for v4 manifests, 0
    /// for manifests written by earlier versions.
    #[field_id = 521]
    pub(crate) format_version: i32,

    /// Number of entries with ADDED status in the manifest.
    #[field_id = 504]
    pub(crate) added_files_count: i32,
    /// Number of entries with EXISTING status in the manifest.
    #[field_id = 505]
    pub(crate) existing_files_count: i32,
    /// Number of entries with DELETED status in the manifest.
    #[field_id = 506]
    pub(crate) deleted_files_count: i32,
    /// Number of entries with REPLACED status in the manifest.
    #[field_id = 523]
    pub(crate) replaced_files_count: i32,
    /// Number of entries with MODIFIED status in the manifest.
    #[field_id = 525]
    pub(crate) modified_files_count: i32,

    /// Total row count across all ADDED entries in the manifest.
    #[field_id = 512]
    pub(crate) added_rows_count: i64,
    /// Total row count across all EXISTING entries in the manifest.
    #[field_id = 513]
    pub(crate) existing_rows_count: i64,
    /// Total row count across all DELETED entries in the manifest.
    #[field_id = 514]
    pub(crate) deleted_rows_count: i64,
    /// Total row count across all REPLACED entries in the manifest.
    #[field_id = 524]
    pub(crate) replaced_rows_count: i64,
    /// Total row count across all MODIFIED entries in the manifest.
    #[field_id = 526]
    pub(crate) modified_rows_count: i64,

    /// Minimum data sequence number of all live entries in the manifest.
    #[field_id = 516]
    pub(crate) min_sequence_number: i64,

    /// Positions in the referenced leaf manifest that are not live, or `None` when every position
    /// is live. Encoded as a [Mumbling bitmap](https://iceberg.apache.org/mumbling-spec/).
    #[field_id = 522]
    pub(crate) dv: Option<Bytes>,
}

// === Helpers ===

/// Builds a struct expression matching `schema` field-for-field. `project` supplies the expression
/// for a named field; an unmatched nullable field (one returning `None`) becomes a typed null
/// literal, so the result matches the schema in field order and type.
///
/// # Errors
/// Returns an error if a non-nullable field has no projection: falling back to a typed null there
/// would silently emit a null in a non-nullable column.
fn struct_expr_from_schema(
    schema: &StructType,
    project: impl Fn(&str) -> Option<Expression>,
) -> KernelResult<Expression> {
    let fields = schema
        .fields()
        .map(|field| match project(field.name().as_str()) {
            Some(expr) => Ok(expr),
            None if field.is_nullable() => Ok(null_lit(field.data_type().clone())),
            None => Err(KernelError::generic(format!(
                "struct_expr_from_schema: no projection for required field '{}'",
                field.name()
            ))),
        })
        .collect::<KernelResult<Vec<_>>>()?;
    Ok(Expression::struct_from(fields))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::content_tree::test_utils::{
        added_data_entry, entry_batch, struct_batch, with_field,
    };
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::RowVisitor;
    use crate::schema::{ColumnMetadataKey, ColumnName, MetadataValue, StructField};
    use crate::unit_test_utils::assert_result_error_with_message;

    /// Collects every row's [`TrackingInfo`] via [`TrackingInfo::try_from_getters`].
    #[derive(Default)]
    struct TrackingVisitor {
        tracking: Vec<TrackingInfo>,
    }

    impl RowVisitor for TrackingVisitor {
        fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
            TRACKING_COLUMNS.as_ref()
        }

        fn visit<'a>(
            &mut self,
            row_count: usize,
            getters: &[&'a dyn GetData<'a>],
        ) -> KernelResult<()> {
            for row in 0..row_count {
                self.tracking
                    .push(TrackingInfo::try_from_getters(row, getters)?);
            }
            Ok(())
        }
    }

    fn visit_tracking(batch: &dyn EngineData) -> KernelResult<Vec<TrackingInfo>> {
        let mut visitor = TrackingVisitor::default();
        visitor.visit_rows_of(batch)?;
        Ok(visitor.tracking)
    }

    #[rstest]
    #[case::all_fields_set(TrackingInfo {
        status: TrackingStatus::Existing,
        snapshot_id: Some(1),
        modified_snapshot_id: Some(2),
        sequence_number: Some(3),
        file_sequence_number: Some(4),
        first_row_id: Some(5),
        deleted_positions: Some(Bytes::from_static(b"deleted")),
        replaced_positions: Some(Bytes::from_static(b"replaced")),
    })]
    #[case::optional_fields_null(TrackingInfo {
        status: TrackingStatus::Deleted,
        snapshot_id: None,
        modified_snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        first_row_id: None,
        deleted_positions: None,
        replaced_positions: None,
    })]
    fn tracking_info_parses_from_getters(#[case] tracking: TrackingInfo) {
        let engine = SyncEngine::new();
        let mut entry = added_data_entry("a.parquet", 1, 1, 0, 0);
        entry.tracking = tracking.clone();
        let batch = entry_batch(&engine, &[entry]);
        assert_eq!(visit_tracking(batch.as_ref()).unwrap(), vec![tracking]);
    }

    #[test]
    fn tracking_info_rejects_unknown_status() {
        let engine = SyncEngine::new();
        let entry = with_field(
            &added_data_entry("a.parquet", 1, 1, 0, 0).into(),
            &[TRACKING, TRACKING_STATUS],
            Scalar::Integer(9),
        );
        let batch = struct_batch(&engine, &[entry]);
        assert_result_error_with_message(
            visit_tracking(batch.as_ref()),
            "Invalid AMT tracking status value: 9",
        );
    }

    #[test]
    fn tracking_info_rejects_wrong_getter_count() {
        assert_result_error_with_message(
            TrackingInfo::try_from_getters(0, &[]),
            "AMT tracking getters, got 0",
        );
    }

    #[test]
    fn struct_expr_from_schema_errors_on_missing_required_field() {
        let schema = StructType::new_unchecked([StructField::not_null("req", DataType::INTEGER)]);
        assert_result_error_with_message(struct_expr_from_schema(&schema, |_| None), "req");
    }

    #[test]
    fn struct_expr_from_schema_fills_missing_nullable_field_with_typed_null() {
        let schema = StructType::new_unchecked([StructField::nullable("opt", DataType::INTEGER)]);
        let expr = struct_expr_from_schema(&schema, |_| None).unwrap();
        assert_eq!(expr, Expression::struct_from([null_lit(DataType::INTEGER)]));
    }

    #[rstest]
    #[case(0, TrackingStatus::Existing)]
    #[case(1, TrackingStatus::Added)]
    #[case(2, TrackingStatus::Deleted)]
    #[case(3, TrackingStatus::Replaced)]
    #[case(4, TrackingStatus::Modified)]
    fn tracking_status_try_from_repr_roundtrips(
        #[case] repr: i32,
        #[case] expected: TrackingStatus,
    ) {
        assert_eq!(TrackingStatus::try_from_repr(repr).unwrap(), expected);
        assert_eq!(expected as i32, repr);
    }

    #[rstest]
    #[case(5)]
    #[case(-1)]
    #[case(i32::MAX)]
    fn tracking_status_try_from_repr_rejects_unknown(#[case] repr: i32) {
        assert!(TrackingStatus::try_from_repr(repr).is_err());
    }

    #[rstest]
    #[case(0, DataContentType::Data)]
    #[case(3, DataContentType::DataManifest)]
    #[case(4, DataContentType::DeleteManifest)]
    fn data_content_type_try_from_repr_roundtrips(
        #[case] repr: i32,
        #[case] expected: DataContentType,
    ) {
        assert_eq!(DataContentType::try_from_repr(repr).unwrap(), expected);
        assert_eq!(expected as i32, repr);
    }

    #[rstest]
    #[case::position_deletes(1)]
    #[case::equality_deletes(2)]
    #[case(5)]
    #[case(-1)]
    #[case(i32::MAX)]
    fn data_content_type_try_from_repr_rejects_unknown(#[case] repr: i32) {
        assert!(DataContentType::try_from_repr(repr).is_err());
    }

    /// The AMT struct Parquet field IDs and nullability are a protocol contract with the Iceberg v4
    /// spec. This pins the name, field ID, and nullability of every field in each `to_schema()`, in
    /// order, so an accidental change fails loudly. `partition` and `content_stats` are
    /// `#[skip_schema]` and only appear in `to_schema_with_content_stats`, so they are absent here.
    /// `tags` is a Delta extension with no field ID (matched by name).
    #[rstest]
    #[case::content_tree_node_entry(ContentTreeNodeEntry::to_schema(), &[
        (CONTENT_TYPE, Some(134), false),
        (LOCATION, Some(100), false),
        (FILE_FORMAT, Some(101), false),
        (TRACKING, Some(147), false),
        (DV_INFO, Some(148), true),
        (PARTITION_SPEC_ID, Some(141), true),
        (SORT_ORDER_ID, Some(140), true),
        (RECORD_COUNT, Some(103), false),
        (FILE_SIZE_IN_BYTES, Some(104), false),
        (MANIFEST_INFO, Some(150), true),
        (KEY_METADATA, Some(131), true),
        (SPLIT_OFFSETS, Some(132), true),
        (COLUMN_FILES, Some(158), true),
        (TAGS, None, true),
    ])]
    #[case::tracking_info(TrackingInfo::to_schema(), &[
        (TRACKING_STATUS, Some(0), false),
        (TRACKING_SNAPSHOT_ID, Some(1), true),
        (MODIFIED_SNAPSHOT_ID, Some(5), true),
        (SEQUENCE_NUMBER, Some(3), true),
        (FILE_SEQUENCE_NUMBER, Some(4), true),
        (FIRST_ROW_ID, Some(142), true),
        (DELETED_POSITIONS, Some(6), true),
        (REPLACED_POSITIONS, Some(7), true),
    ])]
    #[case::deletion_vector_info(DeletionVectorInfo::to_schema(), &[
        (LOCATION, Some(155), false),
        (DV_INFO_OFFSET, Some(144), false),
        (DV_INFO_SIZE_IN_BYTES, Some(145), false),
        (DV_INFO_CARDINALITY, Some(156), false),
        (KEY_METADATA, Some(149), true),
    ])]
    #[case::manifest_info(ManifestInfo::to_schema(), &[
        (FORMAT_VERSION, Some(521), false),
        (ADDED_FILES_COUNT, Some(504), false),
        (EXISTING_FILES_COUNT, Some(505), false),
        (DELETED_FILES_COUNT, Some(506), false),
        (REPLACED_FILES_COUNT, Some(523), false),
        (MODIFIED_FILES_COUNT, Some(525), false),
        (ADDED_ROWS_COUNT, Some(512), false),
        (EXISTING_ROWS_COUNT, Some(513), false),
        (DELETED_ROWS_COUNT, Some(514), false),
        (REPLACED_ROWS_COUNT, Some(524), false),
        (MODIFIED_ROWS_COUNT, Some(526), false),
        (MIN_SEQUENCE_NUMBER, Some(516), false),
        (MANIFEST_DV, Some(522), true),
    ])]
    #[case::column_file(ColumnFile::to_schema(), &[
        (LOCATION, Some(160), false),
        (FIELD_IDS, Some(161), false),
        (FILE_FORMAT, Some(163), false),
        (FILE_SIZE_IN_BYTES, Some(164), false),
        (KEY_METADATA, Some(165), true),
    ])]
    fn amt_schema_field_contract(
        #[case] schema: StructType,
        #[case] expected: &[(&str, Option<i64>, bool)],
    ) {
        // Assert the exact set and order of fields, catching added, removed, or reordered fields.
        let actual_names: Vec<&str> = schema.fields().map(|f| f.name().as_str()).collect();
        let expected_names: Vec<&str> = expected.iter().map(|(name, _, _)| *name).collect();
        assert_eq!(actual_names, expected_names, "schema fields drifted");

        for &(name, field_id, nullable) in expected {
            let field = schema.field(name).expect("field present in schema");
            assert_eq!(
                field.is_nullable(),
                nullable,
                "nullability mismatch for {name}"
            );
            assert_eq!(
                field
                    .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                    .cloned(),
                field_id.map(MetadataValue::Number),
                "field id mismatch for {name}",
            );
        }
    }
}
