//! Adaptive Metadata Tree (AMT) support.
// TODO: remove these allows once the AMT read/write paths consume these types and the public
// types are re-exported from the crate root.
#![allow(dead_code)]
#![allow(unreachable_pub)]

pub(crate) mod stats;

use std::collections::HashMap;

use bytes::Bytes;
use delta_kernel_derive::{IntoEngineData, ToSchema};
use url::Url;

use crate::engine_data::EngineData;
use crate::expressions::{Scalar, StructData};
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use crate::Version;

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
pub(crate) const EQUALITY_IDS: &str = "equalityIds";
pub(crate) const FORMAT_VERSION: &str = "formatVersion";
pub(crate) const TAGS: &str = "tags";

/// Field names for the different fields within content_stats.
pub(crate) const LOWER_BOUND: &str = "lower_bound";
pub(crate) const UPPER_BOUND: &str = "upper_bound";
pub(crate) const TIGHT_BOUNDS: &str = "tight_bounds";
pub(crate) const VALUE_COUNT: &str = "value_count";
pub(crate) const NULL_VALUE_COUNT: &str = "null_value_count";
pub(crate) const NAN_VALUE_COUNT: &str = "nan_value_count";
pub(crate) const AVG_VALUE_SIZE_IN_BYTES: &str = "avg_value_size_in_bytes";

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
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
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
}

/// Sub-struct of ContentTreeNodeEntry that tracks details
/// of the history of a file in the AMT (its current state,
/// a sequence number for when it was added, etc).
#[derive(Debug, Clone, ToSchema, IntoEngineData)]
pub struct TrackingInfo {
    /// Whether this entry is added, existing, or deleted.
    #[field_id = 0]
    pub(crate) status: TrackingStatus,

    /// Snapshot ID where the file was added, or deleted if status is 2. Inherited when `None`.
    /// Must be written in the root file.
    #[field_id = 1]
    pub snapshot_id: Option<i64>,

    /// Snapshot ID in which this entry's deletion vector last changed. Set on Modified entries.
    #[field_id = 5]
    pub(crate) dv_snapshot_id: Option<i64>,

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

    /// Positions deleted from this manifest in the current commit. Cleared between commits.
    /// Encoded as a serialized `RoaringBitmapArray` (the same portable RoaringBitmap framing used
    /// for inline deletion vectors, see [`crate::actions::deletion_vector`]).
    #[field_id = 6]
    pub(crate) deleted_positions: Option<Bytes>,

    /// Positions replaced (DV changed) in this manifest in the current commit. Cleared between
    /// commits. Encoded as a serialized `RoaringBitmapArray`, matching `deleted_positions`.
    // TODO: always `None` until DvCache tracks replaced positions in a later change.
    #[field_id = 7]
    pub(crate) replaced_positions: Option<Bytes>,
}

/// Represents an entry/row in a ContentTree node.
#[derive(Debug, Clone, ToSchema)]
pub(super) struct ContentTreeNodeEntry {
    /// Type of content stored by the entry.
    /// DataManifest and DeleteManifest can only be defined in the root manifest.
    #[field_id = 134]
    pub content_type: DataContentType,

    /// Location of the file. Required for most content types.
    #[field_id = 100]
    pub location: Option<String>,

    /// File format of the entry: `parquet` for data files or `puffin` for deletion vectors (the
    /// only formats kernel supports). See [`DataFileFormat`].
    #[field_id = 101]
    pub(crate) file_format: DataFileFormat,

    #[field_id = 147]
    pub tracking: TrackingInfo,

    #[field_id = 148]
    pub(crate) deletion_vector: Option<DeletionVectorInfo>,

    /// ID of partition spec used to write manifest or data/delete files.
    #[field_id = 141]
    pub(crate) spec_id: i32,

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

    /// Total file size in bytes. Must be defined if location is defined
    #[field_id = 104]
    pub(crate) file_size_in_bytes: Option<i64>,

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

    /// Location of the data file if the content_type is  PositionDeletes
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

    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and must be `None` otherwise.
    /// Fields with ids listed in this column must be present in the delete file
    #[field_id = 135]
    #[nested_field_id = 136]
    pub(crate) equality_ids: Option<Vec<i32>>,

    /// The AMT/Iceberg format version this entry was written at.
    #[field_id = 157]
    pub(crate) format_version: i32,

    /// Metadata tags for this file, propagated from the Add action. Map values can be null.
    ///
    /// Unlike other entry fields, `tags` carries no Parquet field ID. It was added after the
    /// initial Iceberg AMF schema was fixed, so it is matched by column name when reading.
    /// TODO: assign a Parquet field ID dynamically based on Iceberg expressions once they are
    /// supported in the AMF spec.
    pub(crate) tags: Option<HashMap<String, Option<String>>>,
}

/// Type of content stored by the manifest entry
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    Data = 0,
    PositionDeletes = 1,
    EqualityDeletes = 2,
    // Types below are only allowed in the root
    DataManifest = 3,   // manifest of data files with inline DV info
    DeleteManifest = 4, // kept for backwards compat reading only
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
    /// Puffin file format: <https://iceberg.apache.org/puffin-spec/>
    Puffin,
}

impl ToDataType for DataFileFormat {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<DataFileFormat> for Scalar {
    fn from(value: DataFileFormat) -> Self {
        match value {
            DataFileFormat::Parquet => Scalar::String("parquet".to_string()),
            DataFileFormat::Puffin => Scalar::String("puffin".to_string()),
        }
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

#[derive(Debug, Clone, Default, PartialEq, ToSchema, IntoEngineData)]
pub(crate) struct ManifestInfo {
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
    #[field_id = 520]
    pub(crate) replaced_files_count: i32,

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
    #[field_id = 521]
    pub(crate) replaced_rows_count: i64,

    /// Minimum data sequence number of all entries in the manifest.
    #[field_id = 516]
    pub(crate) min_sequence_number: i64,

    /// Serialized deletion vector covering the manifest's entries, or `None` when the manifest has
    /// no associated deletion vector. Encoded as a serialized `RoaringBitmapArray`, matching the
    /// framing used for [`TrackingInfo::deleted_positions`].
    #[field_id = 522]
    pub(crate) dv: Option<Bytes>,
    /// Number of set bits (deleted rows) in [`Self::dv`], or `None` when `dv` is absent.
    #[field_id = 523]
    pub(crate) dv_cardinality: Option<i64>,
}
