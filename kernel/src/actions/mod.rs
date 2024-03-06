/// Code to parse and handle actions from the delta log
pub(crate) mod deletion_vector;
pub(crate) mod schemas;
pub(crate) mod visitors;

use derive_macros::Schema;
use std::collections::HashMap;
use visitors::{AddVisitor, MetadataVisitor, ProtocolVisitor};

use crate::{schema::StructType, DeltaResult, EngineData};

use self::{deletion_vector::DeletionVectorDescriptor, schemas::GetSchema};

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containingconfiguration options for the format
    pub options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Schema)]
#[schema(name = metaData)]
pub struct Metadata {
    /// Unique identifier for this table
    pub id: String,
    /// User-provided identifier for this table
    pub name: Option<String>,
    /// User-provided description for this table
    pub description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub format: Format,
    /// Schema of the table
    pub schema_string: String,
    /// Column names by which the data should be partitioned
    pub partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub created_time: Option<i64>,
    /// Configuration options for the metadata action
    pub configuration: HashMap<String, Option<String>>,
}

impl Metadata {
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        data.extract(Metadata::get_schema(), &mut visitor)?;
        Ok(visitor.metadata)
    }

    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Schema)]
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    pub reader_features: Option<Vec<String>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    pub writer_features: Option<Vec<String>>,
}

impl Protocol {
    pub fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Protocol>> {
        let mut visitor = ProtocolVisitor::default();
        data.extract(Protocol::get_schema(), &mut visitor)?;
        Ok(visitor.protocol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file.
    pub partition_values: HashMap<String, Option<String>>,

    /// The size of this data file in bytes
    pub size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, Option<String>>>,

    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    pub clustering_provider: Option<String>,
}

impl Add {
    /// Since we always want to parse multiple adds from data, we return a `Vec<Add>`
    pub fn parse_from_data(data: &dyn EngineData) -> DeltaResult<Vec<Add>> {
        let mut visitor = AddVisitor::default();
        data.extract(Add::get_schema(), &mut visitor)?;
        Ok(visitor.adds)
    }

    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub(crate) struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) deletion_timestamp: Option<i64>,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    pub(crate) extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    pub(crate) partition_values: Option<HashMap<String, Option<String>>>,

    /// The size of this data file in bytes
    pub(crate) size: Option<i64>,

    /// Map containing metadata about this logical file.
    pub(crate) tags: Option<HashMap<String, Option<String>>>,

    /// Information about deletion vector (DV) associated with this add action
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub(crate) base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub(crate) default_row_commit_version: Option<i64>,
}

impl Remove {
    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        actions::schemas::GetSchema,
        schema::{ArrayType, DataType, MapType, StructField},
    };

    #[test]
    fn test_metadata_schema() {
        let schema = Metadata::get_schema();

        let expected = Arc::new(StructType::new(vec![StructField::new(
            "metaData",
            StructType::new(vec![
                StructField::new("id", DataType::STRING, false),
                StructField::new("name", DataType::STRING, true),
                StructField::new("description", DataType::STRING, true),
                StructField::new(
                    "format",
                    StructType::new(vec![
                        StructField::new("provider", DataType::STRING, false),
                        StructField::new(
                            "options",
                            MapType::new(DataType::STRING, DataType::STRING, false),
                            false,
                        ),
                    ]),
                    false,
                ),
                StructField::new("schemaString", DataType::STRING, false),
                StructField::new(
                    "partitionColumns",
                    ArrayType::new(DataType::STRING, false),
                    false,
                ),
                StructField::new("createdTime", DataType::LONG, true),
                StructField::new(
                    "configuration",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                    false,
                ),
            ]),
            false,
        )]));
        assert_eq!(schema, expected);
    }

    fn tags_field() -> StructField {
        StructField::new(
            "tags",
            MapType::new(DataType::STRING, DataType::STRING, true),
            true,
        )
    }

    fn partition_values_field() -> StructField {
        StructField::new(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, true),
            true,
        )
    }

    fn deletion_vector_field() -> StructField {
        StructField::new(
            "deletionVector",
            DataType::Struct(Box::new(StructType::new(vec![
                StructField::new("storageType", DataType::STRING, false),
                StructField::new("pathOrInlineDv", DataType::STRING, false),
                StructField::new("offset", DataType::INTEGER, true),
                StructField::new("sizeInBytes", DataType::INTEGER, false),
                StructField::new("cardinality", DataType::LONG, false),
            ]))),
            true,
        )
    }

    #[test]
    fn test_remove_schema() {
        let schema = Remove::get_schema();
        let expected = Arc::new(StructType::new(vec![StructField::new(
            "remove",
            StructType::new(vec![
                StructField::new("path", DataType::STRING, false),
                StructField::new("deletionTimestamp", DataType::LONG, true),
                StructField::new("dataChange", DataType::BOOLEAN, false),
                StructField::new("extendedFileMetadata", DataType::BOOLEAN, true),
                partition_values_field(),
                StructField::new("size", DataType::LONG, true),
                tags_field(),
                deletion_vector_field(),
                StructField::new("baseRowId", DataType::LONG, true),
                StructField::new("defaultRowCommitVersion", DataType::LONG, true),
            ]),
            false,
        )]));
        assert_eq!(schema, expected);
    }
}
