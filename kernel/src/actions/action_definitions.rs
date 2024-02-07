//! Define the Delta actions that exist, and how to parse them out of [EngineData]

use std::{
    collections::HashMap,
    io::{Cursor, Read},
    sync::Arc,
};

use roaring::RoaringTreemap;
use url::Url;

use crate::{
    engine_data::{DataItem, DataVisitor, EngineData},
    schema::StructType,
    DeltaResult, EngineClient, Error, FileSystemClient,
};

/// Generic struct to allow us to visit a type or hold an error that the type couldn't be parsed
struct Visitor<T> {
    extracted: Option<DeltaResult<T>>,
    extract_fn: fn(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<T>,
}

impl<T> Visitor<T> {
    fn new(
        extract_fn: fn(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<T>,
    ) -> Self {
        Visitor {
            extracted: None,
            extract_fn,
        }
    }
}

impl<T> DataVisitor for Visitor<T> {
    fn visit(&mut self, row_index: usize, vals: &[Option<DataItem<'_>>]) {
        self.extracted = Some((self.extract_fn)(row_index, vals));
    }
}

/// Generic struct to allow us to visit a type repeatedly or hold an error that the type couldn't be parsed
pub(crate) struct MultiVisitor<T> {
    pub(crate) extracted: Vec<DeltaResult<T>>,
    extract_fn: fn(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<T>,
}

impl<T> MultiVisitor<T> {
    pub(crate) fn new(
        extract_fn: fn(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<T>,
    ) -> Self {
        MultiVisitor {
            extracted: vec![],
            extract_fn,
        }
    }
}

impl<T> DataVisitor for MultiVisitor<T> {
    fn visit(&mut self, row_index: usize, vals: &[Option<DataItem<'_>>]) {
        self.extracted.push((self.extract_fn)(row_index, vals));
    }
}

macro_rules! extract_required_item {
    ($item: expr, $as_func: ident, $typ: expr, $err_msg_missing: expr, $err_msg_type: expr) => {
        $item
            .as_ref()
            .ok_or(Error::Extract($typ, $err_msg_missing))?
            .$as_func()
            .ok_or(Error::Extract($typ, $err_msg_type))?
    };
}

macro_rules! extract_opt_item {
    ($item: expr, $as_func: ident, $typ: expr, $err_msg_type: expr) => {
        $item
            .as_ref()
            .map(|item| item.$as_func().ok_or(Error::Extract($typ, $err_msg_type)))
            .transpose()?
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Format {
    /// Name of the encoding for files in this table
    pub provider: String,
    /// A map containing configuration options for the format
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
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
    pub fn try_new_from_data(
        engine_client: &dyn EngineClient,
        data: &dyn EngineData,
    ) -> DeltaResult<Metadata> {
        let extractor = engine_client.get_data_extactor();
        let mut visitor = Visitor::new(visit_metadata);
        let schema = StructType::new(vec![crate::actions::schemas::METADATA_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .unwrap_or_else(|| Err(Error::Generic("Didn't get expected metadata".to_string())))
    }

    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

fn visit_metadata(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<Metadata> {
    let id = extract_required_item!(
        vals[0],
        as_str,
        "Metadata",
        "Metadata must have an id",
        "id must be str"
    )
    .to_string();

    let name =
        extract_opt_item!(vals[1], as_str, "Metadata", "name must be str").map(|n| n.to_string());

    let description = extract_opt_item!(vals[1], as_str, "Metadata", "description must be str")
        .map(|d| d.to_string());

    // get format out of primitives
    let format_provider = extract_required_item!(
        vals[3],
        as_str,
        "Format",
        "Format must have a provider",
        "format.provider must be a str"
    )
    .to_string();

    // options for format is always empty, so skip vals[4]

    let schema_string = extract_required_item!(
        vals[5],
        as_str,
        "Metadata",
        "schema_string must exist",
        "schema_string must be a str"
    )
    .to_string();

    let partition_columns = vals[6].as_ref().ok_or(Error::Extract(
        "Metadata",
        "Metadata must have partition_columns",
    ))?;
    let partition_columns = if let DataItem::List(lst) = partition_columns {
        let mut partition_columns = vec![];
        for i in 0..lst.len(row_index) {
            partition_columns.push(lst.get(row_index, i));
        }
        Ok(partition_columns)
    } else {
        Err(Error::Extract(
            "Metadata",
            "partition_columns must be a list",
        ))
    }?;

    // todo: partition_columns from vals[6]

    let created_time = extract_required_item!(
        vals[7],
        as_i64,
        "Metadata",
        "Metadata must have a created_time",
        "created_time must be i64"
    );

    let mut configuration = HashMap::new();
    if let Some(m) = vals[8].as_ref() {
        let map = m
            .as_map()
            .ok_or(Error::Extract("Metadata", "configuration must be a map"))?;
        if let Some(mode) = map.get("delta.columnMapping.mode") {
            configuration.insert(
                "delta.columnMapping.mode".to_string(),
                Some(mode.to_string()),
            );
        }
        if let Some(enable) = map.get("delta.enableDeletionVectors") {
            configuration.insert(
                "delta.enableDeletionVectors".to_string(),
                Some(enable.to_string()),
            );
        }
    }

    Ok(Metadata {
        id,
        name,
        description,
        format: Format {
            provider: format_provider,
            options: HashMap::new(),
        },
        schema_string,
        partition_columns,
        created_time: Some(created_time),
        configuration,
    })
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
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
    pub fn try_new_from_data(
        engine_client: &dyn EngineClient,
        data: &dyn EngineData,
    ) -> DeltaResult<Protocol> {
        let extractor = engine_client.get_data_extactor();
        let mut visitor = Visitor::new(visit_protocol);
        let schema = StructType::new(vec![crate::actions::schemas::PROTOCOL_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .unwrap_or_else(|| Err(Error::Generic("Didn't get expected Protocol".to_string())))
    }
}

fn visit_protocol(row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<Protocol> {
    let min_reader_version = extract_required_item!(
        vals[0],
        as_i32,
        "Protocol",
        "Protocol must have a minReaderVersion",
        "minReaderVersion must be i32"
    );

    let min_writer_version = extract_required_item!(
        vals[1],
        as_i32,
        "Protocol",
        "Protocol must have a minWriterVersion",
        "minWriterVersion must be i32"
    );

    let reader_features = vals[2]
        .as_ref()
        .map(|rf_di| {
            if let DataItem::List(lst) = rf_di {
                let mut reader_features = vec![];
                for i in 0..lst.len(row_index) {
                    reader_features.push(lst.get(row_index, i));
                }
                Ok(reader_features)
            } else {
                Err(Error::Extract(
                    "Protocol",
                    "readerFeatures must be a string list",
                ))
            }
        })
        .transpose()?;

    let writer_features = vals[3]
        .as_ref()
        .map(|wf_di| {
            if let DataItem::List(lst) = wf_di {
                let mut writer_features = vec![];
                for i in 0..lst.len(row_index) {
                    writer_features.push(lst.get(row_index, i));
                }
                Ok(writer_features)
            } else {
                Err(Error::Extract(
                    "Protocol",
                    "writerFeatures must be a string list",
                ))
            }
        })
        .transpose()?;

    Ok(Protocol {
        min_reader_version,
        min_writer_version,
        reader_features,
        writer_features,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: String,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorDescriptor {
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset {
            format!("{}{}@{offset}", self.storage_type, self.path_or_inline_dv)
        } else {
            format!("{}{}", self.storage_type, self.path_or_inline_dv)
        }
    }

    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match self.storage_type.as_str() {
            "u" => {
                let prefix_len = self.path_or_inline_dv.len() as i32 - 20;
                if prefix_len < 0 {
                    return Err(Error::DeletionVector("Invalid length".to_string()));
                }
                let decoded = z85::decode(&self.path_or_inline_dv[(prefix_len as usize)..])
                    .map_err(|_| Error::DeletionVector("Failed to decode DV uuid".to_string()))?;
                let uuid = uuid::Uuid::from_slice(&decoded)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let dv_suffix = if prefix_len > 0 {
                    format!(
                        "{}/deletion_vector_{uuid}.bin",
                        &self.path_or_inline_dv[..(prefix_len as usize)]
                    )
                } else {
                    format!("deletion_vector_{uuid}.bin")
                };
                let dv_path = parent
                    .join(&dv_suffix)
                    .map_err(|_| Error::DeletionVector(format!("invalid path: {}", dv_suffix)))?;
                Ok(Some(dv_path))
            }
            "p" => Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
            })?)),
            "i" => Ok(None),
            other => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{other}'."
            ))),
        }
    }

    pub fn read(
        &self,
        fs_client: Arc<dyn FileSystemClient>,
        parent: Url,
    ) -> DeltaResult<RoaringTreemap> {
        match self.absolute_path(&parent)? {
            None => {
                let bytes = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| Error::DeletionVector("Failed to decode DV".to_string()))?;
                RoaringTreemap::deserialize_from(&bytes[12..])
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
            Some(path) => {
                let offset = self.offset;
                let size_in_bytes = self.size_in_bytes;

                let dv_data = fs_client
                    .read_files(vec![(path, None)])?
                    .next()
                    .ok_or(Error::MissingData("No deletion Vector data".to_string()))??;

                let mut cursor = Cursor::new(dv_data);
                if let Some(offset) = offset {
                    // TODO should we read the datasize from the DV file?
                    // offset plus datasize bytes
                    cursor.set_position((offset + 4) as u64);
                }

                let mut buf = vec![0; 4];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                // let magic =
                //     i32::from_le_bytes(buf.try_into().map_err(|_| {
                //         Error::DeletionVector("filed to read magic bytes".to_string())
                //     })?);
                // assert!(magic == 1681511377);

                let mut buf = vec![0; size_in_bytes as usize];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;

                RoaringTreemap::deserialize_from(Cursor::new(buf))
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub tags: HashMap<String, Option<String>>,

    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,
}

impl Add {
    pub fn try_new_from_data(
        engine_client: &dyn EngineClient,
        data: &dyn EngineData,
    ) -> DeltaResult<Add> {
        let extractor = engine_client.get_data_extactor();
        let mut visitor = Visitor::new(visit_add);
        let schema = StructType::new(vec![crate::actions::schemas::ADD_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .unwrap_or_else(|| Err(Error::Generic("Didn't get expected Add".to_string())))
    }

    pub fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

pub(crate) fn visit_add(_row_index: usize, vals: &[Option<DataItem<'_>>]) -> DeltaResult<Add> {
    let path = extract_required_item!(
        vals[0],
        as_str,
        "Add",
        "Add must have path",
        "path must be str"
    )
    .to_string();

    // TODO(nick): Support partition_values

    let size = extract_required_item!(
        vals[2],
        as_i64,
        "Add",
        "Add must have size",
        "size must be i64"
    );

    let modification_time = extract_required_item!(
        vals[3],
        as_i64,
        "Add",
        "Add must have modification_time",
        "modification_time must be i64"
    );

    let data_change = extract_required_item!(
        vals[4],
        as_bool,
        "Add",
        "Add must have data_change",
        "modification_time must be bool"
    );

    let stats = extract_opt_item!(vals[5], as_str, "Add", "stats must be str");

    // TODO(nick) extract tags at vals[6]

    let deletion_vector = if vals[7].is_some() {
        // there is a storageType, so the whole DV must be there
        let storage_type = extract_required_item!(
            vals[7],
            as_str,
            "Add",
            "DV must have storageType",
            "storageType must be a string"
        )
        .to_string();

        let path_or_inline_dv = extract_required_item!(
            vals[8],
            as_str,
            "Add",
            "DV must have pathOrInlineDv",
            "pathOrInlineDv must be a string"
        )
        .to_string();

        let offset = extract_opt_item!(vals[9], as_i32, "Add", "offset must be i32");

        let size_in_bytes = extract_required_item!(
            vals[10],
            as_i32,
            "Add",
            "DV must have sizeInBytes",
            "sizeInBytes must be i32"
        );

        let cardinality = extract_required_item!(
            vals[11],
            as_i64,
            "Add",
            "DV must have cardinality",
            "cardinality must be i64"
        );

        Some(DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        })
    } else {
        None
    };

    let base_row_id = extract_opt_item!(vals[12], as_i64, "Add", "base_row_id must be i64");

    let default_row_commit_version = extract_opt_item!(
        vals[13],
        as_i64,
        "Add",
        "default_row_commit_version must be i64"
    );

    Ok(Add {
        path,
        partition_values: HashMap::new(),
        size,
        modification_time,
        data_change,
        stats: stats.map(|s| s.to_string()),
        tags: HashMap::new(),
        deletion_vector,
        base_row_id,
        default_row_commit_version,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) deletion_timestamp: Option<i64>,

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
    // _try_new_from_data for now, to avoid warning, probably will need at some point
    pub(crate) fn _try_new_from_data(
        engine_client: &dyn EngineClient,
        data: &dyn EngineData,
    ) -> DeltaResult<Remove> {
        let extractor = engine_client.get_data_extactor();
        let mut visitor = Visitor::new(visit_remove);
        let schema = StructType::new(vec![crate::actions::schemas::REMOVE_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .unwrap_or_else(|| Err(Error::Generic("Didn't get expected remove".to_string())))
    }

    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

pub(crate) fn visit_remove(
    _row_index: usize,
    vals: &[Option<DataItem<'_>>],
) -> DeltaResult<Remove> {
    let path = extract_required_item!(
        vals[0],
        as_str,
        "Remove",
        "Remove must have path",
        "path must be str"
    )
    .to_string();

    let deletion_timestamp =
        extract_opt_item!(vals[1], as_i64, "Remove", "deletion_timestamp must be i64");

    let data_change = extract_required_item!(
        vals[2],
        as_bool,
        "Remove",
        "Remove must have data_change",
        "data_change must be a bool"
    );

    let extended_file_metadata = extract_opt_item!(
        vals[3],
        as_bool,
        "Remove",
        "extended_file_metadata must be bool"
    );

    // TODO(nick) handle partition values in vals[4]

    let size = extract_opt_item!(vals[5], as_i64, "Remove", "size must be i64");

    // TODO(nick) stats are skipped in vals[6] and tags are skipped in vals[7]

    let deletion_vector = if vals[8].is_some() {
        // there is a storageType, so the whole DV must be there
        let storage_type = extract_required_item!(
            vals[8],
            as_str,
            "Remove",
            "DV must have storageType",
            "storageType must be a string"
        )
        .to_string();

        let path_or_inline_dv = extract_required_item!(
            vals[9],
            as_str,
            "Remove",
            "DV must have pathOrInlineDv",
            "pathOrInlineDv must be a string"
        )
        .to_string();

        let offset = extract_opt_item!(vals[10], as_i32, "Remove", "offset must be i32");

        let size_in_bytes = extract_required_item!(
            vals[11],
            as_i32,
            "Remove",
            "DV must have sizeInBytes",
            "sizeInBytes must be i32"
        );

        let cardinality = extract_required_item!(
            vals[12],
            as_i64,
            "Remove",
            "DV must have cardinality",
            "cardinality must be i64"
        );

        Some(DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        })
    } else {
        None
    };

    let base_row_id = extract_opt_item!(vals[13], as_i64, "Remove", "base_row_id must be i64");

    let default_row_commit_version = extract_opt_item!(
        vals[14],
        as_i64,
        "Remove",
        "default_row_commit_version must be i64"
    );

    Ok(Remove {
        path,
        data_change,
        deletion_timestamp,
        extended_file_metadata,
        partition_values: None,
        size,
        tags: None,
        deletion_vector,
        base_row_id,
        default_row_commit_version,
    })
}

pub(crate) fn treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    fn combine(high_bits: u32, low_bits: u32) -> usize {
        ((u64::from(high_bits) << 32) | u64::from(low_bits)) as usize
    }

    match treemap.max() {
        Some(max) => {
            // there are values in the map
            //TODO(nick) panic if max is > MAX_USIZE
            let mut result = vec![true; max as usize + 1];
            let bitmaps = treemap.bitmaps();
            for (index, bitmap) in bitmaps {
                for bit in bitmap.iter() {
                    let vec_index = combine(index, bit);
                    result[vec_index] = false;
                }
            }
            result
        }
        None => {
            // empty set, return empty vec
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use roaring::RoaringTreemap;
    use url::Url;

    use crate::{simple_client::SimpleClient, EngineClient};

    use super::DeletionVectorDescriptor;

    fn dv_relateive() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "p".to_string(),
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "i".to_string(),
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[test]
    fn test_deletion_vector_absolute_path() {
        let parent = Url::parse("s3://mytable/").unwrap();

        let relative = dv_relateive();
        let expected =
            Url::parse("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, relative.absolute_path(&parent).unwrap().unwrap());

        let absolute = dv_absolute();
        let expected =
            Url::parse("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, absolute.absolute_path(&parent).unwrap().unwrap());

        let inline = dv_inline();
        assert_eq!(None, inline.absolute_path(&parent).unwrap());

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    #[test]
    fn test_deletion_vector_read() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let simple_client = SimpleClient::new();
        let fs_client = simple_client.get_file_system_client();

        let example = dv_example();
        let tree_map = example.read(fs_client, parent).unwrap();

        let expected: Vec<u64> = vec![0, 9];
        let found = tree_map.iter().collect::<Vec<_>>();
        assert_eq!(found, expected)
    }

    // this test is ignored by default as it's expensive to allocate such big vecs full of `true`. you can run it via:
    // cargo test actions::action_definitions::tests::test_dv_to_bools
    #[test]
    #[ignore]
    fn test_dv_to_bools() {
        let mut rb = RoaringTreemap::new();
        rb.insert(0);
        rb.insert(2);
        rb.insert(7);
        rb.insert(30854);
        rb.insert(4294967297);
        rb.insert(4294967300);
        let bools = super::treemap_to_bools(rb);
        let mut expected = vec![true; 4294967301];
        expected[0] = false;
        expected[2] = false;
        expected[7] = false;
        expected[30854] = false;
        expected[4294967297] = false;
        expected[4294967300] = false;
        assert_eq!(bools, expected);
    }
}
