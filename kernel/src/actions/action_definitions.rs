//! Define the Delta actions that exist, and how to parse them out of [EngineData]

use std::{collections::HashMap, sync::Arc};

use tracing::debug;

use crate::{
    engine_data::{DataItem, DataVisitor, EngineData},
    schema::StructType,
    EngineClient,
    DeltaResult, Error,
};

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
        let mut visitor = MetadataVisitor::default();
        let schema = StructType::new(vec![crate::actions::schemas::METADATA_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .ok_or(Error::Generic("Failed to extract metadata".to_string()))
    }

    pub fn schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }
}

#[derive(Default)]
pub struct MetadataVisitor {
    pub(crate) extracted: Option<Metadata>,
}

impl DataVisitor for MetadataVisitor {
    fn visit(&mut self, vals: &[Option<DataItem<'_>>]) {
        let id = vals[0]
            .as_ref()
            .expect("MetaData must have an id")
            .as_str()
            .expect("id must be str");
        let name = vals[1]
            .as_ref()
            .map(|name_data| name_data.as_str().expect("name must be a str").to_string());
        let description = vals[2].as_ref().map(|desc_data| {
            desc_data
                .as_str()
                .expect("description must be a str")
                .to_string()
        });
        // get format out of primitives
        let format_provider = vals[3]
            .as_ref()
            .expect("format.provider must exist")
            .as_str()
            .expect("format.provider must be a str")
            .to_string();

        // todo: extract relevant values out of the options map at vals[4]

        let schema_string = vals[5]
            .as_ref()
            .expect("schema_string must exist")
            .as_str()
            .expect("schema_string must be a str")
            .to_string();

        // todo: partition_columns from vals[6]

        let created_time = vals[7]
            .as_ref()
            .expect("Action must have a created_time")
            .as_i64()
            .expect("created_time must be i64");

        // todo: config vals from vals[8]

        let extracted = Metadata {
            id: id.to_string(),
            name,
            description,
            format: Format {
                provider: format_provider,
                options: HashMap::new(),
            },
            schema_string,
            partition_columns: vec![],
            created_time: Some(created_time),
            configuration: HashMap::new(),
        };
        debug!("Extracted: {:#?}", extracted);
        self.extracted = Some(extracted)
    }
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
        let mut visitor = ProtocolVisitor::default();
        let schema = StructType::new(vec![crate::actions::schemas::PROTOCOL_FIELD.clone()]);
        extractor.extract(data, Arc::new(schema), &mut visitor);
        visitor
            .extracted
            .ok_or(Error::Generic("Failed to extract protocol".to_string()))
    }
}

#[derive(Default)]
pub struct ProtocolVisitor {
    pub(crate) extracted: Option<Protocol>,
}

impl DataVisitor for ProtocolVisitor {
    fn visit(&mut self, vals: &[Option<DataItem<'_>>]) {
        let min_reader_version = vals[0]
            .as_ref()
            .expect("Protocol must have a minReaderVersion")
            .as_i32()
            .expect("minReaderVersion must be i32");
        let min_writer_version = vals[1]
            .as_ref()
            .expect("Protocol must have a minWriterVersion")
            .as_i32()
            .expect("minWriterVersion must be i32");



        let reader_features = vals[2].as_ref().map(|rf_di| {
            if let DataItem::StrList(lst) = rf_di {
                lst.iter().map(|f| f.to_string()).collect()
            } else {
                panic!("readerFeatures must be a string list")
            }
        });

        let writer_features = vals[3].as_ref().map(|rf_di| {
            if let DataItem::StrList(lst) = rf_di {
                lst.iter().map(|f| f.to_string()).collect()
            } else {
                panic!("readerFeatures must be a string list")
            }
        });

        let extracted = Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        };
        debug!("Extracted: {:#?}", extracted);
        self.extracted = Some(extracted)
    }
}

