use std::collections::HashMap;

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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn new(
        id: impl Into<String>,
        format: Format,
        schema_string: impl Into<String>,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
        configuration: Option<HashMap<String, Option<String>>>,
    ) -> Self {
        Self {
            id: id.into(),
            format,
            schema_string: schema_string.into(),
            partition_columns: partition_columns.into_iter().map(|c| c.into()).collect(),
            configuration: configuration.unwrap_or_default(),
            name: None,
            description: None,
            created_time: None,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_created_time(mut self, created_time: i64) -> Self {
        self.created_time = Some(created_time);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_wrriter_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    pub reader_features: Option<Vec<String>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    pub writer_features: Option<Vec<String>>,
}

impl Protocol {
    pub fn new(min_reader_version: i32, min_wrriter_version: i32) -> Self {
        Self {
            min_reader_version,
            min_wrriter_version,
            reader_features: None,
            writer_features: None,
        }
    }

    pub fn with_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.reader_features = Some(reader_features.into_iter().map(|c| c.into()).collect());
        self
    }

    pub fn with_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.writer_features = Some(writer_features.into_iter().map(|c| c.into()).collect());
        self
    }
}
