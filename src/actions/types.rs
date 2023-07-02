use std::collections::HashMap;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    pub configuration: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_wrriter_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    pub reader_features: Vec<String>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    pub writer_features: Vec<String>,
}
