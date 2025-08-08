use std::collections::HashMap;

use chrono::Utc;
use url::Url;

use crate::actions::{
    get_log_commit_info_schema, get_log_metadata_schema, get_log_protocol_schema, CommitInfo,
    Metadata, Protocol,
};
use crate::path::ParsedLogPath;
use crate::schema::StructType;
use crate::table_features::{
    ReaderFeature, WriterFeature, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::{DeltaResult, Engine, IntoEngineData};

#[cfg(test)]
mod tests;

// default to 3, 7 with all our supported table features
const DEFAULT_MIN_READER_VERSION: i32 = 3;
const DEFAULT_MIN_WRITER_VERSION: i32 = 7;

const DEFAULT_CREATE_TABLE_OPERATION: &str = "CREATE";

/// Builder to create a delta table. Pragmatically, this will just create a new table with all the
/// specified configuration, schema, features at the specified root URL.
///
/// If only the minimal configuration (schema and table root) is provided, it will create a table
/// with the default minimum reader version of 3, minimum writer version of 7, and all table
/// features that kernel supports enabled.
///
/// Note that this builder does not create the table directory or the `_delta_log` directory if
/// they do not already exist. It only writes the initial commit to the `_delta_log` directory at
/// the time of calling `create()`.
///
/// Additionally note: this lazily validates configuration (that is, validates on `create()` call).
///
/// Currently, only CREATE is supported. In the future, REPLACE or CREATE OR REPLACE may be
/// introduced. As a workaround for the REPLACE use cases, users can delete the entire table
/// directory then call CREATE.
#[derive(Debug)]
pub struct CreateTableBuilder {
    table_root: Url,
    schema: StructType,
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Option<Vec<ReaderFeature>>,
    writer_features: Option<Vec<WriterFeature>>,
    partition_columns: Vec<String>,
    timestamp: Option<i64>,
    table_name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, String>>,
    operation: String,
    engine_info: Option<String>,
}

impl CreateTableBuilder {
    /// Create a new `CreateTable` builder for a table root and schema.
    pub fn new(table_root: Url, schema: StructType) -> Self {
        Self {
            table_root,
            schema,
            min_reader_version: DEFAULT_MIN_READER_VERSION,
            min_writer_version: DEFAULT_MIN_WRITER_VERSION,
            reader_features: Some(SUPPORTED_READER_FEATURES.to_vec()),
            writer_features: Some(SUPPORTED_WRITER_FEATURES.to_vec()),
            partition_columns: vec![],
            timestamp: None,
            table_name: None,
            description: None,
            configuration: None,
            operation: DEFAULT_CREATE_TABLE_OPERATION.to_string(),
            engine_info: None,
        }
    }

    /// Create an 'empty' table (i.e., a table with no columns in schema) with the given root URL.
    pub fn empty(table_root: Url) -> Self {
        Self::new(table_root, StructType::new([]))
    }

    /// Set the commit timestamp (in ms since unix epoch) for the table creation commit. If not set,
    /// the current UTC time in ms since unix epoch is used.
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set minimum reader version. Note that if the version is less than 3, reader features must
    /// be empty (reader features are not supported in versions < 3).
    pub fn with_min_reader_version(mut self, version: i32) -> Self {
        self.min_reader_version = version;
        self
    }

    /// Set minimum writer version. Note that if the version is less than 7, writer features must
    /// be empty (writer features are not supported in versions < 7).
    pub fn with_min_writer_version(mut self, version: i32) -> Self {
        self.min_writer_version = version;
        self
    }

    /// Set reader features. Note that the min_reader_version must be at least 3 in order to
    /// support table features.
    // TODO: should we eagerly check?
    pub fn with_reader_features(mut self, features: impl Into<Vec<ReaderFeature>>) -> Self {
        self.reader_features = Some(features.into());
        self
    }

    /// Set writer features. Note that the min_writer_version must be at least 7 in order to
    /// support table features.
    // TODO: should we eagerly check?
    pub fn with_writer_features(mut self, features: impl Into<Vec<WriterFeature>>) -> Self {
        self.writer_features = Some(features.into());
        self
    }

    /// Set partition columns for this table.
    pub fn with_partition_columns(mut self, columns: impl Into<Vec<String>>) -> Self {
        self.partition_columns = columns.into();
        self
    }

    /// Set the table name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = Some(name.into());
        self
    }

    /// Set the table description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the table configuration.
    // TODO: what sort of API do we want for configuration and/or feature enablement? This allows a
    // lot of flexibility, but if user just wants to e.g. enable ICT or append-only, they have to
    // set the table features then additionally enable those features in the configuration
    // manually.
    pub fn with_configuration(mut self, config: HashMap<String, String>) -> Self {
        self.configuration = Some(config);
        self
    }

    /// Set the `commitInfo.operation` field. This is used to indicate the operation that was
    /// occurring when the commit was made. The default is "CREATE".
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }

    /// Set the `commitInfo.engineInfo` field. This is used to identify the engine that was used to
    /// make the commit. The default is `None`, which means no engine information is recorded.
    pub fn with_engine_commit_info(mut self, engine_info: impl Into<String>) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    /// Create the table. This will write the initial commit to the `_delta_log` directory at the
    /// root of the table. Note that this does not create the table directory nor the `_delta_log`
    /// directory if they do not already exist.
    pub fn create(self, engine: &dyn Engine) -> DeltaResult<()> {
        // creating a table requires the following steps:
        // 1. create Metadata/Protocol actions to commit (ensure protocol is supported)
        // 2. create CommitInfo action
        // 3. write out <table_root>/_delta_log/00000000000000000000.json with the actions
        //
        // For now, we take two shortcuts:
        // 1. we don't allow any other metadata/data operations during this commit (0.json only has
        //    Protocol and Metadata and CommitInfo actions)
        // 2. we don't return a snapshot of the table we just created. Note that we could just do
        //    duplicate work to construct a snapshot to hand back, but that would just be the same
        //    as the user calling `Snapshot::try_new` themselves.
        let log_root = self.table_root.join("_delta_log")?;
        let timestamp = self
            .timestamp
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        let metadata = Metadata::try_new(
            self.table_name,
            self.description,
            self.schema,
            self.partition_columns,
            timestamp,
            self.configuration.unwrap_or_default(),
        )?;
        let protocol = Protocol::try_new(
            self.min_reader_version,
            self.min_writer_version,
            self.reader_features,
            self.writer_features,
        )?;

        protocol.ensure_read_supported()?;
        protocol.ensure_write_supported()?;

        // The only dependency that CommitInfo has on other metadata is ICT configuration. Since we
        // don't yet support ICT we can just create CommitInfo in isolation.
        let commit_info = CommitInfo::new(timestamp, Some(self.operation), self.engine_info);

        let actions = vec![
            commit_info.into_engine_data(get_log_commit_info_schema().clone(), engine),
            protocol.into_engine_data(get_log_protocol_schema().clone(), engine),
            metadata.into_engine_data(get_log_metadata_schema().clone(), engine),
        ];

        let json_handler = engine.json_handler();
        let commit_path = ParsedLogPath::new_commit(&log_root, 0)?;
        json_handler.write_json_file(
            &commit_path.location,
            Box::new(actions.into_iter()),
            false,
        )?;

        Ok(())
    }
}
