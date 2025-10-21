//! Definitions of errors that the delta kernel can encounter

use std::{
    backtrace::{Backtrace, BacktraceStatus},
    num::ParseIntError,
    str::Utf8Error,
};

use crate::schema::{DataType, StructType};
use crate::table_properties::ParseIntervalError;
use crate::Version;

#[cfg(feature = "default-engine-base")]
use crate::arrow::error::ArrowError;
#[cfg(feature = "default-engine-base")]
use object_store;

/// A [`std::result::Result`] that has the kernel [`Error`] as the error variant
pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

/// All the types of errors that the kernel can run into
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// This is an error that includes a backtrace. To have a particular type of error include such
    /// backtrace (when RUST_BACKTRACE=1), annotate the error with `#[error(transparent)]` and then
    /// add the error type and enum variant to the `from_with_backtrace!` macro invocation
    /// below. See IOError for an example.
    #[error("{source}\n{backtrace}")]
    Backtraced {
        source: Box<Self>,
        backtrace: Box<Backtrace>,
    },

    /// An error performing operations on arrow data
    #[cfg(feature = "default-engine-base")]
    #[error(transparent)]
    Arrow(ArrowError),

    #[error("Error writing checkpoint: {0}")]
    CheckpointWrite(String),

    /// User tried to convert engine data to the wrong type
    #[error("Invalid engine data type. Could not convert to {0}")]
    EngineDataType(String),

    /// Could not extract the specified type
    #[error("Error extracting type {0}: {1}")]
    Extract(&'static str, &'static str),

    /// A generic error with a message
    #[error("Generic delta kernel error: {0}")]
    Generic(String),

    /// A generic error wrapping another error
    #[error("Generic error: {source}")]
    GenericError {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Some kind of [`std::io::Error`]
    #[error(transparent)]
    IOError(std::io::Error),

    /// An internal error that means kernel found an unexpected situation, which is likely a bug
    #[error("Internal error {0}. This is a kernel bug, please report.")]
    InternalError(String),

    /// An error enountered while working with parquet data
    #[cfg(feature = "default-engine-base")]
    #[error("Arrow error: {0}")]
    Parquet(#[from] crate::parquet::errors::ParquetError),

    /// An error interacting with the object_store crate
    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    #[cfg(feature = "default-engine-base")]
    #[error("Error interacting with object store: {0}")]
    ObjectStore(object_store::Error),

    /// An error working with paths from the object_store crate
    #[cfg(feature = "default-engine-base")]
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[cfg(feature = "default-engine-base")]
    #[error("Reqwest Error: {0}")]
    Reqwest(#[from] reqwest::Error),

    /// A specified file could not be found
    #[error("File not found: {0}")]
    FileNotFound(String),

    /// A column was requested, but not found
    #[error("{0}")]
    MissingColumn(String),

    /// A column was specified with a specific type, but it is not of that type
    #[error("Expected column type: {0}")]
    UnexpectedColumnType(String),

    /// Data was expected, but not found
    #[error("Expected is missing: {0}")]
    MissingData(String),

    /// A version for the delta table could not be found in the log
    #[error("No table version found.")]
    MissingVersion,

    /// An error occurred while working with deletion vectors
    #[error("Deletion Vector error: {0}")]
    DeletionVector(String),

    /// A selection vector is larger than data length
    #[error("Selection vector is larger than data length: {0}")]
    InvalidSelectionVector(String),

    /// A specified URL was invalid
    #[error("Invalid url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// serde encountered malformed json
    #[error(transparent)]
    MalformedJson(serde_json::Error),

    /// There was no metadata action in the delta log
    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    /// There was no protocol action in the delta log
    #[error("No protocol found in delta log.")]
    MissingProtocol,

    /// Invalid protocol action was read from the log
    #[error("Invalid protocol action in the delta log: {0}")]
    InvalidProtocol(String),

    /// Neither metadata nor protocol could be found in the delta log
    #[error("No table metadata or protocol found in delta log.")]
    MissingMetadataAndProtocol,

    /// A string failed to parse as the specified data type
    #[error("Failed to parse value '{0}' as '{1}'")]
    ParseError(String, DataType),

    /// A tokio executor failed to join a task
    #[error("Join failure: {0}")]
    JoinFailure(String),

    /// Could not convert to string from utf-8
    #[error("Could not convert to string from utf-8: {0}")]
    Utf8Error(#[from] Utf8Error),

    /// Could not parse an integer
    #[error("Could not parse int: {0}")]
    ParseIntError(#[from] ParseIntError),

    #[error("Invalid column mapping mode: {0}")]
    InvalidColumnMappingMode(String),

    /// Asked for a table at an invalid location
    #[error("Invalid table location: {0}.")]
    InvalidTableLocation(String),

    /// Precision or scale not compliant with delta specification
    #[error("Invalid decimal: {0}")]
    InvalidDecimal(String),

    /// Inconsistent data passed to struct scalar
    #[error("Invalid struct data: {0}")]
    InvalidStructData(String),

    /// Expressions did not parse or evaluate correctly
    #[error("Invalid expression evaluation: {0}")]
    InvalidExpressionEvaluation(String),

    /// Unable to parse the name of a log path
    #[error("Invalid log path: {0}")]
    InvalidLogPath(String),

    /// The file already exists at the path, prohibiting a non-overwrite write
    #[error("File already exists: {0}")]
    FileAlreadyExists(String),

    /// Some functionality is currently unsupported
    #[error("Unsupported: {0}")]
    Unsupported(String),

    /// Parsing error when attempting to deserialize an interval
    #[error(transparent)]
    ParseIntervalError(#[from] ParseIntervalError),

    #[error("Change data feed is unsupported for the table at version {0}")]
    ChangeDataFeedUnsupported(Version),

    #[error("Change data feed encountered incompatible schema. Expected {0}, got {1}")]
    ChangeDataFeedIncompatibleSchema(String, String),

    /// Invalid checkpoint files
    #[error("Invalid Checkpoint: {0}")]
    InvalidCheckpoint(String),

    /// Error while transforming a schema + leaves into an Expression of literals
    #[error(transparent)]
    LiteralExpressionTransformError(
        #[from] crate::expressions::literal_expression_transform::Error,
    ),

    /// Schema mismatch has occurred or invalid schema used somewhere
    #[error("Schema error: {0}")]
    Schema(String),
}

// Convenience constructors for Error types that take a String argument
impl Error {
    pub(crate) fn checkpoint_write(msg: impl ToString) -> Self {
        Self::CheckpointWrite(msg.to_string())
    }

    pub fn generic_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::GenericError {
            source: source.into(),
        }
    }
    pub fn generic(msg: impl ToString) -> Self {
        Self::Generic(msg.to_string())
    }
    pub fn file_not_found(path: impl ToString) -> Self {
        Self::FileNotFound(path.to_string())
    }
    pub fn missing_column(name: impl ToString) -> Self {
        Self::MissingColumn(name.to_string()).with_backtrace()
    }
    pub fn unexpected_column_type(name: impl ToString) -> Self {
        Self::UnexpectedColumnType(name.to_string())
    }
    pub fn missing_data(name: impl ToString) -> Self {
        Self::MissingData(name.to_string())
    }
    pub fn deletion_vector(msg: impl ToString) -> Self {
        Self::DeletionVector(msg.to_string())
    }
    pub fn engine_data_type(msg: impl ToString) -> Self {
        Self::EngineDataType(msg.to_string())
    }
    pub fn join_failure(msg: impl ToString) -> Self {
        Self::JoinFailure(msg.to_string())
    }
    pub fn invalid_table_location(location: impl ToString) -> Self {
        Self::InvalidTableLocation(location.to_string())
    }
    pub fn invalid_column_mapping_mode(mode: impl ToString) -> Self {
        Self::InvalidColumnMappingMode(mode.to_string())
    }
    pub fn invalid_decimal(msg: impl ToString) -> Self {
        Self::InvalidDecimal(msg.to_string())
    }
    pub fn invalid_struct_data(msg: impl ToString) -> Self {
        Self::InvalidStructData(msg.to_string())
    }
    pub fn invalid_expression(msg: impl ToString) -> Self {
        Self::InvalidExpressionEvaluation(msg.to_string())
    }
    pub(crate) fn invalid_log_path(msg: impl ToString) -> Self {
        Self::InvalidLogPath(msg.to_string())
    }

    pub fn internal_error(msg: impl ToString) -> Self {
        Self::InternalError(msg.to_string()).with_backtrace()
    }

    pub fn invalid_protocol(msg: impl ToString) -> Self {
        Self::InvalidProtocol(msg.to_string())
    }

    pub fn unsupported(msg: impl ToString) -> Self {
        Self::Unsupported(msg.to_string())
    }
    pub fn change_data_feed_unsupported(version: impl Into<Version>) -> Self {
        Self::ChangeDataFeedUnsupported(version.into())
    }
    pub(crate) fn change_data_feed_incompatible_schema(
        expected: &StructType,
        actual: &StructType,
    ) -> Self {
        Self::ChangeDataFeedIncompatibleSchema(format!("{expected:?}"), format!("{actual:?}"))
    }

    pub fn invalid_checkpoint(msg: impl ToString) -> Self {
        Self::InvalidCheckpoint(msg.to_string())
    }

    pub(crate) fn schema(msg: impl ToString) -> Self {
        Self::Schema(msg.to_string())
    }

    // Capture a backtrace when the error is constructed.
    #[must_use]
    pub fn with_backtrace(self) -> Self {
        let backtrace = Backtrace::capture();
        match backtrace.status() {
            BacktraceStatus::Captured => Self::Backtraced {
                source: Box::new(self),
                backtrace: Box::new(backtrace),
            },
            _ => self,
        }
    }
}

macro_rules! from_with_backtrace(
    ( $(($error_type: ty, $error_variant: ident)), * ) => {
        $(
            impl From<$error_type> for Error {
                fn from(value: $error_type) -> Self {
                    Self::$error_variant(value).with_backtrace()
                }
            }
        )*
    };
);

from_with_backtrace!(
    (serde_json::Error, MalformedJson),
    (std::io::Error, IOError)
);

#[cfg(feature = "default-engine-base")]
impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value).with_backtrace()
    }
}

#[cfg(feature = "default-engine-base")]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        match value {
            object_store::Error::NotFound { path, .. } => Self::file_not_found(path),
            err => Self::ObjectStore(err),
        }
    }
}
