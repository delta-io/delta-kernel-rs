//! Defintions of errors that the delta kernel can encounter

use std::{
    backtrace::{Backtrace, BacktraceStatus},
    num::ParseIntError,
    str::Utf8Error,
};

use crate::schema::DataType;

/// A [`std::result::Result`] that has the kernel [`Error`] as the error variant
pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

/// All the types of errors that the kernel can run into
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
    #[cfg(any(feature = "default-engine", feature = "sync-engine"))]
    #[error(transparent)]
    Arrow(arrow_schema::ArrowError),

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
    #[cfg(feature = "parquet")]
    #[error("Arrow error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// An error interacting with the object_store crate
    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    #[cfg(feature = "object_store")]
    #[error("Error interacting with object store: {0}")]
    ObjectStore(object_store::Error),

    /// An error working with paths from the object_store crate
    #[cfg(feature = "object_store")]
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[cfg(feature = "default-engine")]
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

    /// An error occured while working with deletion vectors
    #[error("Deletion Vector error: {0}")]
    DeletionVector(String),

    /// A specified URL was invalid
    #[error("Invalid url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// serde enountered malformed json
    #[error(transparent)]
    MalformedJson(serde_json::Error),

    /// There was no metadata action in the delta log
    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    /// There was no protocol action in the delta log
    #[error("No protocol found in delta log.")]
    MissingProtocol,

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
    #[error("Inavlid decimal: {0}")]
    InvalidDecimal(String),

    /// Incosistent data passed to struct scalar
    #[error("Invalid struct data: {0}")]
    InvalidStructData(String),
}

// Convenience constructors for Error types that take a String argument
impl Error {
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
        Self::MissingColumn(name.to_string())
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

    pub fn internal_error(msg: impl ToString) -> Self {
        Self::InternalError(msg.to_string()).with_backtrace()
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

#[cfg(any(feature = "default-engine", feature = "sync-engine"))]
impl From<arrow_schema::ArrowError> for Error {
    fn from(value: arrow_schema::ArrowError) -> Self {
        Self::Arrow(value).with_backtrace()
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        match value {
            object_store::Error::NotFound { path, .. } => Self::file_not_found(path),
            err => Self::ObjectStore(err),
        }
    }
}
