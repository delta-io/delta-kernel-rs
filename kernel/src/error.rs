use std::{
    backtrace::{Backtrace, BacktraceStatus},
    num::ParseIntError,
    string::FromUtf8Error,
};

use crate::schema::DataType;

pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{source}\n{backtrace}")]
    Backtraced {
        source: Box<Self>,
        backtrace: Box<Backtrace>,
    },

    #[cfg(any(feature = "default-client", feature = "sync-client"))]
    #[error(transparent)]
    Arrow(arrow_schema::ArrowError),

    #[error("Invalid engine data type. Could not convert to {0}")]
    EngineDataType(String),

    #[error("Error extracting type {0}: {1}")]
    Extract(&'static str, &'static str),

    #[error("Generic delta kernel error: {0}")]
    Generic(String),

    #[error("Generic error: {source}")]
    GenericError {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),

    #[cfg(feature = "parquet")]
    #[error("Arrow error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    #[cfg(feature = "object_store")]
    #[error("Error interacting with object store: {0}")]
    ObjectStore(object_store::Error),

    #[cfg(feature = "object_store")]
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("{0}")]
    MissingColumn(String),

    #[error("Expected column type: {0}")]
    UnexpectedColumnType(String),

    #[error("Expected is missing: {0}")]
    MissingData(String),

    #[error("No table version found.")]
    MissingVersion,

    #[error("Deletion Vector error: {0}")]
    DeletionVector(String),

    #[error("Invalid url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Invalid url: {0}")]
    MalformedJson(#[from] serde_json::Error),

    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    #[error("No protocol found in delta log.")]
    MissingProtocol,

    #[error("No table metadata or protocol found in delta log.")]
    MissingMetadataAndProtocol,

    #[error("Failed to parse value '{0}' as '{1}'")]
    ParseError(String, DataType),

    #[error("Join failure: {0}")]
    JoinFailure(String),

    #[error("Could not convert to string from utf-8: {0}")]
    Utf8Error(#[from] FromUtf8Error),

    #[error("Could not parse int: {0}")]
    ParseIntError(#[from] ParseIntError),
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

#[cfg(any(feature = "default-client", feature = "sync-client"))]
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
