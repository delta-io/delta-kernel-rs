use crate::arrow::error::ArrowError;
use crate::object_store;
use crate::parquet::errors::ParquetError;

/// A [`std::result::Result`] that has the engine [`Error`] as the error variant
pub type EngineResult<T, E = EngineError> = std::result::Result<T, E>;

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error(transparent)]
    KernelError(#[from] delta_kernel::Error),

    /// An error performing operations on arrow data
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// An error enountered while working with parquet data
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    /// An error interacting with the object_store crate
    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    // FIXME
    #[error("Error interacting with object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// An error working with paths from the object_store crate
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error("Reqwest Error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("URL ParseError: {0}")]
    Url(#[from] url::ParseError),

    #[error("Tokio JoinError: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
}

impl From<EngineError> for delta_kernel::Error {
    fn from(e: EngineError) -> Self {
        Self::Generic(format!("Engine error: {e}"))
    }
}
