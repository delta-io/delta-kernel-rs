pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("Arrow error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Error interacting with object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("{0}")]
    MissingColumn(String),

    #[error("Expected column type: {0}")]
    UnexpectedColumnType(String),

    #[error("Expected is missing: {0}")]
    MissingData(String),

    #[error("No table version found.")]
    MissingVersion,

    #[error("Deleteion Vecor error: {0}")]
    DeletionVector(String),
}
