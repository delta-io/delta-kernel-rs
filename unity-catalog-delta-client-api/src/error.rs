use thiserror::Error;

/// Errors for Unity Catalog client API traits.
///
/// This error type contains no HTTP-specific variants, allowing any backend
/// (REST, gRPC, in-memory, etc.) to implement the traits without pulling in
/// implementation-specific dependencies.
#[derive(Error, Debug)]
pub enum Error {
    /// The requested table was not found.
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// The number of unpublished commits has exceeded the maximum allowed.
    #[error("Max unpublished commits exceeded (max: {0})")]
    MaxUnpublishedCommitsExceeded(u16),

    /// The requested operation is not supported.
    #[error("Operation not supported: {0}")]
    UnsupportedOperation(String),

    /// Authentication with Unity Catalog failed.
    #[error("Authentication failed")]
    AuthenticationFailed,

    /// A generic error with a descriptive message.
    #[error("{0}")]
    Generic(String),
}

/// A type alias for [`Result`] using [`enum@Error`].
///
/// [`Result`]: std::result::Result
pub type Result<T> = std::result::Result<T, Error>;
