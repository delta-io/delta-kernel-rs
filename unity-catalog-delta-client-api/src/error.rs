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

    /// A different commit has already been ratified at the submitted version.
    ///
    /// Implementations must only return this after confirming a version conflict. An ETag
    /// mismatch or an uncertain request outcome alone is insufficient.
    #[error("Commit version {version} already exists")]
    CommitVersionAlreadyExists {
        /// The rejected commit version.
        version: i64,
    },

    /// The number of unpublished commits has exceeded the maximum allowed.
    #[error("Max unpublished commits exceeded (max: {0})")]
    MaxUnpublishedCommitsExceeded(u16),

    /// A temporary failure known to have prevented acceptance of the update.
    ///
    /// The caller may retry after addressing the failure. Timeouts after sending the update do
    /// not establish that it failed and must not be classified using this variant.
    #[error("Retryable request failure: {0}")]
    Retryable(String),

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
