use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("API error (status {status}): {message}")]
    ApiError { status: u16, message: String },

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Operation not supported: {0}")]
    UnsupportedOperation(String),

    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

pub type Result<T> = std::result::Result<T, Error>;
