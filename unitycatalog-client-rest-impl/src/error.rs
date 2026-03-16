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

    #[error("Max unpublished commits exceeded (max: {0})")]
    MaxUnpublishedCommitsExceeded(u16),

    #[error("Generic Error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for unitycatalog_client_api::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::TableNotFound(msg) => unitycatalog_client_api::Error::TableNotFound(msg),
            Error::AuthenticationFailed => unitycatalog_client_api::Error::AuthenticationFailed,
            Error::UnsupportedOperation(msg) => {
                unitycatalog_client_api::Error::UnsupportedOperation(msg)
            }
            Error::MaxUnpublishedCommitsExceeded(max) => {
                unitycatalog_client_api::Error::MaxUnpublishedCommitsExceeded(max)
            }
            Error::Generic(msg) => unitycatalog_client_api::Error::Generic(msg),
            e => unitycatalog_client_api::Error::Generic(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_error_maps_named_variants_directly() {
        assert!(matches!(
            unitycatalog_client_api::Error::from(Error::TableNotFound("t1".into())),
            unitycatalog_client_api::Error::TableNotFound(msg) if msg == "t1"
        ));
        assert!(matches!(
            unitycatalog_client_api::Error::from(Error::AuthenticationFailed),
            unitycatalog_client_api::Error::AuthenticationFailed
        ));
        assert!(matches!(
            unitycatalog_client_api::Error::from(Error::UnsupportedOperation("op".into())),
            unitycatalog_client_api::Error::UnsupportedOperation(msg) if msg == "op"
        ));
        assert!(matches!(
            unitycatalog_client_api::Error::from(Error::MaxUnpublishedCommitsExceeded(5)),
            unitycatalog_client_api::Error::MaxUnpublishedCommitsExceeded(5)
        ));
        assert!(matches!(
            unitycatalog_client_api::Error::from(Error::Generic("oops".into())),
            unitycatalog_client_api::Error::Generic(msg) if msg == "oops"
        ));
    }

    #[test]
    fn from_error_maps_rest_only_variants_to_generic() {
        // REST-specific errors (no counterpart in the API error type) become Generic.
        let api_err = unitycatalog_client_api::Error::from(Error::MaxRetriesExceeded);
        assert!(
            matches!(api_err, unitycatalog_client_api::Error::Generic(ref msg) if msg == "Max retries exceeded"),
            "unexpected: {api_err:?}"
        );

        let api_err =
            unitycatalog_client_api::Error::from(Error::InvalidConfiguration("bad".into()));
        assert!(
            matches!(api_err, unitycatalog_client_api::Error::Generic(ref msg) if msg == "Invalid configuration: bad"),
            "unexpected: {api_err:?}"
        );
    }
}
