use thiserror::Error;

/// REST-specific errors for the Unity Catalog client.
///
/// API-level errors (table not found, auth failures, etc.) are wrapped in the
/// [`Api`](Error::Api) variant, keeping them in sync with `unity_catalog_delta_client_api::Error`
/// without duplicating variants.
#[derive(Error, Debug)]
pub enum Error {
    /// A transport-agnostic API error.
    #[error(transparent)]
    Api(#[from] unity_catalog_delta_client_api::Error),

    /// An HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// An invalid HTTP header value was provided.
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    /// A URL could not be parsed.
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    /// JSON serialization or deserialization failed.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// The server returned a non-success HTTP status code.
    #[error("HTTP error (status {status}): {message}")]
    HttpStatusError { status: u16, message: String },

    /// The client configuration is invalid.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// All retry attempts have been exhausted.
    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

/// A type alias for [`Result`] using [`enum@Error`].
///
/// [`Result`]: std::result::Result
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for unity_catalog_delta_client_api::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Api(api_err) => api_err,
            e => unity_catalog_delta_client_api::Error::Generic(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_error_unwraps_api_variant() {
        let api_err = unity_catalog_delta_client_api::Error::TableNotFound("t1".into());
        let rest_err = Error::Api(api_err);
        assert!(matches!(
            unity_catalog_delta_client_api::Error::from(rest_err),
            unity_catalog_delta_client_api::Error::TableNotFound(msg) if msg == "t1"
        ));

        let rest_err = Error::Api(unity_catalog_delta_client_api::Error::AuthenticationFailed);
        assert!(matches!(
            unity_catalog_delta_client_api::Error::from(rest_err),
            unity_catalog_delta_client_api::Error::AuthenticationFailed
        ));

        let rest_err =
            Error::Api(unity_catalog_delta_client_api::Error::MaxUnpublishedCommitsExceeded(5));
        assert!(matches!(
            unity_catalog_delta_client_api::Error::from(rest_err),
            unity_catalog_delta_client_api::Error::MaxUnpublishedCommitsExceeded(5)
        ));
    }

    #[test]
    fn from_error_maps_rest_only_variants_to_generic() {
        let api_err = unity_catalog_delta_client_api::Error::from(Error::MaxRetriesExceeded);
        assert!(
            matches!(api_err, unity_catalog_delta_client_api::Error::Generic(ref msg) if msg == "Max retries exceeded"),
            "unexpected: {api_err:?}"
        );

        let api_err =
            unity_catalog_delta_client_api::Error::from(Error::InvalidConfiguration("bad".into()));
        assert!(
            matches!(api_err, unity_catalog_delta_client_api::Error::Generic(ref msg) if msg == "Invalid configuration: bad"),
            "unexpected: {api_err:?}"
        );
    }
}
