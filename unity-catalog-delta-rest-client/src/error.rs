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
    use rstest::rstest;
    use unity_catalog_delta_client_api::Error as ApiError;

    use super::*;

    /// Each rest client error that wraps a UC delta client API error (Error::API)
    /// preserves the API error.
    #[rstest]
    #[case(ApiError::TableNotFound("t1".into()))]
    #[case(ApiError::AuthenticationFailed)]
    #[case(ApiError::MaxUnpublishedCommitsExceeded(5))]
    #[case(ApiError::CommitConflict)]
    #[case(ApiError::InvalidCommit("bad".into()))]
    #[case(ApiError::RateLimited)]
    fn from_error_unwraps_api_variant(#[case] api_err: ApiError) {
        let expected = format!("{api_err:?}");
        let unwrapped = ApiError::from(Error::Api(api_err));
        assert_eq!(format!("{unwrapped:?}"), expected);
    }

    /// Rest client-only variants have no API counterpart, so they collapse to
    /// `Generic` carrying the display message.
    #[rstest]
    #[case(Error::MaxRetriesExceeded, "Max retries exceeded")]
    #[case(
        Error::InvalidConfiguration("bad".into()),
        "Invalid configuration: bad"
    )]
    fn from_error_maps_rest_only_variants_to_generic(
        #[case] rest_err: Error,
        #[case] expected_msg: &str,
    ) {
        let api_err = ApiError::from(rest_err);
        assert!(
            matches!(api_err, ApiError::Generic(ref msg) if msg == expected_msg),
            "unexpected: {api_err:?}"
        );
    }
}
