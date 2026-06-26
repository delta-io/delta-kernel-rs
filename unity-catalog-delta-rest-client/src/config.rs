use std::time::Duration;

use url::Url;

use crate::error::Result;

/// Default `User-Agent` identifying this client. Some catalogs require a specific `User-Agent`
/// value and reject others; override via [`ClientConfigBuilder::with_user_agent`] when your
/// catalog expects a particular value.
fn default_user_agent() -> String {
    format!(
        "Delta/{v} delta-kernel-rs/{v}",
        v = env!("CARGO_PKG_VERSION")
    )
}

/// Connection configuration for the Unity Catalog REST client.
#[derive(Clone)]
pub struct ClientConfig {
    /// Base workspace URL with the `/api/2.1/unity-catalog/` path stamped on.
    pub workspace_url: Url,
    /// Bearer token used to authenticate requests.
    pub token: String,
    /// `User-Agent` header value sent on every request.
    pub user_agent: String,
    /// Overall request timeout.
    pub timeout: Duration,
    /// Connection-establishment timeout.
    pub connect_timeout: Duration,
    /// Maximum number of retries for retryable requests.
    pub max_retries: u32,
    /// Base delay for linear backoff: the nth retry waits `retry_base_delay * n`, clamped to
    /// `retry_max_delay`.
    pub retry_base_delay: Duration,
    /// Upper bound on the backoff delay between retries.
    pub retry_max_delay: Duration,
}

// Manual Debug to avoid leaking the bearer token.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("workspace_url", &self.workspace_url)
            .field("token", &"***")
            .field("user_agent", &self.user_agent)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_retries", &self.max_retries)
            .field("retry_base_delay", &self.retry_base_delay)
            .field("retry_max_delay", &self.retry_max_delay)
            .finish()
    }
}

impl ClientConfig {
    fn new(workspace: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        let workspace_str = workspace.into();
        // add http(s) prefix if not present
        let base_url =
            if workspace_str.starts_with("http://") || workspace_str.starts_with("https://") {
                workspace_str
            } else {
                format!("https://{workspace_str}")
            };
        // parse the URL
        let mut workspace_url = Url::parse(&base_url)?;
        // normalize with trailing slash
        if !workspace_url.path().ends_with('/') {
            workspace_url.set_path(&format!("{}/", workspace_url.path()));
        }
        workspace_url.set_path(&format!("{}api/2.1/unity-catalog/", workspace_url.path()));

        Ok(Self {
            workspace_url,
            token: token.into(),
            user_agent: default_user_agent(),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
        })
    }

    /// Start building a [`ClientConfig`] for `workspace`, authenticating with `token`.
    pub fn build(workspace: impl Into<String>, token: impl Into<String>) -> ClientConfigBuilder {
        ClientConfigBuilder::new(workspace, token)
    }
}

/// Builder for [`ClientConfig`]. Created via [`ClientConfig::build`].
pub struct ClientConfigBuilder {
    workspace: String,
    token: String,
    user_agent: String,
    timeout: Duration,
    connect_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

impl ClientConfigBuilder {
    fn new(workspace: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            workspace: workspace.into(),
            token: token.into(),
            user_agent: default_user_agent(),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
        }
    }

    /// Override the `User-Agent` header with the value the catalog expects for your connector.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// Set the overall request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the connection-establishment timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the maximum number of retries for retryable requests.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the linear-backoff delays: the nth retry waits `base * n`, clamped to `max`.
    pub fn with_retry_delays(mut self, base: Duration, max: Duration) -> Self {
        self.retry_base_delay = base;
        self.retry_max_delay = max;
        self
    }

    /// Build the [`ClientConfig`], normalizing the workspace URL. Errors if the URL is invalid.
    pub fn build(self) -> Result<ClientConfig> {
        let mut config = ClientConfig::new(self.workspace, self.token)?;
        config.user_agent = self.user_agent;
        config.timeout = self.timeout;
        config.connect_timeout = self.connect_timeout;
        config.max_retries = self.max_retries;
        config.retry_base_delay = self.retry_base_delay;
        config.retry_max_delay = self.retry_max_delay;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfig::build("example.com", "token123")
            .with_timeout(Duration::from_secs(60))
            .with_connect_timeout(Duration::from_secs(5))
            .with_max_retries(5)
            .with_retry_delays(Duration::from_millis(200), Duration::from_secs(2))
            .build()
            .unwrap();

        assert_eq!(
            config.workspace_url.as_str(),
            "https://example.com/api/2.1/unity-catalog/"
        );
        assert_eq!(config.token, "token123");
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_base_delay, Duration::from_millis(200));
        assert_eq!(config.retry_max_delay, Duration::from_secs(2));
    }

    #[test]
    fn test_client_config() {
        let config = ClientConfig::new("some-workspace.something.com", "token").unwrap();
        assert!(config
            .workspace_url
            .as_str()
            .contains("api/2.1/unity-catalog"));
        assert_eq!(config.token, "token");
    }

    #[test]
    fn default_user_agent_is_honest_identifier() {
        let config = ClientConfig::new("example.com", "token").unwrap();
        assert!(config.user_agent.starts_with("Delta/"));
        assert!(config.user_agent.contains("delta-kernel-rs"));
    }

    #[test]
    fn debug_redacts_bearer_token() {
        let config = ClientConfig::build("example.com", "super-secret-token")
            .build()
            .unwrap();
        let debug = format!("{config:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token leaked: {debug}"
        );
        assert!(debug.contains("***"), "redaction marker missing: {debug}");
    }

    #[test]
    fn with_user_agent_overrides_default() {
        let config = ClientConfig::build("example.com", "token")
            .with_user_agent("Delta/9 Spark/9")
            .build()
            .unwrap();
        assert_eq!(config.user_agent, "Delta/9 Spark/9");
    }
}
