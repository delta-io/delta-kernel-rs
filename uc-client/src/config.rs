use std::time::Duration;

use url::Url;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub workspace_url: Url,
    pub token: String,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
}

impl ClientConfig {
    pub fn new(workspace: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        let workspace_str = workspace.into();
        let base_url =
            if workspace_str.starts_with("http://") || workspace_str.starts_with("https://") {
                workspace_str
            } else {
                format!("https://{}", workspace_str)
            };

        let mut workspace_url = Url::parse(&base_url)?;
        if !workspace_url.path().ends_with('/') {
            workspace_url.set_path(&format!("{}/", workspace_url.path()));
        }
        workspace_url.set_path(&format!("{}api/2.1/unity-catalog/", workspace_url.path()));

        Ok(Self {
            workspace_url,
            token: token.into(),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
        })
    }

    pub fn builder(workspace: impl Into<String>, token: impl Into<String>) -> ClientConfigBuilder {
        ClientConfigBuilder::new(workspace, token)
    }
}

pub struct ClientConfigBuilder {
    workspace: String,
    token: String,
    timeout: Duration,
    connect_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

impl ClientConfigBuilder {
    pub fn new(workspace: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            workspace: workspace.into(),
            token: token.into(),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn retry_delays(mut self, base: Duration, max: Duration) -> Self {
        self.retry_base_delay = base;
        self.retry_max_delay = max;
        self
    }

    pub fn build(self) -> Result<ClientConfig> {
        let mut config = ClientConfig::new(self.workspace, self.token)?;
        config.timeout = self.timeout;
        config.connect_timeout = self.connect_timeout;
        config.max_retries = self.max_retries;
        config.retry_base_delay = self.retry_base_delay;
        config.retry_max_delay = self.retry_max_delay;
        Ok(config)
    }
}
