mod config;
mod provider;
mod routing;
mod scoring;

pub use async_trait::async_trait;
pub use config::{Config, ProviderConfig, ProviderKind, Route};
pub use provider::{Catalog, Price, ProviderAdapter, ProviderResponse, SseEvent, SseStream};
pub use routing::{RoutedResponse, SiftGate};
pub use scoring::{Candidate, ScoreRequest, ScoringPolicy, Strategy, Telemetry};

use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    ChatCompletions,
    AnthropicMessages,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("invalid request: {0}")]
    Request(String),
    #[error("no available route for {0}")]
    NoRoute(String),
    #[error("upstream HTTP status {0}")]
    Status(u16),
    #[error("upstream timed out")]
    Timeout,
    #[error("upstream transport error")]
    Transport,
    #[error("invalid upstream response: {0}")]
    Upstream(String),
}

impl Error {
    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout
                | Self::Transport
                | Self::Upstream(_)
                | Self::Status(408 | 429 | 500..=599)
        )
    }
}
