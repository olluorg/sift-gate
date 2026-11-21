//! Асинхронная маршрутизация запросов OpenAI Chat Completions и Anthropic Messages.
//!
//! Настройте провайдеров и алиасы моделей в TOML, загрузите конфигурацию через
//! [`Config::from_file`] и переиспользуйте [`SiftGate`] между вызовами.
//!
//! ```no_run
//! use serde_json::json;
//! use sift_gate::{Config, Error, ProviderResponse, SiftGate};
//!
//! async fn ask(gate: &SiftGate) -> Result<(), Error> {
//!     let result = gate.chat_completions(json!({
//!         "model": "claude",
//!         "messages": [{"role": "user", "content": "Привет!"}],
//!         "max_completion_tokens": 256
//!     })).await?;
//!     if let ProviderResponse::Json(body) = result.response {
//!         println!("{body}");
//!     }
//!     Ok(())
//! }
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Error> {
//!     let gate = SiftGate::new(Config::from_file("sift-gate.toml")?)?;
//!     ask(&gate).await
//! }
//! ```
//!
//! Для Anthropic используйте [`SiftGate::messages`]. При включённом стриминге
//! ответ содержит [`ProviderResponse::Stream`] с событиями [`SseEvent`].
//! Правила выбора маршрута описывает [`Strategy`]. Для нестандартного провайдера
//! реализуйте [`ProviderAdapter`], а для собственной оценки маршрута — [`ScoringPolicy`].
//!
//! [Руководство по подключению](https://olluorg.github.io/sift-gate/docs/).

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
