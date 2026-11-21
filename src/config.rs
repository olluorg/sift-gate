use std::{collections::HashMap, path::Path};

use serde::Deserialize;

use crate::{Error, Protocol, Strategy};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub strategy: Strategy,
    pub providers: HashMap<String, ProviderConfig>,
    pub models: HashMap<String, Vec<Route>>,
    pub timeout_ms: u64,
    pub cooldown_ms: u64,
    pub catalog_ttl_ms: u64,
    pub ewma_alpha: f64,
    pub latency_weight: f64,
    pub rub_per_usd: Option<f64>,
    pub retry_timeouts: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            strategy: Strategy::Balanced,
            providers: HashMap::new(),
            models: HashMap::new(),
            timeout_ms: 60_000,
            cooldown_ms: 30_000,
            catalog_ttl_ms: 300_000,
            ewma_alpha: 0.25,
            latency_weight: 0.5,
            rub_per_usd: None,
            retry_timeouts: true,
        }
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let source = std::fs::read_to_string(path).map_err(|e| Error::Config(e.to_string()))?;
        toml::from_str(&source).map_err(|e| Error::Config(e.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    OpenRouter,
    RouterAi,
    Polza,
    AiTunnel,
    Compatible,
    Custom,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub kind: ProviderKind,
    pub api_key_env: Option<String>,
    pub base_url: Option<String>,
    pub currency: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub provider: String,
    pub model: String,
    pub protocols: Vec<Protocol>,
}
