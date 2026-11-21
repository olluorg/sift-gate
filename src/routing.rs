use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{StreamExt, future::join_all, stream};
use serde_json::Value;
use tokio::time::{Instant, timeout, timeout_at};

use crate::{
    Candidate, Catalog, Config, Error, Protocol, ProviderAdapter, ProviderKind, ProviderResponse,
    Route, ScoreRequest, ScoringPolicy, Strategy, Telemetry, provider::HttpProvider,
    scoring::BuiltinPolicy,
};

pub struct RoutedResponse {
    pub provider: String,
    pub model: String,
    pub response: ProviderResponse,
}

#[derive(Clone)]
pub struct SiftGate(Arc<Inner>);

struct Inner {
    config: Config,
    policy: Arc<dyn ScoringPolicy>,
    use_catalogs: bool,
    providers: HashMap<String, Provider>,
}

type MetricKey = (String, Protocol);

struct Provider {
    adapter: Arc<dyn ProviderAdapter>,
    breaker: Arc<Mutex<Breaker>>,
    metrics: Arc<Mutex<HashMap<MetricKey, Telemetry>>>,
    catalog: tokio::sync::Mutex<CachedCatalog>,
}

#[derive(Default)]
struct CachedCatalog {
    prices: Catalog,
    refresh_at: Option<Instant>,
}

#[derive(Default)]
struct Breaker {
    opened_at: Option<Instant>,
    probe_in_progress: bool,
    generation: u64,
}

struct Attempt {
    breaker: Arc<Mutex<Breaker>>,
    metrics: Arc<Mutex<HashMap<MetricKey, Telemetry>>>,
    key: MetricKey,
    generation: u64,
    probe: bool,
    alpha: f64,
    started: Instant,
}

impl Provider {
    fn available(&self, cooldown: Duration) -> bool {
        let breaker = self.breaker.lock().unwrap();
        !breaker.probe_in_progress && breaker.opened_at.is_none_or(|at| at.elapsed() >= cooldown)
    }

    fn attempt(&self, key: MetricKey, config: &Config) -> Option<Attempt> {
        let mut breaker = self.breaker.lock().unwrap();
        if breaker.probe_in_progress
            || breaker
                .opened_at
                .is_some_and(|at| at.elapsed() < Duration::from_millis(config.cooldown_ms))
        {
            return None;
        }
        let probe = breaker.opened_at.is_some();
        breaker.probe_in_progress = probe;
        Some(Attempt {
            breaker: Arc::clone(&self.breaker),
            metrics: Arc::clone(&self.metrics),
            key,
            generation: breaker.generation,
            probe,
            alpha: config.ewma_alpha,
            started: Instant::now(),
        })
    }

    fn telemetry(&self, model: &str, protocol: Protocol) -> Telemetry {
        self.metrics
            .lock()
            .unwrap()
            .get(&(model.into(), protocol))
            .copied()
            .unwrap_or_default()
    }

    async fn catalog(&self, config: &Config) -> Catalog {
        let mut cache = self.catalog.lock().await;
        if cache.refresh_at.is_none_or(|at| at <= Instant::now()) {
            let result = timeout(
                Duration::from_millis(config.timeout_ms),
                self.adapter.catalog(),
            )
            .await;
            let delay = if let Ok(Ok(prices)) = result {
                cache.prices = prices;
                config.catalog_ttl_ms
            } else {
                config.catalog_ttl_ms.min(config.cooldown_ms)
            };
            cache.refresh_at = Some(Instant::now() + Duration::from_millis(delay));
        }
        cache.prices.clone()
    }
}

impl Attempt {
    fn record_ttft(&self) {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics.entry(self.key.clone()).or_default();
        self.smooth(&mut metric.ttft_ms);
    }

    fn finish(self, error: Option<&Error>, streaming: bool) {
        if error.is_none() {
            let mut metrics = self.metrics.lock().unwrap();
            let metric = metrics.entry(self.key.clone()).or_default();
            let total = if streaming {
                &mut metric.streaming_total_ms
            } else {
                &mut metric.total_ms
            };
            self.smooth(total);
        }
        let mut breaker = self.breaker.lock().unwrap();
        if breaker.generation != self.generation {
            return;
        }
        if error.is_some_and(Error::retryable) {
            breaker.opened_at = Some(Instant::now());
            breaker.probe_in_progress = false;
            breaker.generation = breaker.generation.wrapping_add(1);
        } else if self.probe {
            breaker.probe_in_progress = false;
            breaker.opened_at = if error.is_none() {
                None
            } else {
                Some(Instant::now())
            };
            breaker.generation = breaker.generation.wrapping_add(1);
        }
    }

    fn smooth(&self, slot: &mut Option<f64>) {
        let sample = self.started.elapsed().as_secs_f64() * 1_000.0;
        *slot = Some(slot.map_or(sample, |previous| {
            previous + self.alpha * (sample - previous)
        }));
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if self.probe {
            let mut breaker = self.breaker.lock().unwrap();
            if breaker.generation == self.generation {
                breaker.probe_in_progress = false;
            }
        }
    }
}

impl SiftGate {
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_extensions(config, HashMap::new(), None)
    }

    pub fn with_extensions(
        config: Config,
        mut adapters: HashMap<String, Arc<dyn ProviderAdapter>>,
        policy: Option<Arc<dyn ScoringPolicy>>,
    ) -> Result<Self, Error> {
        validate(&config)?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Config("could not create HTTP client".into()))?;
        let mut providers = HashMap::new();
        for (id, provider) in &config.providers {
            let adapter = match adapters.remove(id) {
                Some(custom) => custom,
                None if matches!(provider.kind, ProviderKind::Custom) => {
                    return Err(Error::Config(format!("missing custom adapter for {id}")));
                }
                None => Arc::new(HttpProvider::new(client.clone(), provider)?),
            };
            providers.insert(
                id.clone(),
                Provider {
                    adapter,
                    breaker: Arc::default(),
                    metrics: Arc::default(),
                    catalog: tokio::sync::Mutex::default(),
                },
            );
        }
        if !adapters.is_empty() {
            return Err(Error::Config(
                "adapter registered for an unknown provider".into(),
            ));
        }

        let use_catalogs = policy.is_some() || config.strategy != Strategy::LowestLatency;
        let policy = policy.unwrap_or_else(|| {
            Arc::new(BuiltinPolicy {
                strategy: config.strategy,
                latency_weight: config.latency_weight,
            })
        });
        Ok(Self(Arc::new(Inner {
            config,
            policy,
            use_catalogs,
            providers,
        })))
    }

    pub async fn chat_completions(&self, body: Value) -> Result<RoutedResponse, Error> {
        self.send(Protocol::ChatCompletions, body).await
    }

    pub async fn messages(&self, body: Value) -> Result<RoutedResponse, Error> {
        self.send(Protocol::AnthropicMessages, body).await
    }

    pub fn telemetry(&self, provider: &str, model: &str, protocol: Protocol) -> Option<Telemetry> {
        self.0
            .providers
            .get(provider)
            .map(|provider| provider.telemetry(model, protocol))
    }

    pub async fn send(&self, protocol: Protocol, mut body: Value) -> Result<RoutedResponse, Error> {
        let alias = body["model"]
            .as_str()
            .filter(|alias| !alias.is_empty())
            .ok_or_else(|| Error::Request("model must be a nonempty alias".into()))?
            .to_owned();
        let streaming = match body.get("stream") {
            None => false,
            Some(value) => value
                .as_bool()
                .ok_or_else(|| Error::Request("stream must be a boolean".into()))?,
        };
        let inner = &self.0;
        let config = &inner.config;
        let routes = config
            .models
            .get(&alias)
            .ok_or_else(|| Error::NoRoute(alias.clone()))?;
        let cooldown = Duration::from_millis(config.cooldown_ms);
        let routes: Vec<_> = routes
            .iter()
            .filter(|route| {
                route.protocols.contains(&protocol)
                    && inner.providers[&route.provider].available(cooldown)
            })
            .collect();

        if routes.is_empty() {
            return Err(Error::NoRoute(alias));
        }

        let mut prices = HashMap::new();
        if inner.use_catalogs {
            let provider_ids: HashSet<_> =
                routes.iter().map(|route| route.provider.as_str()).collect();
            prices.extend(
                join_all(
                    provider_ids
                        .into_iter()
                        .map(|id| async move { (id, inner.providers[id].catalog(config).await) }),
                )
                .await,
            );
        }
        let candidates = candidates(&routes, &prices, &body, protocol, inner);
        let scores = inner.policy.scores(
            &ScoreRequest {
                streaming,
                body: &body,
            },
            &candidates,
        );
        if scores.len() != routes.len() || scores.iter().any(|score| score.is_nan()) {
            return Err(Error::Config(
                "scoring policy returned invalid scores".into(),
            ));
        }
        let mut order: Vec<_> = routes.into_iter().zip(scores).collect();
        order.sort_by(|(_, left), (_, right)| left.total_cmp(right));
        let mut last = None;
        for (route, _) in order {
            let provider = &inner.providers[&route.provider];
            let Some(attempt) = provider.attempt((route.model.clone(), protocol), config) else {
                continue;
            };
            let deadline = attempt.started + Duration::from_millis(config.timeout_ms);
            body["model"] = Value::String(route.model.clone());
            let response = timeout_at(deadline, provider.adapter.send(protocol, body.clone()))
                .await
                .unwrap_or(Err(Error::Timeout));
            let error = match response {
                Ok(ProviderResponse::Json(value)) if !streaming => {
                    attempt.finish(None, false);
                    return Ok(routed(route, ProviderResponse::Json(value)));
                }
                Ok(ProviderResponse::Stream(mut source)) if streaming => {
                    match next_event(&mut source, deadline, protocol).await {
                        Ok(first) => {
                            return Ok(routed(
                                route,
                                ProviderResponse::Stream(track_stream(
                                    first, source, attempt, deadline, protocol,
                                )),
                            ));
                        }
                        Err(error) => error,
                    }
                }
                Ok(_) => Error::Upstream("adapter returned the wrong response mode".into()),
                Err(error) => error,
            };
            attempt.finish(Some(&error), streaming);
            if !error.retryable()
                || (matches!(error, Error::Timeout | Error::Status(408)) && !config.retry_timeouts)
            {
                return Err(error);
            }
            last = Some(error);
        }
        Err(last.unwrap_or(Error::NoRoute(alias)))
    }
}

fn routed(route: &Route, response: ProviderResponse) -> RoutedResponse {
    RoutedResponse {
        provider: route.provider.clone(),
        model: route.model.clone(),
        response,
    }
}

fn validate(config: &Config) -> Result<(), Error> {
    for millis in [config.timeout_ms, config.cooldown_ms, config.catalog_ttl_ms] {
        if millis == 0 || millis > 86_400_000 {
            return Err(Error::Config(
                "durations must be between 1 ms and 24 hours".into(),
            ));
        }
    }
    if !(config.ewma_alpha > 0.0 && config.ewma_alpha <= 1.0)
        || !(0.0..=1.0).contains(&config.latency_weight)
        || config
            .rub_per_usd
            .is_some_and(|rate| !rate.is_finite() || rate <= 0.0)
    {
        return Err(Error::Config(
            "invalid EWMA, latency weight or exchange rate".into(),
        ));
    }
    if config.models.is_empty() {
        return Err(Error::Config("at least one model alias is required".into()));
    }
    for (alias, routes) in &config.models {
        if alias.trim().is_empty() || routes.is_empty() {
            return Err(Error::Config(
                "model aliases require at least one route".into(),
            ));
        }
        let mut seen = HashSet::new();
        for route in routes {
            if !config.providers.contains_key(&route.provider)
                || route.model.trim().is_empty()
                || route.protocols.is_empty()
            {
                return Err(Error::Config(format!("invalid route for {alias}")));
            }
            for protocol in &route.protocols {
                if !seen.insert((&route.provider, &route.model, protocol)) {
                    return Err(Error::Config(format!("duplicate route for {alias}")));
                }
            }
        }
    }
    Ok(())
}

fn candidates<'a>(
    routes: &[&'a Route],
    prices: &HashMap<&str, Catalog>,
    body: &Value,
    protocol: Protocol,
    inner: &Inner,
) -> Vec<Candidate<'a>> {
    let mut payload = body.clone();
    payload
        .as_object_mut()
        .expect("validated request object")
        .remove("model");
    let input = payload.to_string().len().div_ceil(4) as f64;
    let output = ["max_completion_tokens", "max_tokens"]
        .iter()
        .find_map(|key| body[key].as_u64())
        .unwrap_or(512) as f64;
    let entries: Vec<_> = routes
        .iter()
        .map(|route| {
            prices
                .get(route.provider.as_str())
                .and_then(|catalog| catalog.get(&route.model))
        })
        .collect();
    let currencies: HashSet<_> = entries
        .iter()
        .flatten()
        .map(|price| price.currency.as_str())
        .collect();
    let common_currency = if currencies.len() == 1 {
        currencies.into_iter().next().unwrap()
    } else {
        "USD"
    };

    routes
        .iter()
        .zip(entries)
        .map(|(route, price)| {
            let estimated_cost = price.and_then(|price| {
                let ratio = match (price.currency.as_str(), common_currency) {
                    (a, b) if a == b => 1.0,
                    ("RUB", "USD") => 1.0 / inner.config.rub_per_usd?,
                    _ => return None,
                };
                let estimate = ratio
                    * (price.per_request
                        + (input * price.input_per_million + output * price.output_per_million)
                            / 1_000_000.0);
                (estimate.is_finite() && estimate >= 0.0).then_some(estimate)
            });
            Candidate {
                route,
                telemetry: inner.providers[&route.provider].telemetry(&route.model, protocol),
                estimated_cost,
            }
        })
        .collect()
}

struct Frame {
    event: crate::SseEvent,
    token: bool,
    done: bool,
}

async fn next_event(
    source: &mut crate::SseStream,
    deadline: Instant,
    protocol: Protocol,
) -> Result<Frame, Error> {
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    let event = timeout_at(deadline, source.next())
        .await
        .map_err(|_| Error::Timeout)?
        .ok_or_else(|| Error::Upstream("SSE ended without a terminal event".into()))??;
    let (token, done) = event.inspect(protocol)?;
    Ok(Frame { event, token, done })
}

fn track_stream(
    first: Frame,
    source: crate::SseStream,
    attempt: Attempt,
    deadline: Instant,
    protocol: Protocol,
) -> crate::SseStream {
    if first.token {
        attempt.record_ttft();
    }
    let remaining = if first.done {
        attempt.finish(None, true);
        None
    } else {
        Some((source, attempt, first.token))
    };
    Box::pin(
        stream::once(async move { Ok(first.event) }).chain(stream::unfold(
            remaining,
            move |state| async move {
                let (mut source, attempt, seen_token) = state?;
                match next_event(&mut source, deadline, protocol).await {
                    Ok(frame) => {
                        if frame.token && !seen_token {
                            attempt.record_ttft();
                        }
                        let remaining = if frame.done {
                            attempt.finish(None, true);
                            None
                        } else {
                            Some((source, attempt, seen_token || frame.token))
                        };
                        Some((Ok(frame.event), remaining))
                    }
                    Err(error) => {
                        attempt.finish(Some(&error), true);
                        Some((Err(error), None))
                    }
                }
            },
        )),
    )
}
