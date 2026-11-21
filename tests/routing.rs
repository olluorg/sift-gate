use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sift_gate::{
    Catalog, Config, Error, Price, Protocol, ProviderAdapter, ProviderResponse, SiftGate, SseEvent,
};

#[derive(Clone, Copy)]
enum Mode {
    Json,
    Limited,
    ErrorFirst,
    ErrorLater,
    Stream,
}

struct Mock {
    mode: Mode,
    price: Price,
    calls: AtomicUsize,
    catalog_calls: AtomicUsize,
    body: Mutex<Option<Value>>,
}

fn event(kind: &str, data: Value) -> Result<SseEvent, Error> {
    Ok(SseEvent {
        event: Some(kind.into()),
        data: data.to_string(),
    })
}

#[sift_gate::async_trait]
impl ProviderAdapter for Mock {
    async fn catalog(&self) -> Result<Catalog, Error> {
        self.catalog_calls.fetch_add(1, Ordering::Relaxed);
        Ok(HashMap::from([("vendor/model".into(), self.price.clone())]))
    }

    async fn send(&self, protocol: Protocol, body: Value) -> Result<ProviderResponse, Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        *self.body.lock().unwrap() = Some(body);
        match self.mode {
            Mode::Limited => Err(Error::Status(429)),
            Mode::Json => Ok(ProviderResponse::Json(json!({"choices": []}))),
            other => {
                assert_eq!(protocol, Protocol::AnthropicMessages);
                let start = || event("message_start", json!({"type": "message_start"}));
                let events = match other {
                    Mode::ErrorFirst => vec![Err(Error::Status(503))],
                    Mode::ErrorLater => vec![start(), Err(Error::Status(503))],
                    _ => vec![
                        start(),
                        event(
                            "content_block_start",
                            json!({
                                "type": "content_block_start",
                                "content_block": {"type": "tool_use", "name": "lookup", "input": {}}
                            }),
                        ),
                        event("message_stop", json!({"type": "message_stop"})),
                    ],
                };
                Ok(ProviderResponse::Stream(Box::pin(stream::iter(events))))
            }
        }
    }
}

fn gate(strategy: &str, first: Mode, second: Mode) -> (SiftGate, Arc<Mock>, Arc<Mock>) {
    let source = format!(
        r#"
strategy = "{strategy}"
rub_per_usd = 100.0
[providers.a]
kind = "custom"
[providers.b]
kind = "custom"
[[models.alias]]
provider = "a"
model = "vendor/model"
protocols = ["chat_completions", "anthropic_messages"]
[[models.alias]]
provider = "b"
model = "vendor/model"
protocols = ["chat_completions", "anthropic_messages"]
"#
    );
    let mock = |mode, currency: &str, amount| {
        Arc::new(Mock {
            mode,
            price: Price {
                currency: currency.into(),
                input_per_million: amount,
                output_per_million: amount,
                per_request: 0.0,
            },
            calls: AtomicUsize::new(0),
            catalog_calls: AtomicUsize::new(0),
            body: Mutex::default(),
        })
    };
    let a = mock(first, "RUB", 100.0);
    let b = mock(second, "USD", 2.0);
    let adapters: HashMap<String, Arc<dyn ProviderAdapter>> = HashMap::from([
        ("a".into(), a.clone() as Arc<dyn ProviderAdapter>),
        ("b".into(), b.clone() as Arc<dyn ProviderAdapter>),
    ]);
    let config = toml::from_str(&source).unwrap();
    (
        SiftGate::with_extensions(config, adapters, None).unwrap(),
        a,
        b,
    )
}

#[test]
fn example_config_parses() {
    let config: Config = toml::from_str(include_str!("../sift-gate.example.toml")).unwrap();
    assert_eq!(config.models["claude"].len(), 3);
}

#[tokio::test]
async fn ranks_catalog_prices_in_common_currency_and_preserves_multimodal_fields() {
    let (gate, a, b) = gate("LowestCost", Mode::Json, Mode::Json);
    let payload = json!({"model": "alias", "messages": [{
        "role": "user", "content": [{"type":"image_url","image_url":{"url":"https://example.com/x.png"}}]
    }], "tools": [{"type":"function","function":{"name":"lookup"}}]});
    for _ in 0..2 {
        assert_eq!(
            gate.chat_completions(payload.clone())
                .await
                .unwrap()
                .provider,
            "a"
        );
    }
    let mut upstream = payload.clone();
    upstream["model"] = json!("vendor/model");
    assert_eq!(*a.body.lock().unwrap(), Some(upstream));
    assert_eq!(a.catalog_calls.load(Ordering::Relaxed), 1);
    assert_eq!(b.catalog_calls.load(Ordering::Relaxed), 1);
    assert_eq!(b.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn rate_limit_opens_breaker_and_falls_back() {
    let (gate, a, b) = gate("LowestLatency", Mode::Limited, Mode::Json);
    for _ in 0..2 {
        assert_eq!(
            gate.chat_completions(json!({"model":"alias"}))
                .await
                .unwrap()
                .provider,
            "b"
        );
    }
    assert_eq!(a.calls.load(Ordering::Relaxed), 1);
    assert_eq!(b.calls.load(Ordering::Relaxed), 2);
    assert!(
        gate.telemetry("b", "vendor/model", Protocol::ChatCompletions)
            .unwrap()
            .total_ms
            .is_some()
    );
}

#[tokio::test]
async fn stream_can_fail_over_before_first_event_and_records_tool_ttft() {
    let (gate, _, _) = gate("LowestLatency", Mode::ErrorFirst, Mode::Stream);
    let result = gate
        .messages(json!({"model":"alias","stream":true}))
        .await
        .unwrap();
    assert_eq!(result.provider, "b");
    let ProviderResponse::Stream(events) = result.response else {
        panic!("expected SSE")
    };
    let events = events
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events.len(), 3);
    let telemetry = gate
        .telemetry("b", "vendor/model", Protocol::AnthropicMessages)
        .unwrap();
    assert!(telemetry.ttft_ms.is_some());
    assert!(telemetry.streaming_total_ms >= telemetry.ttft_ms);
}

#[tokio::test]
async fn stream_failure_after_first_event_is_returned_without_failover() {
    let (gate, _, b) = gate("LowestLatency", Mode::ErrorLater, Mode::Stream);
    let result = gate
        .messages(json!({"model":"alias","stream":true}))
        .await
        .unwrap();
    let ProviderResponse::Stream(mut events) = result.response else {
        panic!("expected SSE")
    };
    assert!(events.next().await.unwrap().is_ok());
    assert!(matches!(events.next().await, Some(Err(Error::Status(503)))));
    assert!(events.next().await.is_none());
    assert_eq!(b.calls.load(Ordering::Relaxed), 0);
}
