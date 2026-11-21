use futures_util::StreamExt;
use serde_json::{Value, json};
use sift_gate::{Config, Error, Protocol, ProviderResponse, Route, SiftGate, Strategy};

const MODEL: &str = "fixture/model";
const PROVIDERS: [&str; 3] = ["openrouter", "routerai", "polza"];
const ALL_PROVIDERS: [&str; 5] = ["openrouter", "routerai", "polza", "proxyapi", "aitunnel"];

fn gate(providers: &[&str]) -> SiftGate {
    gate_with_rate(providers, 100.0)
}

fn gate_with_rate(providers: &[&str], rub_per_usd: f64) -> SiftGate {
    let mut config = mock_config(providers);
    config.rub_per_usd = Some(rub_per_usd);
    SiftGate::new(config).unwrap()
}

fn mock_config(providers: &[&str]) -> Config {
    let root = std::env::var("SIFT_GATE_MOCK_URL").expect("run tests/http/mock.py");
    let mut config: Config = toml::from_str(include_str!("../sift-gate.example.toml")).unwrap();
    let extended: Config = toml::from_str(include_str!("../sift-gate.ru.example.toml")).unwrap();
    config.providers.extend(extended.providers);
    config
        .providers
        .retain(|name, _| providers.contains(&name.as_str()));
    for (name, provider) in &mut config.providers {
        provider.base_url = Some(format!("{}/{name}/api/v1", root.trim_end_matches('/')));
        provider.api_key_env = Some("SIFT_GATE_MOCK_KEY".into());
    }
    config.models.insert(
        "claude".into(),
        providers
            .iter()
            .map(|name| Route {
                provider: (*name).into(),
                model: MODEL.into(),
                protocols: vec![Protocol::ChatCompletions, Protocol::AnthropicMessages],
            })
            .collect(),
    );
    config.timeout_ms = 3_000;
    config.strategy = Strategy::LowestCost;
    config
}

fn request(content: &str, streaming: bool) -> Value {
    json!({
        "model": "claude",
        "messages": [{"role": "user", "content": content}],
        "max_tokens": 32,
        "stream": streaming,
    })
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn all_adapters_support_json_and_sse_in_both_protocols() {
    for provider in ALL_PROVIDERS {
        let gate = gate(&[provider]);
        for protocol in [Protocol::ChatCompletions, Protocol::AnthropicMessages] {
            let result = gate.send(protocol, request("hello", false)).await.unwrap();
            assert_eq!(result.provider, provider);
            assert_eq!(result.model, MODEL);
            let ProviderResponse::Json(body) = result.response else {
                panic!("expected JSON")
            };
            let content = match protocol {
                Protocol::ChatCompletions => &body["choices"][0]["message"]["content"],
                Protocol::AnthropicMessages => &body["content"][0]["text"],
            };
            assert_eq!(content, provider);

            let result = gate.send(protocol, request("hello", true)).await.unwrap();
            let ProviderResponse::Stream(events) = result.response else {
                panic!("expected SSE")
            };
            let events = events
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(events.iter().any(|event| event.data.contains(provider)));
            let last = events.last().unwrap();
            match protocol {
                Protocol::ChatCompletions => assert_eq!(last.data, "[DONE]"),
                Protocol::AnthropicMessages => {
                    assert_eq!(last.event.as_deref(), Some("message_stop"))
                }
            }
            let timings = gate.telemetry(provider, MODEL, protocol).unwrap();
            assert!(timings.total_ms.is_some());
            assert!(timings.ttft_ms.is_some());
            assert!(timings.streaming_total_ms >= timings.ttft_ms);
        }
    }
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn aitunnel_catalog_ranks_prices_per_million_without_credentials() {
    let gate = gate(&["routerai", "aitunnel", "proxyapi"]);
    let result = gate
        .send(Protocol::ChatCompletions, request("hello", false))
        .await
        .unwrap();
    assert_eq!(result.provider, "aitunnel");
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn catalogs_rank_by_currency_and_price_unit() {
    // Fixture prices, converted into USD per million output tokens: 2, 4, 6.
    for (providers, winner) in [
        (&PROVIDERS[..], "routerai"),
        (&["polza", "openrouter"][..], "openrouter"),
    ] {
        let response = gate(providers)
            .chat_completions(request("hello", false))
            .await
            .unwrap();
        assert_eq!(response.provider, winner);
    }
    let response = gate_with_rate(&["openrouter", "polza"], 200.0)
        .chat_completions(request("hello", false))
        .await
        .unwrap();
    assert_eq!(response.provider, "polza");
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn rate_limit_routes_around_open_circuit() {
    let gate = gate(&PROVIDERS);
    for content in ["rate-limit", "hello"] {
        let result = gate
            .chat_completions(request(content, false))
            .await
            .unwrap();
        assert_eq!(result.provider, "openrouter");
    }
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn stream_switches_before_first_event_and_surfaces_later_disconnect() {
    let result = gate(&PROVIDERS)
        .chat_completions(request("fail-before-sse", true))
        .await
        .unwrap();
    assert_eq!(result.provider, "openrouter");

    let result = gate(&PROVIDERS)
        .chat_completions(request("disconnect-after-sse", true))
        .await
        .unwrap();
    assert_eq!(result.provider, "routerai");
    let ProviderResponse::Stream(mut events) = result.response else {
        panic!("expected SSE")
    };
    assert!(events.next().await.unwrap().is_ok());
    assert!(events.next().await.unwrap().is_ok());
    assert!(
        matches!(events.next().await, Some(Err(Error::Upstream(message))) if message.contains("terminal"))
    );
    assert!(events.next().await.is_none());
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn lowest_latency_prefers_fast_provider_after_slow_response() {
    for streaming in [false, true] {
        let mut config = mock_config(&["routerai", "openrouter"]);
        config.strategy = Strategy::LowestLatency;
        config.models.insert(
            "router-only".into(),
            vec![config.models["claude"][0].clone()],
        );
        let gate = SiftGate::new(config).unwrap();

        for content in ["hello", "slow"] {
            let mut body = request(content, streaming);
            body["model"] = json!("router-only");
            let response = gate.chat_completions(body).await.unwrap();
            assert_eq!(response.provider, "routerai");
            drain(response.response).await;
        }

        // First establish a baseline for the unmeasured route, then compare the EWMAs.
        for _ in 0..2 {
            let response = gate
                .chat_completions(request("slow", streaming))
                .await
                .unwrap();
            assert_eq!(response.provider, "openrouter");
            drain(response.response).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires the HTTP fixture server; run tests/http/mock.py"]
async fn silent_provider_times_out_and_circuit_breaker_skips_it() {
    let mut config = mock_config(&PROVIDERS);
    config.timeout_ms = 500;
    let gate = SiftGate::new(config.clone()).unwrap();

    let result = gate
        .chat_completions(request("no-response", false))
        .await
        .unwrap();
    assert_eq!(result.provider, "openrouter");
    let result = gate
        .chat_completions(request("hello", false))
        .await
        .unwrap();
    assert_eq!(result.provider, "openrouter");

    config.retry_timeouts = false;
    let gate = SiftGate::new(config).unwrap();
    assert!(matches!(
        gate.chat_completions(request("no-response", false)).await,
        Err(Error::Timeout)
    ));
}

async fn drain(response: ProviderResponse) {
    if let ProviderResponse::Stream(events) = response {
        events
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    }
}
