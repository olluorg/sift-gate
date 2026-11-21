use std::{collections::HashMap, pin::Pin};

use futures_util::{Stream, stream};
use reqwest::{Client, Response, Url};
use serde_json::Value;

use crate::{Error, Protocol, ProviderConfig, ProviderKind};

pub type Catalog = HashMap<String, Price>;
pub type SseStream = Pin<Box<dyn Stream<Item = Result<SseEvent, Error>> + Send>>;

#[derive(Clone, Debug)]
pub struct Price {
    pub currency: String,
    pub input_per_million: f64,
    pub output_per_million: f64,
    pub per_request: f64,
}

pub enum ProviderResponse {
    Json(Value),
    Stream(SseStream),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

impl SseEvent {
    pub(crate) fn inspect(&self, protocol: Protocol) -> Result<(bool, bool), Error> {
        if protocol == Protocol::ChatCompletions && self.data == "[DONE]" {
            return Ok((false, true));
        }
        let value: Value = serde_json::from_str(&self.data)
            .map_err(|_| Error::Upstream("invalid SSE JSON".into()))?;
        let kind = value["type"].as_str().or(self.event.as_deref());
        if !value["error"].is_null() || kind == Some("error") {
            return Err(response_error(&value));
        }
        let is_token = match protocol {
            Protocol::ChatCompletions => value["choices"].as_array().is_some_and(|choices| {
                choices.iter().any(|choice| {
                    let delta = &choice["delta"];
                    ["content", "reasoning", "reasoning_content", "refusal"]
                        .iter()
                        .any(|key| nonempty(&delta[key]))
                        || nonempty(&delta["function_call"]["arguments"])
                        || delta["tool_calls"].as_array().is_some_and(|calls| {
                            calls.iter().any(|call| {
                                nonempty(&call["function"]["name"])
                                    || nonempty(&call["function"]["arguments"])
                            })
                        })
                })
            }),
            Protocol::AnthropicMessages => {
                let block = match kind {
                    Some("content_block_delta") => &value["delta"],
                    Some("content_block_start") => &value["content_block"],
                    _ => &Value::Null,
                };
                ["text", "partial_json", "thinking", "name"]
                    .iter()
                    .any(|key| nonempty(&block[key]))
            }
        };
        Ok((
            is_token,
            protocol == Protocol::AnthropicMessages && kind == Some("message_stop"),
        ))
    }
}

fn nonempty(value: &Value) -> bool {
    value.as_str().is_some_and(|s| !s.is_empty())
}

fn response_error(value: &Value) -> Error {
    let error = &value["error"];
    if let Some(code) = error["code"].as_u64().and_then(|n| u16::try_from(n).ok())
        && (400..600).contains(&code)
    {
        return Error::Status(code);
    }
    match error["type"].as_str() {
        Some("rate_limit_error") => Error::Status(429),
        Some("overloaded_error") => Error::Status(529),
        Some("invalid_request_error") => Error::Status(400),
        Some("authentication_error") => Error::Status(401),
        Some("permission_error") => Error::Status(403),
        _ => Error::Upstream("provider returned an error payload".into()),
    }
}

#[async_trait::async_trait]
pub trait ProviderAdapter: Send + Sync {
    async fn catalog(&self) -> Result<Catalog, Error>;
    /// The body contains the upstream model ID and the requested stream flag.
    async fn send(&self, protocol: Protocol, body: Value) -> Result<ProviderResponse, Error>;
}

pub(crate) struct HttpProvider {
    client: Client,
    kind: ProviderKind,
    base: Url,
    key: Option<String>,
    currency: String,
}

impl HttpProvider {
    pub fn new(client: Client, config: &ProviderConfig) -> Result<Self, Error> {
        let (base, currency) = match config.kind {
            ProviderKind::OpenRouter => ("https://openrouter.ai/api/v1", "USD"),
            ProviderKind::RouterAi => ("https://routerai.ru/api/v1", "RUB"),
            ProviderKind::Polza => ("https://polza.ai/api/v1", "RUB"),
            ProviderKind::AiTunnel => ("https://api.aitunnel.ru/v1", "RUB"),
            ProviderKind::Compatible | ProviderKind::Custom => ("", "USD"),
        };
        let base = config.base_url.as_deref().unwrap_or(base);
        let base = Url::parse(&format!("{}/", base.trim_end_matches('/')))
            .map_err(|_| Error::Config("invalid provider base_url".into()))?;
        if !matches!(base.scheme(), "https" | "http")
            || base.host().is_none()
            || base.query().is_some()
            || base.fragment().is_some()
            || !base.username().is_empty()
            || base.password().is_some()
        {
            return Err(Error::Config(
                "provider base_url must be an HTTP(S) API root".into(),
            ));
        }
        let key = config
            .api_key_env
            .as_ref()
            .map(|env| {
                std::env::var(env)
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| Error::Config(format!("missing API key in {env}")))
            })
            .transpose()?;
        Ok(Self {
            client,
            kind: config.kind,
            base,
            key,
            currency: config.currency.clone().unwrap_or_else(|| currency.into()),
        })
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        anthropic: bool,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(method, self.base.join(path).expect("static API path"));
        if anthropic {
            request = request.header("anthropic-version", "2023-06-01");
        }
        if let Some(key) = &self.key {
            request = if anthropic && matches!(self.kind, ProviderKind::RouterAi) {
                request.header("x-api-key", key)
            } else {
                request.bearer_auth(key)
            };
        }
        request
    }
}

#[async_trait::async_trait]
impl ProviderAdapter for HttpProvider {
    async fn catalog(&self) -> Result<Catalog, Error> {
        if matches!(self.kind, ProviderKind::AiTunnel) {
            let url = self
                .base
                .join("/public/aitunnel/models/chat")
                .expect("static catalog path");
            let response = checked(self.client.get(url).send().await)?;
            let models: HashMap<String, Value> = response.json().await.map_err(http_error)?;
            return Ok(models
                .into_iter()
                .filter_map(|(id, model)| {
                    Some((
                        id,
                        Price {
                            currency: self.currency.clone(),
                            input_per_million: price_number(&model["prompt_cost"])?,
                            output_per_million: price_number(&model["completion_cost"])?,
                            per_request: 0.0,
                        },
                    ))
                })
                .collect());
        }
        let response = checked(
            self.request(reqwest::Method::GET, "models", false)
                .send()
                .await,
        )?;
        let body: Value = response.json().await.map_err(http_error)?;
        let models = body["data"]
            .as_array()
            .ok_or_else(|| Error::Upstream("missing catalog data".into()))?;
        Ok(models
            .iter()
            .filter_map(|model| self.catalog_price(model))
            .collect())
    }

    async fn send(&self, protocol: Protocol, body: Value) -> Result<ProviderResponse, Error> {
        let anthropic = protocol == Protocol::AnthropicMessages;
        let path = if anthropic {
            "messages"
        } else {
            "chat/completions"
        };
        let response = checked(
            self.request(reqwest::Method::POST, path, anthropic)
                .json(&body)
                .send()
                .await,
        )?;
        if body["stream"] == true {
            let sse = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|header| header.to_str().ok())
                .and_then(|header| header.split(';').next())
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"));
            if !sse {
                return Err(Error::Upstream("expected text/event-stream".into()));
            }
            return Ok(ProviderResponse::Stream(decode_sse(response)));
        }
        let value: Value = response.json().await.map_err(http_error)?;
        if !value["error"].is_null() {
            return Err(response_error(&value));
        }
        let valid = match protocol {
            Protocol::ChatCompletions => value["choices"].is_array(),
            Protocol::AnthropicMessages => {
                value["type"] == "message" && value["content"].is_array()
            }
        };
        if !valid {
            return Err(Error::Upstream("unexpected completion response".into()));
        }
        Ok(ProviderResponse::Json(value))
    }
}

impl HttpProvider {
    fn catalog_price(&self, model: &Value) -> Option<(String, Price)> {
        let id = model["id"].as_str()?;
        let (pricing, input, output, scale) = match self.kind {
            ProviderKind::Polza => (
                &model["top_provider"]["pricing"],
                "prompt_per_million",
                "completion_per_million",
                1.0,
            ),
            _ => (&model["pricing"], "prompt", "completion", 1_000_000.0),
        };
        Some((
            id.into(),
            Price {
                currency: pricing["currency"]
                    .as_str()
                    .unwrap_or(&self.currency)
                    .to_uppercase(),
                input_per_million: price_number(&pricing[input])? * scale,
                output_per_million: price_number(&pricing[output])? * scale,
                per_request: if pricing["request"].is_null() {
                    0.0
                } else {
                    price_number(&pricing["request"])?
                },
            },
        ))
    }
}

fn price_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|number| number.is_finite() && *number >= 0.0)
}

fn checked(result: Result<Response, reqwest::Error>) -> Result<Response, Error> {
    let response = result.map_err(http_error)?;
    if !response.status().is_success() {
        return Err(Error::Status(response.status().as_u16()));
    }
    Ok(response)
}

fn http_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Timeout
    } else if error.is_decode() {
        Error::Upstream("invalid JSON body".into())
    } else {
        Error::Transport
    }
}

#[derive(Default)]
struct SseDecoder {
    pending: Vec<u8>,
    data: Vec<String>,
    event: Option<String>,
    event_bytes: usize,
    started: bool,
}

impl SseDecoder {
    fn next_event(&mut self) -> Result<Option<SseEvent>, Error> {
        const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;
        let mut consumed = 0;
        while let Some(offset) = self.pending[consumed..]
            .iter()
            .position(|b| matches!(b, b'\n' | b'\r'))
        {
            let end = consumed + offset;
            if self.pending[end] == b'\r' && end + 1 == self.pending.len() {
                break;
            }
            let line = std::str::from_utf8(&self.pending[consumed..end])
                .map_err(|_| Error::Upstream("invalid SSE encoding".into()))?;
            consumed = end + 1;
            if self.pending[end] == b'\r' && self.pending.get(consumed) == Some(&b'\n') {
                consumed += 1;
            }
            let line = if self.started {
                line
            } else {
                line.trim_start_matches('\u{feff}')
            };
            self.started = true;
            if line.is_empty() {
                self.event_bytes = 0;
                let event = self.event.take();
                if !self.data.is_empty() {
                    self.pending.drain(..consumed);
                    let data = std::mem::take(&mut self.data).join("\n");
                    return Ok(Some(SseEvent { event, data }));
                }
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            self.event_bytes += line.len();
            if self.event_bytes > MAX_EVENT_BYTES {
                return Err(Error::Upstream("SSE event too large".into()));
            }
            match field {
                "data" => self.data.push(value.into()),
                "event" => self.event = Some(value.into()),
                _ => {}
            }
        }
        self.pending.drain(..consumed);
        if self.pending.len() > MAX_EVENT_BYTES {
            return Err(Error::Upstream("SSE line too large".into()));
        }
        Ok(None)
    }
}

fn decode_sse(response: Response) -> SseStream {
    Box::pin(stream::unfold(
        Some((response, SseDecoder::default())),
        |state| async {
            let (mut response, mut decoder) = state?;
            loop {
                match decoder.next_event() {
                    Ok(Some(event)) => return Some((Ok(event), Some((response, decoder)))),
                    Err(error) => return Some((Err(error), None)),
                    Ok(None) => {}
                }
                match response.chunk().await {
                    Ok(Some(chunk)) => decoder.pending.extend_from_slice(&chunk),
                    Ok(None) if decoder.pending.last() == Some(&b'\r') => {
                        decoder.pending.push(b'\n')
                    }
                    Ok(None) => return None,
                    Err(error) => return Some((Err(http_error(error)), None)),
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_decoder_handles_split_crlf_and_multiline_data() {
        let mut decoder = SseDecoder::default();
        decoder
            .pending
            .extend_from_slice(b": ping\r\nevent: content_block_delta\r");
        assert!(decoder.next_event().unwrap().is_none());
        decoder.pending.extend_from_slice(
            b"\ndata: {\"type\":\"content_block_delta\",\r\ndata: \"delta\":{\"text\":\"hello\"}}\r\n\r\n",
        );
        let event = decoder.next_event().unwrap().unwrap();
        assert_eq!(event.event.as_deref(), Some("content_block_delta"));
        assert_eq!(
            event.inspect(Protocol::AnthropicMessages).unwrap(),
            (true, false)
        );
        assert!(decoder.next_event().unwrap().is_none());
    }

    #[test]
    fn chat_metadata_does_not_count_as_first_token() {
        let chunk = |data: &str| SseEvent {
            event: None,
            data: data.into(),
        };
        assert_eq!(
            chunk(r#"{"choices":[{"delta":{"role":"assistant"}}]}"#)
                .inspect(Protocol::ChatCompletions)
                .unwrap(),
            (false, false)
        );
        assert_eq!(
            chunk(r#"{"choices":[{"delta":{"tool_calls":[{"function":{"name":"lookup"}}]}}]}"#)
                .inspect(Protocol::ChatCompletions)
                .unwrap(),
            (true, false)
        );
        assert_eq!(
            chunk("[DONE]").inspect(Protocol::ChatCompletions).unwrap(),
            (false, true)
        );
    }
}
