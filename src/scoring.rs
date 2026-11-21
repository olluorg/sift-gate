use serde::Deserialize;

use crate::Route;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum Strategy {
    LowestLatency,
    LowestCost,
    Balanced,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Telemetry {
    pub ttft_ms: Option<f64>,
    pub streaming_total_ms: Option<f64>,
    pub total_ms: Option<f64>,
}

#[derive(Debug)]
pub struct Candidate<'a> {
    pub route: &'a Route,
    pub telemetry: Telemetry,
    /// Estimated catalog cost in the common currency for this request.
    pub estimated_cost: Option<f64>,
}

#[derive(Debug)]
pub struct ScoreRequest<'a> {
    pub streaming: bool,
    pub body: &'a serde_json::Value,
}

pub trait ScoringPolicy: Send + Sync {
    /// Return one score per candidate. Lower scores are preferred.
    fn scores(&self, request: &ScoreRequest<'_>, candidates: &[Candidate<'_>]) -> Vec<f64>;
}

pub(crate) struct BuiltinPolicy {
    pub strategy: Strategy,
    pub latency_weight: f64,
}

impl ScoringPolicy for BuiltinPolicy {
    fn scores(&self, request: &ScoreRequest<'_>, candidates: &[Candidate<'_>]) -> Vec<f64> {
        let latency: Vec<_> = candidates
            .iter()
            .map(|c| {
                if request.streaming {
                    c.telemetry.ttft_ms
                } else {
                    c.telemetry.total_ms
                }
            })
            .collect();
        let costs: Vec<_> = candidates.iter().map(|c| c.estimated_cost).collect();

        match self.strategy {
            Strategy::LowestLatency => {
                // Give unmeasured routes a turn to establish a latency baseline.
                latency
                    .into_iter()
                    .map(|value| value.unwrap_or(-1.0))
                    .collect()
            }
            Strategy::LowestCost => costs
                .into_iter()
                .map(|value| value.unwrap_or(f64::INFINITY))
                .collect(),
            Strategy::Balanced => {
                let latency = normalized(&latency, 0.5);
                let costs = normalized(&costs, 1.0);
                latency
                    .into_iter()
                    .zip(costs)
                    .map(|(delay, cost)| {
                        self.latency_weight * delay + (1.0 - self.latency_weight) * cost
                    })
                    .collect()
            }
        }
    }
}

fn normalized(values: &[Option<f64>], missing: f64) -> Vec<f64> {
    let min = values.iter().flatten().copied().reduce(f64::min);
    let max = values.iter().flatten().copied().reduce(f64::max);
    values
        .iter()
        .map(|value| match (value, min, max) {
            (Some(value), Some(min), Some(max)) if max > min => (value - min) / (max - min),
            (Some(_), _, _) => 0.0,
            _ => missing,
        })
        .collect()
}
