use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalKind {
    Extreme,
    Compression,
    Conflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalDirection {
    Positive,
    Negative,
    Neutral,
}

/// Whether the current dynamic candle can participate in structure detection.
/// `Filtered` is a valid observation that did not pass the ATR rank rule; it is
/// different from an observation whose underlying data could not be evaluated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Ready,
    Filtered,
    #[default]
    Missing,
    WarmingUp,
    Stale,
    Gap,
    Recovering,
    Invalid,
}

impl Availability {
    pub fn is_unknown(self) -> bool {
        !matches!(self, Self::Ready | Self::Filtered)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntervalEvidence {
    pub interval: String,
    pub availability: Availability,
    pub reason: Option<String>,
    pub value: Option<i32>,
    pub guaili: Option<f64>,
    pub ma: Option<f64>,
    pub atr14: Option<f64>,
    pub atr_rank: Option<f64>,
    pub long_trend: Option<bool>,
    pub short_trend: Option<bool>,
    pub history_count: usize,
    pub open_time: Option<i64>,
    pub close_time: Option<i64>,
    pub market_event_time: Option<i64>,
    pub is_closed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalRun {
    pub direction: SignalDirection,
    pub intervals: Vec<String>,
    pub min_abs_value: i32,
    pub max_abs_value: i32,
    pub mean_abs_value: f64,
    /// Unrounded guaili statistics preserve the legacy compression tie-breaks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_abs_guaili: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_abs_guaili: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalStructure {
    /// Assigned by the runtime, retained while a structure continues or expands.
    pub id: String,
    pub kind: SignalKind,
    /// For conflict structures this is the direction of the shorter-period run.
    pub direction: SignalDirection,
    pub runs: Vec<SignalRun>,
    /// Largest single run; use `total_level_count` when comparing conflicts.
    pub level_count: usize,
    pub total_level_count: usize,
    pub anchor_interval: String,
    pub first_observed_at: Option<i64>,
    pub formed_at: Option<i64>,
    pub last_changed_at: Option<i64>,
}
