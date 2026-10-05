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

/// Stable machine-readable causes. Human-facing `reason` may change wording;
/// clients should use these codes together with `availability` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceReasonCode {
    SamplingStale,
    MarketStale,
    MarketRecovering,
    WaitingMarket,
    DynamicMissing,
    DynamicTimeMismatch,
    InsufficientHistory,
    HistoryGap,
    HistoryUnavailable,
    MarketTimeInvalid,
    InvalidData,
    IndicatorMissing,
    IndicatorInvalid,
    IndicatorWarmingUp,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntervalEvidence {
    pub interval: String,
    pub availability: Availability,
    pub reason: Option<String>,
    pub reason_code: Option<EvidenceReasonCode>,
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

impl IntervalEvidence {
    /// Keep identity, timestamps and quality for diagnostics, never a live value.
    pub fn clear_values(&mut self) {
        self.value = None;
        self.guaili = None;
        self.ma = None;
        self.atr14 = None;
        self.atr_rank = None;
        self.long_trend = None;
        self.short_trend = None;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalRun {
    pub direction: SignalDirection,
    pub intervals: Vec<String>,
    pub min_abs_value: i32,
    pub max_abs_value: i32,
    pub mean_abs_value: f64,
    /// Unrounded guaili statistics preserve compression tie-break precision.
    pub max_abs_guaili: Option<f64>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_rejects_unknown_reason_codes() {
        let mut evidence = serde_json::to_value(IntervalEvidence::default()).unwrap();
        evidence["reasonCode"] = serde_json::json!("unknown_quality_cause");
        assert!(serde_json::from_value::<IntervalEvidence>(evidence).is_err());
    }
}
