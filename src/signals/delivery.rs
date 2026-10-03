use super::config::{stable_hash, SignalConfig, WecomAlertConfig};
use super::model::{IntervalEvidence, SignalDirection, SignalKind, SignalStructure};
use crate::domain::interval::Interval;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

const RECENT_STATUS_LIMIT: usize = 32;
const JOB_LIMIT_PER_EVALUATION: usize = 256;

/// Private webhook credentials deliberately have no Debug or Serialize implementation.
#[derive(Clone)]
pub struct DeliveryJob {
    pub alert_id: String,
    pub target_id: String,
    pub symbol: String,
    pub event_key: String,
    pub generation: u64,
    pub message: String,
    pub created_at: i64,
    webhook_url: String,
}

impl std::fmt::Debug for DeliveryJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliveryJob")
            .field("alert_id", &self.alert_id)
            .field("target_id", &self.target_id)
            .field("symbol", &self.symbol)
            .field("event_key", &self.event_key)
            .field("generation", &self.generation)
            .field("created_at", &self.created_at)
            .field("webhook_url", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryHealth {
    pub queued: u64,
    pub successful: u64,
    pub failed: u64,
    pub dropped: u64,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub recent: Vec<DeliveryStatus>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryStatus {
    pub alert_id: String,
    pub target_id: String,
    pub symbol: String,
    pub attempted_at: i64,
    pub successful: bool,
    pub error: Option<String>,
}

#[derive(Default)]
struct SubscriptionState {
    baseline: bool,
    matching_ids: HashMap<String, Vec<String>>,
}

#[derive(Clone, Copy)]
enum SnapshotQuality<'a> {
    Complete(bool),
    Evidence(&'a [IntervalEvidence]),
}

impl SnapshotQuality<'_> {
    fn has_known_observation(self) -> bool {
        match self {
            Self::Complete(complete) => complete,
            Self::Evidence(evidence) => evidence
                .iter()
                .any(|point| !point.availability.is_unknown()),
        }
    }

    fn intervals_known(self, intervals: &[String]) -> bool {
        match self {
            Self::Complete(complete) => complete,
            Self::Evidence(evidence) => intervals.iter().all(|interval| {
                evidence
                    .iter()
                    .find(|point| &point.interval == interval)
                    .map(|point| !point.availability.is_unknown())
                    .unwrap_or(false)
            }),
        }
    }
}

#[derive(Default)]
pub struct DeliveryManager {
    generation: Option<u64>,
    subscriptions: HashMap<(String, String), SubscriptionState>,
    // A target's cooldown is shared by overlapping subscriptions, scoped to signal class.
    last_queued: HashMap<(String, String, String), i64>,
    health: DeliveryHealth,
    recent: VecDeque<DeliveryStatus>,
}

impl DeliveryManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.generation = None;
        self.subscriptions.clear();
        self.last_queued.clear();
    }

    /// Recovered market data has a new observation baseline, not a new alert.
    pub fn reset_symbol(&mut self, symbol: &str) {
        self.subscriptions
            .retain(|(_, selected), _| selected != symbol);
    }

    pub fn record_dropped(&mut self) {
        self.health.dropped = self.health.dropped.saturating_add(1);
    }

    pub fn health(&self) -> DeliveryHealth {
        let mut result = self.health.clone();
        result.recent = self.recent.iter().cloned().collect();
        result
    }

    /// Evaluating delivery rules is synchronous and never performs network IO.
    /// Unknown data preserves prior matches; a complete first result establishes a baseline.
    pub fn prepare(
        &mut self,
        config: &SignalConfig,
        results: &[(String, Vec<SignalStructure>, bool)],
        now_ms: i64,
    ) -> Vec<DeliveryJob> {
        let observations = results
            .iter()
            .map(|(symbol, structures, complete)| {
                (
                    symbol.as_str(),
                    structures.as_slice(),
                    SnapshotQuality::Complete(*complete),
                )
            })
            .collect::<Vec<_>>();
        self.prepare_observations(config, &observations, now_ms)
    }

    /// Unknown long-period history does not prevent observing valid short-period structures.
    /// Only matches whose participating periods are unknown are retained until recovery.
    pub fn prepare_with_evidence(
        &mut self,
        config: &SignalConfig,
        results: &[(String, Vec<SignalStructure>, Vec<IntervalEvidence>)],
        now_ms: i64,
    ) -> Vec<DeliveryJob> {
        let observations = results
            .iter()
            .map(|(symbol, structures, evidence)| {
                (
                    symbol.as_str(),
                    structures.as_slice(),
                    SnapshotQuality::Evidence(evidence),
                )
            })
            .collect::<Vec<_>>();
        self.prepare_observations(config, &observations, now_ms)
    }

    fn prepare_observations(
        &mut self,
        config: &SignalConfig,
        results: &[(&str, &[SignalStructure], SnapshotQuality<'_>)],
        now_ms: i64,
    ) -> Vec<DeliveryJob> {
        let generation = config.delivery_hash();
        if self.generation != Some(generation) {
            self.reset();
            self.generation = Some(generation);
        }
        if !config.enabled {
            return Vec::new();
        }
        // Deterministic target/event ordering also makes overlapping subscriptions idempotent.
        let mut jobs = BTreeMap::<(String, String, String), DeliveryJob>::new();
        for alert in &config.wecom_alerts {
            let target_id = format!("{:016x}", stable_hash(&alert.webhook_url));
            for (symbol, signals, quality) in results {
                if !alert.symbols.iter().any(|selected| selected == symbol)
                    || !quality.has_known_observation()
                {
                    continue;
                }
                let matching = signals
                    .iter()
                    .filter(|signal| {
                        subscription_matches(alert, symbol, signal)
                            && quality.intervals_known(&signal_intervals(signal))
                    })
                    .map(|signal| (occurrence_key(signal), signal))
                    .collect::<BTreeMap<_, _>>();
                let state = self
                    .subscriptions
                    .entry((alert.id.clone(), (*symbol).to_owned()))
                    .or_default();
                let mut current_ids = matching
                    .iter()
                    .map(|(id, signal)| (id.clone(), signal_intervals(signal)))
                    .collect::<HashMap<_, _>>();
                if !state.baseline {
                    state.baseline = true;
                    state.matching_ids = current_ids;
                    continue;
                }
                let new_matches = matching
                    .iter()
                    .filter(|(id, _)| !state.matching_ids.contains_key(*id))
                    .map(|(id, signal)| (id.clone(), *signal))
                    .collect::<Vec<_>>();
                for (id, intervals) in &state.matching_ids {
                    if !quality.intervals_known(intervals) {
                        current_ids
                            .entry(id.clone())
                            .or_insert_with(|| intervals.clone());
                    }
                }
                state.matching_ids = current_ids;
                for (occurrence, signal) in new_matches {
                    let class = signal_class(signal);
                    let cooldown_key = (target_id.clone(), (*symbol).to_owned(), class);
                    let event_key = (target_id.clone(), (*symbol).to_owned(), occurrence.clone());
                    if jobs.contains_key(&event_key) {
                        continue;
                    }
                    let cooling_down = self
                        .last_queued
                        .get(&cooldown_key)
                        .map(|last| {
                            now_ms.saturating_sub(*last)
                                < (alert.cooldown_secs.saturating_mul(1000)) as i64
                        })
                        .unwrap_or(false);
                    if cooling_down {
                        continue;
                    }
                    if jobs.len() >= JOB_LIMIT_PER_EVALUATION {
                        self.health.dropped = self.health.dropped.saturating_add(1);
                        continue;
                    }
                    self.last_queued.insert(cooldown_key, now_ms);
                    jobs.insert(
                        event_key,
                        DeliveryJob {
                            alert_id: alert.id.clone(),
                            target_id: target_id.clone(),
                            symbol: (*symbol).to_owned(),
                            event_key: format!("{symbol}:{occurrence}:{now_ms}"),
                            generation,
                            message: format_message(alert, symbol, signal, now_ms),
                            created_at: now_ms,
                            webhook_url: alert.webhook_url.clone(),
                        },
                    );
                }
            }
        }
        self.health.queued = self.health.queued.saturating_add(jobs.len() as u64);
        jobs.into_values().collect()
    }

    /// Result text is produced by send() and never contains URL, response body or credentials.
    pub fn report_result(&mut self, job: &DeliveryJob, result: &Result<(), String>, now_ms: i64) {
        if self.generation != Some(job.generation) {
            return;
        }
        self.health.last_attempt_at = Some(now_ms);
        match result {
            Ok(()) => {
                self.health.successful = self.health.successful.saturating_add(1);
                self.health.last_success_at = Some(now_ms);
                self.health.last_error = None;
            }
            Err(error) => {
                self.health.failed = self.health.failed.saturating_add(1);
                self.health.last_error = Some(error.clone());
            }
        }
        self.recent.push_front(DeliveryStatus {
            alert_id: job.alert_id.clone(),
            target_id: job.target_id.clone(),
            symbol: job.symbol.clone(),
            attempted_at: now_ms,
            successful: result.is_ok(),
            error: result.as_ref().err().cloned(),
        });
        self.recent.truncate(RECENT_STATUS_LIMIT);
    }
}

pub fn subscription_matches(
    alert: &WecomAlertConfig,
    symbol: &str,
    signal: &SignalStructure,
) -> bool {
    if !alert.symbols.iter().any(|selected| selected == symbol)
        || !alert.kinds.contains(&signal.kind)
    {
        return false;
    }
    match (
        Interval::parse(&alert.min_signal_interval),
        Interval::parse(&signal.anchor_interval),
    ) {
        (Ok(minimum), Ok(anchor)) => anchor.as_millis() >= minimum.as_millis(),
        _ => false,
    }
}

fn signal_class(signal: &SignalStructure) -> String {
    format!("{:?}:{:?}", signal.kind, signal.direction)
}

fn signal_intervals(signal: &SignalStructure) -> Vec<String> {
    signal
        .runs
        .iter()
        .flat_map(|run| run.intervals.iter().cloned())
        .collect()
}

fn occurrence_key(signal: &SignalStructure) -> String {
    if signal.id.is_empty() {
        // Runtime normally assigns stable occurrence IDs. This fallback preserves subscriptions
        // if a caller uses raw detector output, avoiding repeated sends on range expansion.
        signal_class(signal)
    } else {
        signal.id.clone()
    }
}

fn format_message(
    alert: &WecomAlertConfig,
    symbol: &str,
    signal: &SignalStructure,
    now_ms: i64,
) -> String {
    let kind = match signal.kind {
        SignalKind::Extreme => "乖离共振",
        SignalKind::Compression => "多周期近均线",
        SignalKind::Conflict => "长短周期分歧",
    };
    let direction = match (signal.kind, signal.direction) {
        (SignalKind::Conflict, SignalDirection::Positive) => "短正长负",
        (SignalKind::Conflict, SignalDirection::Negative) => "短负长正",
        (_, SignalDirection::Positive) => "上方",
        (_, SignalDirection::Negative) => "下方",
        (_, SignalDirection::Neutral) => "近均线",
    };
    let ranges = signal
        .runs
        .iter()
        .map(|run| run.intervals.join("、"))
        .collect::<Vec<_>>()
        .join(" / ");
    let observed = chrono::DateTime::from_timestamp_millis(now_ms)
        .map(|time| {
            time.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                .format("%Y-%m-%d %H:%M:%S +08:00")
                .to_string()
        })
        .unwrap_or_else(|| now_ms.to_string());
    format!(
        "{}\n{} · {} · {}\n周期：{}\n最大级别：{}；覆盖：{}级\n动态K采样时间：{}\n仅表示指标状态，供观察。",
        alert.name,
        symbol,
        kind,
        direction,
        ranges,
        signal.anchor_interval,
        signal.total_level_count,
        observed
    )
}

/// Send one fixed text message. All retries are bounded and response text stays private.
pub async fn send(job: &DeliveryJob) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "unable to initialize wecom sender".to_string())?;
    let mut last_error = "wecom delivery failed".to_string();
    for attempt in 0..3 {
        let retry;
        match client
            .post(&job.webhook_url)
            .json(&serde_json::json!({"msgtype": "text", "text": {"content": job.message}}))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                if !status.is_success() {
                    last_error = format!("wecom HTTP status {}", status.as_u16());
                    retry = status.is_server_error() || status.as_u16() == 429;
                } else {
                    // Keep response bodies bounded; avoid retaining or logging server error text.
                    if response.content_length().unwrap_or(0) > 16_384 {
                        return Err("wecom response exceeds size limit".into());
                    }
                    let mut response = response;
                    let mut bytes = Vec::new();
                    while let Some(chunk) = response
                        .chunk()
                        .await
                        .map_err(|_| "unable to read wecom response".to_string())?
                    {
                        if bytes.len().saturating_add(chunk.len()) > 16_384 {
                            return Err("wecom response exceeds size limit".into());
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    let result: serde_json::Value = serde_json::from_slice(&bytes)
                        .map_err(|_| "invalid wecom JSON response".to_string())?;
                    match result.get("errcode").and_then(serde_json::Value::as_i64) {
                        Some(0) => return Ok(()),
                        Some(code) => {
                            last_error = format!("wecom returned error code {code}");
                            // Invalid credentials and payload cannot improve on retry.
                            retry = matches!(code, -1 | 45009);
                        }
                        None => return Err("wecom response is missing numeric errcode".into()),
                    }
                }
            }
            Err(_) => {
                last_error = "wecom request failed or timed out".into();
                retry = true;
            }
        }
        if !retry || attempt == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
    }
    Err(last_error)
}
