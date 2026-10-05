//! Periodically evaluates complete dynamic snapshots; queries only read results.
use super::config::SignalConfig;
use super::delivery::{self, DeliveryHealth, DeliveryJob, DeliveryManager};
use super::detector::{detect_structures, primary_signal, same_coverage};
use super::model::{Availability, EvidenceReasonCode, IntervalEvidence, SignalStructure};
use crate::domain::{candle::Candle, interval::Interval};
use crate::http::AppState;
use crate::indicators::guaili::compute_guaili;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex, RwLock};

static RUN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalEnvelope {
    pub enabled: bool,
    pub status: String,
    pub config_hash: String,
    pub indicator_config: SignalIndicatorSummary,
    pub rule_config: SignalRuleSummary,
    pub quality_config: SignalQualitySummary,
    pub rule_version: &'static str,
    pub candle_mode: &'static str,
    pub evaluation_mode: &'static str,
    pub evaluation_interval_ms: u64,
    pub server_time: i64,
    pub run_id: String,
    pub snapshot_version: u64,
    pub evaluated_at: Option<i64>,
    pub compute_duration_ms: u64,
    pub config_error: Option<String>,
    pub delivery: DeliveryHealth,
    pub results: Vec<SymbolSignals>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalIndicatorSummary {
    pub ma_type: String,
    pub ma_length: usize,
}

/// Only public calculation metadata; notification credentials are excluded.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalRuleSummary {
    pub extreme_threshold: i32,
    pub compression_band: i32,
    pub minimum_levels: usize,
    pub min_history_bars: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalQualitySummary {
    pub max_market_age_ms: u64,
    pub max_result_age_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolSignals {
    pub symbol: String,
    pub data_status: String,
    pub sampled_at: i64,
    pub market_sequence: Option<u64>,
    pub generation: Option<u64>,
    pub last_market_event_time: Option<i64>,
    pub primary_signal: Option<String>,
    pub signals: Vec<SignalStructure>,
    pub per_interval_quality: Vec<IntervalEvidence>,
    pub missing_intervals: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReloadResponse {
    pub enabled: bool,
    pub status: String,
    pub config_hash: String,
    pub evaluation_interval_ms: u64,
    pub alert_count: usize,
}

#[derive(Clone)]
struct Control {
    config: SignalConfig,
    revision: u64,
}

struct CachedHistory {
    generation: u64,
    open_time: i64,
    candles: Vec<Candle>,
}

#[derive(Default)]
struct Runtime {
    histories: HashMap<(String, String), CachedHistory>,
    previous: HashMap<String, Vec<SignalStructure>>,
    generations: HashMap<String, u64>,
    baseline: HashSet<String>,
    next_id: u64,
}

struct PendingDelivery {
    revision: u64,
    market_generation: Option<u64>,
    job: DeliveryJob,
}

struct AbortTask(tokio::task::JoinHandle<()>);
impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Inner {
    data: AppState,
    path: PathBuf,
    control: RwLock<Control>,
    snapshot: RwLock<SignalEnvelope>,
    runtime: Mutex<Runtime>,
    iteration: Mutex<()>,
    reload_lock: Mutex<()>,
    changed: watch::Sender<u64>,
    delivery: Mutex<DeliveryManager>,
    sender: mpsc::Sender<PendingDelivery>,
    receiver: Mutex<Option<mpsc::Receiver<PendingDelivery>>>,
}

#[derive(Clone)]
pub struct SignalService {
    inner: Arc<Inner>,
}

impl SignalService {
    pub fn new(data: AppState, config_path: PathBuf) -> Self {
        let config = SignalConfig::default();
        let run_id = format!(
            "{}-{}-{}",
            now_ms(),
            std::process::id(),
            RUN_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let snapshot = blank_envelope(&config, run_id, "disabled", 0);
        let (changed, _) = watch::channel(0);
        let (sender, receiver) = mpsc::channel(256);
        Self {
            inner: Arc::new(Inner {
                data,
                path: config_path,
                control: RwLock::new(Control {
                    config,
                    revision: 0,
                }),
                snapshot: RwLock::new(snapshot),
                runtime: Mutex::new(Runtime::default()),
                iteration: Mutex::new(()),
                reload_lock: Mutex::new(()),
                changed,
                delivery: Mutex::new(DeliveryManager::new()),
                sender,
                receiver: Mutex::new(Some(receiver)),
            }),
        }
    }

    pub async fn load_initial(&self) {
        if let Err(error) = self.reload().await {
            let mut snapshot = self.inner.snapshot.write().await;
            snapshot.status = "config_error".into();
            snapshot.config_error = Some(error);
        }
    }

    pub async fn reload(&self) -> Result<ReloadResponse, String> {
        let _reload = self.inner.reload_lock.lock().await;
        let path = self.inner.path.clone();
        let mut config = tokio::task::spawn_blocking(move || SignalConfig::load(&path))
            .await
            .map_err(|_| "unable to load signal configuration".to_string())??;
        let targets = self
            .inner
            .data
            .health_targets
            .iter()
            .map(|target| (target.symbol.clone(), target.interval.clone()))
            .collect::<Vec<_>>();
        config.normalize_and_validate(&targets)?;
        let mut control = self.inner.control.write().await;
        let reset = config.calculation_hash() != control.config.calculation_hash();
        if reset {
            let mut runtime = self.inner.runtime.lock().await;
            let next_id = runtime.next_id;
            *runtime = Runtime {
                next_id,
                ..Default::default()
            };
        }
        if config.delivery_hash() != control.config.delivery_hash() {
            self.inner.delivery.lock().await.reset();
        }
        control.revision += 1;
        control.config = config.clone();
        let mut snapshot = self.inner.snapshot.write().await;
        if reset || !config.enabled || snapshot.status == "config_error" {
            let run_id = snapshot.run_id.clone();
            let version = snapshot.snapshot_version + 1;
            *snapshot = blank_envelope(
                &config,
                run_id,
                if config.enabled {
                    "warming_up"
                } else {
                    "disabled"
                },
                version,
            );
        } else {
            snapshot.config_error = None;
        }
        let response = ReloadResponse {
            enabled: config.enabled,
            status: snapshot.status.clone(),
            config_hash: format!("{:016x}", config.calculation_hash()),
            evaluation_interval_ms: config.evaluation_interval_secs * 1000,
            alert_count: config.wecom_alerts.len(),
        };
        self.inner.changed.send_replace(control.revision);
        Ok(response)
    }

    /// Start one sampler and one network sender. No query handler calls this.
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let sender_service = service.clone();
            let _delivery_task =
                AbortTask(tokio::spawn(
                    async move { sender_service.delivery_loop().await },
                ));
            let mut changes = service.inner.changed.subscribe();
            let interval = service
                .inner
                .control
                .read()
                .await
                .config
                .evaluation_interval_secs;
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let enabled = service.inner.control.read().await.config.enabled;
                        if enabled { service.sample_once_at(now_ms()).await; }
                    },
                    result = changes.changed() => {
                        if result.is_err() { break; }
                        let interval = service.inner.control.read().await.config.evaluation_interval_secs;
                        ticker = tokio::time::interval(Duration::from_secs(interval));
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                }
            }
        })
    }

    pub async fn snapshot_at(&self, time_ms: i64) -> SignalEnvelope {
        let control = self.inner.control.read().await;
        let mut result = self.inner.snapshot.read().await.clone();
        result.server_time = time_ms;
        result.delivery = self.inner.delivery.lock().await.health();
        if control.config.enabled
            && result.evaluated_at.is_some_and(|at| {
                time_ms < at - 2000
                    || time_ms.saturating_sub(at)
                        > (control.config.quality.max_result_age_secs * 1000) as i64
            })
        {
            result.status = "degraded".into();
            for symbol in &mut result.results {
                symbol.data_status = "stale".into();
                symbol.signals.clear();
                symbol.primary_signal = None;
                for evidence in &mut symbol.per_interval_quality {
                    evidence.availability = Availability::Stale;
                    evidence.reason = Some("signal sampling result is stale".into());
                    evidence.reason_code = Some(EvidenceReasonCode::SamplingStale);
                    evidence.clear_values();
                }
            }
        }
        if control.config.enabled {
            for symbol in &mut result.results {
                if !symbol
                    .per_interval_quality
                    .iter()
                    .any(|e| e.value.is_some())
                {
                    continue;
                }
                let live = self.inner.data.latest.live_snapshot(&symbol.symbol).await;
                let maximum_age = (control.config.quality.max_market_age_secs * 1000) as i64;
                let unavailable = live.as_ref().is_some_and(|value| {
                    value.recovering || Some(value.generation) != symbol.generation
                });
                let stale = live.as_ref().is_none_or(|value| {
                    time_ms.saturating_sub(value.market_event_time_ms) > maximum_age
                        || time_ms.saturating_sub(value.received_at_ms) > maximum_age
                });
                if unavailable || stale {
                    symbol.data_status = if unavailable { "recovering" } else { "stale" }.into();
                    symbol.signals.clear();
                    symbol.primary_signal = None;
                    for evidence in &mut symbol.per_interval_quality {
                        evidence.availability = if unavailable {
                            Availability::Recovering
                        } else {
                            Availability::Stale
                        };
                        evidence.reason = Some(
                            "current market state no longer validates the sampled result".into(),
                        );
                        evidence.reason_code = Some(if unavailable {
                            EvidenceReasonCode::MarketRecovering
                        } else {
                            EvidenceReasonCode::MarketStale
                        });
                        evidence.clear_values();
                    }
                    result.status = "degraded".into();
                }
            }
        }
        result
    }

    pub async fn configured_symbols(&self) -> Vec<String> {
        self.inner.control.read().await.config.symbols.clone()
    }

    pub async fn sample_once_at(&self, time_ms: i64) {
        let _iteration = self.inner.iteration.lock().await;
        let control = self.inner.control.read().await.clone();
        if !control.config.enabled {
            return;
        }
        let mut config_changes = self.inner.changed.subscribe();
        if *config_changes.borrow() != control.revision {
            return;
        }
        let started = Instant::now();
        let config = control.config.clone();
        let mut inputs = Vec::new();
        let previous_results = self
            .inner
            .snapshot
            .read()
            .await
            .results
            .iter()
            .map(|value| (value.symbol.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let mut reused = Vec::new();
        // Never hold the runtime lock while waiting for market recovery or database IO:
        // disabling/reloading must remain responsive even while the feed is stalled.
        let mut histories = std::mem::take(&mut self.inner.runtime.lock().await.histories);
        for symbol in &config.symbols {
            let lock = self.inner.data.latest.market_update_lock();
            let _read = tokio::select! {
                guard = lock.read() => guard,
                _ = config_changes.changed() => return,
            };
            if self.inner.control.read().await.revision != control.revision {
                return;
            }
            let live = self.inner.data.latest.live_snapshot(symbol).await;
            if let (Some(live), Some(previous)) = (&live, previous_results.get(symbol)) {
                let age = (config.quality.max_market_age_secs * 1000) as i64;
                if previous.data_status == "ready"
                    && !live.recovering
                    && previous.market_sequence == Some(live.sequence)
                    && previous.generation == Some(live.generation)
                    && time_ms >= live.market_event_time_ms - 2000
                    && time_ms >= live.received_at_ms - 2000
                    && time_ms.saturating_sub(live.market_event_time_ms) <= age
                    && time_ms.saturating_sub(live.received_at_ms) <= age
                    && live
                        .candles
                        .values()
                        .all(|candle| candle.open_time <= time_ms && candle.close_time >= time_ms)
                {
                    let mut previous = previous.clone();
                    previous.sampled_at = time_ms;
                    reused.push(previous);
                    continue;
                }
            }
            let intervals = self.intervals(symbol);
            let mut series = Vec::new();
            for interval in intervals {
                let current = live
                    .as_ref()
                    .and_then(|snapshot| snapshot.candles.get(&interval))
                    .cloned();
                let history = if let (Some(current), Some(live)) = (&current, &live) {
                    let key = (symbol.clone(), interval.clone());
                    let cached = histories.get(&key).filter(|cache| {
                        cache.generation == live.generation && cache.open_time == current.open_time
                    });
                    match cached {
                        Some(cache) => Ok(cache.candles.clone()),
                        None => {
                            match self
                                .history(
                                    symbol,
                                    &interval,
                                    current.open_time,
                                    config.indicator.calc_limit - 1,
                                )
                                .await
                            {
                                Ok(candles) => {
                                    histories.insert(
                                        key,
                                        CachedHistory {
                                            generation: live.generation,
                                            open_time: current.open_time,
                                            candles: candles.clone(),
                                        },
                                    );
                                    Ok(candles)
                                }
                                Err(error) => Err(error),
                            }
                        }
                    }
                } else {
                    Ok(Vec::new())
                };
                series.push((interval, current, history));
            }
            inputs.push((symbol.clone(), live, series));
        }
        if self.inner.control.read().await.revision != control.revision {
            return;
        }
        let evaluated = tokio::task::spawn_blocking(move || {
            inputs
                .into_iter()
                .map(|(symbol, live, series)| {
                    let evidence = series
                        .into_iter()
                        .map(|(interval, current, history)| {
                            evaluate_series(
                                &config,
                                &interval,
                                current,
                                history,
                                live.as_ref(),
                                time_ms,
                            )
                        })
                        .collect::<Vec<_>>();
                    let signals = detect_structures(&evidence, &config.rules);
                    SymbolSignals {
                        symbol,
                        data_status: data_status(&evidence),
                        sampled_at: time_ms,
                        market_sequence: live.as_ref().map(|value| value.sequence),
                        generation: live.as_ref().map(|value| value.generation),
                        last_market_event_time: live
                            .as_ref()
                            .map(|value| value.market_event_time_ms),
                        primary_signal: None,
                        signals,
                        missing_intervals: evidence
                            .iter()
                            .filter(|value| value.availability.is_unknown())
                            .map(|value| value.interval.clone())
                            .collect(),
                        per_interval_quality: evidence,
                    }
                })
                .collect::<Vec<_>>()
        })
        .await;
        let Ok(mut results) = evaluated else {
            return;
        };
        results.extend(reused);
        results.sort_by_key(|value| {
            control
                .config
                .symbols
                .iter()
                .position(|symbol| symbol == &value.symbol)
                .unwrap_or(usize::MAX)
        });
        let current_control = self.inner.control.read().await;
        if current_control.revision != control.revision || !current_control.config.enabled {
            return;
        }
        // A disconnect during CPU evaluation invalidates the captured market generation.
        let market_lock = self.inner.data.latest.market_update_lock();
        let _market_read = market_lock.read().await;
        for result in &mut results {
            let latest = self.inner.data.latest.live_snapshot(&result.symbol).await;
            if latest
                .as_ref()
                .is_some_and(|live| live.recovering || Some(live.generation) != result.generation)
            {
                result.signals.clear();
                result.data_status = "recovering".into();
                result.generation = latest.as_ref().map(|live| live.generation);
                result.market_sequence = latest.as_ref().map(|live| live.sequence);
                for evidence in &mut result.per_interval_quality {
                    evidence.availability = Availability::Recovering;
                    evidence.reason = Some("market generation changed during evaluation".into());
                    evidence.reason_code = Some(EvidenceReasonCode::MarketRecovering);
                }
                result.missing_intervals = result
                    .per_interval_quality
                    .iter()
                    .map(|value| value.interval.clone())
                    .collect();
            }
        }
        let mut runtime = self.inner.runtime.lock().await;
        runtime.histories = histories;
        let run_id = self.inner.snapshot.read().await.run_id.clone();
        let mut rebased_symbols = Vec::new();
        for result in &mut results {
            if result.generation.is_some()
                && runtime.generations.get(&result.symbol).copied() != result.generation
            {
                rebased_symbols.push(result.symbol.clone());
            }
            assign_occurrences(&mut runtime, result, &run_id, time_ms);
            result.primary_signal = primary_signal(&result.signals);
        }
        drop(runtime);
        let observations = results
            .iter()
            .map(|value| {
                (
                    value.symbol.clone(),
                    value.signals.clone(),
                    value.per_interval_quality.clone(),
                )
            })
            .collect::<Vec<_>>();
        let market_generations = results
            .iter()
            .map(|value| (value.symbol.clone(), value.generation))
            .collect::<HashMap<_, _>>();
        let mut delivery = self.inner.delivery.lock().await;
        for symbol in rebased_symbols {
            delivery.reset_symbol(&symbol);
        }
        let jobs = delivery.prepare_with_evidence(&control.config, &observations, time_ms);
        drop(delivery);
        let mut snapshot = self.inner.snapshot.write().await;
        snapshot.enabled = true;
        snapshot.status = if results.iter().all(|value| value.data_status == "ready") {
            "ready"
        } else if results
            .iter()
            .all(|value| matches!(value.data_status.as_str(), "warming_up" | "recovering"))
        {
            "warming_up"
        } else {
            "degraded"
        }
        .into();
        snapshot.config_hash = format!("{:016x}", control.config.calculation_hash());
        snapshot.snapshot_version += 1;
        snapshot.evaluated_at = Some(time_ms);
        snapshot.compute_duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        snapshot.results = results;
        snapshot.config_error = None;
        drop(snapshot);
        for job in jobs {
            if self
                .inner
                .sender
                .try_send(PendingDelivery {
                    revision: control.revision,
                    market_generation: market_generations.get(&job.symbol).copied().flatten(),
                    job,
                })
                .is_err()
            {
                self.inner.delivery.lock().await.record_dropped();
                tracing::warn!("signal notification queue is full; notification dropped");
            }
        }
    }

    fn intervals(&self, symbol: &str) -> Vec<String> {
        let mut intervals = self
            .inner
            .data
            .health_targets
            .iter()
            .filter(|target| target.symbol == symbol)
            .map(|target| target.interval.clone())
            .collect::<Vec<_>>();
        intervals.sort_by_key(|value| {
            Interval::parse(value)
                .map(|period| period.as_millis())
                .unwrap_or(u64::MAX)
        });
        intervals.dedup();
        intervals
    }

    async fn history(
        &self,
        symbol: &str,
        interval: &str,
        before: i64,
        limit: usize,
    ) -> Result<Vec<Candle>, String> {
        let parsed =
            Interval::parse(interval).map_err(|_| "invalid configured period".to_string())?;
        let rows = if parsed.as_millis() < 60_000 {
            self.inner
                .data
                .memory_series
                .query(symbol, interval, None, Some(before - 1), limit as u32)
                .await
        } else {
            // Copy buffered rows first: a concurrent flush cannot make a closed row disappear.
            let buffered = self
                .inner
                .data
                .closed_buffer
                .query(symbol, interval, None, Some(before - 1), limit as u32)
                .await;
            let mut persisted = self
                .inner
                .data
                .store
                .query_klines(symbol, interval, None, Some(before - 1), limit as u32)
                .await
                .map_err(|_| "unable to read candle history".to_string())?;
            persisted.extend(buffered);
            persisted
        };
        let mut by_time = BTreeMap::new();
        for row in rows {
            if row.candle.is_closed && row.candle.close_time < before {
                by_time.insert(row.candle.open_time, row.candle);
            }
        }
        let mut candles = by_time.into_values().collect::<Vec<_>>();
        if candles.len() > limit {
            candles.drain(..candles.len() - limit);
        }
        if let Some(gap) = candles
            .windows(2)
            .rposition(|pair| pair[1].open_time - pair[0].open_time != parsed.as_millis() as i64)
        {
            candles.drain(..=gap);
        }
        Ok(candles)
    }

    async fn delivery_loop(&self) {
        let Some(mut receiver) = self.inner.receiver.lock().await.take() else {
            return;
        };
        let mut changes = self.inner.changed.subscribe();
        let mut recovery_changes = self.inner.data.latest.recovery_changes();
        while let Some(pending) = receiver.recv().await {
            let valid = |control: &Control| {
                control.config.enabled
                    && control.revision == pending.revision
                    && control.config.delivery_hash() == pending.job.generation
            };
            let config = self.inner.control.read().await.clone();
            if !valid(&config)
                || !self.delivery_market_valid(&pending, &config.config).await
                || now_ms().saturating_sub(pending.job.created_at)
                    > (config.config.quality.max_result_age_secs * 1000) as i64
            {
                self.inner.delivery.lock().await.record_dropped();
                continue;
            }
            let request = delivery::send(&pending.job);
            tokio::pin!(request);
            let outcome = loop {
                tokio::select! {
                    result = &mut request => break Some(result),
                    result = changes.changed() => {
                        if result.is_err() || !valid(&*self.inner.control.read().await) { break None; }
                    },
                    result = recovery_changes.changed() => {
                        if result.is_err() || !self.delivery_market_valid(&pending, &config.config).await { break None; }
                    }
                }
            };
            if let Some(result) = outcome {
                self.inner
                    .delivery
                    .lock()
                    .await
                    .report_result(&pending.job, &result, now_ms());
                if let Err(error) = result {
                    tracing::warn!(alert_id = %pending.job.alert_id, "signal notification failed: {error}");
                }
            }
        }
    }

    async fn delivery_market_valid(
        &self,
        pending: &PendingDelivery,
        config: &SignalConfig,
    ) -> bool {
        self.inner
            .data
            .latest
            .live_snapshot(&pending.job.symbol)
            .await
            .is_some_and(|live| {
                !live.recovering
                    && Some(live.generation) == pending.market_generation
                    && now_ms().saturating_sub(live.market_event_time_ms)
                        <= (config.quality.max_market_age_secs * 1000) as i64
                    && now_ms().saturating_sub(live.received_at_ms)
                        <= (config.quality.max_market_age_secs * 1000) as i64
            })
    }
}

fn blank_envelope(
    config: &SignalConfig,
    run_id: String,
    status: &str,
    version: u64,
) -> SignalEnvelope {
    SignalEnvelope {
        enabled: config.enabled,
        status: status.into(),
        config_hash: format!("{:016x}", config.calculation_hash()),
        indicator_config: SignalIndicatorSummary {
            ma_type: config.indicator.ma_type.clone(),
            ma_length: config.indicator.ma_length,
        },
        rule_config: SignalRuleSummary {
            extreme_threshold: config.rules.extreme_threshold,
            compression_band: config.rules.compression_band,
            minimum_levels: config.rules.minimum_levels,
            min_history_bars: config.rules.min_history_bars,
        },
        quality_config: SignalQualitySummary {
            max_market_age_ms: config.quality.max_market_age_secs * 1000,
            max_result_age_ms: config.quality.max_result_age_secs * 1000,
        },
        rule_version: "live-v1",
        candle_mode: "live",
        evaluation_mode: "sampled_live",
        evaluation_interval_ms: config.evaluation_interval_secs * 1000,
        server_time: now_ms(),
        run_id,
        snapshot_version: version,
        evaluated_at: None,
        compute_duration_ms: 0,
        config_error: None,
        delivery: DeliveryHealth::default(),
        results: Vec::new(),
    }
}

fn evaluate_series(
    config: &SignalConfig,
    interval: &str,
    current: Option<Candle>,
    history: Result<Vec<Candle>, String>,
    live: Option<&crate::memory::LiveSymbolSnapshot>,
    time_ms: i64,
) -> IntervalEvidence {
    let mut result = IntervalEvidence {
        interval: interval.into(),
        ..Default::default()
    };
    let unavailable = |mut result: IntervalEvidence,
                       availability: Availability,
                       reason_code: EvidenceReasonCode,
                       reason: &str| {
        result.availability = availability;
        result.reason = Some(reason.into());
        result.reason_code = Some(reason_code);
        result
    };
    let Some(live) = live else {
        return unavailable(
            result,
            Availability::WarmingUp,
            EvidenceReasonCode::WaitingMarket,
            "waiting for a live trade snapshot",
        );
    };
    result.market_event_time = Some(live.market_event_time_ms);
    if live.recovering {
        return unavailable(
            result,
            Availability::Recovering,
            EvidenceReasonCode::MarketRecovering,
            "market stream is recovering",
        );
    }
    if live.market_event_time_ms > time_ms + 2000 || live.received_at_ms > time_ms + 2000 {
        return unavailable(
            result,
            Availability::Invalid,
            EvidenceReasonCode::MarketTimeInvalid,
            "market update time is in the future",
        );
    }
    let maximum_age = (config.quality.max_market_age_secs * 1000) as i64;
    if time_ms.saturating_sub(live.market_event_time_ms) > maximum_age
        || time_ms.saturating_sub(live.received_at_ms) > maximum_age
    {
        return unavailable(
            result,
            Availability::Stale,
            EvidenceReasonCode::MarketStale,
            "live market updates are stale",
        );
    }
    let Some(mut current) = current else {
        return unavailable(
            result,
            Availability::Missing,
            EvidenceReasonCode::DynamicMissing,
            "current dynamic candle is unavailable",
        );
    };
    result.open_time = Some(current.open_time);
    result.close_time = Some(current.close_time);
    result.is_closed = Some(false);
    if current.open_time > live.market_event_time_ms
        || current.close_time < live.market_event_time_ms
        || current.close_time < time_ms
    {
        return unavailable(
            result,
            Availability::Missing,
            EvidenceReasonCode::DynamicTimeMismatch,
            "current candle does not cover this market time",
        );
    }
    current.is_closed = false;
    let Ok(mut candles) = history else {
        return unavailable(
            result,
            Availability::Invalid,
            EvidenceReasonCode::HistoryUnavailable,
            "unable to read candle history",
        );
    };
    result.history_count = candles.len();
    let duration = Interval::parse(interval)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0);
    if candles
        .last()
        .is_some_and(|last| current.open_time - last.open_time != duration)
    {
        return unavailable(
            result,
            Availability::Gap,
            EvidenceReasonCode::HistoryGap,
            "history is not adjacent to the current candle",
        );
    }
    if candles.len() < config.rules.min_history_bars {
        return unavailable(
            result,
            Availability::WarmingUp,
            EvidenceReasonCode::InsufficientHistory,
            "insufficient contiguous closed history",
        );
    }
    candles.push(current);
    let points = compute_guaili(&candles, config.indicator.to_guaili_config());
    let Some(point) = points.last() else {
        return unavailable(
            result,
            Availability::Invalid,
            EvidenceReasonCode::IndicatorMissing,
            "indicator result is missing",
        );
    };
    if !point.has_value() {
        let code = match point.quality.reason_code() {
            Some("invalid_data") => EvidenceReasonCode::InvalidData,
            Some("insufficient_history") => EvidenceReasonCode::InsufficientHistory,
            Some("indicator_warming_up") => EvidenceReasonCode::IndicatorWarmingUp,
            _ => EvidenceReasonCode::IndicatorInvalid,
        };
        let availability = if point.quality.availability() == "warming_up" {
            Availability::WarmingUp
        } else {
            Availability::Invalid
        };
        return unavailable(
            result,
            availability,
            code,
            point.quality.reason().unwrap_or("indicator is unavailable"),
        );
    }
    let previous_atr = points
        .get(points.len().saturating_sub(2))
        .map(|point| point.atr14)
        .unwrap_or(0.0);
    if previous_atr <= 0.0
        || !previous_atr.is_finite()
        || ![point.guaili, point.ma, point.atr14]
            .iter()
            .all(|value| value.is_finite())
    {
        return unavailable(
            result,
            Availability::Invalid,
            EvidenceReasonCode::IndicatorInvalid,
            "indicator volatility denominator is invalid",
        );
    }
    let Some(rank) = point.atr_rank.filter(|value| value.is_finite()) else {
        return unavailable(
            result,
            Availability::WarmingUp,
            EvidenceReasonCode::IndicatorWarmingUp,
            "volatility rank is unavailable",
        );
    };
    result.value = Some(point.value);
    result.guaili = Some(point.guaili);
    result.ma = Some(point.ma);
    result.atr14 = Some(point.atr14);
    result.atr_rank = Some(rank);
    result.long_trend = Some(point.long_trend);
    result.short_trend = Some(point.short_trend);
    result.availability = if point.rank_filter() {
        Availability::Ready
    } else {
        Availability::Filtered
    };
    result
}

fn data_status(evidence: &[IntervalEvidence]) -> String {
    if evidence
        .iter()
        .all(|value| !value.availability.is_unknown())
    {
        "ready"
    } else if evidence
        .iter()
        .any(|value| value.availability == Availability::Recovering)
    {
        "recovering"
    } else if evidence
        .iter()
        .all(|value| value.availability == Availability::Stale)
    {
        "stale"
    } else if evidence.iter().all(|value| value.availability.is_unknown()) {
        "warming_up"
    } else {
        "degraded"
    }
    .into()
}

fn assign_occurrences(
    runtime: &mut Runtime,
    result: &mut SymbolSignals,
    run_id: &str,
    time_ms: i64,
) {
    if let Some(generation) = result.generation {
        if runtime
            .generations
            .insert(result.symbol.clone(), generation)
            != Some(generation)
        {
            runtime.previous.remove(&result.symbol);
            runtime.baseline.remove(&result.symbol);
        }
    }
    let had_baseline = runtime.baseline.contains(&result.symbol);
    if result
        .per_interval_quality
        .iter()
        .any(|value| !value.availability.is_unknown())
    {
        runtime.baseline.insert(result.symbol.clone());
    }
    let mut previous = runtime.previous.remove(&result.symbol).unwrap_or_default();
    let mut retained = Vec::new();
    for signal in &mut result.signals {
        let candidate = previous
            .iter()
            .enumerate()
            .filter(|(_, old)| old.kind == signal.kind && old.direction == signal.direction)
            .map(|(index, old)| {
                (
                    index,
                    old.runs
                        .iter()
                        .flat_map(|run| &run.intervals)
                        .filter(|period| {
                            signal.runs.iter().any(|run| run.intervals.contains(period))
                        })
                        .count(),
                )
            })
            .filter(|(_, overlap)| *overlap > 0)
            .max_by_key(|(_, overlap)| *overlap)
            .map(|(index, _)| index);
        if let Some(index) = candidate {
            let old = previous.remove(index);
            let changed_at = if same_coverage(signal, &old) {
                old.last_changed_at
            } else {
                Some(time_ms)
            };
            signal.id = old.id;
            signal.first_observed_at = old.first_observed_at;
            signal.formed_at = old.formed_at;
            signal.last_changed_at = changed_at;
        } else {
            runtime.next_id += 1;
            signal.id = format!("{run_id}:{}:{}", result.symbol, runtime.next_id);
            signal.first_observed_at = Some(time_ms);
            signal.formed_at = had_baseline.then_some(time_ms);
            signal.last_changed_at = Some(time_ms);
        }
        retained.push(signal.clone());
    }
    for old in previous {
        if old
            .runs
            .iter()
            .flat_map(|run| &run.intervals)
            .any(|period| {
                result.per_interval_quality.iter().any(|evidence| {
                    &evidence.interval == period && evidence.availability.is_unknown()
                })
            })
        {
            retained.push(old);
        }
    }
    runtime.previous.insert(result.symbol.clone(), retained);
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_causes_are_explicit_for_invalid_time_history_gap_and_data() {
        let now = 1_760_000_010_000;
        let current_open = now - now % 60_000;
        let candle = |open_time, is_closed| Candle {
            open_time,
            close_time: open_time + 59_999,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 10.0,
            quote_volume: 1000.0,
            trade_count: 1,
            is_closed,
        };
        let current = candle(current_open, false);
        let history = (1..=60)
            .rev()
            .map(|index| candle(current_open - index * 60_000, true))
            .collect::<Vec<_>>();
        let config = SignalConfig::default();
        let live = crate::memory::LiveSymbolSnapshot {
            candles: HashMap::new(),
            market_event_time_ms: now,
            received_at_ms: now,
            sequence: 1,
            generation: 1,
            recovering: false,
        };
        let evaluate = |current: Candle, history: Result<Vec<Candle>, String>, live| {
            evaluate_series(&config, "1", Some(current), history, Some(live), now)
        };
        assert_eq!(
            evaluate(current.clone(), Err("private storage error".into()), &live).reason_code,
            Some(EvidenceReasonCode::HistoryUnavailable)
        );
        let mut gap = history.clone();
        gap.pop();
        assert_eq!(
            evaluate(current.clone(), Ok(gap), &live).reason_code,
            Some(EvidenceReasonCode::HistoryGap)
        );
        let mut invalid = current.clone();
        invalid.close = f64::NAN;
        let invalid_result = evaluate(invalid, Ok(history.clone()), &live);
        assert_eq!(
            invalid_result.reason_code,
            Some(EvidenceReasonCode::InvalidData)
        );
        assert_eq!(invalid_result.availability, Availability::Invalid);
        assert!(invalid_result.value.is_none());
        let mut wrong_bucket = current.clone();
        wrong_bucket.close_time = now - 1;
        assert_eq!(
            evaluate(wrong_bucket, Ok(history.clone()), &live).reason_code,
            Some(EvidenceReasonCode::DynamicTimeMismatch)
        );
        let future_live = crate::memory::LiveSymbolSnapshot {
            received_at_ms: now + 2001,
            ..live.clone()
        };
        assert_eq!(
            evaluate(current, Ok(history), &future_live).reason_code,
            Some(EvidenceReasonCode::MarketTimeInvalid)
        );
    }
}
