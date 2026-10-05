use crypto_candlestick::domain::{candle::Candle, interval::Interval};
use crypto_candlestick::http::{AppState, HealthTarget};
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::signals::model::{
    Availability, EvidenceReasonCode, SignalDirection, SignalKind,
};
use crypto_candlestick::signals::service::SignalService;
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const SYMBOL: &str = "BTCUSDT";
const PERIODS: &[&str] = &["1", "2", "3", "5", "8"];
// Every fixture period starts at the same boundary, with ample history and
// the dynamic bucket's expected end still in the future.
// Use a past timestamp: SQLite's public query masks stored closed flags for
// buckets whose close time is still in the real clock's future.
const NOW: i64 = 1_759_996_823_000;
const CONFIG: &str = r#"
enabled = true
evaluation_interval_secs = 5
symbols = ["BTCUSDT"]

[indicator]
ma_type = "EMA"
ma_length = 20
calc_limit = 100
atr_len = 1
atr_percent_len = 20
max_atr_rank = 100.0
slope_mul = 0.1
use_slope = true

[rules]
extreme_threshold = 10
compression_band = 2
minimum_levels = 5
min_history_bars = 60

[quality]
max_market_age_secs = 30
max_result_age_secs = 15
"#;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempConfig {
    path: PathBuf,
}

impl TempConfig {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "crypto-candlestick-signal-service-{}-{stamp}-{sequence}.toml",
            std::process::id()
        ));
        fs::write(&path, CONFIG).unwrap();
        Self { path }
    }

    fn write(&self, contents: &str) {
        fs::write(&self.path, contents).unwrap();
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        // Remove only the exact temporary configuration owned by this fixture.
        let _ = fs::remove_file(&self.path);
    }
}

struct Fixture {
    config: TempConfig,
    data: AppState,
    service: SignalService,
}

impl Fixture {
    async fn new(history_count: usize) -> Self {
        let config = TempConfig::new();
        let data = AppState {
            store: SqliteStore::connect("sqlite::memory:").await.unwrap(),
            latest: LatestCache::default(),
            memory_series: MemorySeriesStore::default(),
            closed_buffer: ClosedKlineBuffer::default(),
            health_targets: PERIODS
                .iter()
                .map(|period| HealthTarget {
                    symbol: SYMBOL.to_string(),
                    interval: (*period).to_string(),
                })
                .collect(),
            runtime_health: RuntimeHealth::default(),
        };
        for period in PERIODS {
            let interval = Interval::parse(period).unwrap();
            let duration = interval.as_millis() as i64;
            let current_open = interval.bucket_start_ms(NOW);
            let history: Vec<_> = (1..=history_count)
                .rev()
                .map(|index| {
                    candle(
                        current_open - index as i64 * duration,
                        duration,
                        100.0,
                        true,
                    )
                })
                .collect();
            data.store
                .upsert_candles(SYMBOL, period, &history)
                .await
                .unwrap();
        }
        let service = SignalService::new(data.clone(), config.path.clone());
        service.reload().await.unwrap();
        Self {
            config,
            data,
            service,
        }
    }

    async fn publish(&self, price: f64, now: i64) {
        self.publish_except(price, now, None).await;
    }

    async fn publish_except(&self, price: f64, now: i64, omitted: Option<&str>) {
        let update_lock = self.data.latest.market_update_lock();
        let _guard = update_lock.write().await;
        let mut candles = HashMap::new();
        for period in PERIODS {
            if Some(*period) == omitted {
                continue;
            }
            let interval = Interval::parse(period).unwrap();
            let current = candle(
                interval.bucket_start_ms(now),
                interval.as_millis() as i64,
                price,
                false,
            );
            self.data
                .latest
                .upsert(SYMBOL, period, current.clone())
                .await;
            candles.insert((*period).to_string(), current);
        }
        self.data
            .latest
            .publish_live_symbol(SYMBOL, candles, now, now)
            .await;
    }
}

fn candle(open_time: i64, duration: i64, price: f64, is_closed: bool) -> Candle {
    Candle {
        open_time,
        close_time: open_time + duration - 1,
        open: price,
        high: price + 1.0,
        low: price - 1.0,
        close: price,
        volume: 10.0,
        quote_volume: price * 10.0,
        trade_count: 10,
        is_closed,
    }
}

#[tokio::test]
async fn disabling_does_not_wait_for_a_stalled_market_feed() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    let lock = fixture.data.latest.market_update_lock();
    let blocked = lock.write().await;
    let service = fixture.service.clone();
    let sampling = tokio::spawn(async move { service.sample_once_at(NOW).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    fixture
        .config
        .write(&CONFIG.replace("enabled = true", "enabled = false"));
    tokio::time::timeout(std::time::Duration::from_secs(1), fixture.service.reload())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.service.snapshot_at(NOW).await.status, "disabled");
    tokio::time::timeout(std::time::Duration::from_secs(1), sampling)
        .await
        .unwrap()
        .unwrap();
    drop(blocked);
    assert!(fixture.service.snapshot_at(NOW).await.results.is_empty());
}

#[tokio::test]
async fn recovery_hides_cached_signals_before_the_next_sampling_round() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    assert!(!fixture.service.snapshot_at(NOW).await.results[0]
        .signals
        .is_empty());
    let lock = fixture.data.latest.market_update_lock();
    let _write = lock.write().await;
    fixture.data.latest.mark_recovering(SYMBOL).await;
    let snapshot = fixture.service.snapshot_at(NOW).await;
    assert_eq!(snapshot.status, "degraded");
    assert_eq!(snapshot.results[0].data_status, "recovering");
    assert!(snapshot.results[0].signals.is_empty());
    assert!(snapshot.results[0]
        .per_interval_quality
        .iter()
        .all(|evidence| { evidence.reason_code == Some(EvidenceReasonCode::MarketRecovering) }));
}

#[tokio::test]
async fn configuration_reload_never_reuses_a_previous_signal_id() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let old_id = fixture.service.snapshot_at(NOW).await.results[0].signals[0]
        .id
        .clone();
    fixture
        .config
        .write(&CONFIG.replace("extreme_threshold = 10", "extreme_threshold = 11"));
    fixture.service.reload().await.unwrap();
    fixture.service.sample_once_at(NOW).await;
    let new_id = fixture.service.snapshot_at(NOW).await.results[0].signals[0]
        .id
        .clone();
    assert_ne!(old_id, new_id);
}

#[tokio::test]
async fn query_reads_cached_result_without_triggering_a_calculation() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    let first = fixture.service.snapshot_at(NOW).await;
    assert!(first.enabled);
    assert!(first.evaluated_at.is_none());
    assert!(first.results.iter().all(|row| row.signals.is_empty()));
    fixture.service.sample_once_at(NOW).await;
    let sampled = fixture.service.snapshot_at(NOW).await;
    assert_eq!(sampled.evaluated_at, Some(NOW));
    assert_eq!(sampled.evaluation_interval_ms, 5_000);
    assert_eq!(sampled.server_time, NOW);
    assert!(!sampled.run_id.is_empty());
    assert!(sampled.snapshot_version > first.snapshot_version);
    fixture.publish(80.0, NOW + 5_000).await;
    let queried = fixture.service.snapshot_at(NOW + 5_000).await;
    assert_eq!(queried.evaluated_at, sampled.evaluated_at);
    assert_eq!(queried.snapshot_version, sampled.snapshot_version);
    assert_eq!(
        queried.results[0].signals[0].direction,
        SignalDirection::Positive
    );
}

#[tokio::test]
async fn evaluates_dynamic_kline_with_future_close_time_and_full_history() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let snapshot = fixture.service.snapshot_at(NOW).await;
    assert_eq!(snapshot.results.len(), 1);
    let row = &snapshot.results[0];
    assert_eq!(row.symbol, SYMBOL);
    assert_eq!(row.signals.len(), 1);
    assert_eq!(row.signals[0].kind, SignalKind::Extreme);
    assert_eq!(row.signals[0].direction, SignalDirection::Positive);
    assert_eq!(row.signals[0].level_count, 5);
    assert_eq!(row.signals[0].anchor_interval, "8");
    assert_eq!(row.primary_signal.as_ref(), Some(&row.signals[0].id));
    assert!(row.signals[0].formed_at.is_none());
    for item in &row.per_interval_quality {
        assert_eq!(item.availability, Availability::Ready);
        assert_eq!(item.history_count, 60);
        assert_eq!(item.is_closed, Some(false));
        assert!(item.close_time.unwrap() > NOW);
        assert!(item.value.unwrap() >= 10);
        assert!(item.guaili.unwrap() > 0.0);
        assert!(item.reason_code.is_none());
    }
}

#[tokio::test]
async fn same_open_candle_recomputes_metrics_and_reverses_signal_episode() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let first = fixture.service.snapshot_at(NOW).await;
    let original = &first.results[0].signals[0];
    fixture.publish(125.0, NOW + 5_000).await;
    fixture.service.sample_once_at(NOW + 5_000).await;
    let updated = fixture.service.snapshot_at(NOW + 5_000).await;
    assert_eq!(updated.results[0].signals[0].id, original.id);
    assert_eq!(
        updated.results[0].signals[0].first_observed_at,
        original.first_observed_at
    );
    assert!(
        updated.results[0].per_interval_quality[0].value.unwrap()
            > first.results[0].per_interval_quality[0].value.unwrap()
    );
    assert_eq!(
        updated.results[0].per_interval_quality[0].open_time,
        first.results[0].per_interval_quality[0].open_time
    );
    fixture.publish(80.0, NOW + 10_000).await;
    fixture.service.sample_once_at(NOW + 10_000).await;
    let reversed = fixture.service.snapshot_at(NOW + 10_000).await;
    assert_eq!(
        reversed.results[0].signals[0].direction,
        SignalDirection::Negative
    );
    assert_ne!(reversed.results[0].signals[0].id, original.id);
    assert_eq!(reversed.results[0].signals[0].formed_at, Some(NOW + 10_000));
    assert!(reversed.results[0]
        .per_interval_quality
        .iter()
        .all(|item| item.value.unwrap() <= -10));
}

#[tokio::test]
async fn missing_dynamic_kline_does_not_fall_back_to_a_closed_kline() {
    let fixture = Fixture::new(60).await;
    fixture.publish_except(120.0, NOW, Some("3")).await;
    let interval = Interval::parse("3").unwrap();
    let duration = interval.as_millis() as i64;
    fixture
        .data
        .latest
        .upsert(
            SYMBOL,
            "3",
            candle(
                interval.bucket_start_ms(NOW) - duration,
                duration,
                120.0,
                true,
            ),
        )
        .await;
    fixture.service.sample_once_at(NOW).await;
    let snapshot = fixture.service.snapshot_at(NOW).await;
    assert!(snapshot.results[0].signals.is_empty());
    let missing = snapshot.results[0]
        .per_interval_quality
        .iter()
        .find(|item| item.interval == "3")
        .unwrap();
    assert_eq!(missing.availability, Availability::Missing);
    assert!(missing.value.is_none());
    assert_eq!(
        missing.reason_code,
        Some(EvidenceReasonCode::DynamicMissing)
    );
}

#[tokio::test]
async fn minimum_history_requirement_excludes_the_dynamic_candle() {
    let fixture = Fixture::new(59).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let snapshot = fixture.service.snapshot_at(NOW).await;
    assert!(snapshot.results[0].signals.is_empty());
    assert!(snapshot.results[0].per_interval_quality.iter().all(|item| {
        item.availability == Availability::WarmingUp
            && item.history_count == 59
            && item.reason_code == Some(EvidenceReasonCode::InsufficientHistory)
    }));
}

#[tokio::test]
async fn old_market_snapshot_becomes_stale_even_when_no_new_trade_arrives() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let initial = fixture.service.snapshot_at(NOW).await;
    let initial_id = initial.results[0].signals[0].id.clone();
    fixture.service.sample_once_at(NOW + 31_000).await;
    let stale = fixture.service.snapshot_at(NOW + 31_000).await;
    assert!(stale.results[0].signals.is_empty());
    assert!(stale.results[0]
        .per_interval_quality
        .iter()
        .all(|item| item.availability == Availability::Stale
            && item.reason_code == Some(EvidenceReasonCode::MarketStale)));
    fixture.publish(120.0, NOW + 32_000).await;
    fixture.service.sample_once_at(NOW + 32_000).await;
    let recovered = fixture.service.snapshot_at(NOW + 32_000).await;
    assert_eq!(recovered.results[0].signals[0].id, initial_id);
    assert!(recovered.results[0].signals[0].formed_at.is_none());
}

#[tokio::test]
async fn query_hides_expired_results_without_recomputing_the_snapshot() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let initial = fixture.service.snapshot_at(NOW).await;
    let expired = fixture.service.snapshot_at(NOW + 16_000).await;
    assert_eq!(expired.evaluated_at, initial.evaluated_at);
    assert_eq!(expired.snapshot_version, initial.snapshot_version);
    assert!(expired.results[0].signals.is_empty());
    assert!(initial.results[0]
        .per_interval_quality
        .iter()
        .all(|e| e.value.is_some()));
    assert!(expired.results[0]
        .per_interval_quality
        .iter()
        .all(|e| e.value.is_none() && e.guaili.is_none() && e.ma.is_none() && e.atr14.is_none()));
    // Expiry only changes the response copy, not the retained sampling evidence.
    let retained = fixture.service.snapshot_at(NOW).await;
    assert!(retained.results[0]
        .per_interval_quality
        .iter()
        .all(|e| e.value.is_some()));
    assert!(expired.results[0]
        .per_interval_quality
        .iter()
        .all(|item| item.availability == Availability::Stale
            && item.reason_code == Some(EvidenceReasonCode::SamplingStale)));
}

#[tokio::test]
async fn query_clears_stale_numeric_evidence_even_without_an_active_structure() {
    let fixture = Fixture::new(60).await;
    fixture.config.write(
        &CONFIG
            .replace("minimum_levels = 5", "minimum_levels = 6")
            .replace("max_result_age_secs = 15", "max_result_age_secs = 120"),
    );
    fixture.service.reload().await.unwrap();
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let initial = fixture.service.snapshot_at(NOW).await;
    assert!(initial.results[0].signals.is_empty());
    assert!(initial.results[0]
        .per_interval_quality
        .iter()
        .all(|e| e.value.is_some()));
    let expired = fixture.service.snapshot_at(NOW + 31_000).await;
    assert!(expired.results[0]
        .per_interval_quality
        .iter()
        .all(|e| e.value.is_none()
            && e.guaili.is_none()
            && e.reason_code == Some(EvidenceReasonCode::MarketStale)));
}

#[tokio::test]
async fn reloaded_quality_metadata_matches_exact_market_and_sampling_expiry() {
    let fixture = Fixture::new(60).await;
    fixture.config.write(
        &CONFIG
            .replace("max_market_age_secs = 30", "max_market_age_secs = 7")
            .replace("max_result_age_secs = 15", "max_result_age_secs = 11"),
    );
    fixture.service.reload().await.unwrap();
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let valid = fixture.service.snapshot_at(NOW + 7000).await;
    assert_eq!(valid.quality_config.max_market_age_ms, 7000);
    assert_eq!(valid.quality_config.max_result_age_ms, 11000);
    assert!(!valid.results[0].signals.is_empty());
    let market_expired = fixture.service.snapshot_at(NOW + 7001).await;
    assert_eq!(market_expired.snapshot_version, valid.snapshot_version);
    assert!(market_expired.results[0].signals.is_empty());
    assert!(market_expired.results[0]
        .per_interval_quality
        .iter()
        .all(|evidence| { evidence.reason_code == Some(EvidenceReasonCode::MarketStale) }));
    let sample_expired = fixture.service.snapshot_at(NOW + 11001).await;
    assert_eq!(sample_expired.snapshot_version, valid.snapshot_version);
    assert!(sample_expired.results[0]
        .per_interval_quality
        .iter()
        .all(|evidence| { evidence.reason_code == Some(EvidenceReasonCode::SamplingStale) }));
}

#[tokio::test]
async fn reloaded_rule_metadata_describes_the_rules_that_actually_evaluate() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    assert!(!fixture.service.snapshot_at(NOW).await.results[0]
        .signals
        .is_empty());
    fixture
        .config
        .write(&CONFIG.replace("extreme_threshold = 10", "extreme_threshold = 1000"));
    fixture.service.reload().await.unwrap();
    fixture.service.sample_once_at(NOW).await;
    let higher_threshold = fixture.service.snapshot_at(NOW).await;
    assert_eq!(higher_threshold.rule_config.extreme_threshold, 1000);
    assert!(higher_threshold.results[0].signals.is_empty());
    assert_eq!(higher_threshold.results[0].data_status, "ready");
    fixture
        .config
        .write(&CONFIG.replace("min_history_bars = 60", "min_history_bars = 70"));
    fixture.service.reload().await.unwrap();
    fixture.service.sample_once_at(NOW).await;
    let more_history = fixture.service.snapshot_at(NOW).await;
    assert_eq!(more_history.rule_config.min_history_bars, 70);
    assert!(more_history.results[0].signals.is_empty());
    assert!(more_history.results[0]
        .per_interval_quality
        .iter()
        .all(|evidence| {
            evidence.history_count == 60
                && evidence.reason_code == Some(EvidenceReasonCode::InsufficientHistory)
        }));
}

#[tokio::test]
async fn disabling_and_reloading_clears_results_and_stops_computation() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    assert!(!fixture.service.snapshot_at(NOW).await.results[0]
        .signals
        .is_empty());
    fixture
        .config
        .write(&CONFIG.replace("enabled = true", "enabled = false"));
    fixture.service.reload().await.unwrap();
    let disabled = fixture.service.snapshot_at(NOW + 5_000).await;
    assert!(!disabled.enabled);
    assert_eq!(disabled.status, "disabled");
    assert!(disabled.results.is_empty());
    fixture.publish(80.0, NOW + 5_000).await;
    fixture.service.sample_once_at(NOW + 5_000).await;
    let sampled = fixture.service.snapshot_at(NOW + 5_000).await;
    assert!(!sampled.enabled);
    assert!(sampled.results.is_empty());
    assert_eq!(sampled.snapshot_version, disabled.snapshot_version);
}

#[tokio::test]
async fn invalid_reload_keeps_previous_configuration_and_never_exposes_webhook() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let original = fixture.service.snapshot_at(NOW).await;
    let secret = "NEVER_EXPOSE_SIGNAL_TEST";
    fixture.config.write(&format!(
        "{}\n[[wecom_alerts]]\nid = \"broken\"\nwebhook_url = \"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={secret}\n",
        CONFIG.replace("enabled = true", "enabled = false")
    ));
    let error = fixture.service.reload().await.unwrap_err();
    assert!(!error.contains(secret));
    assert!(!error.contains("qyapi.weixin.qq.com"));
    let unchanged = fixture.service.snapshot_at(NOW + 5_000).await;
    assert!(unchanged.enabled);
    assert_eq!(unchanged.snapshot_version, original.snapshot_version);
    assert_eq!(
        unchanged.results[0].signals[0].id,
        original.results[0].signals[0].id
    );
    let public_json = serde_json::to_string(&unchanged).unwrap();
    assert!(!public_json.contains(secret));
    assert!(!public_json.contains("webhook"));
}

#[tokio::test]
async fn unknown_data_keeps_episode_baseline_without_fabricating_a_new_signal() {
    let fixture = Fixture::new(60).await;
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let first = fixture.service.snapshot_at(NOW).await;
    let original_id = &first.results[0].signals[0].id;
    fixture.publish_except(120.0, NOW + 5_000, Some("3")).await;
    fixture.service.sample_once_at(NOW + 5_000).await;
    let unknown = fixture.service.snapshot_at(NOW + 5_000).await;
    assert!(unknown.results[0].signals.is_empty());
    fixture.publish(120.0, NOW + 10_000).await;
    fixture.service.sample_once_at(NOW + 10_000).await;
    let recovered = fixture.service.snapshot_at(NOW + 10_000).await;
    assert_eq!(&recovered.results[0].signals[0].id, original_id);
    assert!(recovered.results[0].signals[0].formed_at.is_none());
    assert_eq!(
        recovered.results[0].signals[0].first_observed_at,
        first.results[0].signals[0].first_observed_at
    );
}

#[tokio::test]
async fn unflushed_closed_history_participates_in_dynamic_evaluation() {
    let fixture = Fixture::new(59).await;
    for period in PERIODS {
        let interval = Interval::parse(period).unwrap();
        let duration = interval.as_millis() as i64;
        // Add a contiguous earlier row only to the closed buffer. The engine
        // must merge it with the database history before checking warmup.
        fixture
            .data
            .closed_buffer
            .upsert(
                SYMBOL,
                period,
                candle(
                    interval.bucket_start_ms(NOW) - 60 * duration,
                    duration,
                    100.0,
                    true,
                ),
            )
            .await;
    }
    fixture.publish(120.0, NOW).await;
    fixture.service.sample_once_at(NOW).await;
    let snapshot = fixture.service.snapshot_at(NOW).await;
    assert_eq!(snapshot.results[0].signals.len(), 1);
    assert!(snapshot.results[0]
        .per_interval_quality
        .iter()
        .all(|item| item.availability == Availability::Ready && item.history_count == 60));
}
