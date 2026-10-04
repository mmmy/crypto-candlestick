use axum::{routing::post, Json, Router};
use crypto_candlestick::domain::{candle::Candle, interval::Interval};
use crypto_candlestick::http::{AppState, HealthTarget};
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::signals::service::{now_ms, SignalService};
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const PERIODS: &[&str] = &["D", "2D", "3D", "4D", "W"];

struct Fixture {
    data: AppState,
    service: SignalService,
    path: PathBuf,
    calls: Arc<AtomicUsize>,
    messages: Arc<tokio::sync::Mutex<Vec<String>>>,
    mock: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(slow: bool) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let messages = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let messages_clone = messages.clone();
        let app = Router::new().route(
            "/hook",
            post(move |Json(body): Json<serde_json::Value>| {
                let calls = calls_clone.clone();
                let messages = messages_clone.clone();
                async move {
                    assert_eq!(body["msgtype"], "text");
                    assert!(body["text"]["content"]
                        .as_str()
                        .unwrap()
                        .contains("BTCUSDT"));
                    messages
                        .lock()
                        .await
                        .push(body["text"]["content"].as_str().unwrap().to_owned());
                    calls.fetch_add(1, Ordering::SeqCst);
                    if slow {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Json(serde_json::json!({"errcode":0,"errmsg":"ok"}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/hook", listener.local_addr().unwrap());
        let mock = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let path = std::env::temp_dir().join(format!(
            "signal-e2e-{}-{}-{}.toml",
            std::process::id(),
            now_ms(),
            endpoint
                .split(':')
                .nth(2)
                .unwrap()
                .split('/')
                .next()
                .unwrap()
        ));
        fs::write(&path, format!("enabled=true\nevaluation_interval_secs=1\nsymbols=['BTCUSDT']\n[[wecom_alerts]]\nid='test'\nwebhook_url='{endpoint}'\nsymbols=['BTCUSDT']\nmin_signal_interval='D'\nkinds=['extreme']\ncooldown_secs=0\n")).unwrap();
        let data = AppState {
            store: SqliteStore::connect("sqlite::memory:").await.unwrap(),
            latest: LatestCache::default(),
            memory_series: MemorySeriesStore::default(),
            closed_buffer: ClosedKlineBuffer::default(),
            runtime_health: RuntimeHealth::default(),
            health_targets: PERIODS
                .iter()
                .map(|interval| HealthTarget {
                    symbol: "BTCUSDT".into(),
                    interval: (*interval).into(),
                })
                .collect(),
        };
        let time = now_ms();
        for period in PERIODS {
            let interval = Interval::parse(period).unwrap();
            let duration = interval.as_millis() as i64;
            let open = interval.bucket_start_ms(time);
            let candles = (1..=60)
                .rev()
                .map(|index| candle(open - index * duration, duration, 100.0, true))
                .collect::<Vec<_>>();
            data.store
                .upsert_candles("BTCUSDT", period, &candles)
                .await
                .unwrap();
        }
        let service = SignalService::new(data.clone(), path.clone());
        service.reload().await.unwrap();
        Self {
            data,
            service,
            path,
            calls,
            messages,
            mock,
        }
    }

    async fn publish(&self, price: f64) {
        let time = now_ms();
        let lock = self.data.latest.market_update_lock();
        let _write = lock.write().await;
        let candles = PERIODS
            .iter()
            .map(|period| {
                let interval = Interval::parse(period).unwrap();
                (
                    (*period).into(),
                    candle(
                        interval.bucket_start_ms(time),
                        interval.as_millis() as i64,
                        price,
                        false,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        self.data
            .latest
            .publish_live_symbol("BTCUSDT", candles, time, time)
            .await;
    }

    async fn wait_for_calls(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.calls.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.mock.abort();
        let _ = fs::remove_file(&self.path);
    }
}

fn candle(open: i64, duration: i64, price: f64, closed: bool) -> Candle {
    Candle {
        open_time: open,
        close_time: open + duration - 1,
        open: price,
        high: price + 1.0,
        low: price - 1.0,
        close: price,
        volume: 10.0,
        quote_volume: price * 10.0,
        trade_count: 10,
        is_closed: closed,
    }
}

#[tokio::test]
async fn background_sampler_sends_new_dynamic_signal_once_without_http_queries() {
    let fixture = Fixture::new(false).await;
    fixture.publish(100.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.publish(120.0).await;
    let sampler = fixture.service.start();
    fixture.wait_for_calls(1).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .service
            .snapshot_at(now_ms())
            .await
            .delivery
            .successful,
        1
    );
    sampler.abort();
}

#[tokio::test]
async fn disabling_cancels_an_inflight_request_without_retries() {
    let fixture = Fixture::new(true).await;
    fixture.publish(100.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    fixture.publish(120.0).await;
    let sampler = fixture.service.start();
    fixture.wait_for_calls(1).await;
    fs::write(&fixture.path, "enabled=false\n").unwrap();
    fixture.service.reload().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot = fixture.service.snapshot_at(now_ms()).await;
    assert_eq!(snapshot.status, "disabled");
    assert_eq!(snapshot.delivery.successful, 0);
    assert_eq!(snapshot.delivery.failed, 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    sampler.abort();
}

#[tokio::test]
async fn market_recovery_cancels_an_inflight_old_generation_request() {
    let fixture = Fixture::new(true).await;
    fixture.publish(100.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    fixture.publish(120.0).await;
    let sampler = fixture.service.start();
    fixture.wait_for_calls(1).await;
    let lock = fixture.data.latest.market_update_lock();
    {
        let _write = lock.write().await;
        fixture.data.latest.mark_recovering("BTCUSDT").await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot = fixture.service.snapshot_at(now_ms()).await;
    assert_eq!(snapshot.delivery.failed, 0);
    assert_eq!(snapshot.delivery.successful, 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    sampler.abort();
}

#[tokio::test]
async fn format_reload_keeps_calculation_and_sends_next_event_as_compact_text() {
    let fixture = Fixture::new(false).await;
    fixture.publish(100.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    fixture.publish(120.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    let sampler = fixture.service.start();
    fixture.wait_for_calls(1).await;
    assert!(fixture.messages.lock().await[0].contains("\n动态K采样时间："));

    let original = fixture.service.snapshot_at(now_ms()).await;
    let raw = fs::read_to_string(&fixture.path).unwrap();
    fs::write(&fixture.path, format!("{raw}message_format='compact'\n")).unwrap();
    let reloaded = fixture.service.reload().await.unwrap();
    assert_eq!(reloaded.config_hash, original.config_hash);
    let unchanged = fixture.service.snapshot_at(now_ms()).await;
    assert_eq!(
        unchanged.results[0].signals[0].id,
        original.results[0].signals[0].id
    );
    // Reload establishes a notification baseline without resending the active structure.
    fixture.service.sample_once_at(now_ms()).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);

    fixture.publish(100.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    fixture.publish(120.0).await;
    fixture.service.sample_once_at(now_ms()).await;
    fixture.wait_for_calls(2).await;
    let messages = fixture.messages.lock().await;
    assert_eq!(messages.len(), 2);
    assert!(messages[1].starts_with("BTCUSDT 上方乖离共振｜1d–1w·5级｜"));
    assert!(!messages[1].contains(['\n', '\r']));
    chrono::NaiveTime::parse_from_str(messages[1].rsplit('｜').next().unwrap(), "%H:%M:%S")
        .unwrap();
    sampler.abort();
}
