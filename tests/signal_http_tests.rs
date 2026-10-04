use axum::http::StatusCode;
use crypto_candlestick::http::{router_with_signals, AppState, HealthTarget};
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::signals::service::{now_ms, SignalService};
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: SignalService,
    data: AppState,
    path: PathBuf,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(contents: Option<&str>) -> Self {
        let path = std::env::temp_dir().join(format!(
            "signal-http-{}-{}-{}.toml",
            std::process::id(),
            now_ms(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        if let Some(contents) = contents {
            fs::write(&path, contents).unwrap();
        }
        let data = AppState {
            store: SqliteStore::connect("sqlite::memory:").await.unwrap(),
            latest: LatestCache::default(),
            memory_series: MemorySeriesStore::default(),
            closed_buffer: ClosedKlineBuffer::default(),
            runtime_health: RuntimeHealth::default(),
            health_targets: ["BTCUSDT", "XAUUSDT"]
                .iter()
                .flat_map(|symbol| {
                    ["1", "2", "3", "5", "8"]
                        .iter()
                        .map(move |interval| HealthTarget {
                            symbol: (*symbol).into(),
                            interval: (*interval).into(),
                        })
                })
                .collect(),
        };
        let service = SignalService::new(data.clone(), path.clone());
        service.load_initial().await;
        let app = router_with_signals(data.clone(), service.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            service,
            data,
            path,
            url,
            task,
        }
    }

    async fn get(&self, query: &str) -> reqwest::Response {
        reqwest::get(format!("{}/api/signals{query}", self.url))
            .await
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = fs::remove_file(&self.path);
    }
}

#[tokio::test]
async fn absent_config_returns_disabled_without_stopping_other_routes() {
    let fixture = Fixture::new(None).await;
    let response = fixture.get("").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["enabled"], false);
    assert_eq!(body["status"], "disabled");
    assert_eq!(body["candleMode"], "live");
    assert_eq!(body["evaluationIntervalMs"], 5000);
    assert!(body["serverTime"].is_i64());
    assert!(body["results"].as_array().unwrap().is_empty());
    assert!(reqwest::get(format!("{}/api/health", fixture.url))
        .await
        .unwrap()
        .status()
        .is_success());
}

#[tokio::test]
async fn symbol_filter_retains_request_order_and_never_calculates_on_query() {
    let fixture = Fixture::new(Some("enabled=true\n")).await;
    fixture.service.sample_once_at(now_ms()).await;
    let first: serde_json::Value = fixture
        .get("?symbols=xauusdt,BTCUSDT,XAUUSDT")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(first["results"].as_array().unwrap().len(), 2);
    assert_eq!(first["results"][0]["symbol"], "XAUUSDT");
    assert_eq!(first["results"][1]["symbol"], "BTCUSDT");
    assert_eq!(first["status"], "warming_up");
    let second: serde_json::Value = fixture.get("?symbols=BTCUSDT").await.json().await.unwrap();
    assert_eq!(second["snapshotVersion"], first["snapshotVersion"]);
    assert_eq!(second["results"].as_array().unwrap().len(), 1);
    assert_eq!(second["results"][0]["signals"], serde_json::json!([]));
    assert_eq!(
        second["results"][0]["primarySignal"],
        serde_json::Value::Null
    );
}

#[tokio::test]
async fn healthy_symbol_query_is_not_degraded_by_an_excluded_warming_symbol() {
    use crypto_candlestick::domain::{candle::Candle, interval::Interval};
    let fixture = Fixture::new(Some("enabled=true\n")).await;
    let now = now_ms();
    let mut current = std::collections::HashMap::new();
    for value in ["1", "2", "3", "5", "8"] {
        let interval = Interval::parse(value).unwrap();
        let duration = interval.as_millis() as i64;
        let start = interval.bucket_start_ms(now);
        let candle = |open_time, is_closed| Candle {
            open_time,
            close_time: open_time + duration - 1,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 10.0,
            quote_volume: 1000.0,
            trade_count: 10,
            is_closed,
        };
        for index in 1..=60 {
            fixture
                .data
                .store
                .upsert_candle("BTCUSDT", value, &candle(start - index * duration, true))
                .await
                .unwrap();
        }
        current.insert(value.to_string(), candle(start, false));
    }
    fixture
        .data
        .latest
        .publish_live_symbol("BTCUSDT", current, now, now)
        .await;
    fixture.service.sample_once_at(now).await;
    let all: serde_json::Value = fixture.get("").await.json().await.unwrap();
    assert_eq!(all["status"], "degraded");
    let selected: serde_json::Value = fixture.get("?symbols=BTCUSDT").await.json().await.unwrap();
    assert_eq!(selected["status"], "ready");
    assert_eq!(selected["results"][0]["dataStatus"], "ready");
    assert_eq!(selected["snapshotVersion"], all["snapshotVersion"]);
    assert_eq!(selected["indicatorConfig"]["maType"], "EMA");
    assert_eq!(selected["indicatorConfig"]["maLength"], 20);
}

#[tokio::test]
async fn query_rejects_unconfigured_symbols_empty_items_and_unknown_parameters() {
    let fixture = Fixture::new(Some("enabled=true\n")).await;
    for query in [
        "?symbols=UNKNOWN",
        "?symbols=BTCUSDT,",
        "?symbols=",
        "?webhookUrl=hidden",
    ] {
        assert_eq!(fixture.get(query).await.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn reload_is_atomic_and_disabled_is_distinct_from_no_signal() {
    let fixture = Fixture::new(Some("enabled=true\n")).await;
    fixture.service.sample_once_at(now_ms()).await;
    fs::write(&fixture.path, "enabled=false\n").unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/api/signals/reload", fixture.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["enabled"], false);
    assert_eq!(body["status"], "disabled");
    let body: serde_json::Value = fixture.get("").await.json().await.unwrap();
    assert_eq!(body["results"], serde_json::json!([]));
    let version = body["snapshotVersion"].clone();
    fs::write(&fixture.path, "enabled=\"secret-token-do-not-leak\"\n").unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/api/signals/reload", fixture.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.text().await.unwrap().contains("secret-token"));
    let body: serde_json::Value = fixture.get("").await.json().await.unwrap();
    assert_eq!(body["enabled"], false);
    assert_eq!(body["snapshotVersion"], version);
}

#[tokio::test]
async fn invalid_startup_config_reports_config_error_but_health_is_available() {
    let fixture = Fixture::new(Some("webhook_url = 'a-secret-that-must-not-escape'\n")).await;
    let response = fixture.get("").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(!body.contains("a-secret-that"));
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["status"], "config_error");
    assert_eq!(body["enabled"], false);
}

#[tokio::test]
async fn invalid_message_format_reload_returns_400_and_keeps_previous_configuration() {
    let raw = "enabled=true\nsymbols=['BTCUSDT']\n[[wecom_alerts]]\nid='test'\nwebhook_url='http://127.0.0.1:12345/mock?key=TEST_SECRET'\nmessage_format='compact'\n";
    let fixture = Fixture::new(Some(raw)).await;
    fixture.service.sample_once_at(now_ms()).await;
    let original: serde_json::Value = fixture.get("").await.json().await.unwrap();
    fs::write(
        &fixture.path,
        raw.replace("enabled=true", "enabled=false")
            .replace("message_format='compact'", "message_format='brief'"),
    )
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/api/signals/reload", fixture.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error = response.text().await.unwrap();
    assert!(!error.contains("TEST_SECRET"));
    assert!(!error.contains("webhook_url"));
    let unchanged: serde_json::Value = fixture.get("").await.json().await.unwrap();
    assert_eq!(unchanged["enabled"], true);
    assert_eq!(unchanged["configHash"], original["configHash"]);
    assert_eq!(unchanged["snapshotVersion"], original["snapshotVersion"]);
    assert_eq!(unchanged["results"], original["results"]);
}
