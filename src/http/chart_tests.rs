use crate::{
    domain::candle::Candle,
    http::{router, AppState, HealthTarget},
    indicators::{
        chart::compute_android_chart,
        guaili::{compute_guaili, GuailiConfig},
    },
    memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
    runtime_health::RuntimeHealth,
    storage::sqlite::SqliteStore,
};
use axum::http::StatusCode;
use serde_json::Value;
use std::collections::HashMap;

fn candle(t: i64, period: i64, price: f64, closed: bool) -> Candle {
    Candle {
        open_time: t,
        close_time: t + period - 1,
        open: price,
        high: price + 2.0,
        low: price - 1.0,
        close: price + 1.0,
        volume: 10.0,
        quote_volume: price * 10.0,
        trade_count: 4,
        is_closed: closed,
    }
}
async fn fixture(interval: &str, period: i64) -> (AppState, Vec<Candle>) {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let state = AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        health_targets: vec![HealthTarget {
            symbol: "BTCUSDT".into(),
            interval: interval.into(),
        }],
        runtime_health: RuntimeHealth::default(),
    };
    let current = chrono::Utc::now().timestamp_millis().div_euclid(period) * period;
    let bars: Vec<_> = (0..31)
        .map(|i| {
            candle(
                current - (30 - i) * period,
                period,
                100.0 + i as f64,
                i != 30,
            )
        })
        .collect();
    for bar in &bars[..30] {
        if period < 60_000 {
            state
                .memory_series
                .push_closed("BTCUSDT", interval, bar.clone())
                .await;
        } else {
            state
                .store
                .upsert_candle("BTCUSDT", interval, bar)
                .await
                .unwrap();
        }
    }
    publish(&state, interval, bars[30].clone()).await;
    (state, bars)
}
async fn publish(state: &AppState, interval: &str, bar: Candle) {
    let lock = state.latest.market_update_lock();
    let _guard = lock.write().await;
    state.latest.upsert("BTCUSDT", interval, bar.clone()).await;
    let now = chrono::Utc::now().timestamp_millis();
    state
        .latest
        .publish_live_symbol("BTCUSDT", HashMap::from([(interval.into(), bar)]), now, now)
        .await;
}
async fn serve(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    (format!("http://{addr}"), server)
}
#[tokio::test]
async fn ohlc_and_both_indicator_profiles_use_one_input_and_full_calculation_prefix() {
    let (state, input) = fixture("1", 60_000).await;
    let (base, server) = serve(state).await;
    let body: Value = reqwest::get(format!(
        "{base}/api/charts/guaili?symbol=btcusdt&interval=1&limit=10&calcLimit=31"
    ))
    .await
    .unwrap()
    .error_for_status()
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(body["actualCalcBars"], 31);
    assert_eq!(body["bars"].as_array().unwrap().len(), 10);
    assert_eq!(
        body["indicatorContracts"]["androidChart"],
        "android-chart-v1"
    );
    assert_eq!(body["indicatorContracts"]["matrix"], "matrix-v1");
    let matrix = compute_guaili(&input, GuailiConfig::default());
    let channel = compute_android_chart(&input);
    for (i, row) in body["bars"].as_array().unwrap().iter().enumerate() {
        let index = i + 21;
        assert_eq!(row["openTimeMs"], input[index].open_time);
        assert_eq!(row["close"], input[index].close);
        assert_eq!(row["matrix"]["value"], matrix[index].value);
        assert_eq!(row["matrix"]["previousAtr14"], matrix[index - 1].atr14);
        assert_eq!(
            row["matrix"]["previousLongTrend"],
            matrix[index - 1].long_trend
        );
        assert!(
            (row["androidChannel"]["ema20"].as_f64().unwrap() - channel[index].0.ema20).abs()
                < 1e-9
        );
        assert!(
            (row["androidChannel"]["closeDeviation"].as_f64().unwrap()
                - channel[index].0.close_deviation.unwrap())
            .abs()
                < 1e-9
        );
    }
    assert_eq!(body["source"]["sequence"], 1);
    assert_eq!(body["dataQuality"], "ready");
    if let Some(path) = std::env::var_os("CHART_SNAPSHOT_TEST_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&body).unwrap()).unwrap();
    }
    server.abort();
}
#[tokio::test]
async fn same_open_time_revision_changes_ohlc_and_its_indicators_together() {
    let (state, mut input) = fixture("10S", 10_000).await;
    let (base, server) = serve(state.clone()).await;
    let url = format!("{base}/api/charts/guaili?symbol=BTCUSDT&interval=10S&limit=31&calcLimit=31");
    let old: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    input[30].close += 10.0;
    input[30].high = input[30].close + 1.0;
    publish(&state, "10S", input[30].clone()).await;
    let new: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    assert_eq!(old["bars"][30]["openTimeMs"], new["bars"][30]["openTimeMs"]);
    assert_ne!(old["bars"][30]["close"], new["bars"][30]["close"]);
    assert_ne!(old["snapshotId"], new["snapshotId"]);
    let expected = compute_guaili(&input, GuailiConfig::default());
    assert_eq!(new["bars"][30]["matrix"]["value"], expected[30].value);
    assert_eq!(old["source"]["sequence"], 1);
    assert_eq!(new["source"]["sequence"], 2);
    server.abort();
}
#[tokio::test]
async fn closed_mode_and_buffered_prefix_remain_compatible_with_old_endpoints() {
    let (state, input) = fixture("1", 60_000).await;
    state
        .closed_buffer
        .upsert("BTCUSDT", "1", input[29].clone())
        .await;
    let (base, server) = serve(state).await;
    let body: Value = reqwest::get(format!(
        "{base}/api/charts/guaili?symbol=BTCUSDT&interval=1&closedOnly=true&limit=31"
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(body["candleMode"], "closed");
    assert_eq!(body["bars"].as_array().unwrap().len(), 30);
    assert!(body["bars"]
        .as_array()
        .unwrap()
        .iter()
        .all(|b| b["isClosed"] == true));
    assert!(reqwest::get(format!(
        "{base}/api/klines?symbol=BTCUSDT&intervals=1&limit=1"
    ))
    .await
    .unwrap()
    .status()
    .is_success());
    assert!(reqwest::get(format!(
        "{base}/api/indicators/guaili?symbols=BTCUSDT&intervals=1&limit=1"
    ))
    .await
    .unwrap()
    .status()
    .is_success());
    server.abort();
}
#[tokio::test]
async fn unconfigured_limits_and_recovery_are_not_silently_rendered_as_other_data() {
    let (state, _) = fixture("1", 60_000).await;
    let (base, server) = serve(state.clone()).await;
    for query in [
        "symbol=BTCUSDT&interval=5",
        "symbol=BTCUSDT&interval=1&limit=0",
        "symbol=BTCUSDT&interval=1&calcLimit=100000",
    ] {
        assert_eq!(
            reqwest::get(format!("{base}/api/charts/guaili?{query}"))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    {
        let lock = state.latest.market_update_lock();
        let _guard = lock.write().await;
        state.latest.mark_recovering("BTCUSDT").await;
    }
    assert_eq!(
        reqwest::get(format!(
            "{base}/api/charts/guaili?symbol=BTCUSDT&interval=1"
        ))
        .await
        .unwrap()
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    server.abort();
}
#[tokio::test]
async fn atr_warmup_is_explicit_and_matrix_atr_is_not_used_for_android_channel() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let latest = LatestCache::default();
    let state = AppState {
        store,
        latest: latest.clone(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        health_targets: vec![HealthTarget {
            symbol: "BTCUSDT".into(),
            interval: "1".into(),
        }],
        runtime_health: RuntimeHealth::default(),
    };
    let now = chrono::Utc::now().timestamp_millis().div_euclid(60_000) * 60_000;
    publish(&state, "1", candle(now, 60_000, 100.0, false)).await;
    let (base, server) = serve(state).await;
    let body: Value = reqwest::get(format!(
        "{base}/api/charts/guaili?symbol=BTCUSDT&interval=1"
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert!(body["bars"][0]["androidChannel"]["atr14"].is_null());
    assert_eq!(body["bars"][0]["matrix"]["atr14"], 3.0);
    assert_eq!(body["dataQuality"], "warming_up");
    server.abort();
}
