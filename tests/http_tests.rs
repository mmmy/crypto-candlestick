use axum::http::StatusCode;
use chrono::{Local, SecondsFormat, TimeZone};
use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::http::{router, AppState, HealthTarget};
use crypto_candlestick::memory::{LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::storage::sqlite::SqliteStore;

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!("http://{addr}/api/health"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], true);

    server.abort();
}

#[tokio::test]
async fn deep_health_reports_consecutive_closed_klines_from_latest() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let current_bucket_start = chrono::Utc::now().timestamp_millis().div_euclid(60_000) * 60_000;
    let open_times = [
        current_bucket_start - 240_000,
        current_bucket_start - 180_000,
        current_bucket_start - 60_000,
        current_bucket_start,
    ];
    for open_time in open_times {
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &Candle {
                    open_time,
                    close_time: open_time + 59_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 12.5,
                    quote_volume: 1_250.0,
                    trade_count: 3,
                    is_closed: true,
                },
            )
            .await
            .unwrap();
    }

    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: vec![HealthTarget {
            symbol: "BTCUSDT".to_string(),
            interval: "1".to_string(),
        }],
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!("http://{addr}/api/health/deep"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["series"][0]["symbol"], "BTCUSDT");
    assert_eq!(body["series"][0]["interval"], "1");
    let expected_latest_open_time = Local
        .timestamp_millis_opt(current_bucket_start)
        .single()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    assert_eq!(
        body["series"][0]["latestOpenTime"],
        expected_latest_open_time
    );
    assert_eq!(body["series"][0]["latestLagIntervals"], 0);
    assert_eq!(body["series"][0]["consecutiveBarsFromLatest"], 2);
    assert_eq!(body["series"][0]["ok"], true);

    server.abort();
}

#[tokio::test]
async fn deep_health_reports_stale_series_as_unhealthy() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    store
        .upsert_candle(
            "BTCUSDT",
            "1",
            &Candle {
                open_time: 0,
                close_time: 59_999,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 12.5,
                quote_volume: 1_250.0,
                trade_count: 3,
                is_closed: true,
            },
        )
        .await
        .unwrap();

    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: vec![HealthTarget {
            symbol: "BTCUSDT".to_string(),
            interval: "1".to_string(),
        }],
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!("http://{addr}/api/health/deep"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["series"][0]["ok"], false);
    assert_eq!(body["series"][0]["reason"], "latest candle is stale");

    server.abort();
}

#[tokio::test]
async fn deep_health_reports_stale_websocket_as_unhealthy() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let runtime_health = RuntimeHealth::default();
    runtime_health.mark_connected().await;
    runtime_health
        .mark_message_at(chrono::Utc::now().timestamp_millis() - 120_000)
        .await;

    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!("http://{addr}/api/health/deep"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["websocket"]["connected"], true);
    assert!(body["websocket"]["lastMessageAgoMs"].as_i64().unwrap() >= 120_000);
    assert_eq!(body["websocket"]["ok"], false);
    assert_eq!(
        body["websocket"]["reason"],
        "websocket message stream is stale"
    );

    server.abort();
}

#[tokio::test]
async fn deep_health_reports_disconnected_websocket_as_unhealthy() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let runtime_health = RuntimeHealth::default();
    runtime_health.mark_connected().await;
    runtime_health.mark_message_now().await;
    runtime_health.mark_reconnecting("websocket closed").await;

    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!("http://{addr}/api/health/deep"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["websocket"]["connected"], false);
    assert_eq!(body["websocket"]["reconnectCount"], 1);
    assert_eq!(body["websocket"]["lastError"], "websocket closed");
    assert_eq!(body["websocket"]["reason"], "websocket is disconnected");

    server.abort();
}

#[tokio::test]
async fn klines_endpoint_returns_persisted_rows() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let candle = Candle {
        open_time: 1_000,
        close_time: 1_999,
        open: 100.0,
        high: 101.0,
        low: 99.0,
        close: 100.5,
        volume: 12.5,
        quote_volume: 1_250.0,
        trade_count: 3,
        is_closed: true,
    };
    store.upsert_candle("BTCUSDT", "1", &candle).await.unwrap();

    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&interval=1&limit=10"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["interval"], "1");
    assert_eq!(body["limit"], 10);
    assert_eq!(body["timezone"], "Asia/Shanghai");
    assert!(body["serverTime"].as_i64().unwrap() > 0);
    assert_eq!(body["startTime"], "1970-01-01T08:00:01.000+08:00");
    assert_eq!(body["endTime"], "1970-01-01T08:00:01.000+08:00");
    assert_eq!(body["count"], 1);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"][0]["candle"]["openTime"],
        "1970-01-01T08:00:01.000+08:00"
    );
    assert_eq!(
        body["data"][0]["candle"]["closeTime"],
        "1970-01-01T08:00:01.999+08:00"
    );

    server.abort();
}

#[tokio::test]
async fn klines_endpoint_appends_latest_open_candle_from_memory() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let latest = LatestCache::default();

    store
        .upsert_candle(
            "BTCUSDT",
            "1",
            &Candle {
                open_time: 0,
                close_time: 59_999,
                open: 99.0,
                high: 100.0,
                low: 98.0,
                close: 99.5,
                volume: 10.0,
                quote_volume: 995.0,
                trade_count: 10,
                is_closed: true,
            },
        )
        .await
        .unwrap();

    latest
        .upsert(
            "BTCUSDT",
            "1",
            Candle {
                open_time: 60_000,
                close_time: 119_999,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 12.5,
                quote_volume: 1_250.0,
                trade_count: 3,
                is_closed: false,
            },
        )
        .await;

    let app = router(AppState {
        store,
        latest,
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&interval=1&limit=10"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["data"][1]["candle"]["openTime"],
        "1970-01-01T08:01:00.000+08:00"
    );
    assert_eq!(
        body["data"][1]["candle"]["closeTime"],
        "1970-01-01T08:01:59.999+08:00"
    );
    assert_eq!(body["data"][1]["candle"]["isClosed"], false);

    server.abort();
}

#[tokio::test]
async fn klines_endpoint_keeps_latest_contiguous_rows_after_a_gap() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let latest = LatestCache::default();

    for open_time in [0, 60_000, 180_000] {
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &Candle {
                    open_time,
                    close_time: open_time + 59_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 12.5,
                    quote_volume: 1_250.0,
                    trade_count: 3,
                    is_closed: true,
                },
            )
            .await
            .unwrap();
    }

    latest
        .upsert(
            "BTCUSDT",
            "1",
            Candle {
                open_time: 240_000,
                close_time: 299_999,
                open: 101.0,
                high: 102.0,
                low: 100.0,
                close: 101.5,
                volume: 8.0,
                quote_volume: 812.0,
                trade_count: 4,
                is_closed: false,
            },
        )
        .await;

    let app = router(AppState {
        store,
        latest,
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&interval=1&limit=10"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["data"][0]["candle"]["openTime"],
        "1970-01-01T08:03:00.000+08:00"
    );
    assert_eq!(
        body["data"][1]["candle"]["openTime"],
        "1970-01-01T08:04:00.000+08:00"
    );

    server.abort();
}

#[tokio::test]
async fn second_interval_query_reads_closed_rows_from_memory_not_sqlite() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let latest = LatestCache::default();
    let memory_series = MemorySeriesStore::default();

    memory_series
        .push_closed(
            "BTCUSDT",
            "15S",
            Candle {
                open_time: 0,
                close_time: 14_999,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 12.5,
                quote_volume: 1_250.0,
                trade_count: 3,
                is_closed: true,
            },
        )
        .await;

    latest
        .upsert(
            "BTCUSDT",
            "15S",
            Candle {
                open_time: 15_000,
                close_time: 29_999,
                open: 100.5,
                high: 102.0,
                low: 100.0,
                close: 101.5,
                volume: 5.0,
                quote_volume: 507.5,
                trade_count: 2,
                is_closed: false,
            },
        )
        .await;

    let app = router(AppState {
        store,
        latest,
        memory_series,
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&interval=15S&limit=10"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(body["data"][0]["candle"]["isClosed"], true);
    assert_eq!(body["data"][1]["candle"]["isClosed"], false);

    server.abort();
}
