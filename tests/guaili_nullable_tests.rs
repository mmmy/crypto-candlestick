use crypto_candlestick::{
    domain::{candle::Candle, interval::Interval},
    http::{router, AppState, HealthTarget},
    memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
    runtime_health::RuntimeHealth,
    storage::sqlite::SqliteStore,
};

#[tokio::test]
async fn indicator_contract_distinguishes_valid_zero_warmup_invalid_and_filtered() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let cases = [
        ("4D", 46),
        ("W", 27),
        ("10D", 18),
        ("1", 60),
        ("5", 60),
        ("2", 30),
    ];
    for (period, count) in cases {
        let interval = Interval::parse(period).unwrap();
        let step = interval.as_millis() as i64;
        let end = interval.bucket_start_ms(1_759_996_823_000);
        for index in 0..count {
            let open_time = end - (count - index) * step;
            let zero_denominator = period == "5";
            let tiny_positive = period == "2" && index == count - 1;
            let candle = Candle {
                open_time,
                close_time: open_time + step - 1,
                open: if tiny_positive { 100.12 } else { 100.0 },
                high: if tiny_positive {
                    100.13
                } else if zero_denominator {
                    100.0
                } else {
                    101.0
                },
                low: if tiny_positive {
                    100.12
                } else if zero_denominator {
                    100.0
                } else {
                    99.0
                },
                close: if tiny_positive { 100.12 } else { 100.0 },
                volume: 1.0,
                quote_volume: 100.0,
                trade_count: 1,
                is_closed: true,
            };
            store
                .upsert_candle("CLUSDT", period, &candle)
                .await
                .unwrap();
        }
    }
    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        runtime_health: RuntimeHealth::default(),
        health_targets: cases
            .iter()
            .map(|(p, _)| HealthTarget {
                symbol: "CLUSDT".into(),
                interval: (*p).into(),
            })
            .collect(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!(
        "http://{address}/api/indicators/guaili?symbols=CLUSDT&intervals=4D,W,10D,1,5,2&limit=1&closedOnly=true"
    );
    let response: serde_json::Value = reqwest::get(&url)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let series = response["results"][0]["series"].as_array().unwrap();
    let point = |period: &str| &series.iter().find(|s| s["interval"] == period).unwrap()["latest"];
    for period in ["4D", "W", "1"] {
        assert_eq!(point(period)["availability"], "ready");
        assert_eq!(point(period)["value"], 0);
        assert_eq!(point(period)["guaili"], 0.0);
    }
    assert_eq!(point("4D")["historyCount"], 46);
    assert_eq!(point("W")["historyCount"], 27);
    assert_eq!(point("10D")["availability"], "warming_up");
    assert_eq!(point("10D")["reasonCode"], "insufficient_history");
    assert!(point("10D")["value"].is_null() && point("10D")["guaili"].is_null());
    assert_eq!(point("5")["reasonCode"], "indicator_invalid");
    assert!(point("5")["value"].is_null() && point("5")["guaili"].is_null());
    assert_eq!(point("2")["value"], 0);
    assert!(point("2")["guaili"].as_f64().unwrap() > 0.0);
    let filtered: serde_json::Value = reqwest::get(format!("{url}&maxAtrRank=0"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let filtered_point = &filtered["results"][0]["series"][3]["latest"];
    assert_eq!(filtered_point["availability"], "filtered");
    assert_eq!(filtered_point["value"], 0);
    assert_eq!(filtered_point["rankFilter"], false);
    let waiting: serde_json::Value =
        reqwest::get(url.replace("closedOnly=true", "closedOnly=false"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let waiting_point = &waiting["results"][0]["series"][0]["latest"];
    assert!(waiting_point["value"].is_null());
    assert_eq!(waiting_point["reasonCode"], "waiting_market");
    server.abort();
}

#[tokio::test]
async fn live_matrix_invalidates_recovering_and_stale_values_but_not_closed_history() {
    use std::collections::HashMap;
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let open = now.div_euclid(60_000) * 60_000;
    let candle = |time, closed| Candle {
        open_time: time,
        close_time: time + 59_999,
        open: 100.0,
        high: 101.0,
        low: 99.0,
        close: 100.0,
        volume: 1.0,
        quote_volume: 100.0,
        trade_count: 1,
        is_closed: closed,
    };
    for index in (1..=30).rev() {
        store
            .upsert_candle("CLUSDT", "1", &candle(open - index * 60_000, true))
            .await
            .unwrap();
    }
    let latest = LatestCache::default();
    let current = HashMap::from([("1".into(), candle(open, false))]);
    // The separately maintained latest cache must not override the published
    // candle whose timestamps and quality are being validated.
    let mut different = candle(open, false);
    different.open = 120.0;
    different.high = 121.0;
    different.low = 119.0;
    different.close = 120.0;
    latest.upsert("CLUSDT", "1", different).await;
    latest
        .publish_live_symbol("CLUSDT", current.clone(), now, now)
        .await;
    let app = router(AppState {
        store,
        latest: latest.clone(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        runtime_health: RuntimeHealth::default(),
        health_targets: vec![HealthTarget {
            symbol: "CLUSDT".into(),
            interval: "1".into(),
        }],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("http://{address}/api/indicators/guaili?symbols=CLUSDT&intervals=1&limit=1");
    let get = || async {
        reqwest::get(&url)
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    assert_eq!(get().await["results"][0]["series"][0]["latest"]["value"], 0);
    let fresh = get().await;
    let point = &fresh["results"][0]["series"][0]["latest"];
    assert_eq!(point["availability"], "ready");
    assert_eq!(point["isClosed"], false);
    assert_eq!(point["historyCount"], 30);
    // A request arriving during a market update must wait until recovery and
    // candle changes are published together, then observe the new quality.
    let market_lock = latest.market_update_lock();
    let guard = market_lock.write().await;
    let pending_url = url.clone();
    let mut pending = tokio::spawn(async move {
        reqwest::get(pending_url)
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    latest.mark_recovering("CLUSDT").await;
    drop(guard);
    let recovering = pending.await.unwrap();
    let point = &recovering["results"][0]["series"][0]["latest"];
    assert!(point["value"].is_null());
    assert_eq!(point["reasonCode"], "market_recovering");
    latest
        .publish_live_symbol("CLUSDT", current, now - 31_000, now - 31_000)
        .await;
    let stale = get().await;
    let point = &stale["results"][0]["series"][0]["latest"];
    assert!(point["value"].is_null());
    assert_eq!(point["reasonCode"], "market_stale");
    let closed: serde_json::Value = reqwest::get(format!("{url}&closedOnly=true"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(closed["results"][0]["series"][0]["latest"]["value"], 0);
    server.abort();
}
