use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::storage::sqlite::{Alert, SqliteStore};
use std::collections::HashMap;

#[tokio::test]
async fn persists_and_queries_klines() {
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

    store
        .upsert_candle("BTCUSDT", "15S", &candle)
        .await
        .unwrap();

    let rows = store
        .query_klines("BTCUSDT", "15S", None, None, 10)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol, "BTCUSDT");
    assert_eq!(rows[0].interval, "15S");
    assert_eq!(rows[0].candle, candle);
}

#[tokio::test]
async fn returns_max_open_time_for_symbol_interval() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    for open_time in [1_000, 2_000] {
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &Candle {
                    open_time,
                    close_time: open_time + 999,
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

    assert_eq!(
        store.max_open_time("BTCUSDT", "1").await.unwrap(),
        Some(2_000)
    );
    assert_eq!(store.max_open_time("ETHUSDT", "1").await.unwrap(), None);
}

#[tokio::test]
async fn prunes_old_rows_over_retention_limit() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 3)
        .await
        .unwrap();

    for index in 0..5 {
        let open_time = index * 60_000;
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

    let rows = store
        .query_klines("BTCUSDT", "1", None, None, 10)
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].candle.open_time, 120_000);
    assert_eq!(rows[2].candle.open_time, 240_000);
}

#[tokio::test]
async fn batch_upsert_prunes_once_after_inserted_rows() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 3)
        .await
        .unwrap();
    let candles = (0..5)
        .map(|index| {
            let open_time = index * 60_000;
            Candle {
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
            }
        })
        .collect::<Vec<_>>();

    store
        .upsert_candles("BTCUSDT", "1", &candles)
        .await
        .unwrap();

    let rows = store
        .query_klines("BTCUSDT", "1", None, None, 10)
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].candle.open_time, 120_000);
    assert_eq!(rows[2].candle.open_time, 240_000);
}

#[tokio::test]
async fn defers_large_retention_pruning_until_the_series_reaches_its_write_threshold() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 200)
        .await
        .unwrap();
    let candles = (0..300)
        .map(|index| {
            let open_time = index * 60_000;
            Candle {
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
            }
        })
        .collect::<Vec<_>>();

    store
        .upsert_candles("BTCUSDT", "1", &candles[..200])
        .await
        .unwrap();
    store
        .upsert_candles("BTCUSDT", "1", &candles[200..299])
        .await
        .unwrap();

    assert_eq!(
        store
            .query_klines("BTCUSDT", "1", None, None, 500)
            .await
            .unwrap()
            .len(),
        299
    );

    store
        .upsert_candles("BTCUSDT", "1", &candles[299..])
        .await
        .unwrap();
    let rows = store
        .query_klines("BTCUSDT", "1", None, None, 500)
        .await
        .unwrap();

    assert_eq!(rows.len(), 200);
    assert_eq!(rows[0].candle.open_time, 100 * 60_000);
    assert_eq!(rows[199].candle.open_time, 299 * 60_000);
}

#[tokio::test]
async fn batch_upsert_handles_multiple_sql_chunks_and_conflicts() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 0)
        .await
        .unwrap();
    let mut candles = (0..1_101)
        .map(|index| {
            let open_time = index * 60_000;
            Candle {
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
            }
        })
        .collect::<Vec<_>>();

    store
        .upsert_candles("BTCUSDT", "1", &candles)
        .await
        .unwrap();

    for candle in &mut candles {
        candle.close = 101.5;
        candle.trade_count = 4;
    }
    store
        .upsert_candles("BTCUSDT", "1", &candles)
        .await
        .unwrap();

    let rows = store
        .query_klines("BTCUSDT", "1", None, None, 2_000)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1_101);
    assert!(rows.iter().all(|row| row.candle.close == 101.5));
    assert!(rows.iter().all(|row| row.candle.trade_count == 4));
}

#[tokio::test]
async fn grouped_upsert_persists_and_prunes_multiple_series_in_one_batch() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 2)
        .await
        .unwrap();
    let mut grouped = HashMap::new();

    for (symbol, interval) in [("BTCUSDT", "1"), ("ETHUSDT", "5")] {
        let candles = (0..3)
            .map(|index| {
                let open_time = index * 60_000;
                Candle {
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
                }
            })
            .collect::<Vec<_>>();
        grouped.insert((symbol.to_string(), interval.to_string()), candles);
    }

    store.upsert_candle_groups(&grouped).await.unwrap();

    for (symbol, interval) in [("BTCUSDT", "1"), ("ETHUSDT", "5")] {
        let rows = store
            .query_klines(symbol, interval, None, None, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].candle.open_time, 60_000);
        assert_eq!(rows[1].candle.open_time, 120_000);
    }
}

#[tokio::test]
async fn query_without_time_bounds_returns_latest_rows_in_ascending_order() {
    let store = SqliteStore::connect_with_retention("sqlite::memory:", 0)
        .await
        .unwrap();

    for index in 0..5 {
        let open_time = index * 60_000;
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

    let rows = store
        .query_klines("BTCUSDT", "1", None, None, 3)
        .await
        .unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| row.candle.open_time)
            .collect::<Vec<_>>(),
        vec![120_000, 180_000, 240_000]
    );
}

#[tokio::test]
async fn query_marks_rows_with_future_close_time_as_open() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let future_close_time = chrono::Utc::now().timestamp_millis() + 60_000;

    store
        .upsert_candle(
            "BTCUSDT",
            "10",
            &Candle {
                open_time: future_close_time - 599_999,
                close_time: future_close_time,
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

    let rows = store
        .query_klines("BTCUSDT", "10", None, None, 1)
        .await
        .unwrap();

    assert!(!rows[0].candle.is_closed);
}

#[tokio::test]
async fn persists_and_claims_alert_once() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let alert = Alert {
        id: 0,
        symbol: "BTCUSDT".to_string(),
        interval: "1".to_string(),
        price: 100.0,
        direction: "cross_up".to_string(),
        status: "active".to_string(),
        expires_at: None,
        webhook_url: "https://example.com/hook".to_string(),
        message_template: "{}".to_string(),
        created_at: 1,
        updated_at: 1,
        triggered_at: None,
        delivery_status: None,
        delivery_error: None,
    };
    let created = store.insert_alert(&alert).await.unwrap();
    assert_eq!(store.list_alerts().await.unwrap().len(), 1);
    assert!(store.claim_alert(created.id, 2).await.unwrap());
    assert!(!store.claim_alert(created.id, 3).await.unwrap());
    assert_eq!(
        store.get_alert(created.id).await.unwrap().unwrap().status,
        "triggered"
    );
}
