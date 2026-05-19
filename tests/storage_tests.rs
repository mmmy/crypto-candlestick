use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::storage::sqlite::SqliteStore;

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
