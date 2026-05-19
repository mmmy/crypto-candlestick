use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::memory::{LatestCache, MemorySeriesStore};

#[tokio::test]
async fn stores_and_returns_latest_open_candle() {
    let cache = LatestCache::default();
    let candle = Candle {
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
    };

    cache.upsert("BTCUSDT", "1", candle.clone()).await;

    assert_eq!(cache.get("BTCUSDT", "1").await, Some(candle));
    assert_eq!(cache.get("ETHUSDT", "1").await, None);
}

#[tokio::test]
async fn memory_series_store_keeps_latest_rows_by_limit() {
    let store = MemorySeriesStore::new(3);

    for index in 0..5 {
        let open_time = index * 15_000;
        store
            .push_closed(
                "BTCUSDT",
                "15S",
                Candle {
                    open_time,
                    close_time: open_time + 14_999,
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
    }

    let rows = store.query("BTCUSDT", "15S", None, None, 10).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].candle.open_time, 30_000);
    assert_eq!(rows[2].candle.open_time, 60_000);
}
