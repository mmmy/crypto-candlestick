use crypto_candlestick::binance::worker::{flush_closed_buffer, FlushLock};
use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::sync::Arc;
use tokio::sync::Mutex;

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

#[tokio::test]
async fn closed_kline_buffer_deduplicates_and_drains_by_series() {
    let buffer = ClosedKlineBuffer::default();
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
        is_closed: true,
    };
    let mut updated = candle.clone();
    updated.close = 101.5;

    assert_eq!(buffer.upsert("BTCUSDT", "1", candle).await, 1);
    assert_eq!(buffer.upsert("BTCUSDT", "1", updated).await, 1);

    let rows = buffer.query("BTCUSDT", "1", None, None, 10).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].candle.close, 101.5);

    let grouped = buffer.drain_grouped().await;
    assert_eq!(grouped.len(), 1);
    assert_eq!(
        grouped
            .get(&("BTCUSDT".to_string(), "1".to_string()))
            .unwrap()
            .len(),
        1
    );
    assert!(buffer
        .query("BTCUSDT", "1", None, None, 10)
        .await
        .is_empty());
}

#[tokio::test]
async fn closed_kline_buffer_remove_flushed_keeps_newer_same_key_update() {
    let buffer = ClosedKlineBuffer::default();
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
        is_closed: true,
    };
    let mut updated = candle.clone();
    updated.close = 102.0;

    buffer.upsert("BTCUSDT", "1", candle).await;
    let snapshot = buffer.snapshot_grouped().await;
    buffer.upsert("BTCUSDT", "1", updated).await;
    buffer.remove_flushed(&snapshot).await;

    let rows = buffer.query("BTCUSDT", "1", None, None, 10).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].candle.close, 102.0);
}

#[tokio::test]
async fn flush_closed_buffer_persists_all_series_and_clears_snapshot() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let buffer = ClosedKlineBuffer::default();
    let flush_lock: FlushLock = Arc::new(Mutex::new(()));

    for (symbol, interval, open_time) in [("BTCUSDT", "1", 60_000), ("ETHUSDT", "5", 300_000)] {
        buffer
            .upsert(
                symbol,
                interval,
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
                },
            )
            .await;
    }

    flush_closed_buffer(&store, &buffer, &flush_lock).await;

    assert!(buffer
        .query("BTCUSDT", "1", None, None, 10)
        .await
        .is_empty());
    assert!(buffer
        .query("ETHUSDT", "5", None, None, 10)
        .await
        .is_empty());
    assert_eq!(
        store
            .query_klines("BTCUSDT", "1", None, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .query_klines("ETHUSDT", "5", None, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}
