use crypto_candlestick::binance::rest::closed_lookback_window;
use crypto_candlestick::binance::rest::missing_ranges_for_source;
use crypto_candlestick::binance::rest::parse_rest_klines;
use crypto_candlestick::binance::rest::rebuild_custom_klines;
use crypto_candlestick::binance::rest::{detect_missing_kline_ranges, MissingKlineRange};
use crypto_candlestick::binance::rest::{plan_rest_kline_pages, RestKlinePage};
use crypto_candlestick::binance::worker::{KlineSource, SubscriptionPlan};
use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::domain::interval::Interval;
use crypto_candlestick::storage::sqlite::SqliteStore;

#[test]
fn parses_rest_kline_array() {
    let payload = serde_json::json!([[
        1710000000000i64,
        "100.0",
        "102.0",
        "99.0",
        "101.0",
        "12.5",
        1710000059999i64,
        "1250.0",
        42,
        "4.5",
        "450.0",
        "0"
    ]]);

    let rows = parse_rest_klines(payload).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].open_time, 1710000000000);
    assert_eq!(rows[0].close_time, 1710000059999);
    assert_eq!(rows[0].open, 100.0);
    assert_eq!(rows[0].close, 101.0);
    assert_eq!(rows[0].trade_count, 42);
    assert!(rows[0].is_closed);
}

#[tokio::test]
async fn rebuilds_custom_interval_from_native_base_rows() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    for index in 0..2 {
        let open_time = index * 60_000;
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &Candle {
                    open_time,
                    close_time: open_time + 59_999,
                    open: 100.0 + index as f64,
                    high: 102.0 + index as f64,
                    low: 99.0,
                    close: 101.0 + index as f64,
                    volume: 10.0,
                    quote_volume: 1_000.0,
                    trade_count: 10,
                    is_closed: true,
                },
            )
            .await
            .unwrap();
    }

    let plan = SubscriptionPlan::new(
        vec!["BTCUSDT".to_string()],
        vec![Interval::parse("2").unwrap()],
    );
    rebuild_custom_klines(&store, &plan, 100).await.unwrap();

    let rows = store
        .query_klines("BTCUSDT", "2", None, None, 10)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].candle.open_time, 0);
    assert_eq!(rows[0].candle.close_time, 119_999);
    assert_eq!(rows[0].candle.open, 100.0);
    assert_eq!(rows[0].candle.high, 103.0);
    assert_eq!(rows[0].candle.close, 102.0);
    assert_eq!(rows[0].candle.volume, 20.0);
}

#[test]
fn detects_single_missing_kline_range() {
    let ranges = detect_missing_kline_ranges(0, 4 * 60_000, 60_000, &[0, 60_000, 180_000, 240_000]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 120_000,
            end_open_time: 120_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn merges_consecutive_missing_klines_into_one_range() {
    let ranges = detect_missing_kline_ranges(0, 5 * 60_000, 60_000, &[0, 60_000, 300_000]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 120_000,
            end_open_time: 240_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn detects_tail_lag_as_missing_range() {
    let ranges = detect_missing_kline_ranges(0, 4 * 60_000, 60_000, &[0, 60_000, 120_000]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 180_000,
            end_open_time: 240_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn empty_window_fetches_whole_closed_lookback_range() {
    let ranges = detect_missing_kline_ranges(0, 2 * 60_000, 60_000, &[]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 0,
            end_open_time: 120_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn complete_window_has_no_missing_ranges() {
    let ranges = detect_missing_kline_ranges(0, 2 * 60_000, 60_000, &[0, 60_000, 120_000]);

    assert!(ranges.is_empty());
}

#[test]
fn closed_lookback_window_ends_at_latest_closed_bucket() {
    let interval = Interval::parse("5").unwrap();
    let now_ms = 1779868323456; // 2026-05-27 15:52:03.456 +08:00

    let (start, end) = closed_lookback_window(&interval, 3, now_ms);

    assert_eq!(start, 1779867300000); // 15:35 +08:00
    assert_eq!(end, 1779867900000); // 15:45 +08:00
}

#[test]
fn closed_lookback_window_uses_one_bar_minimum() {
    let interval = Interval::parse("1").unwrap();
    let now_ms = 1779868323456;

    let (start, end) = closed_lookback_window(&interval, 0, now_ms);

    assert_eq!(start, end);
    assert_eq!(end, 1779868260000); // 15:51 +08:00
}

#[test]
fn plans_single_rest_page_for_small_gap() {
    let pages = plan_rest_kline_pages(&MissingKlineRange {
        start_open_time: 1779867600000,
        end_open_time: 1779867600000,
        interval_ms: 300_000,
    });

    assert_eq!(
        pages,
        vec![RestKlinePage {
            start_time: 1779867600000,
            end_time: 1779867899999,
            limit: 1,
        }]
    );
}

#[test]
fn splits_large_gap_without_requesting_outside_range() {
    let pages = plan_rest_kline_pages(&MissingKlineRange {
        start_open_time: 0,
        end_open_time: 1500 * 60_000,
        interval_ms: 60_000,
    });

    assert_eq!(pages.len(), 2);
    assert_eq!(
        pages[0],
        RestKlinePage {
            start_time: 0,
            end_time: 1499 * 60_000 + 59_999,
            limit: 1500,
        }
    );
    assert_eq!(
        pages[1],
        RestKlinePage {
            start_time: 1500 * 60_000,
            end_time: 1500 * 60_000 + 59_999,
            limit: 1,
        }
    );
}

#[tokio::test]
async fn finds_only_missing_ranges_from_sqlite_window() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let source = KlineSource {
        symbol: "BTCUSDT".to_string(),
        canonical_interval: "5".to_string(),
        binance_interval: "5m",
        interval: Interval::parse("5").unwrap(),
    };

    for open_time in [0, 300_000, 900_000, 1_200_000] {
        store
            .upsert_candle(
                "BTCUSDT",
                "5",
                &Candle {
                    open_time,
                    close_time: open_time + 299_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 1.0,
                    quote_volume: 100.0,
                    trade_count: 1,
                    is_closed: true,
                },
            )
            .await
            .unwrap();
    }

    let ranges = missing_ranges_for_source(&store, &source, 0, 1_200_000)
        .await
        .unwrap();

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 600_000,
            end_open_time: 600_000,
            interval_ms: 300_000,
        }]
    );
}
