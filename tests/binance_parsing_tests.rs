use crypto_candlestick::binance::types::{parse_combined_stream_message, MarketEvent};

#[test]
fn parses_closed_kline_event() {
    let raw = serde_json::json!({
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1710000000000i64,
            "s": "BTCUSDT",
            "k": {
                "t": 1710000000000i64,
                "T": 1710000059999i64,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "100.0",
                "c": "101.0",
                "h": "102.0",
                "l": "99.0",
                "v": "10.5",
                "n": 42,
                "x": true,
                "q": "1050.0",
                "V": "4.5",
                "Q": "450.0",
                "B": "0"
            }
        }
    });

    let event = parse_combined_stream_message(raw).unwrap();
    match event {
        MarketEvent::ClosedKline {
            symbol,
            interval,
            candle,
        } => {
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(interval, "1m");
            assert_eq!(candle.open_time, 1710000000000);
            assert_eq!(candle.close, 101.0);
        }
        other => panic!("unexpected event: {:?}", other),
    }
}

#[test]
fn parses_open_kline_event_without_marking_it_closed() {
    let raw = serde_json::json!({
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1710000000000i64,
            "s": "BTCUSDT",
            "k": {
                "t": 1710000000000i64,
                "T": 1710000059999i64,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "100.0",
                "c": "100.5",
                "h": "101.0",
                "l": "99.0",
                "v": "10.5",
                "n": 42,
                "x": false,
                "q": "1050.0",
                "V": "4.5",
                "Q": "450.0",
                "B": "0"
            }
        }
    });

    let event = parse_combined_stream_message(raw).unwrap();
    match event {
        MarketEvent::OpenKline {
            symbol,
            interval,
            candle,
        } => {
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(interval, "1m");
            assert_eq!(candle.open_time, 1710000000000);
            assert_eq!(candle.close, 100.5);
            assert!(!candle.is_closed);
        }
        other => panic!("unexpected event: {:?}", other),
    }
}
