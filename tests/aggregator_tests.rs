use crypto_candlestick::domain::interval::Interval;
use crypto_candlestick::engine::aggregator::{Aggregator, TradeTick};

#[test]
fn rolls_seconds_into_correct_bucket() {
    let mut agg = Aggregator::new(Interval::parse("15S").unwrap());

    let first = agg.ingest_trade(TradeTick::new(1_000, 100.0, 2.0)).unwrap();
    assert!(first.is_none());

    let closed = agg
        .ingest_trade(TradeTick::new(16_000, 101.0, 1.0))
        .unwrap();
    let candle = closed.expect("expected first candle to close");

    assert_eq!(candle.open_time, 0);
    assert_eq!(candle.close_time, 14_999);
    assert_eq!(candle.open, 100.0);
    assert_eq!(candle.high, 100.0);
    assert_eq!(candle.low, 100.0);
    assert_eq!(candle.close, 100.0);
    assert_eq!(candle.volume, 2.0);
    assert_eq!(candle.trade_count, 1);
    assert!(candle.is_closed);
}
