use crypto_candlestick::binance::worker::SubscriptionPlan;
use crypto_candlestick::domain::interval::Interval;

#[test]
fn subscribes_native_klines_directly_and_only_aggregates_custom_intervals() {
    let intervals = vec![
        Interval::parse("15S").unwrap(),
        Interval::parse("1").unwrap(),
        Interval::parse("2").unwrap(),
        Interval::parse("60").unwrap(),
        Interval::parse("90").unwrap(),
        Interval::parse("D").unwrap(),
        Interval::parse("2D").unwrap(),
        Interval::parse("3D").unwrap(),
        Interval::parse("W").unwrap(),
    ];

    let plan = SubscriptionPlan::new(vec!["BTCUSDT".to_string()], intervals);
    let streams = plan.streams();

    assert!(streams.contains(&"btcusdt@aggTrade".to_string()));
    assert!(streams.contains(&"btcusdt@kline_1m".to_string()));
    assert!(streams.contains(&"btcusdt@kline_1h".to_string()));
    assert!(streams.contains(&"btcusdt@kline_1d".to_string()));
    assert!(streams.contains(&"btcusdt@kline_3d".to_string()));
    assert!(streams.contains(&"btcusdt@kline_1w".to_string()));
    assert!(!streams.contains(&"btcusdt@kline_2m".to_string()));
    assert!(!streams.contains(&"btcusdt@kline_90m".to_string()));

    let aggregation_targets = plan.aggregation_targets();
    assert!(aggregation_targets.contains(&(
        "BTCUSDT".to_string(),
        "1".to_string(),
        "2".to_string()
    )));
    assert!(aggregation_targets.contains(&(
        "BTCUSDT".to_string(),
        "1".to_string(),
        "90".to_string()
    )));
    assert!(aggregation_targets.contains(&(
        "BTCUSDT".to_string(),
        "D".to_string(),
        "2D".to_string()
    )));
    assert!(!aggregation_targets.contains(&(
        "BTCUSDT".to_string(),
        "1".to_string(),
        "60".to_string()
    )));
}

#[test]
fn exposes_native_kline_sources_for_rest_sync() {
    let intervals = vec![
        Interval::parse("2").unwrap(),
        Interval::parse("60").unwrap(),
        Interval::parse("2D").unwrap(),
        Interval::parse("3D").unwrap(),
    ];

    let plan = SubscriptionPlan::new(vec!["btcusdt".to_string()], intervals);
    let sources = plan
        .kline_sources()
        .into_iter()
        .map(|source| {
            (
                source.symbol,
                source.canonical_interval,
                source.binance_interval.to_string(),
            )
        })
        .collect::<Vec<_>>();

    assert!(sources.contains(&("BTCUSDT".to_string(), "1".to_string(), "1m".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "60".to_string(), "1h".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "D".to_string(), "1d".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "3D".to_string(), "3d".to_string())));
}
