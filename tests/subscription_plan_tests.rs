use crypto_candlestick::binance::worker::SubscriptionPlan;
use crypto_candlestick::config::{RealtimeSource, SymbolSubscription};
use crypto_candlestick::domain::interval::Interval;

fn intervals(values: &[&str]) -> Vec<Interval> {
    values
        .iter()
        .map(|value| Interval::parse(value).unwrap())
        .collect()
}

#[test]
fn auto_selects_one_stream_per_symbol_from_minimum_interval() {
    let plan = SubscriptionPlan::from_subscriptions(vec![
        SymbolSubscription::new(
            "BTCUSDT",
            intervals(&["5", "15", "60"]),
            RealtimeSource::Auto,
        ),
        SymbolSubscription::new(
            "ETHUSDT",
            intervals(&["1", "5", "15"]),
            RealtimeSource::Auto,
        ),
        SymbolSubscription::new("SOLUSDT", intervals(&["15", "60"]), RealtimeSource::Trade),
        SymbolSubscription::new("BNBUSDT", intervals(&["1", "15"]), RealtimeSource::Kline1m),
        SymbolSubscription::new(
            "XRPUSDT",
            intervals(&["15S", "1", "5"]),
            RealtimeSource::Auto,
        ),
    ]);

    assert_eq!(
        plan.streams(),
        vec![
            "bnbusdt@kline_1m".to_string(),
            "btcusdt@kline_1m".to_string(),
            "ethusdt@kline_1m".to_string(),
            "solusdt@aggTrade".to_string(),
            "xrpusdt@aggTrade".to_string(),
        ]
    );
}

#[test]
fn kline_mode_aggregates_every_configured_higher_interval_from_one_minute() {
    let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
        "BTCUSDT",
        intervals(&["5", "20", "60", "D"]),
        RealtimeSource::Auto,
    )]);

    assert_eq!(
        plan.realtime_kline_targets(),
        vec![
            ("BTCUSDT".to_string(), "1".to_string(), "20".to_string()),
            ("BTCUSDT".to_string(), "1".to_string(), "5".to_string()),
            ("BTCUSDT".to_string(), "1".to_string(), "60".to_string()),
            ("BTCUSDT".to_string(), "1".to_string(), "D".to_string()),
        ]
    );
}

#[test]
fn exposes_native_sources_for_history_and_one_minute_for_realtime_seed() {
    let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
        "BTCUSDT",
        intervals(&["5", "20", "60", "W"]),
        RealtimeSource::Auto,
    )]);
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
    assert!(sources.contains(&("BTCUSDT".to_string(), "5".to_string(), "5m".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "60".to_string(), "1h".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "D".to_string(), "1d".to_string())));
    assert!(sources.contains(&("BTCUSDT".to_string(), "W".to_string(), "1w".to_string())));
}

#[test]
fn keeps_native_history_plan_for_trade_mode_custom_intervals() {
    let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
        "BTCUSDT",
        intervals(&["15S", "2", "90", "2D"]),
        RealtimeSource::Trade,
    )]);

    assert_eq!(plan.streams(), vec!["btcusdt@aggTrade".to_string()]);
    assert!(plan.aggregation_targets().contains(&(
        "BTCUSDT".to_string(),
        "1".to_string(),
        "2".to_string()
    )));
    assert!(plan.aggregation_targets().contains(&(
        "BTCUSDT".to_string(),
        "30".to_string(),
        "90".to_string()
    )));
    assert!(plan.aggregation_targets().contains(&(
        "BTCUSDT".to_string(),
        "D".to_string(),
        "2D".to_string()
    )));
}

#[test]
fn builds_market_stream_url_for_mixed_sources() {
    let plan = SubscriptionPlan::from_subscriptions(vec![
        SymbolSubscription::new("BTCUSDT", intervals(&["1"]), RealtimeSource::Auto),
        SymbolSubscription::new("ETHUSDT", intervals(&["15S", "1"]), RealtimeSource::Auto),
    ]);

    assert_eq!(
        plan.stream_url(),
        "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/ethusdt@aggTrade"
    );
}
