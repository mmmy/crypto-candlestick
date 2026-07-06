use crypto_candlestick::domain::interval::Interval;

#[test]
fn parses_custom_and_native_intervals() {
    assert_eq!(Interval::parse("10S").unwrap().as_millis(), 10_000);
    assert_eq!(Interval::parse("15S").unwrap().as_millis(), 15_000);
    assert_eq!(Interval::parse("45").unwrap().as_millis(), 2_700_000);
    assert_eq!(Interval::parse("4D").unwrap().as_millis(), 345_600_000);
    assert_eq!(Interval::parse("W").unwrap().as_millis(), 604_800_000);
}

#[test]
fn maps_binance_native_intervals() {
    assert_eq!(Interval::parse("1").unwrap().binance_interval(), Some("1m"));
    assert_eq!(Interval::parse("3").unwrap().binance_interval(), Some("3m"));
    assert_eq!(
        Interval::parse("60").unwrap().binance_interval(),
        Some("1h")
    );
    assert_eq!(
        Interval::parse("240").unwrap().binance_interval(),
        Some("4h")
    );
    assert_eq!(
        Interval::parse("720").unwrap().binance_interval(),
        Some("12h")
    );
    assert_eq!(Interval::parse("D").unwrap().binance_interval(), Some("1d"));
    assert_eq!(
        Interval::parse("3D").unwrap().binance_interval(),
        Some("3d")
    );
    assert_eq!(Interval::parse("W").unwrap().binance_interval(), Some("1w"));

    assert_eq!(Interval::parse("2").unwrap().binance_interval(), None);
    assert_eq!(Interval::parse("10D").unwrap().binance_interval(), None);
    assert_eq!(Interval::parse("10S").unwrap().binance_interval(), None);
    assert_eq!(Interval::parse("15S").unwrap().binance_interval(), None);
}

#[test]
fn maps_custom_intervals_to_base_interval() {
    assert_eq!(
        Interval::parse("2")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "1"
    );
    assert_eq!(
        Interval::parse("10")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "5"
    );
    assert_eq!(
        Interval::parse("20")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "5"
    );
    assert_eq!(
        Interval::parse("45")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "15"
    );
    assert_eq!(
        Interval::parse("90")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "30"
    );
    assert_eq!(
        Interval::parse("180")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "60"
    );
    assert_eq!(
        Interval::parse("2D")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "D"
    );
    assert_eq!(
        Interval::parse("10D")
            .unwrap()
            .aggregation_base()
            .unwrap()
            .canonical(),
        "D"
    );
    assert!(Interval::parse("60").unwrap().aggregation_base().is_none());
    assert!(Interval::parse("10S").unwrap().aggregation_base().is_none());
    assert!(Interval::parse("15S").unwrap().aggregation_base().is_none());
}

#[test]
fn rejects_unsupported_intervals() {
    for raw in ["7", "11", "6D", "2W", "60S", "1H"] {
        assert!(Interval::parse(raw).is_err(), "{raw} should be unsupported");
    }
}
