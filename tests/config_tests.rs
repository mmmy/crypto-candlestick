use crypto_candlestick::config::{AppConfig, ConfigError, RealtimeSource};

const CONFIG_PREFIX: &str = r#"
[server]
bind_addr = "127.0.0.1:4000"

[database]
url = "sqlite://custom.db"
retention_bars = 1234

[binance]
sync_on_start = false
sync_lookback_bars = 777
"#;

const CONFIG_SUFFIX: &str = r#"
[realtime]
flush_interval_secs = 60
flush_max_rows = 250

[logging]
dir = "custom-logs"
level = "debug"
"#;

#[test]
fn checked_in_config_is_valid() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
    let config = AppConfig::load_from_path(path).unwrap();

    assert!(!config.subscriptions.is_empty());
}

#[test]
fn builds_config_from_toml() {
    let raw = format!(
        r#"{CONFIG_PREFIX}
[[binance.symbols]]
symbol = "BTCUSDT"
intervals = ["15S", "1", "5", "D"]
source = "trade"

[[binance.symbols]]
symbol = "ETHUSDT"
intervals = ["5", "15", "60"]
{CONFIG_SUFFIX}"#
    );
    let config = AppConfig::from_toml(&raw).unwrap();

    assert_eq!(config.database_url, "sqlite://custom.db");
    assert_eq!(config.bind_addr, "127.0.0.1:4000");
    assert_eq!(config.subscriptions.len(), 2);
    assert_eq!(config.subscriptions[0].symbol, "BTCUSDT");
    assert_eq!(config.subscriptions[0].intervals.len(), 4);
    assert_eq!(config.subscriptions[0].source, RealtimeSource::Trade);
    assert_eq!(config.subscriptions[1].source, RealtimeSource::Auto);
    assert_eq!(
        config.subscriptions[1].resolved_source(),
        RealtimeSource::Kline1m
    );
    assert_eq!(config.retention_bars, 1234);
    assert!(!config.sync_on_start);
    assert_eq!(config.sync_lookback_bars, 777);
    assert_eq!(config.realtime_flush_interval_secs, 60);
    assert_eq!(config.realtime_flush_max_rows, 250);
    assert_eq!(config.log_dir, "custom-logs");
    assert_eq!(config.log_level, "debug");
}

#[test]
fn resolves_auto_source_from_each_symbols_minimum_interval() {
    let raw = format!(
        r#"{CONFIG_PREFIX}
[[binance.symbols]]
symbol = "BTCUSDT"
intervals = ["15S", "1", "5", "15"]

[[binance.symbols]]
symbol = "ETHUSDT"
intervals = ["1", "15", "60"]

[[binance.symbols]]
symbol = "SOLUSDT"
intervals = ["15", "60"]
source = "trade"

[[binance.symbols]]
symbol = "BNBUSDT"
intervals = ["1", "15"]
source = "kline_1m"
{CONFIG_SUFFIX}"#
    );
    let config = AppConfig::from_toml(&raw).unwrap();

    assert_eq!(
        config.subscriptions[0].resolved_source(),
        RealtimeSource::Trade
    );
    assert_eq!(
        config.subscriptions[1].resolved_source(),
        RealtimeSource::Kline1m
    );
    assert_eq!(
        config.subscriptions[2].resolved_source(),
        RealtimeSource::Trade
    );
    assert_eq!(
        config.subscriptions[3].resolved_source(),
        RealtimeSource::Kline1m
    );
}

#[test]
fn rejects_sub_minute_interval_with_explicit_kline_source() {
    let raw = format!(
        r#"{CONFIG_PREFIX}
[[binance.symbols]]
symbol = "BTCUSDT"
intervals = ["15S", "5"]
source = "kline_1m"
{CONFIG_SUFFIX}"#
    );
    let error = AppConfig::from_toml(&raw).unwrap_err();

    assert_eq!(
        error,
        ConfigError::KlineWithSubMinute {
            symbol: "BTCUSDT".to_string(),
            interval: "15S".to_string(),
        }
    );
}

#[test]
fn rejects_unknown_toml_fields() {
    let raw = format!(
        r#"{CONFIG_PREFIX}
unknown_setting = true
{CONFIG_SUFFIX}"#
    );

    assert!(matches!(
        AppConfig::from_toml(&raw),
        Err(ConfigError::Toml(_))
    ));
}
