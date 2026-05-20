use crypto_candlestick::config::AppConfig;

#[test]
fn builds_config_from_environment_values() {
    let config = AppConfig::from_pairs([
        ("DATABASE_URL", "sqlite://custom.db"),
        ("BIND_ADDR", "127.0.0.1:4000"),
        ("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT"),
        ("BINANCE_INTERVALS", "15S,1,5,D"),
        ("RETENTION_BARS", "1234"),
        ("SYNC_ON_START", "false"),
        ("SYNC_LOOKBACK_BARS", "777"),
        ("LOG_DIR", "custom-logs"),
    ]);

    assert_eq!(config.database_url, "sqlite://custom.db");
    assert_eq!(config.bind_addr, "127.0.0.1:4000");
    assert_eq!(config.symbols, vec!["BTCUSDT", "ETHUSDT"]);
    assert_eq!(config.intervals.len(), 4);
    assert_eq!(config.retention_bars, 1234);
    assert!(!config.sync_on_start);
    assert_eq!(config.sync_lookback_bars, 777);
    assert_eq!(config.log_dir, "custom-logs");
}
