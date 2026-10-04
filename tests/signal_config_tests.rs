use crypto_candlestick::signals::config::{SignalConfig, WecomMessageFormat};
use std::path::Path;

fn targets() -> Vec<(String, String)> {
    vec![
        ("BTCUSDT".into(), "10S".into()),
        ("BTCUSDT".into(), "15".into()),
        ("XAUUSDT".into(), "60".into()),
    ]
}

fn valid_raw() -> &'static str {
    r#"
enabled = true
symbols = [" btcusdt ", "BTCUSDT", "XAUUSDT"]
[indicator]
ma_type = "ema"
[[wecom_alerts]]
id = "btc"
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=TEST_SECRET"
symbols = ["btcusdt"]
min_signal_interval = "15"
kinds = ["extreme", "compression"]
"#
}

#[test]
fn config_normalizes_and_defaults_without_database() {
    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.normalize_and_validate(&targets()).unwrap();
    assert!(config.enabled);
    assert_eq!(config.symbols, ["BTCUSDT", "XAUUSDT"]);
    assert_eq!(config.evaluation_interval_secs, 5);
    assert_eq!(config.indicator.calc_limit, 500);
    assert_eq!(config.indicator.ma_type, "EMA");
    assert_eq!(config.wecom_alerts[0].name, "btc");
    assert_eq!(config.wecom_alerts[0].symbols, ["BTCUSDT"]);
    assert_eq!(config.wecom_alerts[0].cooldown_secs, 300);
    assert_eq!(
        config.wecom_alerts[0].message_format,
        WecomMessageFormat::Detailed
    );
    assert!(!format!("{config:?}").contains("TEST_SECRET"));
}

#[test]
fn message_format_is_selected_per_subscription_and_only_changes_delivery_hash() {
    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.normalize_and_validate(&targets()).unwrap();
    let calculation_hash = config.calculation_hash();
    let delivery_hash = config.delivery_hash();
    let raw = format!(
        "{}\nmessage_format = \"compact\"\n\n[[wecom_alerts]]\nid = \"detailed\"\nwebhook_url = \"http://127.0.0.1:12345/mock\"\nmessage_format = \"detailed\"\n",
        valid_raw()
    );
    let mut updated = SignalConfig::from_toml(&raw).unwrap();
    updated.normalize_and_validate(&targets()).unwrap();
    assert_eq!(
        updated.wecom_alerts[0].message_format,
        WecomMessageFormat::Compact
    );
    assert_eq!(
        updated.wecom_alerts[1].message_format,
        WecomMessageFormat::Detailed
    );
    // Isolate the format change from adding the second subscription.
    updated.wecom_alerts.pop();
    assert_eq!(updated.calculation_hash(), calculation_hash);
    assert_ne!(updated.delivery_hash(), delivery_hash);
}

#[test]
fn rejects_unknown_message_formats_and_wrong_types_without_echoing_credentials() {
    for value in ["\"brief\"", "\"COMPACT\"", "\"\"", "true", "123", "[]"] {
        let raw = format!("{}\nmessage_format = {value}\n", valid_raw());
        let error = SignalConfig::from_toml(&raw).unwrap_err();
        assert!(!error.contains("TEST_SECRET"));
        assert!(!error.contains("qyapi.weixin.qq.com"));
    }
}

#[test]
fn missing_file_disables_optional_signal_feature() {
    let path = std::env::temp_dir().join(format!(
        "missing-signal-config-{}-{}.toml",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let config = SignalConfig::load(Path::new(&path)).unwrap();
    assert!(!config.enabled);
}

#[test]
fn empty_symbol_lists_expand_to_configured_market_symbols() {
    let mut config = SignalConfig::from_toml(
        r#"
enabled = true
[[wecom_alerts]]
id = "all"
webhook_url = "http://127.0.0.1:12345/mock"
"#,
    )
    .unwrap();
    config.normalize_and_validate(&targets()).unwrap();
    assert_eq!(config.symbols, ["BTCUSDT", "XAUUSDT"]);
    assert_eq!(config.wecom_alerts[0].symbols, config.symbols);
    assert_eq!(config.wecom_alerts[0].kinds.len(), 3);
}

#[test]
fn rejects_unsafe_indicator_thresholds_and_unknown_symbols() {
    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.indicator.max_atr_rank = f64::NAN;
    assert!(config.normalize_and_validate(&targets()).is_err());

    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.rules.min_history_bars = 10;
    assert!(config.normalize_and_validate(&targets()).is_err());

    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.rules.compression_band = config.rules.extreme_threshold;
    assert!(config.normalize_and_validate(&targets()).is_err());

    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.symbols.push("UNKNOWN".into());
    assert!(config.normalize_and_validate(&targets()).is_err());

    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.indicator.ma_type = "invented".into();
    assert!(config.normalize_and_validate(&targets()).is_err());
}

#[test]
fn rejects_duplicate_alert_ids_and_impossible_minimum_interval() {
    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.wecom_alerts.push(config.wecom_alerts[0].clone());
    assert!(config.normalize_and_validate(&targets()).is_err());

    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.wecom_alerts[0].min_signal_interval = "60".into();
    assert!(config.normalize_and_validate(&targets()).is_err());
}

#[test]
fn rejects_bad_webhooks_without_echoing_credentials() {
    for webhook in [
        "file:///private/TEST_SECRET",
        "https://user:TEST_SECRET@example.com/webhook",
        "not-url-TEST_SECRET",
    ] {
        let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
        config.wecom_alerts[0].webhook_url = webhook.into();
        let error = config.normalize_and_validate(&targets()).unwrap_err();
        assert!(!error.contains("TEST_SECRET"));
    }
    let error =
        SignalConfig::from_toml("enabled = true\nwebhook_url = 'TEST_SECRET'\nunknown = true")
            .unwrap_err();
    assert!(!error.contains("TEST_SECRET"));
}

#[test]
fn delivery_changes_do_not_change_calculation_hash() {
    let mut config = SignalConfig::from_toml(valid_raw()).unwrap();
    config.normalize_and_validate(&targets()).unwrap();
    let original_calculation = config.calculation_hash();
    let original_delivery = config.delivery_hash();
    config.wecom_alerts[0].min_signal_interval = "10S".into();
    assert_eq!(config.calculation_hash(), original_calculation);
    assert_ne!(config.delivery_hash(), original_delivery);
    config.indicator.ma_length = 21;
    assert_ne!(config.calculation_hash(), original_calculation);
}
