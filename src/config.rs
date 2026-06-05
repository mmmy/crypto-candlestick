use crate::{domain::interval::Interval, storage::sqlite::DEFAULT_RETENTION_BARS};
use std::{collections::HashMap, env};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub bind_addr: String,
    pub symbols: Vec<String>,
    pub intervals: Vec<Interval>,
    pub retention_bars: u32,
    pub sync_on_start: bool,
    pub sync_lookback_bars: u32,
    pub realtime_flush_interval_secs: u64,
    pub realtime_flush_max_rows: usize,
    pub log_dir: String,
}

impl AppConfig {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();
        Self::from_lookup(|key| env::var(key).ok())
    }

    pub fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Self {
        let values = pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>();
        Self::from_lookup(|key| values.get(key).cloned())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        Self {
            database_url: lookup("DATABASE_URL")
                .unwrap_or_else(|| "sqlite://candles.db".to_string()),
            bind_addr: lookup("BIND_ADDR").unwrap_or_else(|| "127.0.0.1:3000".to_string()),
            symbols: parse_csv(lookup("BINANCE_SYMBOLS").unwrap_or_default()),
            intervals: parse_csv(lookup("BINANCE_INTERVALS").unwrap_or_default())
                .into_iter()
                .filter_map(|raw| Interval::parse(&raw).ok())
                .collect(),
            retention_bars: lookup("RETENTION_BARS")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(DEFAULT_RETENTION_BARS),
            sync_on_start: lookup("SYNC_ON_START")
                .map(|value| parse_bool(&value))
                .unwrap_or(true),
            sync_lookback_bars: lookup("SYNC_LOOKBACK_BARS")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(1500),
            realtime_flush_interval_secs: lookup("REALTIME_FLUSH_INTERVAL_SECS")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(300),
            realtime_flush_max_rows: lookup("REALTIME_FLUSH_MAX_ROWS")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(1_000),
            log_dir: lookup("LOG_DIR").unwrap_or_else(|| "logs".to_string()),
        }
    }
}

fn parse_csv(raw: String) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}
