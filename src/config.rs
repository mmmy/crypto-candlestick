use crate::domain::interval::Interval;
use serde::Deserialize;
use std::{collections::HashSet, fmt, fs, path::Path};

pub const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeSource {
    #[default]
    Auto,
    Trade,
    #[serde(rename = "kline_1m")]
    Kline1m,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolSubscription {
    pub symbol: String,
    pub intervals: Vec<Interval>,
    pub source: RealtimeSource,
}

impl SymbolSubscription {
    pub fn new(
        symbol: impl Into<String>,
        intervals: Vec<Interval>,
        source: RealtimeSource,
    ) -> Self {
        Self {
            symbol: symbol.into().trim().to_uppercase(),
            intervals,
            source,
        }
    }

    pub fn resolved_source(&self) -> RealtimeSource {
        match self.source {
            RealtimeSource::Auto => {
                let minimum_ms = self
                    .intervals
                    .iter()
                    .map(Interval::as_millis)
                    .min()
                    .unwrap_or(0);
                if minimum_ms >= 60_000 {
                    RealtimeSource::Kline1m
                } else {
                    RealtimeSource::Trade
                }
            }
            explicit => explicit,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("failed to read {path}: {message}")]
    Read { path: String, message: String },
    #[error("invalid TOML configuration: {0}")]
    Toml(String),
    #[error("symbol cannot be empty")]
    EmptySymbol,
    #[error("symbol {0} must configure at least one interval")]
    EmptyIntervals(String),
    #[error("invalid interval for {symbol}: {interval}")]
    InvalidInterval { symbol: String, interval: String },
    #[error("duplicate symbol in configuration: {0}")]
    DuplicateSymbol(String),
    #[error("kline_1m source cannot build sub-minute interval {interval} for {symbol}")]
    KlineWithSubMinute { symbol: String, interval: String },
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub bind_addr: String,
    pub subscriptions: Vec<SymbolSubscription>,
    pub retention_bars: u32,
    pub sync_on_start: bool,
    pub sync_lookback_bars: u32,
    pub realtime_flush_interval_secs: u64,
    pub realtime_flush_max_rows: usize,
    pub log_dir: String,
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from_path(CONFIG_FILE)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|error| ConfigError::Read {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;
        Self::from_toml(&raw)
    }

    pub fn from_toml(raw: &str) -> Result<Self, ConfigError> {
        let file: FileConfig =
            toml::from_str(raw).map_err(|error| ConfigError::Toml(error.to_string()))?;
        let subscriptions = parse_subscriptions(file.binance.symbols)?;

        Ok(Self {
            database_url: file.database.url,
            bind_addr: file.server.bind_addr,
            subscriptions,
            retention_bars: file.database.retention_bars,
            sync_on_start: file.binance.sync_on_start,
            sync_lookback_bars: file.binance.sync_lookback_bars,
            realtime_flush_interval_secs: file.realtime.flush_interval_secs,
            realtime_flush_max_rows: file.realtime.flush_max_rows,
            log_dir: file.logging.dir,
            log_level: file.logging.level,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    server: ServerConfig,
    database: DatabaseConfig,
    binance: BinanceConfig,
    realtime: RealtimeConfig,
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfig {
    bind_addr: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseConfig {
    url: String,
    retention_bars: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceConfig {
    sync_on_start: bool,
    sync_lookback_bars: u32,
    symbols: Vec<RawSymbolSubscription>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSymbolSubscription {
    symbol: String,
    intervals: Vec<String>,
    #[serde(default)]
    source: RealtimeSource,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RealtimeConfig {
    flush_interval_secs: u64,
    flush_max_rows: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoggingConfig {
    dir: String,
    level: String,
}

fn parse_subscriptions(
    raw_subscriptions: Vec<RawSymbolSubscription>,
) -> Result<Vec<SymbolSubscription>, ConfigError> {
    let mut subscriptions = Vec::with_capacity(raw_subscriptions.len());
    let mut seen_symbols = HashSet::new();

    for raw in raw_subscriptions {
        let symbol = raw.symbol.trim().to_uppercase();
        if symbol.is_empty() {
            return Err(ConfigError::EmptySymbol);
        }
        if !seen_symbols.insert(symbol.clone()) {
            return Err(ConfigError::DuplicateSymbol(symbol));
        }
        if raw.intervals.is_empty() {
            return Err(ConfigError::EmptyIntervals(symbol));
        }

        let mut intervals = Vec::with_capacity(raw.intervals.len());
        for value in raw.intervals {
            let interval = Interval::parse(&value).map_err(|_| ConfigError::InvalidInterval {
                symbol: symbol.clone(),
                interval: value.clone(),
            })?;
            if !intervals.contains(&interval) {
                intervals.push(interval);
            }
        }

        if raw.source == RealtimeSource::Kline1m {
            if let Some(interval) = intervals
                .iter()
                .find(|interval| interval.as_millis() < 60_000)
            {
                return Err(ConfigError::KlineWithSubMinute {
                    symbol,
                    interval: interval.canonical(),
                });
            }
        }

        subscriptions.push(SymbolSubscription::new(symbol, intervals, raw.source));
    }

    Ok(subscriptions)
}

impl fmt::Display for RealtimeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Auto => "auto",
            Self::Trade => "trade",
            Self::Kline1m => "kline_1m",
        };
        f.write_str(value)
    }
}
