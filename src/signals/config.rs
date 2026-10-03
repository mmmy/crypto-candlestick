use super::model::SignalKind;
use crate::domain::interval::Interval;
use crate::indicators::guaili::{GuailiConfig, MaType};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    fmt, fs,
    hash::{Hash, Hasher},
    path::Path,
};

pub const SIGNAL_CONFIG_FILE: &str = "signals.toml";

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SignalConfig {
    pub enabled: bool,
    pub evaluation_interval_secs: u64,
    pub symbols: Vec<String>,
    pub indicator: IndicatorConfig,
    pub rules: RuleConfig,
    pub quality: QualityConfig,
    pub wecom_alerts: Vec<WecomAlertConfig>,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            evaluation_interval_secs: 5,
            symbols: Vec::new(),
            indicator: IndicatorConfig::default(),
            rules: RuleConfig::default(),
            quality: QualityConfig::default(),
            wecom_alerts: Vec::new(),
        }
    }
}

impl fmt::Debug for SignalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignalConfig")
            .field("enabled", &self.enabled)
            .field("evaluation_interval_secs", &self.evaluation_interval_secs)
            .field("symbols", &self.symbols)
            .field("indicator", &self.indicator)
            .field("rules", &self.rules)
            .field("quality", &self.quality)
            .field("wecom_alerts", &self.wecom_alerts)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndicatorConfig {
    pub ma_type: String,
    pub ma_length: usize,
    pub calc_limit: usize,
    pub atr_len: usize,
    pub atr_percent_len: usize,
    pub max_atr_rank: f64,
    pub slope_mul: f64,
    pub use_slope: bool,
}

impl Default for IndicatorConfig {
    fn default() -> Self {
        Self {
            ma_type: "EMA".into(),
            ma_length: 20,
            calc_limit: 500,
            atr_len: 1,
            atr_percent_len: 20,
            max_atr_rank: 100.0,
            slope_mul: 0.1,
            use_slope: true,
        }
    }
}

impl IndicatorConfig {
    /// Configuration validation runs before this conversion, including MA type.
    pub fn to_guaili_config(&self) -> GuailiConfig {
        GuailiConfig {
            ma_type: match self.ma_type.to_ascii_uppercase().as_str() {
                "SMA" => MaType::Sma,
                "SMMA" => MaType::Smma,
                "WMA" => MaType::Wma,
                "VWMA" => MaType::Vwma,
                _ => MaType::Ema,
            },
            ma_length: self.ma_length,
            atr_len: self.atr_len,
            atr_percent_len: self.atr_percent_len,
            max_atr_rank: self.max_atr_rank,
            slope_mul: self.slope_mul,
            use_slope: self.use_slope,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuleConfig {
    pub extreme_threshold: i32,
    pub compression_band: i32,
    pub minimum_levels: usize,
    pub min_history_bars: usize,
}

impl Default for RuleConfig {
    fn default() -> Self {
        Self {
            extreme_threshold: 10,
            compression_band: 2,
            minimum_levels: 5,
            min_history_bars: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QualityConfig {
    pub max_market_age_secs: u64,
    pub max_result_age_secs: u64,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            max_market_age_secs: 30,
            max_result_age_secs: 15,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WecomAlertConfig {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub webhook_url: String,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default = "default_min_signal_interval")]
    pub min_signal_interval: String,
    #[serde(default = "default_kinds")]
    pub kinds: Vec<SignalKind>,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
}

impl fmt::Debug for WecomAlertConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WecomAlertConfig")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("webhook_url", &"[redacted]")
            .field("symbols", &self.symbols)
            .field("min_signal_interval", &self.min_signal_interval)
            .field("kinds", &self.kinds)
            .field("cooldown_secs", &self.cooldown_secs)
            .finish()
    }
}

fn default_min_signal_interval() -> String {
    "10S".into()
}

fn default_kinds() -> Vec<SignalKind> {
    vec![
        SignalKind::Extreme,
        SignalKind::Compression,
        SignalKind::Conflict,
    ]
}

fn default_cooldown_secs() -> u64 {
    300
}

impl SignalConfig {
    /// A missing optional file keeps the module disabled. Malformed files fail closed.
    pub fn load(path: &Path) -> Result<Self, String> {
        match fs::read_to_string(path) {
            Ok(raw) => Self::from_toml(&raw),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(_) => Err("unable to read signal configuration file".into()),
        }
    }

    pub fn from_toml(raw: &str) -> Result<Self, String> {
        // TOML diagnostics can echo whole source lines, including webhook credentials.
        toml::from_str(raw).map_err(|_| "invalid signal configuration TOML or field type".into())
    }

    pub fn normalize_and_validate(&mut self, targets: &[(String, String)]) -> Result<(), String> {
        if !(1..=300).contains(&self.evaluation_interval_secs) {
            return Err("evaluation_interval_secs must be between 1 and 300".into());
        }
        let indicator = &mut self.indicator;
        indicator.ma_type = indicator.ma_type.trim().to_ascii_uppercase();
        if !["SMA", "EMA", "SMMA", "WMA", "VWMA"].contains(&indicator.ma_type.as_str()) {
            return Err("indicator.ma_type must be SMA, EMA, SMMA, WMA or VWMA".into());
        }
        if !(2..=5000).contains(&indicator.calc_limit) {
            return Err("indicator.calc_limit must be between 2 and 5000".into());
        }
        for (name, value) in [
            ("ma_length", indicator.ma_length),
            ("atr_len", indicator.atr_len),
            ("atr_percent_len", indicator.atr_percent_len),
        ] {
            if value == 0 || value >= indicator.calc_limit {
                return Err(format!(
                    "indicator.{name} must be positive and below calc_limit"
                ));
            }
        }
        if !indicator.max_atr_rank.is_finite() || !(0.0..=100.0).contains(&indicator.max_atr_rank) {
            return Err("indicator.max_atr_rank must be finite and between 0 and 100".into());
        }
        if !indicator.slope_mul.is_finite() || indicator.slope_mul < 0.0 {
            return Err("indicator.slope_mul must be finite and nonnegative".into());
        }
        if self.rules.extreme_threshold <= 0
            || self.rules.compression_band < 0
            || self.rules.compression_band >= self.rules.extreme_threshold
        {
            return Err("rules require 0 <= compression_band < extreme_threshold".into());
        }
        if !(1..=64).contains(&self.rules.minimum_levels) {
            return Err("rules.minimum_levels must be between 1 and 64".into());
        }
        let warmup = indicator
            .ma_length
            .max(indicator.atr_len.saturating_add(indicator.atr_percent_len))
            .max(15);
        if self.rules.min_history_bars < warmup
            || self.rules.min_history_bars >= indicator.calc_limit
        {
            return Err(
                "rules.min_history_bars must cover indicator warmup and be below calc_limit".into(),
            );
        }
        if !(1..=86_400).contains(&self.quality.max_market_age_secs)
            || !(1..=86_400).contains(&self.quality.max_result_age_secs)
            || self.quality.max_result_age_secs < self.evaluation_interval_secs
        {
            return Err("quality ages must be between 1 and 86400 seconds; result age must cover the evaluation interval".into());
        }

        let mut supported = BTreeMap::<String, BTreeSet<u64>>::new();
        for (symbol, interval) in targets {
            let interval = Interval::parse(interval)
                .map_err(|_| "invalid configured market interval".to_string())?;
            supported
                .entry(symbol.trim().to_ascii_uppercase())
                .or_default()
                .insert(interval.as_millis());
        }
        normalize_symbols(&mut self.symbols);
        if self.symbols.is_empty() {
            self.symbols = supported.keys().cloned().collect();
        }
        if self.enabled && self.symbols.is_empty() {
            return Err("enabled signal calculation requires configured symbols".into());
        }
        if self
            .symbols
            .iter()
            .any(|symbol| !supported.contains_key(symbol))
        {
            return Err("signal symbols must be configured market symbols".into());
        }
        if self.wecom_alerts.len() > 64 {
            return Err("at most 64 wecom alerts can be configured".into());
        }
        let mut alert_ids = BTreeSet::new();
        for alert in &mut self.wecom_alerts {
            alert.id = alert.id.trim().into();
            if alert.id.is_empty()
                || alert.id.len() > 64
                || !alert
                    .id
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || "_-".contains(ch))
                || !alert_ids.insert(alert.id.clone())
            {
                return Err("wecom alert ids must be unique, 1-64 ASCII letters, digits, underscores or hyphens".into());
            }
            alert.name = alert.name.trim().into();
            if alert.name.is_empty() {
                alert.name = alert.id.clone();
            }
            if alert.name.chars().count() > 100 {
                return Err("wecom alert names must be at most 100 characters".into());
            }
            let url = reqwest::Url::parse(&alert.webhook_url)
                .map_err(|_| "wecom webhook_url must be a valid HTTP(S) URL".to_string())?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
            {
                return Err(
                    "wecom webhook_url must be an HTTP(S) URL without userinfo or fragment".into(),
                );
            }
            normalize_symbols(&mut alert.symbols);
            if alert.symbols.is_empty() {
                alert.symbols = self.symbols.clone();
            }
            if alert.symbols.is_empty()
                || alert
                    .symbols
                    .iter()
                    .any(|symbol| !self.symbols.contains(symbol))
            {
                return Err("wecom alert symbols must be nonempty calculation symbols".into());
            }
            let minimum = Interval::parse(&alert.min_signal_interval).map_err(|_| {
                "wecom min_signal_interval must be a supported interval".to_string()
            })?;
            alert.min_signal_interval = minimum.canonical();
            if alert.symbols.iter().any(|symbol| {
                supported
                    .get(symbol)
                    .and_then(|intervals| intervals.last())
                    .copied()
                    .unwrap_or(0)
                    < minimum.as_millis()
            }) {
                return Err("wecom min_signal_interval exceeds a selected symbol's maximum configured interval".into());
            }
            if alert.kinds.is_empty() {
                alert.kinds = default_kinds();
            }
            alert.kinds.sort_by_key(|kind| match kind {
                SignalKind::Extreme => 0,
                SignalKind::Compression => 1,
                SignalKind::Conflict => 2,
            });
            alert.kinds.dedup();
            if alert.cooldown_secs > 86_400 {
                return Err("wecom cooldown_secs must be between 0 and 86400".into());
            }
        }
        Ok(())
    }

    pub fn calculation_hash(&self) -> u64 {
        stable_hash(&(
            self.enabled,
            self.evaluation_interval_secs,
            &self.symbols,
            &self.indicator,
            &self.rules,
            &self.quality,
        ))
    }

    pub fn delivery_hash(&self) -> u64 {
        stable_hash(&(self.enabled, self.calculation_hash(), &self.wecom_alerts))
    }
}

fn normalize_symbols(symbols: &mut Vec<String>) {
    for symbol in symbols.iter_mut() {
        *symbol = symbol.trim().to_ascii_uppercase();
    }
    symbols.sort();
    symbols.dedup();
}

pub(crate) fn stable_hash(value: &impl Serialize) -> u64 {
    let mut hasher = DefaultHasher::new();
    serde_json::to_vec(value)
        .unwrap_or_default()
        .hash(&mut hasher);
    hasher.finish()
}
