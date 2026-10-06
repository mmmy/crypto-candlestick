//! Real Binance PRICE_FILTER tick sizes; pricePrecision is deliberately ignored.
use crate::{price_alerts::Error, storage::sqlite::SqliteStore};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{sync::watch, task::JoinHandle};

pub const MAX_TICK_ORDINAL: u64 = 1_u64 << 53;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TickSize {
    pub wire: String,
    numerator: i128,
    scale: i128,
    value: f64,
}
impl TickSize {
    pub(crate) fn value(&self) -> f64 {
        self.value
    }
    pub(crate) fn parse(wire: &str) -> Result<Self, Error> {
        let (numerator, scale) =
            decimal(wire).ok_or_else(|| Error::Invalid("invalid PRICE_FILTER.tickSize".into()))?;
        let value = wire
            .parse::<f64>()
            .map_err(|_| Error::Invalid("invalid tickSize".into()))?;
        if numerator <= 0 || !value.is_finite() || value <= 0.0 {
            return Err(Error::Invalid(
                "tickSize must be positive and finite".into(),
            ));
        }
        Ok(Self {
            wire: wire.into(),
            numerator,
            scale,
            value,
        })
    }
    /// Accepted live prices already use f64; convert only after checking finite,
    /// positive and representable ordinal bounds. No allocation on this path.
    pub(crate) fn ordinal(&self, price: f64) -> Option<u64> {
        Self::ordinal_at(self.value, price)
    }
    pub(crate) fn ordinal_at(step: f64, price: f64) -> Option<u64> {
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        let ticks = price / step;
        if !ticks.is_finite() || ticks < 0.5 || ticks > MAX_TICK_ORDINAL as f64 {
            return None;
        }
        let rounded = ticks.round();
        (rounded >= 1.0 && rounded <= MAX_TICK_ORDINAL as f64).then_some(rounded as u64)
    }
    /// Cold coordinate saves use the shortest decimal representation and an
    /// exact quotient/remainder, avoiding binary .15/.1 tie rounding errors.
    pub(crate) fn normalize(&self, price: f64) -> Result<(u64, f64), Error> {
        if !price.is_finite() || price <= 0.0 {
            return Err(Error::Invalid("price must be positive and finite".into()));
        }
        let (numerator, scale) = decimal(&price.to_string())
            .ok_or_else(|| Error::Invalid("price exceeds decimal bounds".into()))?;
        let numerator = numerator
            .checked_mul(self.scale)
            .ok_or_else(|| Error::Invalid("price exceeds tick bounds".into()))?;
        let denominator = scale
            .checked_mul(self.numerator)
            .ok_or_else(|| Error::Invalid("tickSize exceeds decimal bounds".into()))?;
        let mut ticks = numerator / denominator;
        if (numerator % denominator)
            .checked_mul(2)
            .is_some_and(|r| r >= denominator)
        {
            ticks += 1;
        }
        if ticks <= 0 || ticks > i128::from(MAX_TICK_ORDINAL) {
            return Err(Error::Invalid(
                "price tick ordinal must be in 1..=2^53".into(),
            ));
        }
        let raw = ticks
            .checked_mul(self.numerator)
            .ok_or_else(|| Error::Invalid("normalized price overflow".into()))?;
        // Serialize a decimal, then parse, so acknowledgements such as .3 don't
        // gain an unnecessary .30000000000000004 multiplication artifact.
        let whole = raw / self.scale;
        let remainder = raw % self.scale;
        let decimals = self.scale.to_string().len() - 1;
        let text = if decimals == 0 {
            whole.to_string()
        } else {
            format!("{whole}.{remainder:0decimals$}")
        };
        let normalized = text
            .parse::<f64>()
            .map_err(|_| Error::Invalid("normalized price is invalid".into()))?;
        Ok((ticks as u64, normalized))
    }
}

fn decimal(value: &str) -> Option<(i128, i128)> {
    if value.is_empty() || value.len() > 80 {
        return None;
    }
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (value, 0),
    };
    if !(-18..=18).contains(&exponent) {
        return None;
    }
    let mut numerator = 0i128;
    let mut decimals = 0u32;
    let mut fractional = false;
    for byte in mantissa.bytes() {
        if byte == b'.' && !fractional {
            fractional = true;
            continue;
        }
        if !byte.is_ascii_digit() {
            return None;
        }
        numerator = numerator
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
        if fractional {
            decimals += 1;
        }
    }
    if decimals > 18 {
        return None;
    }
    let mut scale = 10_i128.checked_pow(decimals)?;
    if exponent >= 0 {
        numerator = numerator.checked_mul(10_i128.checked_pow(exponent as u32)?)?;
    } else {
        scale = scale.checked_mul(10_i128.checked_pow((-exponent) as u32)?)?;
    }
    Some((numerator, scale))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketMetadata {
    pub symbol: String,
    pub tv_symbol: String,
    pub tick_size: Option<String>,
    pub status: String,
    pub reason: Option<String>,
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<ExchangeSymbol>,
}
#[derive(Deserialize)]
struct ExchangeSymbol {
    symbol: String,
    filters: Vec<Filter>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Filter {
    filter_type: String,
    tick_size: Option<String>,
}

pub struct MetadataTask {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl MetadataTask {
    pub async fn stop(self) {
        let _ = self.cancel.send(true);
        let _ = self.task.await;
    }
}

pub async fn start_metadata(
    store: SqliteStore,
    client: reqwest::Client,
    symbols: Vec<String>,
) -> MetadataTask {
    for symbol in &symbols {
        let _ = store
            .apply_price_alert_metadata(symbol, None, "loading", None)
            .await;
    }
    let notify = store
        .price_alert_runtime
        .lock()
        .await
        .metadata_notify
        .clone();
    let (cancel, mut cancelled) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            let operation = async {
                let fetched = fetch(&client).await;
                match fetched {
                    Ok(info) => {
                        for symbol in &symbols {
                            let size = info
                                .symbols
                                .iter()
                                .find(|s| s.symbol == *symbol)
                                .and_then(|s| {
                                    s.filters.iter().find(|f| f.filter_type == "PRICE_FILTER")
                                })
                                .and_then(|f| f.tick_size.as_deref());
                            match size {
                                Some(size) => {
                                    if let Err(error) = store
                                        .apply_price_alert_metadata(
                                            symbol,
                                            Some(size),
                                            "ready",
                                            None,
                                        )
                                        .await
                                    {
                                        let reason =
                                            format!("PRICE_FILTER metadata invalid: {error}");
                                        let _ = store
                                            .apply_price_alert_metadata(
                                                symbol,
                                                None,
                                                "unavailable",
                                                Some(&reason),
                                            )
                                            .await;
                                    }
                                }
                                None => {
                                    let _=store.apply_price_alert_metadata(symbol,None,"unavailable",Some("Binance PRICE_FILTER.tickSize is unavailable for this symbol")).await;
                                }
                            }
                        }
                        true
                    }
                    Err(error) => {
                        for symbol in &symbols {
                            let _ = store
                                .apply_price_alert_metadata(
                                    symbol,
                                    None,
                                    "unavailable",
                                    Some(&error),
                                )
                                .await;
                        }
                        false
                    }
                }
            };
            let success =
                tokio::select! {biased;_=cancelled.changed()=>break,result=operation=>result};
            let delay = if success {
                Duration::from_secs(3600)
            } else {
                Duration::from_secs(60)
            };
            tokio::select! {biased;_=cancelled.changed()=>break,_=notify.notified()=>{},_=tokio::time::sleep(delay)=>{}}
        }
    });
    MetadataTask { cancel, task }
}

async fn fetch(client: &reqwest::Client) -> Result<ExchangeInfo, String> {
    let mut response = client
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("Binance metadata request failed: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Binance exchangeInfo returned {}",
            response.status()
        ));
    }
    let mut bytes = Vec::with_capacity(512 * 1024);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("Binance metadata response failed: {e}"))?
    {
        if bytes.len() + chunk.len() > 8 * 1024 * 1024 {
            return Err("Binance metadata response exceeded 8 MiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|e| format!("invalid Binance exchangeInfo: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimals_normalize_without_binary_tie_errors_and_bound_large_values() {
        let tenth = TickSize::parse("0.10000000").unwrap();
        assert_eq!(tenth.normalize(0.15).unwrap(), (2, 0.2));
        assert_eq!(tenth.normalize(0.3).unwrap(), (3, 0.3));
        assert_eq!(tenth.ordinal(0.3), Some(3));
        let cent = TickSize::parse("0.01").unwrap();
        assert_eq!(cent.normalize(12.345).unwrap(), (1235, 12.35));
        let unit = TickSize::parse("1").unwrap();
        assert_eq!(
            unit.ordinal(MAX_TICK_ORDINAL as f64),
            Some(MAX_TICK_ORDINAL)
        );
        assert_eq!(unit.ordinal(MAX_TICK_ORDINAL as f64 + 2.0), None);
        assert!(unit.normalize(MAX_TICK_ORDINAL as f64 + 2.0).is_err());
        assert!(TickSize::parse("0").is_err());
        assert!(TickSize::parse("NaN").is_err());
    }
    #[test]
    fn exchange_metadata_uses_price_filter_instead_of_price_precision() {
        let info:ExchangeInfo=serde_json::from_str(r#"{"symbols":[{"symbol":"BTCUSDT","pricePrecision":9,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.001"},{"filterType":"PRICE_FILTER","tickSize":"0.10"}]}]}"#).unwrap();
        assert_eq!(
            info.symbols[0]
                .filters
                .iter()
                .find(|f| f.filter_type == "PRICE_FILTER")
                .unwrap()
                .tick_size
                .as_deref(),
            Some("0.10")
        );
    }
}
