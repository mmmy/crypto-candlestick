use crate::{
    binance::worker::SubscriptionPlan,
    domain::{candle::Candle, interval::Interval},
    storage::sqlite::SqliteStore,
};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;

const BINANCE_FAPI_BASE: &str = "https://fapi.binance.com";
const MAX_KLINE_LIMIT: u32 = 1500;
const DEFAULT_REBUILD_LIMIT: u32 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingKlineRange {
    pub start_open_time: i64,
    pub end_open_time: i64,
    pub interval_ms: i64,
}

impl MissingKlineRange {
    fn missing_count(&self) -> u32 {
        ((self.end_open_time - self.start_open_time) / self.interval_ms + 1) as u32
    }
}

pub fn detect_missing_kline_ranges(
    window_start_open_time: i64,
    window_end_open_time: i64,
    interval_ms: i64,
    existing_open_times: &[i64],
) -> Vec<MissingKlineRange> {
    let existing = existing_open_times
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut ranges = Vec::new();
    let mut current_start = None;
    let mut expected = window_start_open_time;

    while expected <= window_end_open_time {
        if existing.contains(&expected) {
            if let Some(start) = current_start.take() {
                ranges.push(MissingKlineRange {
                    start_open_time: start,
                    end_open_time: expected - interval_ms,
                    interval_ms,
                });
            }
        } else if current_start.is_none() {
            current_start = Some(expected);
        }

        expected += interval_ms;
    }

    if let Some(start) = current_start {
        ranges.push(MissingKlineRange {
            start_open_time: start,
            end_open_time: window_end_open_time,
            interval_ms,
        });
    }

    ranges
}

pub fn closed_lookback_window(interval: &Interval, lookback_bars: u32, now_ms: i64) -> (i64, i64) {
    let interval_ms = interval.as_millis() as i64;
    let current_bucket_start = interval.bucket_start_ms(now_ms);
    let latest_closed_open_time = current_bucket_start - interval_ms;
    let bar_count = i64::from(lookback_bars.max(1));
    let start_open_time = latest_closed_open_time - (bar_count - 1) * interval_ms;

    (start_open_time, latest_closed_open_time)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestKlinePage {
    pub start_time: i64,
    pub end_time: i64,
    pub limit: u32,
}

pub fn plan_rest_kline_pages(range: &MissingKlineRange) -> Vec<RestKlinePage> {
    let mut pages = Vec::new();
    let mut next_start = range.start_open_time;
    let mut remaining = range.missing_count();

    while remaining > 0 {
        let limit = remaining.min(MAX_KLINE_LIMIT);
        let page_end_open_time = next_start + (i64::from(limit) - 1) * range.interval_ms;
        pages.push(RestKlinePage {
            start_time: next_start,
            end_time: page_end_open_time + range.interval_ms - 1,
            limit,
        });
        next_start = page_end_open_time + range.interval_ms;
        remaining -= limit;
    }

    pages
}

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("invalid kline payload")]
    InvalidPayload,
    #[error("invalid number: {0}")]
    Number(#[from] std::num::ParseFloatError),
}

pub fn parse_rest_klines(payload: Value) -> Result<Vec<Candle>, RestError> {
    let rows = payload.as_array().ok_or(RestError::InvalidPayload)?;
    rows.iter().map(parse_rest_kline_row).collect()
}

pub async fn sync_native_klines(
    store: &SqliteStore,
    symbols: Vec<String>,
    intervals: Vec<Interval>,
    lookback_bars: u32,
) -> Result<(), RestError> {
    let plan = SubscriptionPlan::new(symbols, intervals);
    let client = reqwest::Client::new();

    for source in plan.kline_sources() {
        let last_open_time = store
            .max_open_time(&source.symbol, &source.canonical_interval)
            .await?;
        let mut start_time = match last_open_time {
            Some(value) => value + source.interval.as_millis() as i64,
            None => {
                chrono::Utc::now().timestamp_millis()
                    - source.interval.as_millis() as i64 * i64::from(lookback_bars.max(1))
            }
        };

        loop {
            let candles = fetch_klines_page(
                &client,
                &source.symbol,
                source.binance_interval,
                start_time,
                MAX_KLINE_LIMIT,
            )
            .await?;

            if candles.is_empty() {
                break;
            }

            let mut newest_open_time = start_time;
            for candle in &candles {
                newest_open_time = newest_open_time.max(candle.open_time);
                store
                    .upsert_candle(&source.symbol, &source.canonical_interval, candle)
                    .await?;
            }

            if candles.len() < MAX_KLINE_LIMIT as usize {
                break;
            }

            start_time = newest_open_time + source.interval.as_millis() as i64;
            sleep(Duration::from_millis(120)).await;
        }
    }

    rebuild_custom_klines(store, &plan, DEFAULT_REBUILD_LIMIT).await?;

    Ok(())
}

pub async fn rebuild_custom_klines(
    store: &SqliteStore,
    plan: &SubscriptionPlan,
    base_limit: u32,
) -> Result<(), RestError> {
    for (symbol, base, target) in plan.aggregation_targets() {
        let target_interval = Interval::parse(&target).map_err(|_| RestError::InvalidPayload)?;
        let start_time = store
            .max_open_time(&symbol, &target)
            .await?
            .map(|open_time| open_time.saturating_sub(target_interval.as_millis() as i64));
        let source_rows = store
            .query_klines(&symbol, &base, start_time, None, base_limit)
            .await?;
        let mut aggregator = crate::engine::aggregator::Aggregator::new(target_interval);

        for row in source_rows {
            if let Ok(Some(closed)) = aggregator.ingest_candle(row.candle) {
                store.upsert_candle(&symbol, &target, &closed).await?;
            }
            if let Some(current) = aggregator.current() {
                store.upsert_candle(&symbol, &target, &current).await?;
            }
        }
    }

    Ok(())
}

async fn fetch_klines_page(
    client: &reqwest::Client,
    symbol: &str,
    interval: &str,
    start_time: i64,
    limit: u32,
) -> Result<Vec<Candle>, RestError> {
    let payload = client
        .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/klines"))
        .query(&[
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("startTime", start_time.to_string()),
            ("limit", limit.min(MAX_KLINE_LIMIT).to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    parse_rest_klines(payload)
}

fn parse_rest_kline_row(row: &Value) -> Result<Candle, RestError> {
    let values = row.as_array().ok_or(RestError::InvalidPayload)?;
    if values.len() < 9 {
        return Err(RestError::InvalidPayload);
    }

    Ok(Candle {
        open_time: values[0].as_i64().ok_or(RestError::InvalidPayload)?,
        open: string_number(&values[1])?,
        high: string_number(&values[2])?,
        low: string_number(&values[3])?,
        close: string_number(&values[4])?,
        volume: string_number(&values[5])?,
        close_time: values[6].as_i64().ok_or(RestError::InvalidPayload)?,
        quote_volume: string_number(&values[7])?,
        trade_count: values[8].as_u64().ok_or(RestError::InvalidPayload)?,
        is_closed: true,
    })
}

fn string_number(value: &Value) -> Result<f64, RestError> {
    Ok(value
        .as_str()
        .ok_or(RestError::InvalidPayload)?
        .parse::<f64>()?)
}
