use crate::{
    binance::worker::{KlineSource, SubscriptionPlan},
    domain::{candle::Candle, interval::Interval},
    storage::sqlite::SqliteStore,
};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
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
    let existing = existing_open_times.iter().copied().collect::<BTreeSet<_>>();
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

pub async fn missing_ranges_for_source(
    store: &SqliteStore,
    source: &KlineSource,
    window_start_open_time: i64,
    window_end_open_time: i64,
) -> Result<Vec<MissingKlineRange>, RestError> {
    let interval_ms = source.interval.as_millis() as i64;
    let expected_bars = ((window_end_open_time - window_start_open_time) / interval_ms + 1) as u32;
    let rows = store
        .query_klines(
            &source.symbol,
            &source.canonical_interval,
            Some(window_start_open_time),
            Some(window_end_open_time),
            expected_bars,
        )
        .await?;
    let existing_open_times = rows
        .into_iter()
        .map(|row| row.candle.open_time)
        .collect::<Vec<_>>();

    Ok(detect_missing_kline_ranges(
        window_start_open_time,
        window_end_open_time,
        interval_ms,
        &existing_open_times,
    ))
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
    plan: &SubscriptionPlan,
    lookback_bars: u32,
) -> Result<(), RestError> {
    let client = reqwest::Client::new();

    for source in plan.kline_sources() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let (window_start, window_end) =
            closed_lookback_window(&source.interval, lookback_bars, now_ms);
        let ranges = missing_ranges_for_source(store, &source, window_start, window_end).await?;

        for range in ranges {
            for page in plan_rest_kline_pages(&range) {
                let candles = fetch_klines_page(
                    &client,
                    &source.symbol,
                    source.binance_interval,
                    page.start_time,
                    page.end_time,
                    page.limit,
                )
                .await?;

                store
                    .upsert_candles(&source.symbol, &source.canonical_interval, &candles)
                    .await?;

                sleep(Duration::from_millis(120)).await;
            }
        }
    }

    rebuild_custom_klines(store, plan, DEFAULT_REBUILD_LIMIT).await?;

    Ok(())
}

pub async fn rebuild_custom_klines(
    store: &SqliteStore,
    plan: &SubscriptionPlan,
    base_limit: u32,
) -> Result<(), RestError> {
    for (symbol, base, target) in plan.aggregation_targets() {
        let target_interval = Interval::parse(&target).map_err(|_| RestError::InvalidPayload)?;
        let base_interval = Interval::parse(&base).map_err(|_| RestError::InvalidPayload)?;
        let source_rows = store
            .query_klines(&symbol, &base, None, None, base_limit)
            .await?;

        let candles = aggregate_complete_custom_klines(source_rows, base_interval, target_interval);
        if candles.is_empty() {
            continue;
        }

        let start_time = candles.first().map(|candle| candle.open_time);
        let end_time = candles.last().map(|candle| candle.open_time);
        let Some((start_time, end_time)) = start_time.zip(end_time) else {
            continue;
        };

        let existing_rows = store
            .query_klines(
                &symbol,
                &target,
                Some(start_time),
                Some(end_time),
                candles.len() as u32,
            )
            .await?;
        let existing_open_times = existing_rows
            .into_iter()
            .map(|row| row.candle.open_time)
            .collect::<HashSet<_>>();
        let missing_candles = candles
            .into_iter()
            .filter(|candle| !existing_open_times.contains(&candle.open_time))
            .collect::<Vec<_>>();

        store
            .upsert_candles(&symbol, &target, &missing_candles)
            .await?;
    }

    Ok(())
}

fn aggregate_complete_custom_klines(
    source_rows: Vec<crate::storage::sqlite::StoredKline>,
    base_interval: Interval,
    target_interval: Interval,
) -> Vec<Candle> {
    let base_ms = base_interval.as_millis() as i64;
    let target_ms = target_interval.as_millis() as i64;
    if target_ms % base_ms != 0 {
        return Vec::new();
    }

    let mut output = Vec::new();
    let mut bucket_rows = Vec::new();
    let mut current_bucket_start = None;

    for row in source_rows {
        let bucket_start = target_interval.bucket_start_ms(row.candle.open_time);
        if current_bucket_start.is_some_and(|current| current != bucket_start) {
            if let Some(candle) = aggregate_complete_bucket(&bucket_rows, base_ms, target_interval)
            {
                output.push(candle);
            }
            bucket_rows.clear();
        }

        current_bucket_start = Some(bucket_start);
        bucket_rows.push(row.candle);
    }

    if let Some(candle) = aggregate_complete_bucket(&bucket_rows, base_ms, target_interval) {
        output.push(candle);
    }

    output
}

fn aggregate_complete_bucket(
    bucket_rows: &[Candle],
    base_ms: i64,
    target_interval: Interval,
) -> Option<Candle> {
    let first = bucket_rows.first()?;
    let bucket_start = target_interval.bucket_start_ms(first.open_time);
    let expected_count = (target_interval.as_millis() as i64 / base_ms) as usize;

    if bucket_rows.len() != expected_count || first.open_time != bucket_start {
        return None;
    }

    for pair in bucket_rows.windows(2) {
        if pair[1].open_time - pair[0].open_time != base_ms {
            return None;
        }
    }

    let mut aggregator = crate::engine::aggregator::Aggregator::new(target_interval);
    for candle in bucket_rows {
        if aggregator.ingest_candle(candle.clone()).is_err() {
            return None;
        }
    }

    aggregator.flush()
}

async fn fetch_klines_page(
    client: &reqwest::Client,
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
    limit: u32,
) -> Result<Vec<Candle>, RestError> {
    let payload = client
        .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/klines"))
        .query(&[
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
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
