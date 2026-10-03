use crate::{
    binance::worker::{KlineSource, SubscriptionPlan},
    domain::{candle::Candle, interval::Interval},
    engine::aggregator::TradeTick,
    storage::sqlite::SqliteStore,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;
use tokio::time::sleep;

const BINANCE_FAPI_BASE: &str = "https://fapi.binance.com";
const MAX_KLINE_LIMIT: u32 = 1500;
const DEFAULT_REBUILD_LIMIT: u32 = 1_000_000;
const STARTUP_REFRESH_CLOSED_BARS: u32 = 2;
const MAX_AGGREGATE_TRADE_LIMIT: i64 = 1_000;
const MAX_RECOVERY_TRADES: i64 = 100_000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AggregateTrade {
    pub id: i64,
    pub tick: TradeTick,
}

/// Trade IDs identify the exact replay boundary, unlike a live candle's end time.
pub(crate) async fn fetch_aggregate_trade_gap(
    symbol: &str,
    first_id: i64,
    last_id: i64,
) -> Result<Vec<AggregateTrade>, RestError> {
    let client = reqwest::Client::new();
    fetch_aggregate_trade_gap_from(&client, BINANCE_FAPI_BASE, symbol, first_id, last_id).await
}

async fn fetch_aggregate_trade_gap_from(
    client: &reqwest::Client,
    base_url: &str,
    symbol: &str,
    first_id: i64,
    last_id: i64,
) -> Result<Vec<AggregateTrade>, RestError> {
    if first_id > last_id {
        return Ok(Vec::new());
    }
    if first_id < 0 || last_id.saturating_sub(first_id) >= MAX_RECOVERY_TRADES {
        return Err(RestError::InvalidAggregateTradeGap(first_id));
    }
    let mut next_id = first_id;
    let mut trades = Vec::new();
    loop {
        let limit = (last_id - next_id + 1).min(MAX_AGGREGATE_TRADE_LIMIT);
        let payload = client
            .get(format!("{base_url}/fapi/v1/aggTrades"))
            .query(&[
                ("symbol", symbol.to_string()),
                ("fromId", next_id.to_string()),
                ("limit", limit.to_string()),
            ])
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let rows = payload.as_array().ok_or(RestError::InvalidPayload)?;
        if rows.is_empty() {
            return Err(RestError::InvalidAggregateTradeGap(next_id));
        }
        for row in rows {
            let id = row["a"].as_i64().ok_or(RestError::InvalidPayload)?;
            if id != next_id {
                return Err(RestError::InvalidAggregateTradeGap(next_id));
            }
            let price = string_number(&row["p"])?;
            let quantity = string_number(&row["q"])?;
            if !price.is_finite() || price <= 0.0 || !quantity.is_finite() || quantity <= 0.0 {
                return Err(RestError::InvalidPayload);
            }
            trades.push(AggregateTrade {
                id,
                tick: TradeTick::new(
                    row["T"].as_i64().ok_or(RestError::InvalidPayload)?,
                    price,
                    quantity,
                ),
            });
            if id == last_id {
                return Ok(trades);
            }
            next_id += 1;
        }
        sleep(Duration::from_millis(250)).await;
    }
}

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
    closed_lookback_window_with_anchor(interval, lookback_bars, now_ms, None)
}

pub fn closed_lookback_window_with_anchor(
    interval: &Interval,
    lookback_bars: u32,
    now_ms: i64,
    anchor_ms: Option<i64>,
) -> (i64, i64) {
    let interval_ms = interval.as_millis() as i64;
    let current_bucket_start = anchor_ms
        .map(|anchor| interval.bucket_start_ms_with_anchor(now_ms, anchor))
        .unwrap_or_else(|| interval.bucket_start_ms(now_ms));
    let latest_closed_open_time = current_bucket_start - interval_ms;
    let bar_count = i64::from(lookback_bars.max(1));
    let start_open_time = latest_closed_open_time - (bar_count - 1) * interval_ms;

    (start_open_time, latest_closed_open_time)
}

pub fn startup_refresh_window(interval: &Interval, now_ms: i64) -> RestKlinePage {
    startup_refresh_window_with_anchor(interval, now_ms, None)
}

pub fn startup_refresh_window_with_anchor(
    interval: &Interval,
    now_ms: i64,
    anchor_ms: Option<i64>,
) -> RestKlinePage {
    let interval_ms = interval.as_millis() as i64;
    let current_bucket_start = anchor_ms
        .map(|anchor| interval.bucket_start_ms_with_anchor(now_ms, anchor))
        .unwrap_or_else(|| interval.bucket_start_ms(now_ms));

    RestKlinePage {
        start_time: current_bucket_start - i64::from(STARTUP_REFRESH_CLOSED_BARS) * interval_ms,
        end_time: now_ms,
        limit: STARTUP_REFRESH_CLOSED_BARS + 1,
    }
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
    #[error("aggregate trade recovery is incomplete or too large at ID {0}")]
    InvalidAggregateTradeGap(i64),
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
        let anchor_ms = source_bucket_anchor(store, &source).await?;
        let (window_start, window_end) =
            closed_lookback_window_with_anchor(&source.interval, lookback_bars, now_ms, anchor_ms);
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

pub async fn rebuild_custom_kline_tail(
    store: &SqliteStore,
    plan: &SubscriptionPlan,
    now_ms: i64,
) -> Result<(), RestError> {
    for (symbol, base, target) in plan.aggregation_targets() {
        let target_interval = Interval::parse(&target).map_err(|_| RestError::InvalidPayload)?;
        let base_interval = Interval::parse(&base).map_err(|_| RestError::InvalidPayload)?;
        let base_ms = base_interval.as_millis() as i64;
        let current_base_start = base_interval.bucket_start_ms(now_ms);
        let refreshed_tail_start =
            current_base_start - i64::from(STARTUP_REFRESH_CLOSED_BARS) * base_ms;
        let rebuild_start = target_interval.bucket_start_ms(refreshed_tail_start);
        let source_limit = ((now_ms - rebuild_start).max(0) / base_ms + 1) as u32;
        let source_rows = store
            .query_klines(
                &symbol,
                &base,
                Some(rebuild_start),
                Some(now_ms),
                source_limit.max(1),
            )
            .await?;

        let candles = aggregate_complete_custom_klines(source_rows, base_interval, target_interval);
        store.upsert_candles(&symbol, &target, &candles).await?;
    }

    Ok(())
}

/// Force-refreshes the two latest closed source candles plus the current source
/// candle. Unlike the normal missing-range sync, this repairs rows whose open
/// time exists but whose OHLCV only contains trades observed after a restart.
pub async fn refresh_startup_kline_tail(
    store: &SqliteStore,
    plan: &SubscriptionPlan,
) -> Result<(), RestError> {
    let client = reqwest::Client::new();

    for source in plan.kline_sources() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let anchor_ms = source_bucket_anchor(store, &source).await?;
        let page = startup_refresh_window_with_anchor(&source.interval, now_ms, anchor_ms);
        let mut candles = fetch_klines_page(
            &client,
            &source.symbol,
            source.binance_interval,
            page.start_time,
            page.end_time,
            page.limit,
        )
        .await?;

        for candle in &mut candles {
            candle.is_closed = candle.close_time < now_ms;
        }

        if source.interval.uses_symbol_specific_binance_alignment() {
            let open_times = candles
                .iter()
                .map(|candle| candle.open_time)
                .collect::<Vec<_>>();
            if let Some(phase_ms) = source.interval.infer_bucket_anchor_ms(&open_times) {
                let deleted = store
                    .delete_klines_with_different_phase(
                        &source.symbol,
                        &source.canonical_interval,
                        source.interval.as_millis() as i64,
                        phase_ms,
                        page.start_time,
                        page.end_time,
                    )
                    .await?;
                if deleted > 0 {
                    tracing::info!(
                        symbol = %source.symbol,
                        interval = %source.canonical_interval,
                        deleted,
                        "removed klines with a stale bucket phase"
                    );
                }
            }
        }

        store
            .upsert_candles(&source.symbol, &source.canonical_interval, &candles)
            .await?;

        sleep(Duration::from_millis(120)).await;
    }

    rebuild_custom_kline_tail(store, plan, chrono::Utc::now().timestamp_millis()).await?;

    Ok(())
}

/// Initial history synchronization can finish minutes after the first symbol
/// was fetched. Repair only this symbol's missing *closed* tail at the first
/// observed trade's minute boundary; dynamic REST candles remain excluded.
pub(crate) async fn refresh_trade_bootstrap_history(
    store: &SqliteStore,
    plan: &SubscriptionPlan,
    symbol: &str,
    closed_before_ms: i64,
    lookback_bars: u32,
) -> Result<(), RestError> {
    refresh_trade_bootstrap_history_from(
        &reqwest::Client::new(),
        BINANCE_FAPI_BASE,
        store,
        plan,
        symbol,
        closed_before_ms,
        lookback_bars,
    )
    .await
}

async fn refresh_trade_bootstrap_history_from(
    client: &reqwest::Client,
    base_url: &str,
    store: &SqliteStore,
    plan: &SubscriptionPlan,
    symbol: &str,
    closed_before_ms: i64,
    lookback_bars: u32,
) -> Result<(), RestError> {
    let mut repaired_starts = BTreeMap::<String, i64>::new();
    for source in plan
        .kline_sources()
        .into_iter()
        .filter(|source| source.symbol == symbol)
    {
        let anchor_ms = source_bucket_anchor(store, &source).await?;
        let (window_start, window_end) = closed_lookback_window_with_anchor(
            &source.interval,
            lookback_bars,
            closed_before_ms,
            anchor_ms,
        );
        let mut rows = store
            .query_klines(
                symbol,
                &source.canonical_interval,
                Some(window_start),
                Some(window_end),
                lookback_bars.max(1),
            )
            .await?;
        // Native multi-day history can legitimately change phase. Only the
        // latest uninterrupted phase suffix belongs to this closed window.
        // Merely filtering wrong-phase rows can expose an older, coincidentally
        // matching phase and request a false gap across years of other phases.
        if let Some(barrier) = rows.iter().rposition(|row| {
            (row.candle.open_time - window_start) % source.interval.as_millis() as i64 != 0
        }) {
            rows.drain(..=barrier);
        }
        if rows.is_empty() {
            // An absent source may predate this contract's listing. Do not
            // repeat startup's entire history request or prevent healthy
            // periods from bootstrapping; quality gating keeps it unknown.
            continue;
        }
        // Do not request unavailable pre-listing history again on each symbol
        // bootstrap. Startup synchronization already owns the full lookback.
        let repair_start = rows
            .first()
            .map_or(window_start, |row| row.candle.open_time);
        let closed_open_times = rows
            .iter()
            .filter(|row| row.candle.is_closed && row.candle.close_time < closed_before_ms)
            .map(|row| row.candle.open_time)
            .collect::<Vec<_>>();
        let ranges = detect_missing_kline_ranges(
            repair_start,
            window_end,
            source.interval.as_millis() as i64,
            &closed_open_times,
        );
        for range in ranges {
            repaired_starts
                .entry(source.canonical_interval.clone())
                .and_modify(|start| *start = (*start).min(range.start_open_time))
                .or_insert(range.start_open_time);
            for page in plan_rest_kline_pages(&range) {
                let candles = fetch_klines_page_from(
                    client,
                    base_url,
                    symbol,
                    source.binance_interval,
                    page.start_time,
                    page.end_time,
                    page.limit,
                )
                .await
                .map_err(|error| {
                    tracing::warn!(
                        symbol,
                        interval = source.canonical_interval,
                        start_time = page.start_time,
                        end_time = page.end_time,
                        closed_before_ms,
                        "bootstrap closed-history request failed: {error}"
                    );
                    error
                })?;
                let mut closed_candles = Vec::new();
                for candle in candles {
                    if candle.close_time >= closed_before_ms
                        || candle.open_time < page.start_time
                        || candle.open_time > page.end_time
                    {
                        // REST uses open-time bounds. A returned dynamic or
                        // neighboring edge bucket is ordinary unavailable
                        // history, not a reason to restart every symbol.
                        tracing::debug!(
                            symbol,
                            interval = source.canonical_interval,
                            start_time = page.start_time,
                            end_time = page.end_time,
                            open_time = candle.open_time,
                            close_time = candle.close_time,
                            closed_before_ms,
                            "excluded bootstrap REST boundary candle"
                        );
                        continue;
                    }
                    if (candle.open_time - page.start_time) % range.interval_ms != 0
                        || candle.close_time != candle.open_time + range.interval_ms - 1
                    {
                        tracing::warn!(
                            symbol,
                            interval = source.canonical_interval,
                            start_time = page.start_time,
                            end_time = page.end_time,
                            open_time = candle.open_time,
                            close_time = candle.close_time,
                            expected_interval_ms = range.interval_ms,
                            "bootstrap REST closed candle has incompatible alignment or duration"
                        );
                        return Err(RestError::InvalidPayload);
                    }
                    closed_candles.push(candle);
                }
                store
                    .upsert_candles(symbol, &source.canonical_interval, &closed_candles)
                    .await?;
            }
        }
    }
    for (target_symbol, base, target) in plan.aggregation_targets() {
        if target_symbol != symbol {
            continue;
        }
        let Some(affected_start) = repaired_starts.get(&base) else {
            continue;
        };
        let base_interval = Interval::parse(&base).map_err(|_| RestError::InvalidPayload)?;
        let target_interval = Interval::parse(&target).map_err(|_| RestError::InvalidPayload)?;
        let rebuild_start = target_interval.bucket_start_ms(*affected_start);
        let limit = ((closed_before_ms - rebuild_start).max(0) / base_interval.as_millis() as i64
            + 1) as u32;
        let rows = store
            .query_klines(
                symbol,
                &base,
                Some(rebuild_start),
                Some(closed_before_ms - 1),
                limit.max(1),
            )
            .await?
            .into_iter()
            .filter(|row| row.candle.is_closed && row.candle.close_time < closed_before_ms)
            .collect();
        let candles = aggregate_complete_custom_klines(rows, base_interval, target_interval);
        store.upsert_candles(symbol, &target, &candles).await?;
    }
    Ok(())
}

async fn source_bucket_anchor(
    store: &SqliteStore,
    source: &KlineSource,
) -> Result<Option<i64>, RestError> {
    if !source.interval.uses_symbol_specific_binance_alignment() {
        return Ok(None);
    }

    let rows = store
        .query_klines(&source.symbol, &source.canonical_interval, None, None, 32)
        .await?;
    let open_times = rows
        .into_iter()
        .map(|row| row.candle.open_time)
        .collect::<Vec<_>>();

    Ok(source.interval.infer_bucket_anchor_ms(&open_times))
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

    if bucket_rows.len() != expected_count
        || first.open_time != bucket_start
        || bucket_rows.iter().any(|candle| !candle.is_closed)
    {
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
    fetch_klines_page_from(
        client,
        BINANCE_FAPI_BASE,
        symbol,
        interval,
        start_time,
        end_time,
        limit,
    )
    .await
}

async fn fetch_klines_page_from(
    client: &reqwest::Client,
    base_url: &str,
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
    limit: u32,
) -> Result<Vec<Candle>, RestError> {
    let payload = client
        .get(format!("{base_url}/fapi/v1/klines"))
        .query(&[
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
            ("limit", limit.min(MAX_KLINE_LIMIT).to_string()),
        ])
        .timeout(Duration::from_secs(10))
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

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use axum::{extract::Query, routing::get, Json, Router};
    use std::collections::HashMap;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, task)
    }

    fn bootstrap_history_candle(open_time: i64, interval: Interval, closed: bool) -> Candle {
        Candle {
            open_time,
            close_time: open_time + interval.as_millis() as i64 - 1,
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 105.0,
            volume: 2.0,
            quote_volume: 200.0,
            trade_count: 3,
            is_closed: closed,
        }
    }

    #[tokio::test]
    async fn bootstrap_repairs_only_symbol_closed_gaps_and_rebuilds_custom_tail() {
        use crate::config::{RealtimeSource, SymbolSubscription};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let plan = SubscriptionPlan::from_subscriptions(vec![
            SymbolSubscription::new(
                "BTCUSDT",
                vec![
                    Interval::Minutes(1),
                    Interval::Minutes(2),
                    Interval::Minutes(5),
                    Interval::Minutes(15),
                ],
                RealtimeSource::Trade,
            ),
            SymbolSubscription::new(
                "ETHUSDT",
                vec![Interval::Minutes(1), Interval::Minutes(5)],
                RealtimeSource::Trade,
            ),
        ]);
        for index in -80..4 {
            store
                .upsert_candle(
                    "BTCUSDT",
                    "1",
                    &bootstrap_history_candle(index * 60_000, Interval::Minutes(1), true),
                )
                .await
                .unwrap();
        }
        for index in -80..0 {
            store
                .upsert_candle(
                    "BTCUSDT",
                    "5",
                    &bootstrap_history_candle(index * 300_000, Interval::Minutes(5), true),
                )
                .await
                .unwrap();
        }
        // Old REST current rows now lie before the first WS minute cutoff.
        // Open-time presence alone must not count them as closed history.
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &bootstrap_history_candle(240_000, Interval::Minutes(1), false),
            )
            .await
            .unwrap();
        store
            .upsert_candle(
                "BTCUSDT",
                "5",
                &bootstrap_history_candle(0, Interval::Minutes(5), false),
            )
            .await
            .unwrap();
        rebuild_custom_klines(&store, &plan, 1000).await.unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let router = Router::new().route(
            "/fapi/v1/klines",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let count = request_count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(query["symbol"], "BTCUSDT");
                    let start: i64 = query["startTime"].parse().unwrap();
                    let end: i64 = query["endTime"].parse().unwrap();
                    let limit: i64 = query["limit"].parse().unwrap();
                    let duration = match query["interval"].as_str() {
                        "1m" => {
                            assert_eq!((start, end, limit), (240_000, 359_999, 2));
                            60_000
                        }
                        "5m" => {
                            assert_eq!((start, end, limit), (0, 299_999, 1));
                            300_000
                        }
                        other => panic!("unexpected healthy-source request {other}"),
                    };
                    Json(Value::Array(
                        (0..limit)
                            .map(|index| {
                                let open = start + index * duration;
                                serde_json::json!([
                                    open,
                                    "100",
                                    "110",
                                    "90",
                                    "105",
                                    "2",
                                    open + duration - 1,
                                    "200",
                                    3
                                ])
                            })
                            .collect(),
                    ))
                }
            }),
        );
        let (url, task) = serve(router).await;
        refresh_trade_bootstrap_history_from(
            &reqwest::Client::new(),
            &url,
            &store,
            &plan,
            "BTCUSDT",
            360_000,
            100,
        )
        .await
        .unwrap();
        task.abort();
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        let minute_rows = store
            .query_klines("BTCUSDT", "1", None, Some(359_999), 100)
            .await
            .unwrap();
        assert_eq!(minute_rows.len(), 86);
        assert!(minute_rows.iter().all(|row| row.candle.is_closed));
        assert!(minute_rows
            .windows(2)
            .all(|pair| pair[1].candle.open_time - pair[0].candle.open_time == 60_000));
        let custom = store
            .query_klines("BTCUSDT", "2", Some(240_000), Some(240_000), 1)
            .await
            .unwrap();
        assert_eq!(custom.len(), 1);
        assert_eq!(custom[0].candle.volume, 4.0);
        assert!(custom[0].candle.is_closed);
        assert!(store
            .query_klines("ETHUSDT", "1", None, None, 100)
            .await
            .unwrap()
            .is_empty());
        // An up-to-date symbol requires no extra REST calls at all.
        refresh_trade_bootstrap_history_from(
            &reqwest::Client::new(),
            "invalid://unused",
            &store,
            &plan,
            "BTCUSDT",
            360_000,
            100,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bootstrap_does_not_accept_dynamic_or_out_of_range_rest_history() {
        use crate::config::{RealtimeSource, SymbolSubscription};
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![Interval::Minutes(1)],
            RealtimeSource::Trade,
        )]);
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &bootstrap_history_candle(0, Interval::Minutes(1), true),
            )
            .await
            .unwrap();
        let (url, task) = serve(Router::new().route(
            "/fapi/v1/klines",
            get(|| async {
                // The required 60k candle is replaced by the unclosed 120k one.
                Json(serde_json::json!([[
                    120000, "100", "110", "90", "105", "2", 179999, "200", 3
                ]]))
            }),
        ))
        .await;
        let result = refresh_trade_bootstrap_history_from(
            &reqwest::Client::new(),
            &url,
            &store,
            &plan,
            "BTCUSDT",
            120_000,
            2,
        )
        .await;
        task.abort();
        assert!(result.is_ok());
        assert_eq!(
            store
                .query_klines("BTCUSDT", "1", None, None, 100)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn bootstrap_excludes_current_rest_edge_without_reconnecting() {
        use crate::config::{RealtimeSource, SymbolSubscription};
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![Interval::Minutes(5)],
            RealtimeSource::Trade,
        )]);
        store
            .upsert_candle(
                "BTCUSDT",
                "5",
                &bootstrap_history_candle(0, Interval::Minutes(5), false),
            )
            .await
            .unwrap();
        let (url, task) = serve(Router::new().route(
            "/fapi/v1/klines",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query["interval"], "5m");
                assert_eq!(query["startTime"], "0");
                assert_eq!(query["endTime"], "299999");
                Json(serde_json::json!([
                    [0, "100", "110", "90", "105", "2", 299999, "200", 3],
                    [300000, "100", "110", "90", "105", "2", 599999, "200", 3]
                ]))
            }),
        ))
        .await;
        refresh_trade_bootstrap_history_from(
            &reqwest::Client::new(),
            &url,
            &store,
            &plan,
            "BTCUSDT",
            360_000,
            3,
        )
        .await
        .unwrap();
        task.abort();
        let stored = store
            .query_klines("BTCUSDT", "5", None, None, 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert!(stored[0].candle.is_closed);
        assert_eq!(stored[0].candle.open_time, 0);
    }

    #[tokio::test]
    async fn bootstrap_repairs_only_latest_phase_suffix_even_if_older_phase_matches() {
        use crate::config::{RealtimeSource, SymbolSubscription};
        let day = 86_400_000;
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![Interval::Days(3)],
            RealtimeSource::Trade,
        )]);
        // Day 85 accidentally matches the current grid, but day 87 proves
        // that it belongs to an earlier phase regime. Its stale closed flag
        // must not cause a repair request to cross that phase barrier.
        for open_day in [85, 87, 88, 91, 94, 97] {
            store
                .upsert_candle(
                    "BTCUSDT",
                    "3D",
                    &bootstrap_history_candle(
                        open_day * day,
                        Interval::Days(3),
                        open_day != 85 && open_day != 91,
                    ),
                )
                .await
                .unwrap();
        }
        let router = Router::new().route(
            "/fapi/v1/klines",
            get(
                move |Query(query): Query<HashMap<String, String>>| async move {
                    assert_eq!(query["interval"], "3d");
                    assert_eq!(query["startTime"], (91 * day).to_string());
                    assert_eq!(query["endTime"], (94 * day - 1).to_string());
                    assert_eq!(query["limit"], "1");
                    Json(serde_json::json!([[
                        91 * day,
                        "100",
                        "110",
                        "90",
                        "105",
                        "2",
                        94 * day - 1,
                        "200",
                        3
                    ]]))
                },
            ),
        );
        let (url, task) = serve(router).await;
        refresh_trade_bootstrap_history_from(
            &reqwest::Client::new(),
            &url,
            &store,
            &plan,
            "BTCUSDT",
            100 * day + 60_000,
            100,
        )
        .await
        .unwrap();
        task.abort();
        let repaired = store
            .query_klines("BTCUSDT", "3D", Some(91 * day), Some(91 * day), 1)
            .await
            .unwrap();
        assert!(repaired[0].candle.is_closed);
        let historical = store
            .query_klines("BTCUSDT", "3D", Some(85 * day), Some(85 * day), 1)
            .await
            .unwrap();
        assert!(!historical[0].candle.is_closed);
    }

    async fn page(Query(query): Query<HashMap<String, String>>) -> Json<Value> {
        assert_eq!(query["symbol"], "BTCUSDT");
        assert!(!query.contains_key("startTime") && !query.contains_key("endTime"));
        let first: i64 = query["fromId"].parse().unwrap();
        let count: i64 = query["limit"].parse().unwrap();
        assert!((1..=1_000).contains(&count));
        // Short pages exercise pagination without thousands of fixtures.
        Json(Value::Array(
            (first..first + count.min(2))
                .map(|id| serde_json::json!({"a": id, "p": "100", "q": "2", "T": id * 1_000}))
                .collect(),
        ))
    }

    #[tokio::test]
    async fn recovery_fetches_exact_id_range_across_short_pages() {
        let (url, task) = serve(Router::new().route("/fapi/v1/aggTrades", get(page))).await;
        let trades =
            fetch_aggregate_trade_gap_from(&reqwest::Client::new(), &url, "BTCUSDT", 11, 15)
                .await
                .unwrap();
        task.abort();
        assert_eq!(
            trades.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![11, 12, 13, 14, 15]
        );
        assert_eq!(trades[0].tick, TradeTick::new(11_000, 100.0, 2.0));
    }

    #[tokio::test]
    async fn recovery_rejects_missing_ids_instead_of_returning_partial_data() {
        let (url, task) = serve(Router::new().route(
            "/fapi/v1/aggTrades",
            get(|| async {
                Json(serde_json::json!([
                    {"a": 11, "p": "100", "q": "2", "T": 11_000},
                    {"a": 13, "p": "100", "q": "2", "T": 13_000}
                ]))
            }),
        ))
        .await;
        let result =
            fetch_aggregate_trade_gap_from(&reqwest::Client::new(), &url, "BTCUSDT", 11, 13).await;
        task.abort();
        assert!(matches!(
            result,
            Err(RestError::InvalidAggregateTradeGap(12))
        ));
    }

    #[tokio::test]
    async fn recovery_rejects_unavailable_or_invalid_trade_history() {
        let (url, task) = serve(Router::new().route(
            "/fapi/v1/aggTrades",
            get(|| async { Json(serde_json::json!([])) }),
        ))
        .await;
        let result =
            fetch_aggregate_trade_gap_from(&reqwest::Client::new(), &url, "BTCUSDT", 11, 12).await;
        task.abort();
        assert!(matches!(
            result,
            Err(RestError::InvalidAggregateTradeGap(11))
        ));
        let (url, task) = serve(Router::new().route(
            "/fapi/v1/aggTrades",
            get(|| async {
                Json(serde_json::json!([{"a": 11, "p": "NaN", "q": "2", "T": 11_000}]))
            }),
        ))
        .await;
        let result =
            fetch_aggregate_trade_gap_from(&reqwest::Client::new(), &url, "BTCUSDT", 11, 11).await;
        task.abort();
        assert!(matches!(result, Err(RestError::InvalidPayload)));
    }

    #[tokio::test]
    async fn empty_gap_and_recovery_limit_do_not_make_requests() {
        let client = reqwest::Client::new();
        let empty = fetch_aggregate_trade_gap_from(&client, "invalid://unused", "BTCUSDT", 12, 11)
            .await
            .unwrap();
        assert!(empty.is_empty());
        let result = fetch_aggregate_trade_gap_from(
            &client,
            "invalid://unused",
            "BTCUSDT",
            1,
            MAX_RECOVERY_TRADES + 1,
        )
        .await;
        assert!(matches!(
            result,
            Err(RestError::InvalidAggregateTradeGap(1))
        ));
    }
}
