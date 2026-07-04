use crate::{
    domain::{candle::Candle, interval::Interval},
    http::routes::AppState,
    runtime_health::WebSocketHealth,
    storage::sqlite::StoredKline,
    time_format::format_timestamp_ms,
};
use axum::{
    extract::{Query, State},
    Json,
};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

const DEEP_HEALTH_SCAN_LIMIT: u32 = 5_000;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepHealthResponse {
    pub ok: bool,
    pub websocket: WebSocketHealth,
    pub series: Vec<SeriesHealth>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthSummaryResponse {
    pub ok: bool,
    pub websocket_ok: bool,
    pub total_series: usize,
    pub ok_series: usize,
    pub bad_series: usize,
    pub symbols: Vec<SymbolHealthSummary>,
    pub reasons: Vec<HealthReasonSummary>,
    pub server_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolHealthSummary {
    pub symbol: String,
    pub total: usize,
    pub ok: usize,
    pub bad: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthReasonSummary {
    pub reason: &'static str,
    pub count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesHealth {
    pub symbol: String,
    pub interval: String,
    pub latest_open_time: Option<String>,
    pub latest_lag_intervals: Option<u32>,
    pub consecutive_bars_from_latest: u32,
    pub checked_bars: usize,
    pub source: &'static str,
    pub ok: bool,
    pub reason: Option<&'static str>,
}

pub async fn deep_health(
    State(state): State<AppState>,
) -> Result<Json<DeepHealthResponse>, (axum::http::StatusCode, String)> {
    Ok(Json(build_deep_health_response(&state).await?))
}

pub async fn health_summary(
    State(state): State<AppState>,
) -> Result<Json<HealthSummaryResponse>, (axum::http::StatusCode, String)> {
    let deep = build_deep_health_response(&state).await?;
    let mut symbols = BTreeMap::<String, SymbolHealthSummary>::new();
    let mut reasons = BTreeMap::<&'static str, usize>::new();

    for item in &deep.series {
        let summary = symbols
            .entry(item.symbol.clone())
            .or_insert_with(|| SymbolHealthSummary {
                symbol: item.symbol.clone(),
                total: 0,
                ok: 0,
                bad: 0,
            });
        summary.total += 1;
        if item.ok {
            summary.ok += 1;
        } else {
            summary.bad += 1;
            if let Some(reason) = item.reason {
                *reasons.entry(reason).or_insert(0) += 1;
            }
        }
    }

    let ok_series = deep.series.iter().filter(|item| item.ok).count();
    let total_series = deep.series.len();

    Ok(Json(HealthSummaryResponse {
        ok: deep.ok,
        websocket_ok: deep.websocket.ok,
        total_series,
        ok_series,
        bad_series: total_series - ok_series,
        symbols: symbols.into_values().collect(),
        reasons: reasons
            .into_iter()
            .map(|(reason, count)| HealthReasonSummary { reason, count })
            .collect(),
        server_time: format_timestamp_ms(chrono::Utc::now().timestamp_millis()),
    }))
}

async fn build_deep_health_response(
    state: &AppState,
) -> Result<DeepHealthResponse, (axum::http::StatusCode, String)> {
    let mut series = Vec::with_capacity(state.health_targets.len());

    for target in &state.health_targets {
        let interval = Interval::parse(&target.interval)
            .map_err(|err| (axum::http::StatusCode::BAD_REQUEST, err.to_string()))?;
        let canonical_interval = interval.canonical();
        let interval_ms = interval.as_millis() as i64;

        let (source, candles_desc) = if interval.as_millis() < 60_000 {
            let rows = state
                .memory_series
                .query(
                    &target.symbol,
                    &canonical_interval,
                    None,
                    None,
                    DEEP_HEALTH_SCAN_LIMIT,
                )
                .await;
            (
                "memory",
                rows.into_iter()
                    .rev()
                    .map(|row| row.candle)
                    .collect::<Vec<_>>(),
            )
        } else {
            let rows = state
                .store
                .query_latest_klines_desc(
                    &target.symbol,
                    &canonical_interval,
                    DEEP_HEALTH_SCAN_LIMIT,
                )
                .await
                .map_err(|err| {
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    )
                })?;
            let buffered_rows = state
                .closed_buffer
                .query(
                    &target.symbol,
                    &canonical_interval,
                    None,
                    None,
                    DEEP_HEALTH_SCAN_LIMIT,
                )
                .await;
            let rows = merge_kline_rows(rows.into_iter().rev().collect(), buffered_rows);
            let candles_desc = rows
                .into_iter()
                .rev()
                .take(DEEP_HEALTH_SCAN_LIMIT as usize)
                .map(|row| row.candle)
                .collect::<Vec<_>>();
            ("sqlite+buffer", candles_desc)
        };

        let latest_open_time = candles_desc
            .first()
            .map(|candle| format_timestamp_ms(candle.open_time));
        let latest_lag_intervals = candles_desc
            .first()
            .map(|candle| latest_lag_intervals(&interval, candle.open_time));
        let consecutive_bars_from_latest = consecutive_bars_from_latest(&candles_desc, interval_ms);
        let is_stale = latest_lag_intervals
            .map(|lag| lag > max_allowed_lag_intervals(&interval))
            .unwrap_or(true);
        let ok = consecutive_bars_from_latest > 0 && !is_stale;
        let reason = if consecutive_bars_from_latest == 0 {
            Some("no closed candles")
        } else if is_stale {
            Some("latest candle is stale")
        } else {
            None
        };

        series.push(SeriesHealth {
            symbol: target.symbol.to_uppercase(),
            interval: canonical_interval,
            latest_open_time,
            latest_lag_intervals,
            consecutive_bars_from_latest,
            checked_bars: candles_desc.len(),
            source,
            ok,
            reason,
        });
    }

    let websocket = state.runtime_health.websocket_snapshot().await;
    let ok = websocket.ok && series.iter().all(|item| item.ok);
    Ok(DeepHealthResponse {
        ok,
        websocket,
        series,
    })
}

fn consecutive_bars_from_latest(candles_desc: &[Candle], interval_ms: i64) -> u32 {
    let Some(latest) = candles_desc.first() else {
        return 0;
    };

    let mut count = 1;
    let mut expected_next_open_time = latest.open_time - interval_ms;

    for candle in candles_desc.iter().skip(1) {
        if candle.open_time != expected_next_open_time {
            break;
        }
        count += 1;
        expected_next_open_time -= interval_ms;
    }

    count
}

fn latest_lag_intervals(interval: &Interval, latest_open_time: i64) -> u32 {
    let now_ms = Local::now().timestamp_millis();
    latest_lag_intervals_at(interval, latest_open_time, now_ms)
}

fn max_allowed_lag_intervals(interval: &Interval) -> u32 {
    if interval.as_millis() < 60_000 {
        4
    } else {
        2
    }
}

fn latest_lag_intervals_at(interval: &Interval, latest_open_time: i64, now_ms: i64) -> u32 {
    let interval_ms = interval.as_millis() as i64;
    let current_bucket_start = interval.bucket_start_ms(now_ms);
    if current_bucket_start <= latest_open_time {
        return 0;
    }

    ((current_bucket_start - latest_open_time) / interval_ms) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_latest_lag_from_current_bucket_start() {
        let interval = Interval::parse("1").unwrap();
        let now_ms = 1779105961000; // 2026-05-19 20:06:01 +08:00
        let latest_open_time = 1779105720000; // 2026-05-19 20:02:00 +08:00

        assert_eq!(
            latest_lag_intervals_at(&interval, latest_open_time, now_ms),
            4
        );
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineQuery {
    pub symbol: String,
    pub intervals: Option<String>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<u32>,
    pub closed_only: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineResponse {
    pub symbol: String,
    pub interval: String,
    pub candle: ApiCandle,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineEnvelope {
    pub symbol: String,
    pub intervals: Vec<String>,
    pub limit: u32,
    pub closed_only: bool,
    pub timezone: &'static str,
    pub server_time: i64,
    pub series: Vec<KlineSeries>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineSeries {
    pub interval: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub count: usize,
    pub data: Vec<KlineResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiCandle {
    pub open_time: String,
    pub close_time: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
    pub is_closed: bool,
}

impl From<StoredKline> for KlineResponse {
    fn from(row: StoredKline) -> Self {
        Self {
            symbol: row.symbol,
            interval: row.interval,
            candle: row.candle.into(),
        }
    }
}

impl From<Candle> for ApiCandle {
    fn from(candle: Candle) -> Self {
        Self {
            open_time: format_timestamp_ms(candle.open_time),
            close_time: format_timestamp_ms(candle.close_time),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            quote_volume: candle.quote_volume,
            trade_count: candle.trade_count,
            is_closed: candle.is_closed,
        }
    }
}

fn parse_intervals(input: Option<&str>) -> Result<Vec<Interval>, (axum::http::StatusCode, String)> {
    let input = input
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "missing intervals".to_string(),
            )
        })?;

    input
        .split(',')
        .map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    "empty interval in intervals".to_string(),
                ));
            }

            Interval::parse(item)
                .map_err(|err| (axum::http::StatusCode::BAD_REQUEST, err.to_string()))
        })
        .collect()
}

pub async fn klines(
    State(state): State<AppState>,
    Query(query): Query<KlineQuery>,
) -> Result<Json<KlineEnvelope>, (axum::http::StatusCode, String)> {
    let intervals = parse_intervals(query.intervals.as_deref())?;
    let canonical_intervals = intervals
        .iter()
        .map(Interval::canonical)
        .collect::<Vec<_>>();
    let limit = query.limit.unwrap_or(200);
    let closed_only = query.closed_only.unwrap_or(false);
    let mut series = Vec::with_capacity(intervals.len());

    for interval in intervals {
        series.push(
            query_kline_series(
                &state,
                &query.symbol,
                interval,
                query.start_time,
                query.end_time,
                limit,
                closed_only,
            )
            .await?,
        );
    }

    Ok(Json(KlineEnvelope {
        symbol: query.symbol.to_uppercase(),
        intervals: canonical_intervals,
        limit,
        closed_only,
        timezone: "Asia/Shanghai",
        server_time: Local::now().timestamp_millis(),
        series,
    }))
}

async fn query_kline_series(
    state: &AppState,
    symbol: &str,
    interval: Interval,
    start_time: Option<i64>,
    end_time: Option<i64>,
    limit: u32,
    closed_only: bool,
) -> Result<KlineSeries, (axum::http::StatusCode, String)> {
    let canonical_interval = interval.canonical();
    let query_limit = if closed_only {
        limit.saturating_add(1)
    } else {
        limit
    };
    let mut rows = if interval.as_millis() < 60_000 {
        state
            .memory_series
            .query(
                symbol,
                &canonical_interval,
                start_time,
                end_time,
                query_limit,
            )
            .await
    } else {
        let rows = state
            .store
            .query_klines(
                symbol,
                &canonical_interval,
                start_time,
                end_time,
                query_limit,
            )
            .await
            .map_err(|err| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )
            })?;
        let buffered_rows = state
            .closed_buffer
            .query(
                symbol,
                &canonical_interval,
                start_time,
                end_time,
                query_limit,
            )
            .await;
        merge_kline_rows(rows, buffered_rows)
    };

    if !closed_only {
        if let Some(latest) = state
            .latest
            .get(symbol, &canonical_interval)
            .await
            .filter(|candle| {
                start_time
                    .map(|start| candle.open_time >= start)
                    .unwrap_or(true)
                    && end_time.map(|end| candle.open_time <= end).unwrap_or(true)
            })
        {
            let latest_row = StoredKline {
                symbol: symbol.to_uppercase(),
                interval: canonical_interval.clone(),
                candle: latest,
            };
            if let Some(last) = rows.last_mut() {
                if last.candle.open_time == latest_row.candle.open_time {
                    *last = latest_row;
                } else if last.candle.open_time < latest_row.candle.open_time {
                    rows.push(latest_row);
                }
            } else {
                rows.push(latest_row);
            }
        }
    }

    if closed_only {
        rows.retain(|row| row.candle.is_closed);
    }

    if rows.len() > limit as usize {
        let start = rows.len() - limit as usize;
        rows = rows.split_off(start);
    }

    keep_latest_contiguous_rows(&mut rows, interval.as_millis() as i64);

    let start_time = rows
        .first()
        .map(|row| format_timestamp_ms(row.candle.open_time));
    let end_time = rows
        .last()
        .map(|row| format_timestamp_ms(row.candle.open_time));
    let data: Vec<KlineResponse> = rows.into_iter().map(KlineResponse::from).collect();

    Ok(KlineSeries {
        interval: canonical_interval,
        start_time,
        end_time,
        count: data.len(),
        data,
    })
}

fn merge_kline_rows(
    persisted_rows: Vec<StoredKline>,
    buffered_rows: Vec<StoredKline>,
) -> Vec<StoredKline> {
    let mut rows_by_open_time = BTreeMap::new();
    for row in persisted_rows {
        rows_by_open_time.insert(row.candle.open_time, row);
    }
    for row in buffered_rows {
        rows_by_open_time.insert(row.candle.open_time, row);
    }

    rows_by_open_time.into_values().collect()
}

fn keep_latest_contiguous_rows(rows: &mut Vec<StoredKline>, interval_ms: i64) {
    let Some(last_gap_index) = rows
        .windows(2)
        .rposition(|pair| pair[1].candle.open_time - pair[0].candle.open_time != interval_ms)
    else {
        return;
    };

    rows.drain(..=last_gap_index);
}
