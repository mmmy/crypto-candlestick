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
            (
                "sqlite",
                rows.into_iter().map(|row| row.candle).collect::<Vec<_>>(),
            )
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
    Ok(Json(DeepHealthResponse {
        ok,
        websocket,
        series,
    }))
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
    pub interval: String,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<u32>,
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

pub async fn klines(
    State(state): State<AppState>,
    Query(query): Query<KlineQuery>,
) -> Result<Json<Vec<KlineResponse>>, (axum::http::StatusCode, String)> {
    let interval = Interval::parse(&query.interval)
        .map_err(|err| (axum::http::StatusCode::BAD_REQUEST, err.to_string()))?;
    let canonical_interval = interval.canonical();
    let mut rows = if interval.as_millis() < 60_000 {
        state
            .memory_series
            .query(
                &query.symbol,
                &canonical_interval,
                query.start_time,
                query.end_time,
                query.limit.unwrap_or(1000),
            )
            .await
    } else {
        state
            .store
            .query_klines(
                &query.symbol,
                &canonical_interval,
                query.start_time,
                query.end_time,
                query.limit.unwrap_or(1000),
            )
            .await
            .map_err(|err| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )
            })?
    };

    if let Some(latest) = state
        .latest
        .get(&query.symbol, &canonical_interval)
        .await
        .filter(|candle| {
            query
                .start_time
                .map(|start| candle.open_time >= start)
                .unwrap_or(true)
                && query
                    .end_time
                    .map(|end| candle.open_time <= end)
                    .unwrap_or(true)
        })
    {
        let latest_row = StoredKline {
            symbol: query.symbol.to_uppercase(),
            interval: canonical_interval,
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

    if let Some(limit) = query.limit {
        if rows.len() > limit as usize {
            let start = rows.len() - limit as usize;
            rows = rows.split_off(start);
        }
    }

    truncate_at_first_gap(&mut rows, interval.as_millis() as i64);

    Ok(Json(rows.into_iter().map(KlineResponse::from).collect()))
}

fn truncate_at_first_gap(rows: &mut Vec<StoredKline>, interval_ms: i64) {
    let Some(gap_index) = rows
        .windows(2)
        .position(|pair| pair[1].candle.open_time - pair[0].candle.open_time != interval_ms)
    else {
        return;
    };

    rows.truncate(gap_index + 1);
}
