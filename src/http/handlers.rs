use crate::{
    domain::{candle::Candle, interval::Interval},
    http::routes::AppState,
    storage::sqlite::StoredKline,
};
use axum::{
    extract::{Query, State},
    Json,
};
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
    pub series: Vec<SeriesHealth>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesHealth {
    pub symbol: String,
    pub interval: String,
    pub latest_open_time: Option<i64>,
    pub consecutive_bars_from_latest: u32,
    pub checked_bars: usize,
    pub source: &'static str,
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

        let latest_open_time = candles_desc.first().map(|candle| candle.open_time);
        let consecutive_bars_from_latest = consecutive_bars_from_latest(&candles_desc, interval_ms);

        series.push(SeriesHealth {
            symbol: target.symbol.to_uppercase(),
            interval: canonical_interval,
            latest_open_time,
            consecutive_bars_from_latest,
            checked_bars: candles_desc.len(),
            source,
        });
    }

    let ok = series
        .iter()
        .all(|item| item.consecutive_bars_from_latest > 0);
    Ok(Json(DeepHealthResponse { ok, series }))
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineQuery {
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<u32>,
}

pub async fn klines(
    State(state): State<AppState>,
    Query(query): Query<KlineQuery>,
) -> Result<Json<Vec<StoredKline>>, (axum::http::StatusCode, String)> {
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

    Ok(Json(rows))
}
