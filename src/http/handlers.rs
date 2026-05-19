use crate::{domain::interval::Interval, http::routes::AppState, storage::sqlite::StoredKline};
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
