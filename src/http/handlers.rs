use crate::storage::sqlite::{Alert, AlertEvent};
use crate::{
    domain::{candle::Candle, interval::Interval},
    http::routes::AppState,
    indicators::guaili::{compute_guaili, GuailiConfig, GuailiPoint, MaType},
    runtime_health::WebSocketHealth,
    storage::sqlite::StoredKline,
    time_format::format_timestamp_ms,
};
use axum::http::StatusCode;
use axum::{
    extract::{Path, Query, State},
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertRequest {
    pub symbol: String,
    pub interval: String,
    pub price: f64,
    pub direction: String,
    pub expires_at: Option<i64>,
    pub webhook_url: String,
    #[serde(alias = "message")]
    pub message_template: String,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertPatch {
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub price: Option<f64>,
    pub direction: Option<String>,
    pub expires_at: Option<Option<i64>>,
    pub webhook_url: Option<String>,
    #[serde(alias = "message")]
    pub message_template: Option<String>,
    pub status: Option<String>,
}

fn validate_alert(
    state: &AppState,
    symbol: &str,
    interval: &str,
    price: f64,
    direction: &str,
    url: &str,
    message: &str,
) -> Result<(String, String), (StatusCode, String)> {
    let symbol = symbol.trim().to_uppercase();
    let parsed = Interval::parse(interval).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let interval = parsed.canonical();
    if !state
        .health_targets
        .iter()
        .any(|t| t.symbol == symbol && t.interval == interval)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "symbol and interval are not subscribed".to_string(),
        ));
    }
    if !price.is_finite() || price <= 0.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "price must be a positive finite number".to_string(),
        ));
    }
    if !matches!(direction, "cross_up" | "cross_down" | "cross_any") {
        return Err((
            StatusCode::BAD_REQUEST,
            "direction must be cross_up, cross_down, or cross_any".to_string(),
        ));
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err((
            StatusCode::BAD_REQUEST,
            "webhookUrl must be an http(s) URL".to_string(),
        ));
    }
    serde_json::from_str::<serde_json::Value>(message).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("messageTemplate must be valid JSON: {e}"),
        )
    })?;
    Ok((symbol, interval))
}

pub async fn alerts(
    State(state): State<AppState>,
) -> Result<Json<Vec<Alert>>, (StatusCode, String)> {
    state
        .store
        .list_alerts()
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_alert(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Alert>, (StatusCode, String)> {
    state
        .store
        .get_alert(id)
        .await
        .map_err(internal_error)?
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "alert not found".to_string()))
}

pub async fn create_alert(
    State(state): State<AppState>,
    Json(request): Json<AlertRequest>,
) -> Result<(StatusCode, Json<Alert>), (StatusCode, String)> {
    let (symbol, interval) = validate_alert(
        &state,
        &request.symbol,
        &request.interval,
        request.price,
        &request.direction,
        &request.webhook_url,
        &request.message_template,
    )?;
    if state
        .store
        .list_alerts()
        .await
        .map_err(internal_error)?
        .iter()
        .any(|item| {
            item.symbol == symbol
                && item.interval == interval
                && (item.price - request.price).abs() < f64::EPSILON
        })
    {
        return Err((
            StatusCode::CONFLICT,
            "an alert already exists at this price line".to_string(),
        ));
    }
    let now = chrono::Utc::now().timestamp_millis();
    let status = request.status.unwrap_or_else(|| "active".to_string());
    if !matches!(status.as_str(), "active" | "disabled") {
        return Err((
            StatusCode::BAD_REQUEST,
            "status must be active or disabled".to_string(),
        ));
    }
    let alert = Alert {
        id: 0,
        symbol,
        interval,
        price: request.price,
        direction: request.direction,
        status,
        expires_at: request.expires_at,
        webhook_url: request.webhook_url,
        message_template: request.message_template,
        created_at: now,
        updated_at: now,
        triggered_at: None,
        delivery_status: None,
        delivery_error: None,
    };
    state
        .store
        .insert_alert(&alert)
        .await
        .map(|created| (StatusCode::CREATED, Json(created)))
        .map_err(internal_error)
}

pub async fn update_alert(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(patch): Json<AlertPatch>,
) -> Result<Json<Alert>, (StatusCode, String)> {
    let mut alert = state
        .store
        .get_alert(id)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::NOT_FOUND, "alert not found".to_string()))?;
    let symbol = patch.symbol.as_deref().unwrap_or(&alert.symbol);
    let interval = patch.interval.as_deref().unwrap_or(&alert.interval);
    let price = patch.price.unwrap_or(alert.price);
    let direction = patch.direction.as_deref().unwrap_or(&alert.direction);
    let url = patch.webhook_url.as_deref().unwrap_or(&alert.webhook_url);
    let message = patch
        .message_template
        .as_deref()
        .unwrap_or(&alert.message_template);
    let (symbol, interval) =
        validate_alert(&state, symbol, interval, price, direction, url, message)?;
    if state
        .store
        .list_alerts()
        .await
        .map_err(internal_error)?
        .iter()
        .any(|item| {
            item.id != id
                && item.symbol == symbol
                && item.interval == interval
                && (item.price - price).abs() < f64::EPSILON
        })
    {
        return Err((
            StatusCode::CONFLICT,
            "an alert already exists at this price line".to_string(),
        ));
    }
    alert.symbol = symbol;
    alert.interval = interval;
    alert.price = price;
    alert.direction = direction.to_string();
    alert.webhook_url = url.to_string();
    alert.message_template = message.to_string();
    if let Some(status) = patch.status {
        if !matches!(status.as_str(), "active" | "disabled") {
            return Err((
                StatusCode::BAD_REQUEST,
                "status must be active or disabled".to_string(),
            ));
        }
        alert.status = status;
        if alert.status == "active" {
            alert.triggered_at = None;
            alert.delivery_status = None;
            alert.delivery_error = None;
        }
    }
    if let Some(expires_at) = patch.expires_at {
        alert.expires_at = expires_at;
    }
    alert.updated_at = chrono::Utc::now().timestamp_millis();
    if !state
        .store
        .update_alert(&alert)
        .await
        .map_err(internal_error)?
    {
        return Err((StatusCode::NOT_FOUND, "alert not found".to_string()));
    }
    Ok(Json(alert))
}

pub async fn alert_events(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<AlertEvent>>, (StatusCode, String)> {
    if state
        .store
        .get_alert(id)
        .await
        .map_err(internal_error)?
        .is_none()
    {
        return Err((StatusCode::NOT_FOUND, "alert not found".to_string()));
    }
    state
        .store
        .list_alert_events(id)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn delete_alert(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.store.delete_alert(id).await.map_err(internal_error)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "alert not found".to_string()))
    }
}

fn internal_error(error: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
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
    let rows = query_kline_rows(
        state,
        symbol,
        interval,
        start_time,
        end_time,
        limit,
        closed_only,
    )
    .await?;

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

async fn query_kline_rows(
    state: &AppState,
    symbol: &str,
    interval: Interval,
    start_time: Option<i64>,
    end_time: Option<i64>,
    limit: u32,
    closed_only: bool,
) -> Result<Vec<StoredKline>, (axum::http::StatusCode, String)> {
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

    Ok(rows)
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuailiQuery {
    pub symbols: Option<String>,
    pub intervals: Option<String>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<u32>,
    pub calc_limit: Option<u32>,
    pub closed_only: Option<bool>,
    pub ma_length: Option<usize>,
    pub ma_type: Option<String>,
    pub atr_len: Option<usize>,
    pub atr_percent_len: Option<usize>,
    pub max_atr_rank: Option<f64>,
    pub slope_mul: Option<f64>,
    pub use_slope: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GuailiEnvelope {
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
    pub limit: u32,
    pub calc_limit: u32,
    pub closed_only: bool,
    pub config: ApiGuailiConfig,
    pub timezone: &'static str,
    pub server_time: i64,
    pub results: Vec<GuailiSymbolResult>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGuailiConfig {
    pub ma_length: usize,
    pub ma_type: &'static str,
    pub atr_len: usize,
    pub atr_percent_len: usize,
    pub max_atr_rank: f64,
    pub slope_mul: f64,
    pub use_slope: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GuailiSymbolResult {
    pub symbol: String,
    pub series: Vec<GuailiSeries>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GuailiSeries {
    pub interval: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub count: usize,
    pub latest: Option<ApiGuailiPoint>,
    pub data: Vec<ApiGuailiPoint>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGuailiPoint {
    pub open_time: String,
    pub close_time: String,
    pub ma: f64,
    pub atr14: f64,
    pub atr_rank: Option<f64>,
    pub rank_filter: bool,
    pub guaili: f64,
    pub value: i32,
    pub long_trend: bool,
    pub short_trend: bool,
    pub is_closed: bool,
}

impl From<GuailiPoint> for ApiGuailiPoint {
    fn from(point: GuailiPoint) -> Self {
        Self {
            open_time: format_timestamp_ms(point.open_time),
            close_time: format_timestamp_ms(point.close_time),
            ma: point.ma,
            atr14: point.atr14,
            atr_rank: point.atr_rank,
            rank_filter: point.rank_filter,
            guaili: point.guaili,
            value: point.value,
            long_trend: point.long_trend,
            short_trend: point.short_trend,
            is_closed: point.is_closed,
        }
    }
}

pub async fn guaili(
    State(state): State<AppState>,
    Query(query): Query<GuailiQuery>,
) -> Result<Json<GuailiEnvelope>, (axum::http::StatusCode, String)> {
    let symbols = parse_symbols(query.symbols.as_deref())?;
    let intervals = parse_intervals(query.intervals.as_deref())?;
    let canonical_intervals = intervals
        .iter()
        .map(Interval::canonical)
        .collect::<Vec<_>>();
    let limit = query.limit.unwrap_or(200);
    let calc_limit = guaili_calc_limit(&query, limit);
    let closed_only = query.closed_only.unwrap_or(false);
    let config = guaili_config_from_query(&query)?;
    let mut results = Vec::with_capacity(symbols.len());

    for symbol in &symbols {
        let mut series = Vec::with_capacity(intervals.len());
        for interval in &intervals {
            let canonical_interval = interval.canonical();
            let rows = query_kline_rows(
                &state,
                symbol,
                *interval,
                query.start_time,
                query.end_time,
                calc_limit,
                closed_only,
            )
            .await?;
            let candles = rows
                .iter()
                .map(|row| row.candle.clone())
                .collect::<Vec<_>>();
            let response_points = compute_guaili(&candles, config)
                .into_iter()
                .rev()
                .take(limit as usize)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>();
            let start_time = response_points
                .first()
                .map(|point| format_timestamp_ms(point.open_time));
            let end_time = response_points
                .last()
                .map(|point| format_timestamp_ms(point.open_time));
            let data = response_points
                .into_iter()
                .map(ApiGuailiPoint::from)
                .collect::<Vec<_>>();
            let latest = data.last().cloned();

            series.push(GuailiSeries {
                interval: canonical_interval,
                start_time,
                end_time,
                count: data.len(),
                latest,
                data,
            });
        }

        results.push(GuailiSymbolResult {
            symbol: symbol.clone(),
            series,
        });
    }

    Ok(Json(GuailiEnvelope {
        symbols,
        intervals: canonical_intervals,
        limit,
        calc_limit,
        closed_only,
        config: ApiGuailiConfig::from(config),
        timezone: "Asia/Shanghai",
        server_time: Local::now().timestamp_millis(),
        results,
    }))
}

fn parse_symbols(input: Option<&str>) -> Result<Vec<String>, (axum::http::StatusCode, String)> {
    let input = input
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "missing symbols".to_string(),
            )
        })?;

    input
        .split(',')
        .map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    "empty symbol in symbols".to_string(),
                ));
            }
            Ok(item.to_uppercase())
        })
        .collect()
}

fn guaili_calc_limit(query: &GuailiQuery, response_limit: u32) -> u32 {
    const DEFAULT_GUAILI_CALC_LIMIT: u32 = 500;
    query
        .calc_limit
        .unwrap_or(DEFAULT_GUAILI_CALC_LIMIT)
        .max(response_limit)
        .max(query.ma_length.unwrap_or(GuailiConfig::default().ma_length) as u32)
        .max(
            query
                .atr_percent_len
                .unwrap_or(GuailiConfig::default().atr_percent_len) as u32,
        )
        .max(15)
}

impl From<GuailiConfig> for ApiGuailiConfig {
    fn from(config: GuailiConfig) -> Self {
        Self {
            ma_length: config.ma_length,
            ma_type: ma_type_name(config.ma_type),
            atr_len: config.atr_len,
            atr_percent_len: config.atr_percent_len,
            max_atr_rank: config.max_atr_rank,
            slope_mul: config.slope_mul,
            use_slope: config.use_slope,
        }
    }
}

fn guaili_config_from_query(
    query: &GuailiQuery,
) -> Result<GuailiConfig, (axum::http::StatusCode, String)> {
    let default = GuailiConfig::default();
    Ok(GuailiConfig {
        ma_length: query.ma_length.unwrap_or(default.ma_length).max(1),
        ma_type: parse_ma_type(query.ma_type.as_deref())?,
        atr_len: query.atr_len.unwrap_or(default.atr_len).max(1),
        atr_percent_len: query
            .atr_percent_len
            .unwrap_or(default.atr_percent_len)
            .max(2),
        max_atr_rank: query.max_atr_rank.unwrap_or(default.max_atr_rank),
        slope_mul: query.slope_mul.unwrap_or(default.slope_mul),
        use_slope: query.use_slope.unwrap_or(default.use_slope),
    })
}

fn parse_ma_type(input: Option<&str>) -> Result<MaType, (axum::http::StatusCode, String)> {
    let value = input.unwrap_or("EMA").trim().to_ascii_uppercase();
    match value.as_str() {
        "SMA" => Ok(MaType::Sma),
        "EMA" => Ok(MaType::Ema),
        "SMMA" | "SMMA (RMA)" | "RMA" => Ok(MaType::Smma),
        "WMA" => Ok(MaType::Wma),
        "VWMA" => Ok(MaType::Vwma),
        _ => Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("unsupported maType: {}", input.unwrap_or_default()),
        )),
    }
}

fn ma_type_name(ma_type: MaType) -> &'static str {
    match ma_type {
        MaType::Sma => "SMA",
        MaType::Ema => "EMA",
        MaType::Smma => "SMMA (RMA)",
        MaType::Wma => "WMA",
        MaType::Vwma => "VWMA",
    }
}
