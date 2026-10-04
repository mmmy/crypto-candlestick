//! An additive chart API: one owned input vector supplies OHLC and every indicator.
use super::{handlers::ApiGuailiConfig, routes::AppState};
#[cfg(test)]
#[path = "chart_tests.rs"]
mod tests;
use crate::{
    domain::{candle::Candle, interval::Interval},
    indicators::{
        chart::{compute_android_chart, Amplitude, AndroidChannel},
        guaili::{compute_guaili, GuailiConfig},
    },
    memory::LiveSymbolSnapshot,
    storage::sqlite::StoredKline,
};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};

type ApiError = (StatusCode, String);
static RUN_ID: OnceLock<String> = OnceLock::new();
static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(1);
fn run_id() -> &'static str {
    RUN_ID.get_or_init(|| {
        format!(
            "{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        )
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChartQuery {
    pub symbol: String,
    pub interval: String,
    pub limit: Option<u32>,
    pub calc_limit: Option<u32>,
    pub closed_only: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartCandle {
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
    pub is_closed: bool,
}
impl From<Candle> for ChartCandle {
    fn from(c: Candle) -> Self {
        Self {
            open_time_ms: c.open_time,
            close_time_ms: c.close_time,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume,
            quote_volume: c.quote_volume,
            trade_count: c.trade_count,
            is_closed: c.is_closed,
        }
    }
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatrixIndicators {
    pub ma: f64,
    pub atr14: f64,
    pub previous_atr14: f64,
    pub raw_guaili: f64,
    pub value: i32,
    pub atr_rank: Option<f64>,
    pub rank_filter: bool,
    pub current_long_trend: bool,
    pub current_short_trend: bool,
    pub previous_long_trend: Option<bool>,
    pub previous_short_trend: Option<bool>,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartBar {
    #[serde(flatten)]
    pub candle: ChartCandle,
    pub android_channel: AndroidChannel,
    pub amplitude: Amplitude,
    pub matrix: MatrixIndicators,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartSource {
    pub run_id: &'static str,
    pub sequence: u64,
    pub generation: u64,
    pub market_event_time: i64,
    pub received_at: i64,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorContracts {
    pub android_chart: &'static str,
    pub matrix: &'static str,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartSnapshot {
    pub symbol: String,
    pub interval: String,
    pub candle_mode: &'static str,
    pub snapshot_id: String,
    pub server_time: i64,
    pub captured_at: i64,
    pub source: Option<ChartSource>,
    pub indicator_contracts: IndicatorContracts,
    pub matrix_config: ApiGuailiConfig,
    pub calc_limit: u32,
    pub actual_calc_bars: usize,
    pub actual_calc_start_time_ms: Option<i64>,
    pub indicator_coverage_start_time_ms: Option<i64>,
    pub indicator_coverage_end_time_ms: Option<i64>,
    pub data_quality: &'static str,
    pub reasons: Vec<String>,
    pub next_before: Option<i64>,
    pub bars: Vec<ChartBar>,
}

pub async fn chart(
    State(state): State<AppState>,
    Query(query): Query<ChartQuery>,
) -> Result<Json<ChartSnapshot>, ApiError> {
    let symbol = query.symbol.trim().to_ascii_uppercase();
    let interval =
        Interval::parse(&query.interval).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let canonical = interval.canonical();
    if !state
        .health_targets
        .iter()
        .any(|t| t.symbol == symbol && t.interval == canonical)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "requested symbol/interval is not configured".into(),
        ));
    }
    let limit = query.limit.unwrap_or(300);
    let calc_limit = query.calc_limit.unwrap_or(500).max(limit).max(20);
    if limit == 0 || limit > 2000 || calc_limit > 2000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "limit and effective calcLimit must be between 1 and 2000".into(),
        ));
    }
    let closed_only = query.closed_only.unwrap_or(false);
    let (candles, live, captured_at, reasons) =
        capture(&state, &symbol, interval, calc_limit, closed_only).await?;
    let result = build_snapshot(
        symbol,
        canonical,
        candles,
        live,
        captured_at,
        calc_limit,
        limit,
        closed_only,
        reasons,
    )?;
    Ok(Json(result))
}

/// Copy memory before DB IO. Buffered rows retained before a flush are not lost when DB commits later.
/// A recovery/bucket change causes a bounded retry, not a mix of two live generations.
async fn capture(
    state: &AppState,
    symbol: &str,
    interval: Interval,
    limit: u32,
    closed_only: bool,
) -> Result<(Vec<Candle>, Option<LiveSymbolSnapshot>, i64, Vec<String>), ApiError> {
    let canonical = interval.canonical();
    for _ in 0..3 {
        let lock = state.latest.market_update_lock();
        let (live, current, memory, captured_at) = {
            let _guard = lock.read().await;
            let live = state.latest.live_snapshot(symbol).await;
            if live.as_ref().is_some_and(|s| s.recovering) {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "recovering: market snapshot is being rebuilt".into(),
                ));
            }
            let current = match &live {
                Some(s) => s.candles.get(&canonical).cloned(),
                None => state.latest.get(symbol, &canonical).await,
            };
            let end = current.as_ref().and_then(|c| c.open_time.checked_sub(1));
            let memory = if interval.as_millis() < 60_000 {
                state
                    .memory_series
                    .query(symbol, &canonical, None, end, limit)
                    .await
            } else {
                state
                    .closed_buffer
                    .query(symbol, &canonical, None, end, limit)
                    .await
            };
            (live, current, memory, Utc::now().timestamp_millis())
        };
        let end = current.as_ref().and_then(|c| c.open_time.checked_sub(1));
        let persisted = if interval.as_millis() < 60_000 {
            Vec::new()
        } else {
            state
                .store
                .query_klines(symbol, &canonical, None, end, limit)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        };
        {
            let _guard = lock.read().await;
            let after = state.latest.live_snapshot(symbol).await;
            let after_current = match &after {
                Some(s) => s.candles.get(&canonical).cloned(),
                None => state.latest.get(symbol, &canonical).await,
            };
            let before_generation = live.as_ref().map(|s| (s.generation, s.recovering));
            let after_generation = after.as_ref().map(|s| (s.generation, s.recovering));
            if before_generation != after_generation
                || current.as_ref().map(|c| c.open_time)
                    != after_current.as_ref().map(|c| c.open_time)
            {
                continue;
            }
        }
        let mut rows = BTreeMap::<i64, Candle>::new();
        for StoredKline { candle, .. } in persisted.into_iter().chain(memory) {
            if candle.is_closed {
                rows.insert(candle.open_time, candle);
            }
        }
        let missing_dynamic = !closed_only && current.is_none();
        if !closed_only || current.as_ref().is_some_and(|c| c.is_closed) {
            if let Some(candle) = current {
                rows.insert(candle.open_time, candle);
            }
        }
        let mut candles: Vec<_> = rows.into_values().rev().take(limit as usize).collect();
        candles.reverse();
        let mut reasons = Vec::new();
        if missing_dynamic {
            reasons.push("current dynamic candle is missing; showing closed history".into());
        }
        let invalid = candles.iter().rposition(|c| !valid_candle(c));
        if let Some(at) = invalid {
            candles.drain(..=at);
            reasons.push("invalid OHLC: only the valid latest suffix is calculable".into());
        }
        if let Some(at) = candles
            .windows(2)
            .rposition(|p| p[1].open_time - p[0].open_time != interval.as_millis() as i64)
        {
            candles.drain(..=at);
            reasons.push("history gap: indicator calculation starts after the gap".into());
        }
        return Ok((candles, live, captured_at, reasons));
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        "recovering: snapshot bucket/generation changed; retry request".into(),
    ))
}

fn valid_candle(c: &Candle) -> bool {
    [c.open, c.high, c.low, c.close, c.volume, c.quote_volume]
        .iter()
        .all(|v| v.is_finite())
        && c.high >= c.open.max(c.close)
        && c.low <= c.open.min(c.close)
        && c.high >= c.low
        && c.volume >= 0.0
        && c.quote_volume >= 0.0
        && c.close_time >= c.open_time
}

#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    symbol: String,
    interval: String,
    candles: Vec<Candle>,
    live: Option<LiveSymbolSnapshot>,
    captured_at: i64,
    calc_limit: u32,
    limit: u32,
    closed_only: bool,
    mut reasons: Vec<String>,
) -> Result<ChartSnapshot, ApiError> {
    let config = GuailiConfig::default();
    let matrix = compute_guaili(&candles, config);
    let android = compute_android_chart(&candles);
    if matrix.iter().any(|m| {
        ![m.ma, m.atr14, m.guaili].iter().all(|v| v.is_finite())
            || m.atr_rank.is_some_and(|v| !v.is_finite())
    }) || android.iter().any(|(c, a)| {
        !c.ema20.is_finite()
            || !a.normalized_range.is_finite()
            || [
                c.atr14,
                c.upper,
                c.lower,
                c.close_deviation,
                a.threshold,
                a.edge_distance,
            ]
            .iter()
            .flatten()
            .any(|v| !v.is_finite())
    }) {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "indicator arithmetic exceeds finite range".into(),
        ));
    }
    let actual_calc_bars = candles.len();
    let actual_calc_start_time_ms = candles.first().map(|c| c.open_time);
    let start = candles.len().saturating_sub(limit as usize);
    let mut bars = Vec::with_capacity(candles.len() - start);
    for (i, candle) in candles.into_iter().enumerate().skip(start) {
        let m = &matrix[i];
        let previous = i.checked_sub(1).map(|p| &matrix[p]);
        let (channel, amplitude) = android[i].clone();
        bars.push(ChartBar {
            candle: candle.into(),
            android_channel: channel,
            amplitude,
            matrix: MatrixIndicators {
                ma: m.ma,
                atr14: m.atr14,
                previous_atr14: previous.map_or(m.atr14, |p| p.atr14),
                raw_guaili: m.guaili,
                value: m.value,
                atr_rank: m.atr_rank,
                rank_filter: m.rank_filter,
                current_long_trend: m.long_trend,
                current_short_trend: m.short_trend,
                previous_long_trend: previous.map(|p| p.long_trend),
                previous_short_trend: previous.map(|p| p.short_trend),
            },
        });
    }
    let now = Utc::now().timestamp_millis();
    let stale = live.as_ref().is_some_and(|s| {
        now.saturating_sub(s.market_event_time_ms) > 30_000
            || now.saturating_sub(s.received_at_ms) > 30_000
            || s.market_event_time_ms > now + 2000
            || s.received_at_ms > now + 2000
    });
    if stale {
        reasons.push("market input is stale or has invalid future timestamps".into());
    }
    let data_quality = if bars.is_empty() {
        "missing"
    } else if stale {
        "stale"
    } else if !reasons.is_empty() {
        "degraded"
    } else if actual_calc_bars < 20 {
        "warming_up"
    } else if live.is_none() && !closed_only {
        "unverified"
    } else {
        "ready"
    };
    Ok(ChartSnapshot {
        symbol,
        interval,
        candle_mode: if closed_only { "closed" } else { "live" },
        snapshot_id: format!(
            "{}:{}",
            run_id(),
            SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed)
        ),
        server_time: now,
        captured_at,
        source: live.map(|s| ChartSource {
            run_id: run_id(),
            sequence: s.sequence,
            generation: s.generation,
            market_event_time: s.market_event_time_ms,
            received_at: s.received_at_ms,
        }),
        indicator_contracts: IndicatorContracts {
            android_chart: "android-chart-v1",
            matrix: "matrix-v1",
        },
        matrix_config: config.into(),
        calc_limit,
        actual_calc_bars,
        actual_calc_start_time_ms,
        indicator_coverage_start_time_ms: bars.first().map(|b| b.candle.open_time_ms),
        indicator_coverage_end_time_ms: bars.last().map(|b| b.candle.open_time_ms),
        data_quality,
        reasons,
        next_before: bars
            .first()
            .and_then(|b| b.candle.open_time_ms.checked_sub(1)),
        bars,
    })
}
