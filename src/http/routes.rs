use crate::{
    memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
    runtime_health::RuntimeHealth,
    storage::sqlite::SqliteStore,
};
use axum::{routing::get, Router};

use super::handlers::{
    alert_events, alerts, create_alert, deep_health, delete_alert, get_alert, guaili, health,
    health_summary, klines, update_alert,
};

#[derive(Clone)]
pub struct AppState {
    pub store: SqliteStore,
    pub latest: LatestCache,
    pub memory_series: MemorySeriesStore,
    pub closed_buffer: ClosedKlineBuffer,
    pub health_targets: Vec<HealthTarget>,
    pub runtime_health: RuntimeHealth,
}

#[derive(Debug, Clone)]
pub struct HealthTarget {
    pub symbol: String,
    pub interval: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/summary", get(health_summary))
        .route("/health/deep", get(deep_health))
        .route("/api/health", get(health))
        .route("/api/health/summary", get(health_summary))
        .route("/api/health/deep", get(deep_health))
        .route("/api/klines", get(klines))
        .route("/api/indicators/guaili", get(guaili))
        .route("/api/alerts", get(alerts).post(create_alert))
        .route(
            "/api/alerts/:id",
            get(get_alert).patch(update_alert).delete(delete_alert),
        )
        .route("/api/alerts/:id/events", get(alert_events))
        .with_state(state)
}
