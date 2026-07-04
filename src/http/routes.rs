use crate::{
    memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
    runtime_health::RuntimeHealth,
    storage::sqlite::SqliteStore,
};
use axum::{routing::get, Router};

use super::handlers::{deep_health, health, health_summary, klines};

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
        .with_state(state)
}
