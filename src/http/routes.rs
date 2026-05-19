use crate::{
    memory::{LatestCache, MemorySeriesStore},
    storage::sqlite::SqliteStore,
};
use axum::{routing::get, Router};

use super::handlers::{deep_health, health, klines};

#[derive(Clone)]
pub struct AppState {
    pub store: SqliteStore,
    pub latest: LatestCache,
    pub memory_series: MemorySeriesStore,
    pub health_targets: Vec<HealthTarget>,
}

#[derive(Debug, Clone)]
pub struct HealthTarget {
    pub symbol: String,
    pub interval: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/health/deep", get(deep_health))
        .route("/api/klines", get(klines))
        .with_state(state)
}
