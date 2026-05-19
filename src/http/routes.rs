use crate::{
    memory::{LatestCache, MemorySeriesStore},
    storage::sqlite::SqliteStore,
};
use axum::{routing::get, Router};

use super::handlers::{health, klines};

#[derive(Clone)]
pub struct AppState {
    pub store: SqliteStore,
    pub latest: LatestCache,
    pub memory_series: MemorySeriesStore,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/klines", get(klines))
        .with_state(state)
}
