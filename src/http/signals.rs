use crate::signals::service::{now_ms, ReloadResponse, SignalEnvelope, SignalService};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignalQuery {
    symbols: Option<String>,
}

pub(super) fn routes(service: SignalService) -> Router {
    Router::new()
        .route("/api/signals", get(query_signals))
        .route("/api/signals/reload", post(reload_signals))
        .with_state(service)
}

async fn query_signals(
    State(service): State<SignalService>,
    Query(query): Query<SignalQuery>,
) -> Result<Json<SignalEnvelope>, (StatusCode, String)> {
    let mut snapshot = service.snapshot_at(now_ms()).await;
    if let Some(input) = query.symbols {
        let mut symbols = Vec::new();
        let mut seen = HashSet::new();
        for symbol in input.split(',') {
            let symbol = symbol.trim().to_ascii_uppercase();
            if symbol.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "symbols must contain nonempty configured symbols".into(),
                ));
            }
            if seen.insert(symbol.clone()) {
                symbols.push(symbol);
            }
        }
        let configured = service.configured_symbols().await;
        if symbols.iter().any(|symbol| !configured.contains(symbol)) {
            return Err((
                StatusCode::BAD_REQUEST,
                "requested symbols are not configured for signal calculation".into(),
            ));
        }
        snapshot.results.sort_by_key(|value| {
            symbols
                .iter()
                .position(|symbol| symbol == &value.symbol)
                .unwrap_or(usize::MAX)
        });
        snapshot
            .results
            .retain(|value| seen.contains(&value.symbol));
        // The response's data status describes the requested symbols, rather
        // than an unrelated warming-up symbol excluded by the query.
        if !snapshot.results.is_empty()
            && matches!(
                snapshot.status.as_str(),
                "ready" | "degraded" | "warming_up"
            )
        {
            snapshot.status = if snapshot
                .results
                .iter()
                .all(|value| value.data_status == "ready")
            {
                "ready"
            } else if snapshot
                .results
                .iter()
                .all(|value| matches!(value.data_status.as_str(), "warming_up" | "recovering"))
            {
                "warming_up"
            } else {
                "degraded"
            }
            .into();
        }
    }
    Ok(Json(snapshot))
}

async fn reload_signals(
    State(service): State<SignalService>,
) -> Result<Json<ReloadResponse>, (StatusCode, String)> {
    service
        .reload()
        .await
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))
}
