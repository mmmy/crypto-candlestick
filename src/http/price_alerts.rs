use super::routes::AppState;
use crate::price_alerts::{CreateRequest, Error, PatchRequest, PriceAlert};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::Value;

fn error(error: Error) -> (StatusCode, String) {
    let status = match &error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::MetadataUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string())
}
fn subscribed(state: &AppState, symbol: &str, interval: &str) -> Result<(), (StatusCode, String)> {
    let interval = crate::domain::interval::Interval::parse(interval)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .canonical();
    if !state
        .health_targets
        .iter()
        .any(|t| t.symbol == symbol.trim().to_uppercase() && t.interval == interval)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "symbol and interval are not subscribed".into(),
        ));
    }
    Ok(())
}
#[derive(Default, Deserialize)]
pub struct Filter {
    symbol: Option<String>,
    interval: Option<String>,
}
pub async fn list(
    State(state): State<AppState>,
    Query(filter): Query<Filter>,
) -> Result<Json<Vec<PriceAlert>>, (StatusCode, String)> {
    let mut alerts = state.store.list_price_alerts().await.map_err(error)?;
    let interval = filter
        .interval
        .as_deref()
        .map(crate::domain::interval::Interval::parse)
        .transpose()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .map(|i| i.canonical());
    alerts.retain(|a| {
        filter
            .symbol
            .as_ref()
            .is_none_or(|s| a.symbol == s.trim().to_uppercase())
            && interval.as_ref().is_none_or(|i| a.interval == *i)
    });
    Ok(Json(alerts))
}
pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<PriceAlert>, (StatusCode, String)> {
    state
        .store
        .get_price_alert(id)
        .await
        .map(Json)
        .map_err(error)
}
pub async fn create(
    State(state): State<AppState>,
    Json(value): Json<Value>,
) -> Result<(StatusCode, Json<PriceAlert>), (StatusCode, String)> {
    let request: CreateRequest = serde_json::from_value(value.clone())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    subscribed(&state, &request.symbol, &request.interval)?;
    state
        .store
        .create_price_alert(request, &value.to_string())
        .await
        .map(|a| (StatusCode::CREATED, Json(a)))
        .map_err(error)
}
pub async fn patch(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(value): Json<Value>,
) -> Result<Json<PriceAlert>, (StatusCode, String)> {
    let request: PatchRequest = serde_json::from_value(value.clone())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Check recovery before reading the current drawing (which may have been deleted).
    if request.symbol.is_some() || request.interval.is_some() {
        let old = state.store.get_price_alert(id).await.map_err(error)?;
        subscribed(
            &state,
            request.symbol.as_deref().unwrap_or(&old.symbol),
            request.interval.as_deref().unwrap_or(&old.interval),
        )?;
    }
    state
        .store
        .patch_price_alert(id, request, &value.to_string())
        .await
        .map(Json)
        .map_err(error)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delete {
    mutation_id: String,
    expected_revision: i64,
}
pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(request): Query<Delete>,
) -> Result<Json<Value>, (StatusCode, String)> {
    state
        .store
        .delete_price_alert(id, request.expected_revision, &request.mutation_id)
        .await
        .map(Json)
        .map_err(error)
}
pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<crate::price_alerts::PriceAlertEvent>>, (StatusCode, String)> {
    state
        .store
        .price_alert_events_page(id, query.limit.unwrap_or(1000), query.before_arm_generation)
        .await
        .map(Json)
        .map_err(error)
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventQuery {
    limit: Option<u32>,
    before_arm_generation: Option<i64>,
}
pub async fn mutation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    state
        .store
        .price_alert_mutation(&id)
        .await
        .map(Json)
        .map_err(error)
}

#[derive(Deserialize)]
pub struct MarketQuery {
    symbol: String,
}
pub async fn market(
    State(state): State<AppState>,
    Query(query): Query<MarketQuery>,
) -> Result<Json<crate::price_alert_metadata::MarketMetadata>, (StatusCode, String)> {
    if !state
        .health_targets
        .iter()
        .any(|target| target.symbol == query.symbol.trim().to_uppercase())
    {
        return Err((StatusCode::BAD_REQUEST, "symbol is not subscribed".into()));
    }
    state
        .store
        .price_alert_market(&query.symbol)
        .await
        .map(Json)
        .map_err(error)
}
pub async fn refresh_market(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    state.store.refresh_price_alert_metadata().await;
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"refreshQueued":true})),
    )
}
