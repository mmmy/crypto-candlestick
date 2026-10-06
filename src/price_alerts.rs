//! Drawing-bound, once-only price alerts. The market hot path reads an indexed
//! in-memory ruleset; SQLite is touched only on mutations and actual triggers.
use crate::{
    domain::interval::Interval,
    price_alert_metadata::{MarketMetadata, TickSize, MAX_TICK_ORDINAL},
    storage::sqlite::{Alert, SqliteStore},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Point {
    pub time_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Geometry {
    pub kind: String,
    pub first: Point,
    pub second: Point,
    pub extend: String,
}

impl Geometry {
    fn contains_time(&self, time_ms: i64) -> bool {
        (self.extend == "both" || time_ms >= self.first.time_ms)
            && (self.extend != "none" || time_ms <= self.second.time_ms)
    }
    pub fn line_price(&self, time_ms: i64) -> Option<f64> {
        let start = self.first.time_ms.min(self.second.time_ms);
        let end = self.first.time_ms.max(self.second.time_ms);
        if (self.extend == "none" && !(start..=end).contains(&time_ms))
            || (self.extend == "right" && time_ms < start)
        {
            return None;
        }
        let price = if self.kind == "horizontal_segment" {
            self.first.price
        } else {
            let fraction = (time_ms as f64 - self.first.time_ms as f64)
                / (self.second.time_ms as f64 - self.first.time_ms as f64);
            self.first.price + (self.second.price - self.first.price) * fraction
        };
        (price.is_finite() && price > 0.0).then_some(price)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceAlert {
    pub id: i64,
    pub revision: i64,
    pub arm_generation: i64,
    pub symbol: String,
    pub interval: String,
    pub tv_symbol: String,
    pub name: String,
    pub geometry: Geometry,
    pub direction: String,
    pub frequency: String,
    pub status: String,
    pub expires_at: Option<i64>,
    pub webhook_url: String,
    pub message_template: String,
    pub label: String,
    pub color: String,
    pub line_width: f64,
    pub triggered_at: Option<i64>,
    pub delivery_status: Option<String>,
    pub delivery_error: Option<String>,
    pub data_status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceAlertEvent {
    pub id: String,
    pub alert_id: i64,
    pub arm_generation: i64,
    pub triggered_at: i64,
    pub trigger_price: f64,
    pub line_price: f64,
    pub direction: String,
    pub payload: Value,
    pub delivery_status: String,
    pub delivery_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateRequest {
    pub mutation_id: String,
    pub symbol: String,
    pub interval: String,
    pub geometry: Geometry,
    #[serde(default)]
    pub tv_symbol: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default = "cross_any")]
    pub direction: String,
    #[serde(default = "once")]
    pub frequency: String,
    #[serde(default = "disabled")]
    pub status: String,
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default)]
    pub message_template: String,
    #[serde(default)]
    pub label: String,
    #[serde(default = "green")]
    pub color: String,
    #[serde(default = "line_width")]
    pub line_width: f64,
}
fn cross_any() -> String {
    "cross_any".into()
}
fn once() -> String {
    "once".into()
}
fn disabled() -> String {
    "disabled".into()
}
fn green() -> String {
    "#22AB94".into()
}
fn line_width() -> f64 {
    2.0
}

/// Missing and explicit null must be distinct for clearing expiry.
pub fn nullable<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Option<i64>>, D::Error> {
    Option::<i64>::deserialize(d).map(Some)
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PatchRequest {
    pub mutation_id: String,
    pub expected_revision: i64,
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub tv_symbol: Option<String>,
    pub name: Option<String>,
    pub geometry: Option<Geometry>,
    pub direction: Option<String>,
    pub frequency: Option<String>,
    pub status: Option<String>,
    #[serde(default, deserialize_with = "nullable")]
    pub expires_at: Option<Option<i64>>,
    pub webhook_url: Option<String>,
    pub message_template: Option<String>,
    pub label: Option<String>,
    pub color: Option<String>,
    pub line_width: Option<f64>,
    pub rearm: Option<bool>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("alert not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    MetadataUnavailable(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
struct Rule {
    alert: PriceAlert,
    side: Option<i8>,
    last_time: Option<i64>,
    ticks: Option<(u64, u64)>,
    time_active: bool,
}
#[derive(Debug, Default)]
struct SymbolRules {
    rules: HashMap<i64, Rule>,
    // Real PRICE_FILTER tick ordinals; equal levels have separate IDs.
    horizontal: BTreeMap<u64, HashSet<i64>>,
    trends: HashSet<i64>,
    pending: HashSet<i64>,
    boundaries: BTreeMap<i64, HashSet<i64>>,
    candidates: Vec<i64>,
    last_price: Option<f64>,
    last_ticks: Option<u64>,
    tick_size: Option<TickSize>,
    metadata_status: String,
    metadata_reason: Option<String>,
    last_time: Option<i64>,
    received_at: Option<i64>,
    range_time: Option<i64>,
    legacy: HashMap<i64, (Alert, Option<i8>)>,
}
impl SymbolRules {
    fn remove(&mut self, id: i64) {
        if let Some(rule) = self.rules.remove(&id) {
            let g = &rule.alert.geometry;
            for time in [
                g.first.time_ms.min(g.second.time_ms),
                g.first.time_ms.max(g.second.time_ms).saturating_add(1),
            ]
            .into_iter()
            .chain(rule.alert.expires_at)
            {
                if let Some(ids) = self.boundaries.get_mut(&time) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.boundaries.remove(&time);
                    }
                }
            }
            let key = rule.ticks.map(|(first, _)| first).unwrap_or(0);
            if let Some(ids) = self.horizontal.get_mut(&key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.horizontal.remove(&key);
                }
            }
        }
        self.trends.remove(&id);
        self.pending.remove(&id);
    }
    fn insert(&mut self, alert: PriceAlert, preserve: Option<(Option<i8>, Option<i64>)>) {
        self.remove(alert.id);
        if alert.status != "active" {
            return;
        }
        let g = &alert.geometry;
        if g.extend != "both" {
            self.boundaries
                .entry(g.first.time_ms)
                .or_default()
                .insert(alert.id);
        }
        if g.extend == "none" {
            self.boundaries
                .entry(g.second.time_ms.saturating_add(1))
                .or_default()
                .insert(alert.id);
        }
        if let Some(expiry) = alert.expires_at {
            self.boundaries.entry(expiry).or_default().insert(alert.id);
        }
        let ticks = self.tick_size.as_ref().and_then(|tick| {
            Some((
                tick.ordinal(alert.geometry.first.price)?,
                tick.ordinal(alert.geometry.second.price)?,
            ))
        });
        let time_active = self
            .last_time
            .is_none_or(|time| g.contains_time(time) && alert.expires_at.is_none_or(|e| e > time));
        if time_active && alert.geometry.kind == "horizontal_segment" {
            if let Some((first, _)) = ticks {
                self.horizontal.entry(first).or_default().insert(alert.id);
            }
        } else if time_active {
            self.trends.insert(alert.id);
        }
        let (side, last_time) = preserve.unwrap_or((None, None));
        if side.is_none() && time_active {
            self.pending.insert(alert.id);
        }
        self.rules.insert(
            alert.id,
            Rule {
                alert,
                side,
                last_time,
                ticks,
                time_active,
            },
        );
    }
    // Dormant drawings retain their configuration, but only their scheduled
    // start/end boundaries participate in the hot path.
    fn schedule_range(&mut self, id: i64, time: i64) {
        let Some(rule) = self.rules.get_mut(&id) else {
            return;
        };
        let active = rule.alert.geometry.contains_time(time)
            && rule.alert.expires_at.is_none_or(|e| e > time);
        if active == rule.time_active {
            if !active {
                self.pending.remove(&id);
            }
            return;
        }
        rule.time_active = active;
        rule.side = None;
        rule.last_time = Some(time);
        if active {
            self.pending.insert(id);
            if rule.alert.geometry.kind == "horizontal_segment" {
                if let Some((first, _)) = rule.ticks {
                    self.horizontal.entry(first).or_default().insert(id);
                }
            } else {
                self.trends.insert(id);
            }
        } else {
            self.pending.remove(&id);
            self.trends.remove(&id);
            if let Some((first, _)) = rule.ticks {
                if let Some(ids) = self.horizontal.get_mut(&first) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.horizontal.remove(&first);
                    }
                }
            }
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct Runtime {
    symbols: HashMap<String, SymbolRules>,
    pub(crate) metadata_notify: std::sync::Arc<tokio::sync::Notify>,
}
impl Runtime {
    pub(crate) fn legacy_insert(&mut self, alert: Alert) {
        self.legacy_remove(alert.id);
        if alert.status == "active" {
            self.symbols
                .entry(alert.symbol.clone())
                .or_default()
                .legacy
                .insert(alert.id, (alert, None));
        }
    }
    pub(crate) fn legacy_remove(&mut self, id: i64) {
        for symbol in self.symbols.values_mut() {
            symbol.legacy.remove(&id);
        }
    }
    fn upsert(&mut self, alert: PriceAlert, preserve: bool) {
        let mut old = None;
        for rules in self.symbols.values_mut() {
            if preserve {
                if let Some(r) = rules.rules.get(&alert.id) {
                    old = Some((r.side, r.last_time));
                }
            }
            rules.remove(alert.id);
        }
        self.symbols
            .entry(alert.symbol.clone())
            .or_default()
            .insert(alert, old);
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS price_alerts (id INTEGER PRIMARY KEY AUTOINCREMENT,revision INTEGER NOT NULL,arm_generation INTEGER NOT NULL,status TEXT NOT NULL,data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS price_alert_events (event_id TEXT PRIMARY KEY,alert_id INTEGER NOT NULL,arm_generation INTEGER NOT NULL,triggered_at INTEGER NOT NULL,data TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS price_alert_events_alert ON price_alert_events(alert_id,triggered_at DESC);
CREATE INDEX IF NOT EXISTS price_alert_events_generation ON price_alert_events(alert_id,arm_generation DESC);
CREATE TABLE IF NOT EXISTS price_alert_mutations (mutation_id TEXT PRIMARY KEY,request TEXT NOT NULL,response TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS price_alert_outbox (event_id TEXT PRIMARY KEY,kind TEXT NOT NULL,alert_id INTEGER NOT NULL,arm_generation INTEGER NOT NULL,triggered_at INTEGER NOT NULL,webhook_url TEXT NOT NULL,payload TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'pending',error TEXT);
CREATE INDEX IF NOT EXISTS price_alert_outbox_due ON price_alert_outbox(status,next_attempt_at);
CREATE TABLE IF NOT EXISTS price_alert_identity (id INTEGER PRIMARY KEY CHECK(id=1),namespace TEXT NOT NULL);
INSERT OR IGNORE INTO price_alert_identity(id,namespace) VALUES (1,lower(hex(randomblob(16))));
"#;

pub(crate) async fn initialize(store: &SqliteStore) -> Result<(), sqlx::Error> {
    for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement).execute(&store.write_pool).await?;
    }
    let rows =
        sqlx::query_scalar::<_, String>("SELECT data FROM price_alerts WHERE status='active'")
            .fetch_all(&store.read_pool)
            .await?;
    let legacy = store.list_alerts().await?;
    let mut runtime = store.price_alert_runtime.lock().await;
    for row in rows {
        let alert: PriceAlert =
            serde_json::from_str(&row).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        runtime.upsert(alert, false);
    }
    for alert in legacy {
        runtime.legacy_insert(alert);
    }
    Ok(())
}

fn valid_mutation(id: &str) -> Result<(), Error> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_:".contains(&b))
    {
        return Err(Error::Invalid(
            "mutationId must contain 1..128 ASCII letters, digits, -, _ or :".into(),
        ));
    }
    Ok(())
}

fn normalize_geometry(tick: &TickSize, alert: &mut PriceAlert) -> Result<(), Error> {
    alert.geometry.first.price = tick.normalize(alert.geometry.first.price)?.1;
    alert.geometry.second.price = tick.normalize(alert.geometry.second.price)?.1;
    Ok(())
}

impl SqliteStore {
    pub async fn price_alert_market(&self, symbol: &str) -> Result<MarketMetadata, Error> {
        let symbol = symbol.trim().to_uppercase();
        if symbol.is_empty()
            || symbol.len() > 64
            || !symbol.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(Error::Invalid("invalid symbol".into()));
        }
        let runtime = self.price_alert_runtime.lock().await;
        let rules = runtime.symbols.get(&symbol);
        Ok(MarketMetadata {
            tv_symbol: format!("{symbol}.P"),
            symbol,
            tick_size: rules.and_then(|r| r.tick_size.as_ref().map(|s| s.wire.clone())),
            status: rules
                .map(|r| {
                    if r.metadata_status.is_empty() {
                        "loading"
                    } else {
                        &r.metadata_status
                    }
                })
                .unwrap_or("loading")
                .into(),
            reason: rules.and_then(|r| r.metadata_reason.clone()),
        })
    }
    pub async fn refresh_price_alert_metadata(&self) {
        self.price_alert_runtime
            .lock()
            .await
            .metadata_notify
            .notify_one();
    }
    #[doc(hidden)]
    pub async fn set_price_alert_metadata_fixture(
        &self,
        symbol: &str,
        tick: Option<&str>,
        reason: Option<&str>,
    ) -> Result<(), Error> {
        if !self.price_alert_fixture {
            return Err(Error::Invalid(
                "metadata injection requires an isolated fixture store".into(),
            ));
        }
        self.apply_price_alert_metadata(
            symbol,
            tick,
            if tick.is_some() {
                "ready"
            } else {
                "unavailable"
            },
            reason,
        )
        .await
    }
    pub(crate) async fn apply_price_alert_metadata(
        &self,
        symbol: &str,
        tick: Option<&str>,
        status: &str,
        reason: Option<&str>,
    ) -> Result<(), Error> {
        let tick = tick.map(TickSize::parse).transpose()?;
        let symbol = symbol.trim().to_uppercase();
        let mut runtime = self.price_alert_runtime.lock().await;
        let rules = runtime.symbols.entry(symbol).or_default();
        if rules.tick_size == tick
            && rules.metadata_status == status
            && rules.metadata_reason.as_deref() == reason
        {
            return Ok(());
        }
        let alerts = rules
            .rules
            .values()
            .map(|r| r.alert.clone())
            .collect::<Vec<_>>();
        let mut tx = self.write_pool.begin().await?;
        let mut updated = Vec::with_capacity(alerts.len());
        for mut alert in alerts {
            if let Some(tick) = &tick {
                let old = alert.geometry.clone();
                if let Err(error) = normalize_geometry(tick, &mut alert) {
                    tracing::warn!(alert_id=alert.id,"existing drawing is outside tick ordinal bounds and remains suspended: {error}");
                    alert.geometry = old;
                    updated.push(alert);
                    continue;
                }
                if alert.geometry != old {
                    alert.revision += 1;
                    alert.arm_generation += 1;
                    alert.updated_at = chrono::Utc::now().timestamp_millis();
                    alert.triggered_at = None;
                    alert.delivery_status = None;
                    alert.delivery_error = None;
                    persist(&mut tx, &alert).await?;
                }
            }
            updated.push(alert);
        }
        tx.commit().await?;
        rules.tick_size = tick;
        rules.metadata_status = status.into();
        rules.metadata_reason = reason.map(str::to_owned);
        rules.last_ticks = None;
        for alert in updated {
            rules.insert(alert, None);
        }
        Ok(())
    }
}

pub fn validate(alert: &mut PriceAlert) -> Result<(), Error> {
    if alert.geometry.first.time_ms > alert.geometry.second.time_ms {
        std::mem::swap(&mut alert.geometry.first, &mut alert.geometry.second);
    }
    alert.symbol = alert.symbol.trim().to_uppercase();
    alert.interval = Interval::parse(&alert.interval)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .canonical();
    if alert.symbol.is_empty()
        || alert.symbol.len() > 64
        || !alert.symbol.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err(Error::Invalid("invalid symbol".into()));
    }
    if alert.tv_symbol.is_empty()
        || alert.tv_symbol.len() > 80
        || !alert
            .tv_symbol
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
    {
        return Err(Error::Invalid("invalid tvSymbol".into()));
    }
    let g = &alert.geometry;
    if !matches!(g.kind.as_str(), "horizontal_segment" | "trend_segment")
        || !matches!(g.extend.as_str(), "none" | "right" | "both")
    {
        return Err(Error::Invalid("invalid geometry kind or extension".into()));
    }
    if g.first.time_ms < 0
        || g.second.time_ms < 0
        || g.first.time_ms == g.second.time_ms
        || !g.first.price.is_finite()
        || !g.second.price.is_finite()
        || g.first.price <= 0.0
        || g.second.price <= 0.0
        || (g.kind == "horizontal_segment" && g.first.price != g.second.price)
    {
        return Err(Error::Invalid("geometry needs distinct nonnegative times, positive finite prices, and equal horizontal prices".into()));
    }
    if !matches!(
        alert.direction.as_str(),
        "cross_any" | "cross_up" | "cross_down"
    ) || alert.frequency != "once"
    {
        return Err(Error::Invalid(
            "direction must be cross_any/cross_up/cross_down; frequency must be once".into(),
        ));
    }
    if !matches!(
        alert.status.as_str(),
        "active" | "disabled" | "triggered" | "expired"
    ) {
        return Err(Error::Invalid("invalid status".into()));
    }
    if !alert.line_width.is_finite()
        || !(1.0..=8.0).contains(&alert.line_width)
        || alert.color.len() != 7
        || !alert.color.starts_with('#')
        || !alert.color[1..].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::Invalid(
            "color must be #RRGGBB and lineWidth 1..8".into(),
        ));
    }
    if alert.name.len() > 256
        || alert.label.len() > 256
        || alert.message_template.len() > 16384
        || alert.webhook_url.len() > 2048
    {
        return Err(Error::Invalid("name/label/template/url is too long".into()));
    }
    if !alert.webhook_url.is_empty() || alert.status == "active" {
        let url = reqwest::Url::parse(&alert.webhook_url)
            .map_err(|_| Error::Invalid("invalid webhookUrl".into()))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(Error::Invalid(
                "webhookUrl must be an http(s) URL without credentials".into(),
            ));
        }
    }
    if !alert.message_template.is_empty() || alert.status == "active" {
        let json: Value = serde_json::from_str(&alert.message_template)
            .map_err(|e| Error::Invalid(format!("invalid messageTemplate JSON: {e}")))?;
        if !json.is_object() {
            return Err(Error::Invalid(
                "messageTemplate must be a JSON object".into(),
            ));
        }
        validate_placeholders(&json)?;
    }
    Ok(())
}

fn validate_placeholders(value: &Value) -> Result<(), Error> {
    match value {
        Value::String(s) => {
            let mut rest = s.as_str();
            while let Some(pos) = rest.find("{{") {
                rest = &rest[pos + 2..];
                let Some(end) = rest.find("}}") else {
                    return Err(Error::Invalid("unclosed template placeholder".into()));
                };
                if !matches!(
                    &rest[..end],
                    "ticker"
                        | "symbol"
                        | "exchange"
                        | "interval"
                        | "price"
                        | "close"
                        | "alertId"
                        | "time"
                        | "eventId"
                        | "linePrice"
                        | "direction"
                ) {
                    return Err(Error::Invalid(format!(
                        "unsupported placeholder: {}",
                        &rest[..end]
                    )));
                }
                rest = &rest[end + 2..];
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_placeholders(item)?;
            }
        }
        Value::Object(items) => {
            for (key, item) in items {
                if key.contains("{{") {
                    return Err(Error::Invalid(
                        "template keys cannot contain placeholders".into(),
                    ));
                }
                validate_placeholders(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn replay(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    request: &str,
) -> Result<Option<Value>, Error> {
    let row = sqlx::query("SELECT request,response FROM price_alert_mutations WHERE mutation_id=?")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
    if let Some(row) = row {
        if row.get::<String, _>("request") != request {
            return Err(Error::Conflict(
                "mutationId was already used with a different request".into(),
            ));
        }
        return Ok(Some(serde_json::from_str(
            &row.get::<String, _>("response"),
        )?));
    }
    Ok(None)
}
async fn record(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    request: &str,
    response: &Value,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO price_alert_mutations(mutation_id,request,response,created_at) VALUES(?,?,?,?)")
        .bind(id).bind(request).bind(serde_json::to_string(response)?).bind(chrono::Utc::now().timestamp_millis()).execute(&mut **tx).await?;
    Ok(())
}
async fn persist(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    alert: &PriceAlert,
) -> Result<(), Error> {
    sqlx::query("UPDATE price_alerts SET revision=?,arm_generation=?,status=?,data=? WHERE id=?")
        .bind(alert.revision)
        .bind(alert.arm_generation)
        .bind(&alert.status)
        .bind(serde_json::to_string(alert)?)
        .bind(alert.id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl SqliteStore {
    pub async fn create_price_alert(
        &self,
        request: CreateRequest,
        fingerprint: &str,
    ) -> Result<PriceAlert, Error> {
        valid_mutation(&request.mutation_id)?;
        let mut runtime = self.price_alert_runtime.lock().await;
        let mut tx = self.write_pool.begin().await?;
        let fingerprint = format!("POST:{fingerprint}");
        if let Some(value) = replay(&mut tx, &request.mutation_id, &fingerprint).await? {
            return Ok(serde_json::from_value(value)?);
        }
        let now = chrono::Utc::now().timestamp_millis();
        if !matches!(request.status.as_str(), "active" | "disabled") {
            return Err(Error::Invalid(
                "create status must be active or disabled".into(),
            ));
        }
        let mut alert = PriceAlert {
            id: 0,
            revision: 1,
            arm_generation: 1,
            tv_symbol: request
                .tv_symbol
                .unwrap_or_else(|| format!("{}.P", request.symbol.trim().to_uppercase())),
            symbol: request.symbol,
            interval: request.interval,
            name: request.name,
            geometry: request.geometry,
            direction: request.direction,
            frequency: request.frequency,
            status: request.status,
            expires_at: request.expires_at,
            webhook_url: request.webhook_url,
            message_template: request.message_template,
            label: request.label,
            color: request.color,
            line_width: request.line_width,
            triggered_at: None,
            delivery_status: None,
            delivery_error: None,
            data_status: "waiting_for_price".into(),
            created_at: now,
            updated_at: now,
        };
        validate(&mut alert)?;
        if let Some(tick) = runtime
            .symbols
            .get(&alert.symbol)
            .and_then(|r| r.tick_size.as_ref())
        {
            normalize_geometry(tick, &mut alert)?;
        } else if alert.status == "active" {
            return Err(Error::MetadataUnavailable("Binance PRICE_FILTER.tickSize is not ready; save disabled or wait for market metadata".into()));
        }
        if alert.status == "active" && alert.expires_at.is_some_and(|e| e <= now) {
            alert.status = "expired".into();
        }
        alert.id = sqlx::query(
            "INSERT INTO price_alerts(revision,arm_generation,status,data) VALUES(1,1,?,'')",
        )
        .bind(&alert.status)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        persist(&mut tx, &alert).await?;
        record(
            &mut tx,
            &request.mutation_id,
            &fingerprint,
            &serde_json::to_value(&alert)?,
        )
        .await?;
        tx.commit().await?;
        runtime.upsert(alert.clone(), false);
        Ok(alert)
    }

    pub async fn patch_price_alert(
        &self,
        id: i64,
        patch: PatchRequest,
        fingerprint: &str,
    ) -> Result<PriceAlert, Error> {
        valid_mutation(&patch.mutation_id)?;
        let mut runtime = self.price_alert_runtime.lock().await;
        let mut tx = self.write_pool.begin().await?;
        let fingerprint = format!("PATCH:{id}:{fingerprint}");
        if let Some(value) = replay(&mut tx, &patch.mutation_id, &fingerprint).await? {
            return Ok(serde_json::from_value(value)?);
        }
        let json = sqlx::query_scalar::<_, String>("SELECT data FROM price_alerts WHERE id=?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NotFound)?;
        let mut alert: PriceAlert = serde_json::from_str(&json)?;
        if alert.revision != patch.expected_revision {
            return Err(Error::Conflict(format!(
                "revision mismatch: current revision is {}",
                alert.revision
            )));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let old = alert.clone();
        let was_expired = old.status == "expired" || old.expires_at.is_some_and(|e| e <= now);
        macro_rules! set {($($field:ident),*)=>{$(if let Some(value)=patch.$field {alert.$field=value;})*};}
        set!(
            symbol,
            interval,
            tv_symbol,
            name,
            geometry,
            direction,
            frequency,
            webhook_url,
            message_template,
            label,
            color,
            line_width
        );
        if let Some(value) = patch.expires_at {
            alert.expires_at = value;
        }
        if alert.symbol != old.symbol && alert.tv_symbol == old.tv_symbol {
            alert.tv_symbol = format!("{}.P", alert.symbol.trim().to_uppercase());
        }
        alert.symbol = alert.symbol.trim().to_uppercase();
        let fixing_expiry = was_expired && alert.expires_at.is_none_or(|e| e > now);
        let geometry_edit = alert.geometry != old.geometry
            || alert.symbol != old.symbol
            || alert.interval != old.interval
            || alert.direction != old.direction;
        let wants_active = patch.status.as_deref() != Some("disabled")
            && ((old.status == "active" && !was_expired)
                || patch.status.as_deref() == Some("active")
                || patch.rearm == Some(true)
                || (old.status == "triggered" && geometry_edit && !was_expired)
                || (fixing_expiry && old.status != "disabled"));
        if let Some(tick) = runtime
            .symbols
            .get(&alert.symbol)
            .and_then(|r| r.tick_size.as_ref())
        {
            normalize_geometry(tick, &mut alert)?;
        } else if wants_active {
            return Err(Error::MetadataUnavailable("Binance PRICE_FILTER.tickSize is not ready; wait for metadata before arming or moving an active alert".into()));
        }
        let geometry_changed = alert.geometry != old.geometry
            || alert.symbol != old.symbol
            || alert.interval != old.interval
            || alert.direction != old.direction;
        let expiry_fixed = was_expired && alert.expires_at.is_none_or(|e| e > now);
        let mut rearm = patch.rearm == Some(true)
            || (geometry_changed && matches!(old.status.as_str(), "active" | "triggered"))
            || expiry_fixed;
        let explicit_enable =
            patch.rearm == Some(true) || patch.status.as_deref() == Some("active");
        if let Some(status) = patch.status {
            if !matches!(status.as_str(), "active" | "disabled") {
                return Err(Error::Invalid(
                    "patch status must be active or disabled".into(),
                ));
            }
            if status == "disabled" {
                rearm = false;
            } else {
                rearm |= old.status != "active";
            }
            alert.status = status;
        } else if old.status == "disabled" && patch.rearm != Some(true) {
            rearm = false;
        }
        if was_expired && !expiry_fixed {
            if explicit_enable {
                return Err(Error::Invalid(
                    "expired alert needs a future or null expiresAt before rearming".into(),
                ));
            }
            rearm = false;
            if alert.status != "disabled" {
                alert.status = "expired".into();
            }
        }
        if rearm {
            alert.status = "active".into();
            alert.arm_generation += 1;
            alert.triggered_at = None;
            alert.delivery_status = None;
            alert.delivery_error = None;
            alert.data_status = "waiting_for_price".into();
        }
        if alert.status == "active" && alert.expires_at.is_some_and(|e| e <= now) {
            alert.status = "expired".into();
        }
        validate(&mut alert)?;
        alert.revision += 1;
        alert.updated_at = now;
        persist(&mut tx, &alert).await?;
        record(
            &mut tx,
            &patch.mutation_id,
            &fingerprint,
            &serde_json::to_value(&alert)?,
        )
        .await?;
        tx.commit().await?;
        runtime.upsert(alert.clone(), !rearm && !geometry_changed);
        Ok(alert)
    }

    pub async fn delete_price_alert(
        &self,
        id: i64,
        revision: i64,
        mutation_id: &str,
    ) -> Result<Value, Error> {
        valid_mutation(mutation_id)?;
        let mut runtime = self.price_alert_runtime.lock().await;
        let mut tx = self.write_pool.begin().await?;
        let request = format!("DELETE:{id}:{revision}");
        if let Some(value) = replay(&mut tx, mutation_id, &request).await? {
            return Ok(value);
        }
        let current = sqlx::query_scalar::<_, i64>("SELECT revision FROM price_alerts WHERE id=?")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NotFound)?;
        if current != revision {
            return Err(Error::Conflict(format!(
                "revision mismatch: current revision is {current}"
            )));
        }
        sqlx::query("DELETE FROM price_alerts WHERE id=?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        // Historical triggers stay durable. Deleting a drawing cancels only pending sends.
        let rows=sqlx::query_scalar::<_,String>("SELECT e.data FROM price_alert_events e JOIN price_alert_outbox o ON o.event_id=e.event_id WHERE o.kind='v2' AND o.alert_id=? AND o.status='pending'")
            .bind(id).fetch_all(&mut *tx).await?;
        for json in rows {
            let mut event: PriceAlertEvent = serde_json::from_str(&json)?;
            event.delivery_status = "cancelled".into();
            event.delivery_error = Some("drawing deleted".into());
            sqlx::query("UPDATE price_alert_events SET data=? WHERE event_id=?")
                .bind(serde_json::to_string(&event)?)
                .bind(&event.id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE price_alert_outbox SET status='cancelled',error='drawing deleted' WHERE kind='v2' AND alert_id=? AND status='pending'").bind(id).execute(&mut *tx).await?;
        let result = serde_json::json!({"deleted":true,"id":id,"revision":revision});
        record(&mut tx, mutation_id, &request, &result).await?;
        tx.commit().await?;
        for rules in runtime.symbols.values_mut() {
            rules.remove(id);
        }
        Ok(result)
    }

    pub async fn price_alert_mutation(&self, id: &str) -> Result<Value, Error> {
        let json = sqlx::query_scalar::<_, String>(
            "SELECT response FROM price_alert_mutations WHERE mutation_id=?",
        )
        .bind(id)
        .fetch_optional(&self.read_pool)
        .await?
        .ok_or(Error::NotFound)?;
        Ok(serde_json::from_str(&json)?)
    }
    pub async fn list_price_alerts(&self) -> Result<Vec<PriceAlert>, Error> {
        let rows =
            sqlx::query_scalar::<_, String>("SELECT data FROM price_alerts ORDER BY id DESC")
                .fetch_all(&self.read_pool)
                .await?;
        let runtime = self.price_alert_runtime.lock().await;
        rows.into_iter()
            .map(|json| {
                let mut alert: PriceAlert = serde_json::from_str(&json)?;
                let now = chrono::Utc::now().timestamp_millis();
                if alert.status == "active" && alert.expires_at.is_some_and(|e| e <= now) {
                    alert.status = "expired".into();
                }
                alert.data_status = match runtime.symbols.get(&alert.symbol) {
                    _ if alert.geometry.extend != "both" && now < alert.geometry.first.time_ms => {
                        "waiting_for_range"
                    }
                    _ if alert.geometry.extend == "none" && now > alert.geometry.second.time_ms => {
                        "range_ended"
                    }
                    Some(rules) if rules.tick_size.is_none() => {
                        if rules.metadata_status == "unavailable" {
                            "unavailable"
                        } else {
                            "waiting_for_metadata"
                        }
                    }
                    Some(rules)
                        if rules
                            .rules
                            .get(&alert.id)
                            .is_some_and(|r| r.ticks.is_none()) =>
                    {
                        "invalid_line"
                    }
                    Some(rules)
                        if rules.tick_size.as_ref().is_some_and(|tick| {
                            tick.ordinal(alert.geometry.first.price)
                                .zip(tick.ordinal(alert.geometry.second.price))
                                .and_then(|points| {
                                    integer_line_side(&alert.geometry, points, 1, now)
                                })
                                .is_none()
                        }) =>
                    {
                        "invalid_line"
                    }
                    Some(rules) if rules.received_at.is_some_and(|t| now - t <= 60_000) => {
                        if rules.pending.contains(&alert.id) {
                            "waiting_for_price"
                        } else {
                            "live"
                        }
                    }
                    Some(rules) if rules.received_at.is_some() => "disconnected",
                    _ => "waiting_for_price",
                }
                .into();
                Ok(alert)
            })
            .collect()
    }
    pub async fn get_price_alert(&self, id: i64) -> Result<PriceAlert, Error> {
        self.list_price_alerts()
            .await?
            .into_iter()
            .find(|a| a.id == id)
            .ok_or(Error::NotFound)
    }
    pub async fn price_alert_events(&self, id: i64) -> Result<Vec<PriceAlertEvent>, Error> {
        self.price_alert_events_page(id, 1000, None).await
    }
    pub async fn price_alert_events_page(
        &self,
        id: i64,
        limit: u32,
        before_arm_generation: Option<i64>,
    ) -> Result<Vec<PriceAlertEvent>, Error> {
        if limit == 0 || limit > 1000 || before_arm_generation.is_some_and(|g| g <= 0) {
            return Err(Error::Invalid(
                "limit must be 1..1000 and beforeArmGeneration positive".into(),
            ));
        }
        let rows = if let Some(generation) = before_arm_generation {
            sqlx::query_scalar::<_,String>("SELECT data FROM price_alert_events WHERE alert_id=? AND arm_generation<? ORDER BY arm_generation DESC LIMIT ?")
                .bind(id).bind(generation).bind(limit).fetch_all(&self.read_pool).await?
        } else {
            sqlx::query_scalar::<_,String>("SELECT data FROM price_alert_events WHERE alert_id=? ORDER BY arm_generation DESC LIMIT ?")
                .bind(id).bind(limit).fetch_all(&self.read_pool).await?
        };
        rows.into_iter()
            .map(|json| serde_json::from_str(&json).map_err(Error::from))
            .collect()
    }
    /// Disconnect/recovery is a discontinuity, never a synthetic crossing.
    pub async fn reset_price_alert_baselines(&self, symbol: Option<&str>) {
        let mut runtime = self.price_alert_runtime.lock().await;
        for (name, rules) in &mut runtime.symbols {
            if symbol.is_some_and(|s| s != name) {
                continue;
            }
            rules.last_price = None;
            rules.last_ticks = None;
            rules.last_time = None;
            rules.range_time = None;
            rules.received_at = None;
            for (id, rule) in &mut rules.rules {
                rule.side = None;
                rule.last_time = None;
                rules.pending.insert(*id);
            }
            for (_, side) in rules.legacy.values_mut() {
                *side = None;
            }
        }
    }
}

fn side(price: f64, line: f64) -> i8 {
    if price > line {
        1
    } else if price < line {
        -1
    } else {
        0
    }
}
fn crossing(previous: Option<i8>, current: i8, wanted: &str) -> Option<&'static str> {
    match previous {
        Some(-1) if current >= 0 && matches!(wanted, "cross_any" | "cross_up") => Some("cross_up"),
        Some(1) if current <= 0 && matches!(wanted, "cross_any" | "cross_down") => {
            Some("cross_down")
        }
        _ => None,
    }
}

fn integer_line_side(
    geometry: &Geometry,
    endpoints: (u64, u64),
    price: u64,
    time: i64,
) -> Option<(i8, i128, i128)> {
    if (geometry.extend == "none"
        && !(geometry.first.time_ms..=geometry.second.time_ms).contains(&time))
        || (geometry.extend == "right" && time < geometry.first.time_ms)
    {
        return None;
    }
    let span =
        i128::from(geometry.second.time_ms).checked_sub(i128::from(geometry.first.time_ms))?;
    if span <= 0 {
        return None;
    }
    let offset = i128::from(time).checked_sub(i128::from(geometry.first.time_ms))?;
    let slope = i128::from(endpoints.1).checked_sub(i128::from(endpoints.0))?;
    let line = i128::from(endpoints.0)
        .checked_mul(span)?
        .checked_add(slope.checked_mul(offset)?)?;
    if line <= 0 || line > i128::from(MAX_TICK_ORDINAL).checked_mul(span)? {
        return None;
    }
    let observed = i128::from(price).checked_mul(span)?;
    let side = match observed.cmp(&line) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    Some((side, line, span))
}

fn render(template: &str, values: &[(&str, String)]) -> Result<Value, serde_json::Error> {
    fn visit(value: &mut Value, values: &[(&str, String)]) {
        match value {
            Value::String(s) => {
                for (key, value) in values {
                    *s = s.replace(key, value);
                }
            }
            Value::Array(items) => {
                for item in items {
                    visit(item, values);
                }
            }
            Value::Object(items) => {
                for item in items.values_mut() {
                    visit(item, values);
                }
            }
            _ => {}
        }
    }
    let mut value = serde_json::from_str(template)?;
    visit(&mut value, values);
    Ok(value)
}

impl SqliteStore {
    /// Exposed for replayable isolated fixtures; callers pass only accepted live data.
    pub async fn evaluate_drawing_alerts(
        &self,
        symbol: &str,
        price: f64,
        time_ms: i64,
    ) -> Result<(), Error> {
        if !price.is_finite() || price <= 0.0 || time_ms < 0 {
            return Ok(());
        }
        let mut runtime = self.price_alert_runtime.lock().await;
        let Some(rules) = runtime.symbols.get_mut(symbol) else {
            return Ok(());
        };
        if rules.last_time.is_some_and(|t| time_ms < t) {
            return Ok(());
        }
        let received = chrono::Utc::now().timestamp_millis();
        // Silent stale-source gaps also invalidate the old baseline.
        if rules.received_at.is_some_and(|t| received - t > 60_000) {
            for (id, rule) in &mut rules.rules {
                rule.side = None;
                rule.last_time = None;
                if rule.time_active {
                    rules.pending.insert(*id);
                }
            }
            for (_, previous) in rules.legacy.values_mut() {
                *previous = None;
            }
            rules.last_price = None;
            rules.last_ticks = None;
        }
        let step = rules.tick_size.as_ref().map(TickSize::value);
        let observed_ticks = step.and_then(|s| TickSize::ordinal_at(s, price));
        if observed_ticks.is_none() && rules.last_ticks.is_some() {
            for (id, rule) in &mut rules.rules {
                rule.side = None;
                rule.last_time = None;
                if rule.time_active {
                    rules.pending.insert(*id);
                }
            }
        }
        let mut candidates = std::mem::take(&mut rules.candidates);
        candidates.clear();
        if let Some(observed) = observed_ticks {
            candidates.extend(rules.trends.iter().copied());
            candidates.extend(rules.pending.iter().copied());
            if let Some(previous) = rules.last_ticks {
                for ids in rules
                    .horizontal
                    .range(observed.min(previous)..=observed.max(previous))
                    .map(|(_, ids)| ids)
                {
                    candidates.extend(ids.iter().copied());
                }
            }
        }
        if observed_ticks.is_some() {
            use std::ops::Bound::{Excluded, Included, Unbounded};
            let start = rules.range_time.map(Excluded).unwrap_or(Unbounded);
            for ids in rules
                .boundaries
                .range((start, Included(time_ms)))
                .map(|(_, ids)| ids)
            {
                candidates.extend(ids.iter().copied());
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        let mut fired = Vec::<i64>::new();
        for &id in &candidates {
            rules.schedule_range(id, time_ms);
            let Some(rule) = rules.rules.get_mut(&id) else {
                continue;
            };
            if !rule.time_active {
                continue;
            }
            if rule.alert.expires_at.is_some_and(|e| e <= time_ms) {
                fired.push(id);
                continue;
            }
            let Some((current, line_numerator, line_span)) = rule.ticks.and_then(|endpoints| {
                integer_line_side(&rule.alert.geometry, endpoints, observed_ticks?, time_ms)
            }) else {
                rule.side = None;
                rule.last_time = Some(time_ms);
                rules.pending.remove(&id);
                continue;
            };
            if let Some(direction) = crossing(rule.side, current, &rule.alert.direction) {
                let alert = rule.alert.clone();
                let line = if alert.geometry.kind == "horizontal_segment" {
                    alert.geometry.first.price
                } else {
                    (line_numerator as f64 / line_span as f64) * step.unwrap()
                };
                match persist_crossing(symbol, alert.id, || {
                    self.claim_drawing_trigger(&alert, price, line, time_ms, direction)
                })
                .await
                {
                    Ok(_) => fired.push(id),
                    Err(error) => {
                        for id in fired {
                            rules.remove(id);
                        }
                        rules.candidates = candidates;
                        return Err(error);
                    }
                }
            } else {
                if current != 0 || rule.side.is_none() {
                    rule.side = Some(current);
                }
                rule.last_time = Some(time_ms);
                rules.pending.remove(&id);
            }
        }
        for id in fired {
            rules.remove(id);
        }
        rules.candidates = candidates;
        let mut legacy_fired = Vec::new();
        for (id, (alert, previous)) in &mut rules.legacy {
            if alert.expires_at.is_some_and(|e| e <= time_ms) {
                legacy_fired.push(*id);
                continue;
            }
            let current = side(price, alert.price);
            if let Some(direction) = crossing(*previous, current, &alert.direction) {
                match persist_crossing(symbol, alert.id, || {
                    self.claim_legacy_trigger(alert, price, time_ms, direction)
                })
                .await
                {
                    Ok(_) => legacy_fired.push(*id),
                    Err(error) => {
                        for id in legacy_fired {
                            rules.legacy.remove(&id);
                        }
                        return Err(error);
                    }
                }
            } else if current != 0 || previous.is_none() {
                *previous = Some(current);
            }
        }
        for id in legacy_fired {
            rules.legacy.remove(&id);
        }
        rules.last_price = Some(price);
        rules.last_ticks = observed_ticks;
        rules.last_time = Some(time_ms);
        if observed_ticks.is_some() {
            rules.range_time = Some(time_ms);
        }
        rules.received_at = Some(received);
        Ok(())
    }

    async fn claim_drawing_trigger(
        &self,
        alert: &PriceAlert,
        price: f64,
        line: f64,
        time: i64,
        direction: &str,
    ) -> Result<bool, Error> {
        let mut tx = self.write_pool.begin().await?;
        let namespace = sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM price_alert_identity WHERE id=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let event_id = format!("{namespace}:v2:{}:{}", alert.id, alert.arm_generation);
        let payload = render(
            &alert.message_template,
            &[
                ("{{ticker}}", alert.symbol.clone()),
                ("{{symbol}}", alert.symbol.clone()),
                ("{{exchange}}", "BINANCE".into()),
                ("{{interval}}", alert.interval.clone()),
                ("{{price}}", price.to_string()),
                ("{{close}}", price.to_string()),
                ("{{alertId}}", alert.id.to_string()),
                ("{{time}}", time.to_string()),
                ("{{eventId}}", event_id.clone()),
                ("{{linePrice}}", line.to_string()),
                ("{{direction}}", direction.into()),
            ],
        )?;
        let mut triggered = alert.clone();
        triggered.status = "triggered".into();
        triggered.revision += 1;
        triggered.triggered_at = Some(time);
        triggered.updated_at = time;
        triggered.delivery_status = Some("pending".into());
        triggered.delivery_error = None;
        let result=sqlx::query("UPDATE price_alerts SET status='triggered',revision=?,data=? WHERE id=? AND revision=? AND arm_generation=? AND status='active'")
            .bind(triggered.revision).bind(serde_json::to_string(&triggered)?).bind(alert.id).bind(alert.revision).bind(alert.arm_generation).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Ok(false);
        }
        let event = PriceAlertEvent {
            id: event_id.clone(),
            alert_id: alert.id,
            arm_generation: alert.arm_generation,
            triggered_at: time,
            trigger_price: price,
            line_price: line,
            direction: direction.into(),
            payload: payload.clone(),
            delivery_status: "pending".into(),
            delivery_error: None,
        };
        sqlx::query("INSERT INTO price_alert_events(event_id,alert_id,arm_generation,triggered_at,data) VALUES(?,?,?,?,?)")
            .bind(&event_id).bind(alert.id).bind(alert.arm_generation).bind(time).bind(serde_json::to_string(&event)?).execute(&mut *tx).await?;
        insert_job(
            &mut tx,
            &event_id,
            "v2",
            alert.id,
            alert.arm_generation,
            time,
            &alert.webhook_url,
            &payload,
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }
    async fn claim_legacy_trigger(
        &self,
        alert: &Alert,
        price: f64,
        time: i64,
        direction: &str,
    ) -> Result<bool, Error> {
        let mut tx = self.write_pool.begin().await?;
        let result=sqlx::query("UPDATE alerts SET status='triggered',triggered_at=?,updated_at=?,delivery_status='pending',delivery_error=NULL WHERE id=? AND status='active' AND updated_at=? AND (expires_at IS NULL OR expires_at>?)")
            .bind(time).bind(time).bind(alert.id).bind(alert.updated_at).bind(time).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Ok(false);
        }
        let event=sqlx::query("INSERT INTO alert_events(alert_id,triggered_at,trigger_price,direction,delivery_status,created_at) VALUES(?,?,?,?,'pending',?)")
            .bind(alert.id).bind(time).bind(price).bind(direction).bind(time).execute(&mut *tx).await?.last_insert_rowid();
        let namespace = sqlx::query_scalar::<_, String>(
            "SELECT namespace FROM price_alert_identity WHERE id=1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let event_id = format!("{namespace}:v1:{event}");
        let payload = render(
            &alert.message_template,
            &[
                ("{{ticker}}", alert.symbol.clone()),
                ("{{symbol}}", alert.symbol.clone()),
                ("{{exchange}}", "BINANCE".into()),
                ("{{interval}}", alert.interval.clone()),
                ("{{price}}", price.to_string()),
                ("{{close}}", price.to_string()),
                ("{{alertId}}", alert.id.to_string()),
                ("{{time}}", time.to_string()),
                ("{{eventId}}", event_id.clone()),
            ],
        )?;
        insert_job(
            &mut tx,
            &event_id,
            "v1",
            alert.id,
            event,
            time,
            &alert.webhook_url,
            &payload,
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}

// Keep the runtime gate while an observed crossing cannot be committed. A
// concurrent move must not replace its geometry during database backpressure.
async fn persist_crossing<T, F, Fut>(symbol: &str, alert_id: i64, mut action: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut delay = Duration::from_millis(100);
    loop {
        match action().await {
            Err(Error::Database(error)) => {
                tracing::error!(symbol,alert_id,"price alert storage unavailable; retaining crossing and serializing mutations: {error}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            result => return result,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_job(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    kind: &str,
    alert: i64,
    generation: i64,
    time: i64,
    url: &str,
    payload: &Value,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO price_alert_outbox(event_id,kind,alert_id,arm_generation,triggered_at,webhook_url,payload,next_attempt_at) VALUES(?,?,?,?,?,?,?,?)")
        .bind(id).bind(kind).bind(alert).bind(generation).bind(time).bind(url).bind(serde_json::to_string(payload)?).bind(chrono::Utc::now().timestamp_millis()).execute(&mut **tx).await?;
    Ok(())
}

/// A single supervisor uses one pooled client and at most eight concurrent
/// requests. Pending work survives cancellation/restart; attempts are bounded.
pub struct DeliveryTask {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl DeliveryTask {
    pub async fn stop(self) {
        let _ = self.cancel.send(true);
        let _ = self.task.await;
    }
}
pub fn start_delivery(store: SqliteStore) -> Result<DeliveryTask, reqwest::Error> {
    Ok(start_delivery_with_client(store, pooled_http_client()?))
}

pub fn pooled_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(8)
        .build()
}
pub fn start_delivery_with_client(store: SqliteStore, client: reqwest::Client) -> DeliveryTask {
    let (cancel, mut cancellation) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! { biased; _ = cancellation.changed() => break, _ = timer.tick() => {} }
            let now = chrono::Utc::now().timestamp_millis();
            let query = sqlx::query("SELECT event_id,kind,alert_id,arm_generation,triggered_at,webhook_url,payload,attempts FROM price_alert_outbox WHERE status='pending' AND next_attempt_at<=? ORDER BY next_attempt_at LIMIT 8")
                .bind(now).fetch_all(&store.read_pool);
            let result = tokio::select! { biased; _ = cancellation.changed() => break, result = query => result };
            let rows = match result {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!("price alert outbox: {error}");
                    continue;
                }
            };
            let deliveries = rows.into_iter().map(|row| {
                let client = client.clone();
                let store = store.clone();
                let mut cancelled = cancellation.clone();
                async move {
                    tokio::select! { biased;
                        _ = cancelled.changed() => {},
                        result = deliver_job(&store, &client, &row) => {
                            if let Err(error) = result { tracing::warn!("price alert delivery persistence: {error}"); }
                        }
                    }
                }
            });
            futures_util::future::join_all(deliveries).await;
        }
    });
    DeliveryTask { cancel, task }
}

async fn deliver_job(
    store: &SqliteStore,
    client: &reqwest::Client,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(), Error> {
    let id: String = row.get("event_id");
    let time: i64 = row.get("triggered_at");
    let attempts: i64 = row.get("attempts");
    let now = chrono::Utc::now().timestamp_millis();
    if attempts >= 3 || now - time > 30 * 86_400_000 {
        return finish_job(
            store,
            row,
            false,
            Some("retry budget or delivery age exceeded".into()),
            true,
        )
        .await;
    }
    // Debit before sending. A crash during a request never resets its budget.
    let debited = sqlx::query("UPDATE price_alert_outbox SET attempts=attempts+1,next_attempt_at=? WHERE event_id=? AND status='pending'")
        .bind(now + 30_000).bind(&id).execute(&store.write_pool).await?;
    if debited.rows_affected() != 1 {
        return Ok(());
    }
    let request = client
        .post(row.get::<String, _>("webhook_url"))
        .header("Content-Type", "application/json")
        .header("X-Guaili-Event-Id", &id)
        .header("X-Guaili-Event-Time", time.to_string())
        .body(row.get::<String, _>("payload"));
    let outcome = async {
        let mut response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        // Drain a bounded response to reuse pooled connections; don't allocate
        // an unbounded body or log potentially sensitive receiver content.
        let mut received = 0usize;
        while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
            received += chunk.len();
            if received > 65_536 {
                return Err("webhook response exceeded 64 KiB".into());
            }
        }
        if status.is_success() {
            Ok(())
        } else {
            Err(format!("webhook returned {status}"))
        }
    }
    .await;
    let (success, error) = match outcome {
        Ok(()) => (true, None),
        Err(error) => (false, Some(error)),
    };
    finish_job(store, row, success, error, attempts >= 2).await
}

async fn finish_job(
    store: &SqliteStore,
    row: &sqlx::sqlite::SqliteRow,
    success: bool,
    error: Option<String>,
    final_attempt: bool,
) -> Result<(), Error> {
    let id: String = row.get("event_id");
    let kind: String = row.get("kind");
    let alert_id: i64 = row.get("alert_id");
    let generation: i64 = row.get("arm_generation");
    let status = if success {
        "success"
    } else if final_attempt {
        "failed"
    } else {
        "pending"
    };
    let mut runtime = store.price_alert_runtime.lock().await;
    let mut tx = store.write_pool.begin().await?;
    let current_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM price_alert_outbox WHERE event_id=?")
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?;
    if current_status.as_deref() != Some("pending") {
        return Ok(());
    }
    sqlx::query(
        "UPDATE price_alert_outbox SET status=?,error=?,next_attempt_at=? WHERE event_id=?",
    )
    .bind(status)
    .bind(&error)
    .bind(chrono::Utc::now().timestamp_millis() + 1000 * (1 << row.get::<i64, _>("attempts")))
    .bind(&id)
    .execute(&mut *tx)
    .await?;
    if kind == "v2" {
        let json =
            sqlx::query_scalar::<_, String>("SELECT data FROM price_alert_events WHERE event_id=?")
                .bind(&id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(json) = json {
            let mut event: PriceAlertEvent = serde_json::from_str(&json)?;
            event.delivery_status = status.into();
            event.delivery_error = error.clone();
            sqlx::query("UPDATE price_alert_events SET data=? WHERE event_id=?")
                .bind(serde_json::to_string(&event)?)
                .bind(&id)
                .execute(&mut *tx)
                .await?;
        }
        let json = sqlx::query_scalar::<_, String>(
            "SELECT data FROM price_alerts WHERE id=? AND arm_generation=? AND status='triggered'",
        )
        .bind(alert_id)
        .bind(generation)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(json) = json {
            let mut alert: PriceAlert = serde_json::from_str(&json)?;
            alert.delivery_status = Some(status.into());
            alert.delivery_error = error.clone();
            persist(&mut tx, &alert).await?;
            runtime.upsert(alert, true);
        }
    } else {
        sqlx::query(
            "UPDATE alert_events SET delivery_status=?,delivery_error=? WHERE id=? AND alert_id=?",
        )
        .bind(status)
        .bind(&error)
        .bind(generation)
        .bind(alert_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE alerts SET delivery_status=?,delivery_error=? WHERE id=? AND status='triggered' AND triggered_at=? AND ?=(SELECT MAX(id) FROM alert_events WHERE alert_id=?)")
            .bind(status).bind(&error).bind(alert_id).bind(row.get::<i64,_>("triggered_at")).bind(generation).bind(alert_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    async fn fixture() -> SqliteStore {
        let store = SqliteStore::connect_price_alert_fixture("sqlite::memory:")
            .await
            .unwrap();
        store
            .set_price_alert_metadata_fixture("BTCUSDT", Some("0.01"), None)
            .await
            .unwrap();
        store
    }
    #[tokio::test]
    async fn future_and_ended_rules_leave_all_hot_indexes_and_enter_with_fresh_baselines() {
        let store = fixture().await;
        for id in 0..1000 {
            let future = id % 2 == 0;
            let first = if future { 10000 } else { 1 };
            let second = if future { 20000 } else { 2 };
            let kind = if id % 4 < 2 {
                "horizontal_segment"
            } else {
                "trend_segment"
            };
            let value = serde_json::json!({"mutationId":format!("dormant-{id}"),"symbol":"BTCUSDT","interval":"60","status":"active","geometry":{"kind":kind,"first":{"timeMs":first,"price":100.0},"second":{"timeMs":second,"price":if kind=="horizontal_segment"{100.0}else{101.0}},"extend":"none"},"webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{}"});
            store
                .create_price_alert(
                    serde_json::from_value(value.clone()).unwrap(),
                    &value.to_string(),
                )
                .await
                .unwrap();
        }
        store
            .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
            .await
            .unwrap();
        for time in 11..1011 {
            store
                .evaluate_drawing_alerts("BTCUSDT", if time % 2 == 0 { 90.0 } else { 110.0 }, time)
                .await
                .unwrap();
        }
        {
            let runtime = store.price_alert_runtime.lock().await;
            let rules = &runtime.symbols["BTCUSDT"];
            assert_eq!(rules.rules.len(), 1000);
            assert!(rules.trends.is_empty());
            assert!(rules.horizontal.is_empty());
            assert!(rules.pending.is_empty());
            assert!(rules.candidates.is_empty());
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM price_alert_events")
                .fetch_one(&store.read_pool)
                .await
                .unwrap(),
            0
        );
        // Entering directly on the opposite side never uses the out-of-range sample.
        store
            .evaluate_drawing_alerts("BTCUSDT", 110.0, 10000)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM price_alert_events")
                .fetch_one(&store.read_pool)
                .await
                .unwrap(),
            0
        );
        {
            let runtime = store.price_alert_runtime.lock().await;
            let rules = &runtime.symbols["BTCUSDT"];
            assert_eq!(rules.trends.len(), 250);
            assert_eq!(rules.horizontal[&10000].len(), 250);
            assert!(rules.pending.is_empty());
        }
        store
            .evaluate_drawing_alerts("BTCUSDT", 110.0, 20001)
            .await
            .unwrap();
        let runtime = store.price_alert_runtime.lock().await;
        let rules = &runtime.symbols["BTCUSDT"];
        assert!(rules.trends.is_empty());
        assert!(rules.horizontal.is_empty());
        assert!(rules.pending.is_empty());
    }
    #[tokio::test]
    async fn skipped_ranges_and_invalid_quote_boundaries_do_not_lose_scheduling() {
        let store = fixture().await;
        let value = serde_json::json!({"mutationId":"skip-range","symbol":"BTCUSDT","interval":"60","status":"active","geometry":{"kind":"trend_segment","first":{"timeMs":100,"price":100.0},"second":{"timeMs":200,"price":101.0},"extend":"none"},"webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{}"});
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        store
            .evaluate_drawing_alerts("BTCUSDT", 90.0, 90)
            .await
            .unwrap();
        store
            .evaluate_drawing_alerts("BTCUSDT", 1e20, 100)
            .await
            .unwrap();
        store
            .evaluate_drawing_alerts("BTCUSDT", 110.0, 101)
            .await
            .unwrap();
        assert!(store.price_alert_events(alert.id).await.unwrap().is_empty());
        {
            let runtime = store.price_alert_runtime.lock().await;
            assert!(runtime.symbols["BTCUSDT"].trends.contains(&alert.id));
        }
        store.reset_price_alert_baselines(Some("BTCUSDT")).await;
        store
            .evaluate_drawing_alerts("BTCUSDT", 90.0, 201)
            .await
            .unwrap();
        assert!(store.price_alert_events(alert.id).await.unwrap().is_empty());
        {
            let runtime = store.price_alert_runtime.lock().await;
            assert!(runtime.symbols["BTCUSDT"].trends.is_empty());
            assert!(runtime.symbols["BTCUSDT"].pending.is_empty());
        }
    }
    #[tokio::test]
    async fn query_states_show_future_ended_invalid_and_actual_expiry() {
        let store = fixture().await;
        let now = chrono::Utc::now().timestamp_millis();
        let geometry = serde_json::json!({"kind":"trend_segment","first":{"timeMs":now+60000,"price":100.0},"second":{"timeMs":now+120000,"price":101.0},"extend":"none"});
        let value = serde_json::json!({"mutationId":"future-status","symbol":"BTCUSDT","interval":"60","status":"active","geometry":geometry,"webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{}"});
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_price_alert(alert.id).await.unwrap().data_status,
            "waiting_for_range"
        );
        let mut value = value;
        value["mutationId"] = serde_json::json!("ended-status");
        value["geometry"]["first"]["timeMs"] = serde_json::json!(now - 120000);
        value["geometry"]["second"]["timeMs"] = serde_json::json!(now - 60000);
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_price_alert(alert.id).await.unwrap().data_status,
            "range_ended"
        );
        value["mutationId"] = serde_json::json!("invalid-line-status");
        value["geometry"]["extend"] = serde_json::json!("both");
        value["geometry"]["first"]["price"] = serde_json::json!(1.0);
        value["geometry"]["second"]["price"] = serde_json::json!(0.5);
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_price_alert(alert.id).await.unwrap().data_status,
            "invalid_line"
        );
        value["mutationId"] = serde_json::json!("expired-status");
        value["expiresAt"] = serde_json::json!(now - 1);
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_price_alert(alert.id).await.unwrap().status,
            "expired"
        );
    }
    #[test]
    fn integer_trends_keep_fractional_levels_and_large_timestamp_products_exact() {
        let geometry = Geometry {
            kind: "trend_segment".into(),
            first: Point {
                time_ms: 0,
                price: 0.2,
            },
            second: Point {
                time_ms: 2,
                price: 0.21,
            },
            extend: "both".into(),
        };
        assert_eq!(integer_line_side(&geometry, (20, 21), 20, 1).unwrap().0, -1);
        assert_eq!(integer_line_side(&geometry, (20, 21), 21, 1).unwrap().0, 1);
        let geometry = Geometry {
            kind: "trend_segment".into(),
            first: Point {
                time_ms: 0,
                price: 1.0,
            },
            second: Point {
                time_ms: i64::MAX - 1,
                price: 1.0,
            },
            extend: "both".into(),
        };
        let center = (i64::MAX - 1) / 2;
        assert_eq!(
            integer_line_side(
                &geometry,
                (MAX_TICK_ORDINAL - 2, MAX_TICK_ORDINAL),
                MAX_TICK_ORDINAL - 1,
                center
            )
            .unwrap()
            .0,
            0
        );
        assert!(integer_line_side(
            &geometry,
            (MAX_TICK_ORDINAL - 2, MAX_TICK_ORDINAL),
            MAX_TICK_ORDINAL,
            i64::MAX
        )
        .is_none());
    }
    #[tokio::test]
    async fn database_backpressure_retains_crossing_before_concurrent_move() {
        let store = fixture().await;
        let value = serde_json::json!({"mutationId":"storage-fault-create","symbol":"BTCUSDT","interval":"60","status":"active","geometry":{"kind":"horizontal_segment","first":{"timeMs":1,"price":100.0},"second":{"timeMs":2,"price":100.0},"extend":"both"},"webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{}"});
        let alert = store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        store
            .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
            .await
            .unwrap();
        sqlx::query("CREATE TRIGGER isolated_alert_fault BEFORE INSERT ON price_alert_events BEGIN SELECT RAISE(FAIL,'isolated temporary fault'); END")
            .execute(&store.write_pool).await.unwrap();
        let evaluator = store.clone();
        let crossing = tokio::spawn(async move {
            evaluator
                .evaluate_drawing_alerts("BTCUSDT", 110.0, 11)
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!crossing.is_finished());
        let mut geometry = alert.geometry.clone();
        geometry.first.price = 120.0;
        geometry.second.price = 120.0;
        let patch = serde_json::json!({"mutationId":"storage-fault-move","expectedRevision":1,"geometry":geometry});
        let editor = store.clone();
        let edit = tokio::spawn(async move {
            editor
                .patch_price_alert(
                    alert.id,
                    serde_json::from_value(patch.clone()).unwrap(),
                    &patch.to_string(),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!edit.is_finished());
        sqlx::query("DROP TRIGGER isolated_alert_fault")
            .execute(&store.write_pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), crossing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(edit.await.unwrap(), Err(Error::Conflict(_))));
        let events = store.price_alert_events(alert.id).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trigger_price, 110.0);
        assert_eq!(events[0].line_price, 100.0);
    }
    #[tokio::test]
    async fn delivery_shutdown_does_not_wait_for_a_blocked_mutation_gate() {
        let store = fixture().await;
        let value = serde_json::json!({"mutationId":"shutdown-lock","symbol":"BTCUSDT","interval":"60","status":"active","geometry":{"kind":"horizontal_segment","first":{"timeMs":1,"price":100.0},"second":{"timeMs":2,"price":100.0},"extend":"both"},"webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{}"});
        store
            .create_price_alert(
                serde_json::from_value(value.clone()).unwrap(),
                &value.to_string(),
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        store
            .evaluate_drawing_alerts("BTCUSDT", 90.0, now)
            .await
            .unwrap();
        store
            .evaluate_drawing_alerts("BTCUSDT", 110.0, now + 1)
            .await
            .unwrap();
        let _guard = store.price_alert_runtime.lock().await;
        let delivery = start_delivery(store.clone()).unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        tokio::time::timeout(Duration::from_secs(1), delivery.stop())
            .await
            .unwrap();
    }
}
