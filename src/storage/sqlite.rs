use crate::domain::candle::Candle;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

pub const DEFAULT_RETENTION_BARS: u32 = 5_000;
const DEFAULT_READ_CONNECTIONS: u32 = 4;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    retention_bars: u32,
    prune_pending: Arc<Mutex<HashMap<(String, String), usize>>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    pub id: i64,
    pub symbol: String,
    pub interval: String,
    pub price: f64,
    pub direction: String,
    pub status: String,
    pub expires_at: Option<i64>,
    pub webhook_url: String,
    pub message_template: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub triggered_at: Option<i64>,
    pub delivery_status: Option<String>,
    pub delivery_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertEvent {
    pub id: i64,
    pub alert_id: i64,
    pub triggered_at: i64,
    pub trigger_price: f64,
    pub direction: String,
    pub delivery_status: Option<String>,
    pub delivery_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredKline {
    pub symbol: String,
    pub interval: String,
    pub candle: Candle,
}

const UPSERT_CANDLE_CHUNK_ROWS: usize = 500;

const UPSERT_CANDLE_CONFLICT_SQL: &str = r#"
    ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
        close_time = excluded.close_time,
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume,
        quote_volume = excluded.quote_volume,
        trade_count = excluded.trade_count,
        is_closed = excluded.is_closed,
        updated_at = excluded.updated_at
    "#;

const PRUNE_SERIES_SQL: &str = r#"
    DELETE FROM klines
    WHERE symbol = ?
      AND interval = ?
      AND open_time < (
        SELECT COALESCE(MIN(open_time), 9223372036854775807)
        FROM (
            SELECT open_time
            FROM klines
            WHERE symbol = ?
              AND interval = ?
            ORDER BY open_time DESC
            LIMIT ?
        )
      )
    "#;

const CREATE_KLINES_TABLE_SQL: &str = r#"
    CREATE TABLE klines (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        quote_volume REAL NOT NULL,
        trade_count INTEGER NOT NULL,
        is_closed INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (symbol, interval, open_time)
    ) WITHOUT ROWID;
    "#;

const CREATE_ALERTS_TABLE_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        price REAL NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        expires_at INTEGER,
        webhook_url TEXT NOT NULL,
        message_template TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        triggered_at INTEGER,
        delivery_status TEXT,
        delivery_error TEXT
    );
    "#;

const CREATE_ALERT_EVENTS_TABLE_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS alert_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id INTEGER NOT NULL,
        triggered_at INTEGER NOT NULL,
        trigger_price REAL NOT NULL,
        direction TEXT NOT NULL,
        delivery_status TEXT,
        delivery_error TEXT,
        created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_alert_events_alert_id ON alert_events(alert_id, id DESC);
    "#;

#[derive(Clone, Copy)]
struct CandleUpsertRow<'a> {
    symbol: &'a str,
    interval: &'a str,
    candle: &'a Candle,
}

async fn upsert_candle_rows(
    tx: &mut Transaction<'_, Sqlite>,
    rows: &[CandleUpsertRow<'_>],
    updated_at: i64,
) -> Result<(), sqlx::Error> {
    for rows in rows.chunks(UPSERT_CANDLE_CHUNK_ROWS) {
        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO klines (\
                symbol, interval, open_time, close_time, open, high, low, close, \
                volume, quote_volume, trade_count, is_closed, updated_at\
            ) ",
        );
        query_builder.push_values(rows, |mut query_row, row| {
            query_row
                .push_bind(row.symbol)
                .push_bind(row.interval)
                .push_bind(row.candle.open_time)
                .push_bind(row.candle.close_time)
                .push_bind(row.candle.open)
                .push_bind(row.candle.high)
                .push_bind(row.candle.low)
                .push_bind(row.candle.close)
                .push_bind(row.candle.volume)
                .push_bind(row.candle.quote_volume)
                .push_bind(row.candle.trade_count as i64)
                .push_bind(i64::from(row.candle.is_closed))
                .push_bind(updated_at);
        });
        query_builder.push(UPSERT_CANDLE_CONFLICT_SQL);
        query_builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

impl SqliteStore {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::connect_with_retention(database_url, DEFAULT_RETENTION_BARS).await
    }

    pub async fn connect_with_retention(
        database_url: &str,
        retention_bars: u32,
    ) -> Result<Self, sqlx::Error> {
        let write_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("PRAGMA synchronous = NORMAL;")
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("PRAGMA busy_timeout = 5000;")
                        .execute(&mut *connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        let mut store = Self {
            read_pool: write_pool.clone(),
            write_pool,
            retention_bars,
            prune_pending: Arc::new(Mutex::new(HashMap::new())),
        };
        store.init().await?;

        if !is_memory_database(database_url) {
            store.read_pool = SqlitePoolOptions::new()
                .max_connections(DEFAULT_READ_CONNECTIONS)
                .after_connect(|connection, _metadata| {
                    Box::pin(async move {
                        sqlx::query("PRAGMA busy_timeout = 5000;")
                            .execute(&mut *connection)
                            .await?;
                        sqlx::query("PRAGMA query_only = TRUE;")
                            .execute(&mut *connection)
                            .await?;
                        Ok(())
                    })
                })
                .connect(database_url)
                .await?;
        }

        Ok(store)
    }

    async fn init(&self) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&self.write_pool)
            .await?;
        self.ensure_without_rowid_schema().await?;
        sqlx::query(CREATE_ALERTS_TABLE_SQL)
            .execute(&self.write_pool)
            .await?;
        for statement in CREATE_ALERT_EVENTS_TABLE_SQL
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            sqlx::query(statement).execute(&self.write_pool).await?;
        }
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_alerts_symbol_status ON alerts(symbol, status)",
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn ensure_without_rowid_schema(&self) -> Result<(), sqlx::Error> {
        let schema = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'klines'",
        )
        .fetch_optional(&self.write_pool)
        .await?;

        match schema {
            None => {
                sqlx::query(CREATE_KLINES_TABLE_SQL)
                    .execute(&self.write_pool)
                    .await?;
            }
            Some(schema) if schema.to_ascii_uppercase().contains("WITHOUT ROWID") => {}
            Some(_) => return Err(sqlx::Error::Protocol(
                "klines table must use WITHOUT ROWID; migrate the database before starting the service"
                    .to_string(),
            )),
        }

        Ok(())
    }

    pub async fn upsert_candle(
        &self,
        symbol: &str,
        interval: &str,
        candle: &Candle,
    ) -> Result<(), sqlx::Error> {
        self.upsert_candles(symbol, interval, std::slice::from_ref(candle))
            .await
    }

    pub async fn upsert_candles(
        &self,
        symbol: &str,
        interval: &str,
        candles: &[Candle],
    ) -> Result<(), sqlx::Error> {
        if candles.is_empty() {
            return Ok(());
        }

        let key = (symbol.to_string(), interval.to_string());
        let mut prune_pending = self.prune_pending.lock().await;
        let should_prune = self.should_prune_series(&prune_pending, &key, candles.len());
        let updated_at = chrono::Utc::now().timestamp_millis();
        let mut tx = self.write_pool.begin().await?;
        let rows = candles
            .iter()
            .map(|candle| CandleUpsertRow {
                symbol,
                interval,
                candle,
            })
            .collect::<Vec<_>>();
        upsert_candle_rows(&mut tx, &rows, updated_at).await?;

        if should_prune {
            self.prune_series_in_transaction(&mut tx, symbol, interval)
                .await?;
        }
        tx.commit().await?;
        self.record_prune_progress(&mut prune_pending, key, candles.len(), should_prune);
        Ok(())
    }

    pub async fn upsert_candle_groups(
        &self,
        grouped: &HashMap<(String, String), Vec<Candle>>,
    ) -> Result<(), sqlx::Error> {
        if grouped.values().all(Vec::is_empty) {
            return Ok(());
        }

        let series_rows = grouped
            .iter()
            .filter(|(_, candles)| !candles.is_empty())
            .map(|(key, candles)| (key.clone(), candles.len()))
            .collect::<Vec<_>>();
        let mut prune_pending = self.prune_pending.lock().await;
        let series_to_prune = series_rows
            .iter()
            .filter(|(key, rows)| self.should_prune_series(&prune_pending, key, *rows))
            .map(|(key, _)| key.clone())
            .collect::<HashSet<_>>();
        let updated_at = chrono::Utc::now().timestamp_millis();
        let mut tx = self.write_pool.begin().await?;

        let rows = grouped
            .iter()
            .flat_map(|((symbol, interval), candles)| {
                candles.iter().map(move |candle| CandleUpsertRow {
                    symbol,
                    interval,
                    candle,
                })
            })
            .collect::<Vec<_>>();
        upsert_candle_rows(&mut tx, &rows, updated_at).await?;

        for (symbol, interval) in &series_to_prune {
            self.prune_series_in_transaction(&mut tx, symbol, interval)
                .await?;
        }

        tx.commit().await?;
        for (key, rows) in series_rows {
            let pruned = series_to_prune.contains(&key);
            self.record_prune_progress(&mut prune_pending, key, rows, pruned);
        }
        Ok(())
    }

    pub async fn delete_klines_with_different_phase(
        &self,
        symbol: &str,
        interval: &str,
        interval_ms: i64,
        phase_ms: i64,
        start_time: i64,
        end_time: i64,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM klines \
             WHERE symbol = ? AND interval = ? \
               AND open_time >= ? AND open_time <= ? \
               AND (open_time % ?) != ?",
        )
        .bind(symbol)
        .bind(interval)
        .bind(start_time)
        .bind(end_time)
        .bind(interval_ms)
        .bind(phase_ms)
        .execute(&self.write_pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn query_klines(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Result<Vec<StoredKline>, sqlx::Error> {
        let select_columns = "symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, trade_count, is_closed";
        let mut sql = if start_time.is_some() {
            format!("SELECT {select_columns} FROM klines WHERE symbol = ? AND interval = ?")
        } else {
            format!(
                "SELECT {select_columns} FROM (SELECT {select_columns} FROM klines WHERE symbol = ? AND interval = ?"
            )
        };
        if start_time.is_some() {
            sql.push_str(" AND open_time >= ?");
        }
        if end_time.is_some() {
            sql.push_str(" AND open_time <= ?");
        }
        if start_time.is_some() {
            sql.push_str(" ORDER BY open_time ASC LIMIT ?");
        } else {
            sql.push_str(" ORDER BY open_time DESC LIMIT ?) ORDER BY open_time ASC");
        }

        let mut query = sqlx::query(&sql).bind(symbol).bind(interval);
        if let Some(value) = start_time {
            query = query.bind(value);
        }
        if let Some(value) = end_time {
            query = query.bind(value);
        }
        query = query.bind(i64::from(limit));

        let rows = query.fetch_all(&self.read_pool).await?;
        let mut items = Vec::with_capacity(rows.len());
        let now_ms = chrono::Utc::now().timestamp_millis();
        for row in rows {
            let close_time = row.try_get::<i64, _>("close_time")?;
            items.push(StoredKline {
                symbol: row.try_get::<String, _>("symbol")?,
                interval: row.try_get::<String, _>("interval")?,
                candle: Candle {
                    open_time: row.try_get::<i64, _>("open_time")?,
                    close_time,
                    open: row.try_get::<f64, _>("open")?,
                    high: row.try_get::<f64, _>("high")?,
                    low: row.try_get::<f64, _>("low")?,
                    close: row.try_get::<f64, _>("close")?,
                    volume: row.try_get::<f64, _>("volume")?,
                    quote_volume: row.try_get::<f64, _>("quote_volume")?,
                    trade_count: row.try_get::<i64, _>("trade_count")? as u64,
                    is_closed: row.try_get::<i64, _>("is_closed")? != 0 && close_time < now_ms,
                },
            });
        }
        Ok(items)
    }

    pub async fn query_latest_klines_desc(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<StoredKline>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT symbol, interval, open_time, close_time, open, high, low, close, volume, quote_volume, trade_count, is_closed \
             FROM klines WHERE symbol = ? AND interval = ? ORDER BY open_time DESC LIMIT ?",
        )
        .bind(symbol)
        .bind(interval)
        .bind(i64::from(limit))
        .fetch_all(&self.read_pool)
        .await?;

        let mut items = Vec::with_capacity(rows.len());
        let now_ms = chrono::Utc::now().timestamp_millis();
        for row in rows {
            let close_time = row.try_get::<i64, _>("close_time")?;
            items.push(StoredKline {
                symbol: row.try_get::<String, _>("symbol")?,
                interval: row.try_get::<String, _>("interval")?,
                candle: Candle {
                    open_time: row.try_get::<i64, _>("open_time")?,
                    close_time,
                    open: row.try_get::<f64, _>("open")?,
                    high: row.try_get::<f64, _>("high")?,
                    low: row.try_get::<f64, _>("low")?,
                    close: row.try_get::<f64, _>("close")?,
                    volume: row.try_get::<f64, _>("volume")?,
                    quote_volume: row.try_get::<f64, _>("quote_volume")?,
                    trade_count: row.try_get::<i64, _>("trade_count")? as u64,
                    is_closed: row.try_get::<i64, _>("is_closed")? != 0 && close_time < now_ms,
                },
            });
        }
        Ok(items)
    }

    pub async fn max_open_time(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<Option<i64>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT MAX(open_time) AS max_open_time FROM klines WHERE symbol = ? AND interval = ?",
        )
        .bind(symbol)
        .bind(interval)
        .fetch_one(&self.read_pool)
        .await?;

        row.try_get::<Option<i64>, _>("max_open_time")
    }

    fn should_prune_series(
        &self,
        prune_pending: &HashMap<(String, String), usize>,
        key: &(String, String),
        new_rows: usize,
    ) -> bool {
        if self.retention_bars == 0 {
            return false;
        }

        let Some(pending) = prune_pending.get(key).copied() else {
            return true;
        };

        pending.saturating_add(new_rows) >= prune_check_rows(self.retention_bars)
    }

    fn record_prune_progress(
        &self,
        prune_pending: &mut HashMap<(String, String), usize>,
        key: (String, String),
        new_rows: usize,
        pruned: bool,
    ) {
        if self.retention_bars == 0 {
            return;
        }

        if pruned {
            prune_pending.insert(key, 0);
        } else {
            let pending = prune_pending.entry(key).or_default();
            *pending = pending.saturating_add(new_rows);
        }
    }

    async fn prune_series_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        symbol: &str,
        interval: &str,
    ) -> Result<(), sqlx::Error> {
        if self.retention_bars == 0 {
            return Ok(());
        }

        sqlx::query(PRUNE_SERIES_SQL)
            .bind(symbol)
            .bind(interval)
            .bind(symbol)
            .bind(interval)
            .bind(i64::from(self.retention_bars))
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub async fn list_alerts(&self) -> Result<Vec<Alert>, sqlx::Error> {
        let rows = sqlx::query("SELECT id,symbol,interval,price,direction,status,expires_at,webhook_url,message_template,created_at,updated_at,triggered_at,delivery_status,delivery_error FROM alerts ORDER BY id DESC")
            .fetch_all(&self.read_pool).await?;
        rows.into_iter().map(alert_from_row).collect()
    }

    pub async fn get_alert(&self, id: i64) -> Result<Option<Alert>, sqlx::Error> {
        let row = sqlx::query("SELECT id,symbol,interval,price,direction,status,expires_at,webhook_url,message_template,created_at,updated_at,triggered_at,delivery_status,delivery_error FROM alerts WHERE id = ?")
            .bind(id).fetch_optional(&self.read_pool).await?;
        row.map(alert_from_row).transpose()
    }

    pub async fn insert_alert(&self, alert: &Alert) -> Result<Alert, sqlx::Error> {
        let result = sqlx::query("INSERT INTO alerts (symbol,interval,price,direction,status,expires_at,webhook_url,message_template,created_at,updated_at,triggered_at,delivery_status,delivery_error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&alert.symbol).bind(&alert.interval).bind(alert.price).bind(&alert.direction)
            .bind(&alert.status).bind(alert.expires_at).bind(&alert.webhook_url).bind(&alert.message_template)
            .bind(alert.created_at).bind(alert.updated_at).bind(alert.triggered_at).bind(&alert.delivery_status).bind(&alert.delivery_error)
            .execute(&self.write_pool).await?;
        let mut created = alert.clone();
        created.id = result.last_insert_rowid();
        Ok(created)
    }

    pub async fn update_alert(&self, alert: &Alert) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE alerts SET symbol=?,interval=?,price=?,direction=?,status=?,expires_at=?,webhook_url=?,message_template=?,updated_at=? WHERE id=?")
            .bind(&alert.symbol).bind(&alert.interval).bind(alert.price).bind(&alert.direction)
            .bind(&alert.status).bind(alert.expires_at).bind(&alert.webhook_url).bind(&alert.message_template)
            .bind(alert.updated_at).bind(alert.id).execute(&self.write_pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn delete_alert(&self, id: i64) -> Result<bool, sqlx::Error> {
        let mut tx = self.write_pool.begin().await?;
        sqlx::query("DELETE FROM alert_events WHERE alert_id=?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        let deleted = sqlx::query("DELETE FROM alerts WHERE id=?")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        tx.commit().await?;
        Ok(deleted)
    }

    pub async fn active_alerts_for_symbol(
        &self,
        symbol: &str,
        now_ms: i64,
    ) -> Result<Vec<Alert>, sqlx::Error> {
        let rows = sqlx::query("SELECT id,symbol,interval,price,direction,status,expires_at,webhook_url,message_template,created_at,updated_at,triggered_at,delivery_status,delivery_error FROM alerts WHERE symbol=? AND status='active' AND (expires_at IS NULL OR expires_at > ?)")
            .bind(symbol).bind(now_ms).fetch_all(&self.read_pool).await?;
        rows.into_iter().map(alert_from_row).collect()
    }

    pub async fn claim_alert(&self, id: i64, now_ms: i64) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE alerts SET status='triggered',triggered_at=?,updated_at=? WHERE id=? AND status='active' AND (expires_at IS NULL OR expires_at > ?)")
            .bind(now_ms).bind(now_ms).bind(id).bind(now_ms).execute(&self.write_pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn claim_alert_with_event(
        &self,
        id: i64,
        now_ms: i64,
        trigger_price: f64,
        direction: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.write_pool.begin().await?;
        let result = sqlx::query("UPDATE alerts SET status='triggered',triggered_at=?,updated_at=? WHERE id=? AND status='active' AND (expires_at IS NULL OR expires_at > ?)")
            .bind(now_ms).bind(now_ms).bind(id).bind(now_ms).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query("INSERT INTO alert_events (alert_id,triggered_at,trigger_price,direction,delivery_status,delivery_error,created_at) VALUES (?,?,?,?,?,?,?)")
            .bind(id).bind(now_ms).bind(trigger_price).bind(direction).bind(Option::<String>::None).bind(Option::<String>::None).bind(now_ms)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn list_alert_events(&self, alert_id: i64) -> Result<Vec<AlertEvent>, sqlx::Error> {
        let rows = sqlx::query("SELECT id,alert_id,triggered_at,trigger_price,direction,delivery_status,delivery_error FROM alert_events WHERE alert_id=? ORDER BY id DESC")
            .bind(alert_id).fetch_all(&self.read_pool).await?;
        rows.into_iter().map(alert_event_from_row).collect()
    }

    pub async fn set_alert_delivery(
        &self,
        id: i64,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE alerts SET delivery_status=?,delivery_error=?,updated_at=? WHERE id=?")
            .bind(status)
            .bind(error)
            .bind(chrono::Utc::now().timestamp_millis())
            .bind(id)
            .execute(&self.write_pool)
            .await?;
        sqlx::query("UPDATE alert_events SET delivery_status=?,delivery_error=? WHERE id=(SELECT id FROM alert_events WHERE alert_id=? ORDER BY id DESC LIMIT 1)")
            .bind(status).bind(error).bind(id).execute(&self.write_pool).await?;
        Ok(())
    }
}

fn alert_from_row(row: sqlx::sqlite::SqliteRow) -> Result<Alert, sqlx::Error> {
    Ok(Alert {
        id: row.try_get("id")?,
        symbol: row.try_get("symbol")?,
        interval: row.try_get("interval")?,
        price: row.try_get("price")?,
        direction: row.try_get("direction")?,
        status: row.try_get("status")?,
        expires_at: row.try_get("expires_at")?,
        webhook_url: row.try_get("webhook_url")?,
        message_template: row.try_get("message_template")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        triggered_at: row.try_get("triggered_at")?,
        delivery_status: row.try_get("delivery_status")?,
        delivery_error: row.try_get("delivery_error")?,
    })
}

fn alert_event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AlertEvent, sqlx::Error> {
    Ok(AlertEvent {
        id: row.try_get("id")?,
        alert_id: row.try_get("alert_id")?,
        triggered_at: row.try_get("triggered_at")?,
        trigger_price: row.try_get("trigger_price")?,
        direction: row.try_get("direction")?,
        delivery_status: row.try_get("delivery_status")?,
        delivery_error: row.try_get("delivery_error")?,
    })
}

fn is_memory_database(database_url: &str) -> bool {
    let database_url = database_url.to_ascii_lowercase();
    database_url.contains(":memory:") || database_url.contains("mode=memory")
}

fn prune_check_rows(retention_bars: u32) -> usize {
    if retention_bars < 100 {
        1
    } else {
        (retention_bars / 20).max(100) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::timeout;

    #[tokio::test]
    async fn removes_only_klines_outside_the_authoritative_phase() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let interval_ms = 3 * 86_400_000;
        for open_time in [86_400_000, 172_800_000, 345_600_000, 518_400_000] {
            store
                .upsert_candle(
                    "BTCUSDT",
                    "3D",
                    &Candle {
                        open_time,
                        close_time: open_time + interval_ms - 1,
                        open: 100.0,
                        high: 101.0,
                        low: 99.0,
                        close: 100.5,
                        volume: 1.0,
                        quote_volume: 100.5,
                        trade_count: 1,
                        is_closed: true,
                    },
                )
                .await
                .unwrap();
        }

        let deleted = store
            .delete_klines_with_different_phase(
                "BTCUSDT",
                "3D",
                interval_ms,
                86_400_000,
                0,
                400_000_000,
            )
            .await
            .unwrap();
        let rows = store
            .query_klines("BTCUSDT", "3D", None, None, 10)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(
            rows.iter()
                .map(|row| row.candle.open_time)
                .collect::<Vec<_>>(),
            vec![86_400_000, 345_600_000, 518_400_000]
        );
    }

    #[tokio::test]
    async fn file_database_reads_do_not_wait_for_the_writer_connection() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database_path = std::env::temp_dir().join(format!(
            "crypto-candlestick-read-pool-{}-{unique}.db",
            std::process::id()
        ));
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.to_string_lossy().replace('\\', "/")
        );
        let store = SqliteStore::connect_with_retention(&database_url, 0)
            .await
            .unwrap();

        assert_eq!(
            write_connection_pragmas(&store.write_pool).await,
            (1, 5_000)
        );
        store
            .write_pool
            .acquire()
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        assert_eq!(
            write_connection_pragmas(&store.write_pool).await,
            (1, 5_000)
        );

        assert!(store
            .query_klines("BTCUSDT", "1", None, None, 10)
            .await
            .unwrap()
            .is_empty());

        let candle = Candle {
            open_time: 60_000,
            close_time: 119_999,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 12.5,
            quote_volume: 1_250.0,
            trade_count: 3,
            is_closed: true,
        };
        let mut tx = store.write_pool.begin().await.unwrap();
        upsert_candle_rows(
            &mut tx,
            &[CandleUpsertRow {
                symbol: "BTCUSDT",
                interval: "1",
                candle: &candle,
            }],
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();

        let rows = timeout(
            Duration::from_millis(500),
            store.query_klines("BTCUSDT", "1", None, None, 10),
        )
        .await
        .expect("read pool should not wait for the writer connection")
        .unwrap();
        assert!(rows.is_empty());

        tx.commit().await.unwrap();
        assert_eq!(
            store
                .query_klines("BTCUSDT", "1", None, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        store.read_pool.close().await;
        store.write_pool.close().await;
        for path in [
            database_path.clone(),
            database_path.with_extension("db-wal"),
            database_path.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn rejects_a_legacy_rowid_table() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database_path = std::env::temp_dir().join(format!(
            "crypto-candlestick-legacy-rowid-{}-{unique}.db",
            std::process::id()
        ));
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.to_string_lossy().replace('\\', "/")
        );

        let legacy_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let legacy_schema = CREATE_KLINES_TABLE_SQL.replace(" WITHOUT ROWID", "");
        sqlx::query(&legacy_schema)
            .execute(&legacy_pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO klines (
                symbol, interval, open_time, close_time, open, high, low, close,
                volume, quote_volume, trade_count, is_closed, updated_at
            ) VALUES ('BTCUSDT', '1', 60000, 119999, 100.0, 101.0, 99.0, 100.5,
                      12.5, 1250.0, 3, 1, 60000)
            "#,
        )
        .execute(&legacy_pool)
        .await
        .unwrap();
        legacy_pool.close().await;

        let error = SqliteStore::connect_with_retention(&database_url, 0)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("must use WITHOUT ROWID"));

        let verification_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let schema = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'klines'",
        )
        .fetch_one(&verification_pool)
        .await
        .unwrap();
        let row_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM klines")
            .fetch_one(&verification_pool)
            .await
            .unwrap();
        assert!(!schema.to_ascii_uppercase().contains("WITHOUT ROWID"));
        assert_eq!(row_count, 1);
        verification_pool.close().await;

        for path in [
            database_path.clone(),
            database_path.with_extension("db-wal"),
            database_path.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn first_write_after_restart_prunes_existing_rows() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database_path = std::env::temp_dir().join(format!(
            "crypto-candlestick-restart-prune-{}-{unique}.db",
            std::process::id()
        ));
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.to_string_lossy().replace('\\', "/")
        );

        let store = SqliteStore::connect_with_retention(&database_url, 0)
            .await
            .unwrap();
        let candles = (0..299)
            .map(|index| {
                let open_time = index * 60_000;
                Candle {
                    open_time,
                    close_time: open_time + 59_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 12.5,
                    quote_volume: 1_250.0,
                    trade_count: 3,
                    is_closed: true,
                }
            })
            .collect::<Vec<_>>();
        store
            .upsert_candles("BTCUSDT", "1", &candles)
            .await
            .unwrap();
        store.read_pool.close().await;
        store.write_pool.close().await;

        let store = SqliteStore::connect_with_retention(&database_url, 200)
            .await
            .unwrap();
        let open_time = 299 * 60_000;
        store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &Candle {
                    open_time,
                    close_time: open_time + 59_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 12.5,
                    quote_volume: 1_250.0,
                    trade_count: 3,
                    is_closed: true,
                },
            )
            .await
            .unwrap();

        let rows = store
            .query_klines("BTCUSDT", "1", None, None, 500)
            .await
            .unwrap();
        assert_eq!(rows.len(), 200);
        assert_eq!(rows[0].candle.open_time, 100 * 60_000);
        assert_eq!(rows[199].candle.open_time, 299 * 60_000);

        store.read_pool.close().await;
        store.write_pool.close().await;
        for path in [
            database_path.clone(),
            database_path.with_extension("db-wal"),
            database_path.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }

    async fn write_connection_pragmas(pool: &SqlitePool) -> (i64, i64) {
        let synchronous = sqlx::query("PRAGMA synchronous;")
            .fetch_one(pool)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        let busy_timeout = sqlx::query("PRAGMA busy_timeout;")
            .fetch_one(pool)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        (synchronous, busy_timeout)
    }
}
