use crate::domain::candle::Candle;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use std::collections::HashMap;

pub const DEFAULT_RETENTION_BARS: u32 = 5_000;
const DEFAULT_READ_CONNECTIONS: u32 = 4;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    retention_bars: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredKline {
    pub symbol: String,
    pub interval: String,
    pub candle: Candle,
}

const UPSERT_CANDLE_SQL: &str = r#"
    INSERT INTO klines (
        symbol, interval, open_time, close_time, open, high, low, close,
        volume, quote_volume, trade_count, is_closed, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS klines (
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
            );
            "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_candle(
        &self,
        symbol: &str,
        interval: &str,
        candle: &Candle,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(UPSERT_CANDLE_SQL)
            .bind(symbol)
            .bind(interval)
            .bind(candle.open_time)
            .bind(candle.close_time)
            .bind(candle.open)
            .bind(candle.high)
            .bind(candle.low)
            .bind(candle.close)
            .bind(candle.volume)
            .bind(candle.quote_volume)
            .bind(candle.trade_count as i64)
            .bind(i64::from(candle.is_closed))
            .bind(chrono::Utc::now().timestamp_millis())
            .execute(&self.write_pool)
            .await?;
        self.prune_series(symbol, interval).await?;
        Ok(())
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

        self.prune_series_in_transaction(&mut tx, symbol, interval)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_candle_groups(
        &self,
        grouped: &HashMap<(String, String), Vec<Candle>>,
    ) -> Result<(), sqlx::Error> {
        if grouped.values().all(Vec::is_empty) {
            return Ok(());
        }

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

        for ((symbol, interval), candles) in grouped {
            if candles.is_empty() {
                continue;
            }

            self.prune_series_in_transaction(&mut tx, symbol, interval)
                .await?;
        }

        tx.commit().await?;
        Ok(())
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

    async fn prune_series(&self, symbol: &str, interval: &str) -> Result<(), sqlx::Error> {
        if self.retention_bars == 0 {
            return Ok(());
        }

        sqlx::query(PRUNE_SERIES_SQL)
            .bind(symbol)
            .bind(interval)
            .bind(symbol)
            .bind(interval)
            .bind(i64::from(self.retention_bars))
            .execute(&self.write_pool)
            .await?;
        Ok(())
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
}

fn is_memory_database(database_url: &str) -> bool {
    let database_url = database_url.to_ascii_lowercase();
    database_url.contains(":memory:") || database_url.contains("mode=memory")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::time::timeout;

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
