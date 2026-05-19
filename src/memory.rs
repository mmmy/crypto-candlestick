use crate::{domain::candle::Candle, storage::sqlite::StoredKline};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub const DEFAULT_MEMORY_SERIES_LIMIT: usize = 5_000;

#[derive(Debug, Clone, Default)]
pub struct LatestCache {
    inner: Arc<RwLock<HashMap<(String, String), Candle>>>,
}

impl LatestCache {
    pub async fn upsert(&self, symbol: &str, interval: &str, candle: Candle) {
        self.inner
            .write()
            .await
            .insert((symbol.to_uppercase(), interval.to_string()), candle);
    }

    pub async fn remove(&self, symbol: &str, interval: &str) {
        self.inner
            .write()
            .await
            .remove(&(symbol.to_uppercase(), interval.to_string()));
    }

    pub async fn get(&self, symbol: &str, interval: &str) -> Option<Candle> {
        self.inner
            .read()
            .await
            .get(&(symbol.to_uppercase(), interval.to_string()))
            .cloned()
    }
}

#[derive(Debug, Clone)]
pub struct MemorySeriesStore {
    limit: usize,
    inner: Arc<RwLock<HashMap<(String, String), Vec<Candle>>>>,
}

impl Default for MemorySeriesStore {
    fn default() -> Self {
        Self::new(DEFAULT_MEMORY_SERIES_LIMIT)
    }
}

impl MemorySeriesStore {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn push_closed(&self, symbol: &str, interval: &str, mut candle: Candle) {
        candle.is_closed = true;
        let mut inner = self.inner.write().await;
        let rows = inner
            .entry((symbol.to_uppercase(), interval.to_string()))
            .or_default();

        match rows.binary_search_by_key(&candle.open_time, |row| row.open_time) {
            Ok(index) => rows[index] = candle,
            Err(index) => rows.insert(index, candle),
        }

        if rows.len() > self.limit {
            let excess = rows.len() - self.limit;
            rows.drain(0..excess);
        }
    }

    pub async fn query(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Vec<StoredKline> {
        let inner = self.inner.read().await;
        let Some(rows) = inner.get(&(symbol.to_uppercase(), interval.to_string())) else {
            return Vec::new();
        };

        let mut result = rows
            .iter()
            .filter(|candle| {
                start_time
                    .map(|start| candle.open_time >= start)
                    .unwrap_or(true)
            })
            .filter(|candle| end_time.map(|end| candle.open_time <= end).unwrap_or(true))
            .cloned()
            .map(|candle| StoredKline {
                symbol: symbol.to_uppercase(),
                interval: interval.to_string(),
                candle,
            })
            .collect::<Vec<_>>();

        if result.len() > limit as usize {
            let start = result.len() - limit as usize;
            result = result.split_off(start);
        }

        result
    }
}
