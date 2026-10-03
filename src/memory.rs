use crate::{domain::candle::Candle, storage::sqlite::StoredKline};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::{watch, RwLock};

pub const DEFAULT_MEMORY_SERIES_LIMIT: usize = 5_000;

#[derive(Debug, Clone)]
pub struct LatestCache {
    inner: Arc<RwLock<HashMap<(String, String), Candle>>>,
    live_symbols: Arc<RwLock<HashMap<String, LiveSymbolSnapshot>>>,
    market_update_lock: Arc<RwLock<()>>,
    recovery_changes: Arc<watch::Sender<u64>>,
}

impl Default for LatestCache {
    fn default() -> Self {
        let (recovery_changes, _) = watch::channel(0);
        Self {
            inner: Arc::default(),
            live_symbols: Arc::default(),
            market_update_lock: Arc::default(),
            recovery_changes: Arc::new(recovery_changes),
        }
    }
}

/// One fully applied trade across every configured interval for a symbol.
/// `close_time` on a dynamic candle is the future bucket end; freshness instead
/// comes from the actual market event and local receipt timestamps below.
#[derive(Debug, Clone)]
pub struct LiveSymbolSnapshot {
    pub candles: HashMap<String, Candle>,
    pub market_event_time_ms: i64,
    pub received_at_ms: i64,
    pub sequence: u64,
    pub generation: u64,
    pub recovering: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ClosedKlineBuffer {
    inner: Arc<RwLock<BTreeMap<(String, String, i64), Candle>>>,
}

impl ClosedKlineBuffer {
    pub async fn upsert(&self, symbol: &str, interval: &str, mut candle: Candle) -> usize {
        candle.is_closed = true;
        let mut inner = self.inner.write().await;
        inner.insert(
            (
                symbol.to_uppercase(),
                interval.to_string(),
                candle.open_time,
            ),
            candle,
        );
        inner.len()
    }

    pub async fn query(
        &self,
        symbol: &str,
        interval: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: u32,
    ) -> Vec<StoredKline> {
        let symbol = symbol.to_uppercase();
        let interval = interval.to_string();
        let inner = self.inner.read().await;
        let mut result = inner
            .iter()
            .filter(|((row_symbol, row_interval, open_time), _)| {
                row_symbol == &symbol
                    && row_interval == &interval
                    && start_time.map(|start| *open_time >= start).unwrap_or(true)
                    && end_time.map(|end| *open_time <= end).unwrap_or(true)
            })
            .map(|((row_symbol, row_interval, _), candle)| StoredKline {
                symbol: row_symbol.clone(),
                interval: row_interval.clone(),
                candle: candle.clone(),
            })
            .collect::<Vec<_>>();

        if result.len() > limit as usize {
            let start = result.len() - limit as usize;
            result = result.split_off(start);
        }

        result
    }

    pub async fn drain_grouped(&self) -> HashMap<(String, String), Vec<Candle>> {
        let mut inner = self.inner.write().await;
        let rows = std::mem::take(&mut *inner);
        let mut grouped = HashMap::<(String, String), Vec<Candle>>::new();

        for ((symbol, interval, _), candle) in rows {
            grouped.entry((symbol, interval)).or_default().push(candle);
        }

        grouped
    }

    pub async fn snapshot_grouped(&self) -> HashMap<(String, String), Vec<Candle>> {
        let inner = self.inner.read().await;
        let mut grouped = HashMap::<(String, String), Vec<Candle>>::new();

        for ((symbol, interval, _), candle) in inner.iter() {
            grouped
                .entry((symbol.clone(), interval.clone()))
                .or_default()
                .push(candle.clone());
        }

        grouped
    }

    pub async fn remove_flushed(&self, grouped: &HashMap<(String, String), Vec<Candle>>) -> usize {
        let mut inner = self.inner.write().await;

        for ((symbol, interval), candles) in grouped {
            for candle in candles {
                let key = (symbol.clone(), interval.clone(), candle.open_time);
                if inner.get(&key) == Some(candle) {
                    inner.remove(&key);
                }
            }
        }

        inner.len()
    }

    pub async fn requeue_grouped(&self, grouped: HashMap<(String, String), Vec<Candle>>) -> usize {
        let mut inner = self.inner.write().await;
        for ((symbol, interval), candles) in grouped {
            for candle in candles {
                inner.insert((symbol.clone(), interval.clone(), candle.open_time), candle);
            }
        }
        inner.len()
    }
}

impl LatestCache {
    /// Writers hold this lock while changing historical rows and live candles.
    /// Readers hold it only while copying a consistent input snapshot, then
    /// release it before indicator calculation. Cache methods deliberately do
    /// not acquire it again, so they can be called inside either guard.
    pub fn market_update_lock(&self) -> Arc<RwLock<()>> {
        self.market_update_lock.clone()
    }

    /// Subscribe to the beginning of a new recovery generation for any symbol.
    /// Consumers revalidate their own symbol; repeated retries and ordinary
    /// market updates do not wake senders or cancel unrelated requests.
    pub fn recovery_changes(&self) -> watch::Receiver<u64> {
        self.recovery_changes.subscribe()
    }

    pub async fn live_snapshot(&self, symbol: &str) -> Option<LiveSymbolSnapshot> {
        self.live_symbols
            .read()
            .await
            .get(&symbol.to_uppercase())
            .cloned()
    }

    /// Lightweight timestamp lookup for the hot trade path, without copying
    /// every interval's candle merely to reject an old message.
    pub async fn live_market_event_time_ms(&self, symbol: &str) -> Option<i64> {
        self.live_symbols
            .read()
            .await
            .get(&symbol.to_uppercase())
            .map(|snapshot| snapshot.market_event_time_ms)
    }

    /// Publishes once all intervals have incorporated the same accepted trade.
    /// The caller owns the market update write guard.
    pub async fn publish_live_symbol(
        &self,
        symbol: &str,
        candles: HashMap<String, Candle>,
        market_event_time_ms: i64,
        received_at_ms: i64,
    ) -> u64 {
        let mut live_symbols = self.live_symbols.write().await;
        let previous = live_symbols.get(&symbol.to_uppercase());
        let sequence = previous.map_or(1, |snapshot| snapshot.sequence.saturating_add(1));
        let generation = previous.map_or(1, |snapshot| snapshot.generation);
        live_symbols.insert(
            symbol.to_uppercase(),
            LiveSymbolSnapshot {
                candles,
                market_event_time_ms,
                received_at_ms,
                sequence,
                generation,
                recovering: false,
            },
        );
        sequence
    }

    /// Invalidate a symbol without promoting old data to a fresh market update.
    /// Repeated failed reconnect attempts remain in the same generation.
    /// The caller owns the market update write guard.
    pub async fn mark_recovering(&self, symbol: &str) {
        let mut live_symbols = self.live_symbols.write().await;
        let snapshot = live_symbols
            .entry(symbol.to_uppercase())
            .or_insert_with(|| LiveSymbolSnapshot {
                candles: HashMap::new(),
                market_event_time_ms: 0,
                received_at_ms: 0,
                sequence: 0,
                generation: 0,
                recovering: false,
            });
        let new_recovery = !snapshot.recovering;
        if new_recovery {
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.recovering = true;
        }
        snapshot.candles.clear();
        drop(live_symbols);
        if new_recovery {
            self.recovery_changes
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }

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

    pub async fn clear(&self, symbol: &str, interval: &str) {
        self.inner
            .write()
            .await
            .remove(&(symbol.to_uppercase(), interval.to_string()));
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
