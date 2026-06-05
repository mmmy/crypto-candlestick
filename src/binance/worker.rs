use super::types::{parse_combined_stream_message, MarketEvent};
use crate::{
    binance::rest::sync_native_klines,
    domain::{candle::Candle, interval::Interval},
    engine::aggregator::{Aggregator, TradeTick},
    memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
    runtime_health::{RuntimeHealth, WS_IDLE_TIMEOUT},
    storage::sqlite::SqliteStore,
};
use futures_util::StreamExt;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const BINANCE_USDM_MARKET_WS_BASE: &str = "wss://fstream.binance.com/market/stream";

#[derive(Debug, Clone)]
pub struct KlineSource {
    pub symbol: String,
    pub canonical_interval: String,
    pub binance_interval: &'static str,
    pub interval: Interval,
}

#[derive(Debug, Clone)]
pub struct SubscriptionPlan {
    symbols: Vec<String>,
    intervals: Vec<Interval>,
}

impl SubscriptionPlan {
    pub fn new(symbols: Vec<String>, intervals: Vec<Interval>) -> Self {
        Self {
            symbols: symbols
                .into_iter()
                .map(|symbol| symbol.trim().to_uppercase())
                .filter(|symbol| !symbol.is_empty())
                .collect(),
            intervals,
        }
    }

    pub fn streams(&self) -> Vec<String> {
        let mut streams = BTreeSet::new();

        for symbol in &self.symbols {
            let stream_symbol = symbol.to_lowercase();
            for interval in &self.intervals {
                if interval.as_millis() < 60_000 {
                    streams.insert(format!("{stream_symbol}@aggTrade"));
                    continue;
                }

                if let Some(binance_interval) = interval.binance_interval().or_else(|| {
                    interval
                        .aggregation_base()
                        .and_then(|base| base.binance_interval())
                }) {
                    streams.insert(format!("{stream_symbol}@kline_{binance_interval}"));
                }
            }
        }

        streams.into_iter().collect()
    }

    pub fn stream_url(&self) -> String {
        format!(
            "{BINANCE_USDM_MARKET_WS_BASE}?streams={}",
            self.streams().join("/")
        )
    }

    pub fn aggregation_targets(&self) -> Vec<(String, String, String)> {
        let mut targets = BTreeSet::new();

        for symbol in &self.symbols {
            for interval in &self.intervals {
                if let Some(base) = interval.aggregation_base() {
                    targets.insert((symbol.clone(), base.canonical(), interval.canonical()));
                }
            }
        }

        targets.into_iter().collect()
    }

    pub fn kline_sources(&self) -> Vec<KlineSource> {
        let mut seen = BTreeSet::new();
        let mut sources = Vec::new();

        for symbol in &self.symbols {
            for interval in &self.intervals {
                if interval.as_millis() < 60_000 {
                    continue;
                }

                let source_interval = interval.aggregation_base().unwrap_or(*interval);
                if let Some(binance_interval) = source_interval.binance_interval() {
                    let key = (symbol.clone(), source_interval.canonical());
                    if seen.insert(key.clone()) {
                        sources.push(KlineSource {
                            symbol: symbol.clone(),
                            canonical_interval: key.1,
                            binance_interval,
                            interval: source_interval,
                        });
                    }
                }
            }
        }

        sources
    }

    fn second_targets(&self) -> Vec<(String, String)> {
        let mut targets = BTreeSet::new();

        for symbol in &self.symbols {
            for interval in &self.intervals {
                if interval.as_millis() < 60_000 {
                    targets.insert((symbol.clone(), interval.canonical()));
                }
            }
        }

        targets.into_iter().collect()
    }
}

pub struct BinanceWorker {
    store: SqliteStore,
    latest: LatestCache,
    memory_series: MemorySeriesStore,
    closed_buffer: ClosedKlineBuffer,
    runtime_health: RuntimeHealth,
    plan: SubscriptionPlan,
    sync_lookback_bars: u32,
    catch_up_on_first_connect: bool,
    flush_max_rows: usize,
    flush_lock: FlushLock,
    custom_aggregators: HashMap<(String, String, String), Aggregator>,
    second_aggregators: HashMap<(String, String), Aggregator>,
}

pub type FlushLock = Arc<Mutex<()>>;

impl BinanceWorker {
    pub fn new(
        store: SqliteStore,
        latest: LatestCache,
        memory_series: MemorySeriesStore,
        closed_buffer: ClosedKlineBuffer,
        runtime_health: RuntimeHealth,
        symbols: Vec<String>,
        intervals: Vec<Interval>,
        sync_lookback_bars: u32,
        catch_up_on_first_connect: bool,
        flush_max_rows: usize,
        flush_lock: FlushLock,
    ) -> Self {
        let plan = SubscriptionPlan::new(symbols, intervals);
        let mut custom_aggregators = HashMap::new();
        let mut second_aggregators = HashMap::new();

        for (symbol, base, target) in plan.aggregation_targets() {
            if let Ok(interval) = Interval::parse(&target) {
                custom_aggregators.insert((symbol, base, target), Aggregator::new(interval));
            }
        }

        for (symbol, target) in plan.second_targets() {
            if let Ok(interval) = Interval::parse(&target) {
                second_aggregators.insert((symbol, target), Aggregator::new(interval));
            }
        }

        Self {
            store,
            latest,
            memory_series,
            closed_buffer,
            runtime_health,
            plan,
            sync_lookback_bars,
            catch_up_on_first_connect,
            flush_max_rows,
            flush_lock,
            custom_aggregators,
            second_aggregators,
        }
    }

    pub async fn run(mut self) {
        let streams = self.plan.streams();
        if streams.is_empty() {
            return;
        }

        if let Err(err) = self.seed_custom_aggregators().await {
            tracing::warn!("failed to seed custom aggregators: {}", err);
        }

        let url = self.plan.stream_url();

        let mut backoff_secs = 1u64;
        let mut should_catch_up_on_connect = self.catch_up_on_first_connect;
        loop {
            match connect_async(&url).await {
                Ok((ws, _)) => {
                    tracing::info!("connected to Binance websocket");
                    self.runtime_health.mark_connected().await;
                    backoff_secs = 1;
                    if should_catch_up_on_connect {
                        if let Err(err) = sync_native_klines(
                            &self.store,
                            self.plan.symbols.clone(),
                            self.plan.intervals.clone(),
                            self.sync_lookback_bars,
                        )
                        .await
                        {
                            tracing::warn!("websocket reconnect kline catch-up failed: {}", err);
                        }
                    } else {
                        should_catch_up_on_connect = true;
                    }
                    let (_, mut read) = ws.split();
                    loop {
                        let message = match timeout(WS_IDLE_TIMEOUT, read.next()).await {
                            Ok(Some(message)) => message,
                            Ok(None) => {
                                tracing::warn!("binance websocket stream ended");
                                self.runtime_health
                                    .mark_reconnecting("websocket stream ended")
                                    .await;
                                break;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    idle_timeout_ms = WS_IDLE_TIMEOUT.as_millis(),
                                    "binance websocket idle timeout"
                                );
                                self.runtime_health
                                    .mark_reconnecting("websocket idle timeout")
                                    .await;
                                break;
                            }
                        };
                        match message {
                            Ok(Message::Text(text)) => {
                                self.runtime_health.mark_message_now().await;
                                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
                                {
                                    if let Ok(event) = parse_combined_stream_message(value) {
                                        self.handle_event(event).await;
                                    }
                                }
                            }
                            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                                self.runtime_health.mark_message_now().await;
                            }
                            Ok(Message::Close(_)) => {
                                tracing::warn!("binance websocket closed");
                                self.runtime_health
                                    .mark_reconnecting("websocket closed")
                                    .await;
                                break;
                            }
                            Ok(_) => {}
                            Err(err) => {
                                tracing::warn!("binance websocket read failed: {}", err);
                                self.runtime_health
                                    .mark_reconnecting(format!("websocket read failed: {err}"))
                                    .await;
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("binance connect failed: {}", err);
                    self.runtime_health
                        .mark_reconnecting(format!("connect failed: {err}"))
                        .await;
                }
            }

            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }

    async fn handle_event(&mut self, event: MarketEvent) {
        match event {
            MarketEvent::OpenKline {
                symbol,
                interval,
                candle,
            } => {
                if let Ok(source_interval) = Interval::parse(&interval) {
                    let source = source_interval.canonical();
                    self.latest.upsert(&symbol, &source, candle.clone()).await;
                    if let Err(err) = self
                        .refresh_custom_latest_from_open(&symbol, &source, candle)
                        .await
                    {
                        tracing::warn!("failed to refresh custom latest: {}", err);
                    }
                }
            }
            MarketEvent::ClosedKline {
                symbol,
                interval,
                candle,
            } => {
                if let Ok(source_interval) = Interval::parse(&interval) {
                    let source = source_interval.canonical();
                    self.buffer_closed_candle(&symbol, &source, candle.clone())
                        .await;
                    self.latest.remove(&symbol, &source).await;

                    for (key, agg) in self.custom_aggregators.iter_mut() {
                        if key.0 == symbol && key.1 == source {
                            if let Ok(Some(closed)) = agg.ingest_candle(candle.clone()) {
                                let rows = self.closed_buffer.upsert(&symbol, &key.2, closed).await;
                                if rows >= self.flush_max_rows {
                                    flush_closed_buffer(
                                        &self.store,
                                        &self.closed_buffer,
                                        &self.flush_lock,
                                    )
                                    .await;
                                }
                                self.latest.remove(&symbol, &key.2).await;
                            }
                            if agg
                                .current()
                                .map(|current| current.close_time <= candle.close_time)
                                .unwrap_or(false)
                            {
                                if let Some(closed) = agg.flush() {
                                    let rows =
                                        self.closed_buffer.upsert(&symbol, &key.2, closed).await;
                                    if rows >= self.flush_max_rows {
                                        flush_closed_buffer(
                                            &self.store,
                                            &self.closed_buffer,
                                            &self.flush_lock,
                                        )
                                        .await;
                                    }
                                    self.latest.remove(&symbol, &key.2).await;
                                }
                            } else if let Some(mut current) = agg.current() {
                                if chrono::Utc::now().timestamp_millis() <= current.close_time {
                                    current.is_closed = false;
                                }
                                self.latest.upsert(&symbol, &key.2, current).await;
                            }
                        }
                    }
                }
            }
            MarketEvent::AggTrade { symbol, trade } => {
                for (key, agg) in self.second_aggregators.iter_mut() {
                    if key.0 == symbol {
                        if let Ok(Some(closed)) = agg.ingest_trade(TradeTick {
                            timestamp_ms: trade.timestamp_ms,
                            price: trade.price,
                            quantity: trade.quantity,
                        }) {
                            self.memory_series
                                .push_closed(&symbol, &key.1, closed)
                                .await;
                            self.latest.remove(&symbol, &key.1).await;
                        }
                        if let Some(current) = agg.current() {
                            self.latest.upsert(&symbol, &key.1, current).await;
                        }
                    }
                }
            }
            MarketEvent::Ignored => {}
        }
    }

    async fn buffer_closed_candle(&self, symbol: &str, interval: &str, candle: Candle) {
        let rows = self.closed_buffer.upsert(symbol, interval, candle).await;
        if rows >= self.flush_max_rows {
            flush_closed_buffer(&self.store, &self.closed_buffer, &self.flush_lock).await;
        }
    }

    async fn seed_custom_aggregators(&mut self) -> Result<(), sqlx::Error> {
        for (key, agg) in self.custom_aggregators.iter_mut() {
            let Ok(target_interval) = Interval::parse(&key.2) else {
                continue;
            };

            let Some(latest_base_open_time) = self.store.max_open_time(&key.0, &key.1).await?
            else {
                continue;
            };

            let start_time = target_interval.bucket_start_ms(latest_base_open_time);
            let rows = self
                .store
                .query_klines(&key.0, &key.1, Some(start_time), None, 10_000)
                .await?;

            let now_ms = chrono::Utc::now().timestamp_millis();
            for row in rows {
                if row.candle.close_time < now_ms {
                    let _ = agg.ingest_candle(row.candle);
                }
            }

            if let Some(mut current) = agg.current() {
                if chrono::Utc::now().timestamp_millis() <= current.close_time {
                    current.is_closed = false;
                }
                self.latest.upsert(&key.0, &key.2, current).await;
            }
        }

        Ok(())
    }

    async fn refresh_custom_latest_from_open(
        &self,
        symbol: &str,
        source: &str,
        open_candle: crate::domain::candle::Candle,
    ) -> Result<(), sqlx::Error> {
        let targets = self
            .custom_aggregators
            .keys()
            .filter(|key| key.0 == symbol && key.1 == source)
            .cloned()
            .collect::<Vec<_>>();

        for key in targets {
            let Ok(target_interval) = Interval::parse(&key.2) else {
                continue;
            };
            let start_time = target_interval.bucket_start_ms(open_candle.open_time);
            let rows = self
                .store
                .query_klines(
                    &key.0,
                    &key.1,
                    Some(start_time),
                    Some(open_candle.open_time.saturating_sub(1)),
                    10_000,
                )
                .await?;
            let mut aggregator = Aggregator::new(target_interval);

            for row in rows {
                if row.candle.close_time < open_candle.open_time {
                    let _ = aggregator.ingest_candle(row.candle);
                }
            }
            let _ = aggregator.ingest_candle(open_candle.clone());

            if let Some(mut current) = aggregator.current() {
                current.is_closed = false;
                self.latest.upsert(&key.0, &key.2, current).await;
            }
        }

        Ok(())
    }
}

pub async fn flush_closed_buffer(
    store: &SqliteStore,
    closed_buffer: &ClosedKlineBuffer,
    flush_lock: &FlushLock,
) {
    let _guard = flush_lock.lock().await;
    let grouped = closed_buffer.snapshot_grouped().await;
    if grouped.is_empty() {
        return;
    }

    let mut flushed = HashMap::new();
    for ((symbol, interval), candles) in grouped {
        if let Err(err) = store.upsert_candles(&symbol, &interval, &candles).await {
            tracing::warn!(
                symbol = %symbol,
                interval = %interval,
                rows = candles.len(),
                "failed to flush closed kline buffer: {}",
                err
            );
        } else {
            flushed.insert((symbol, interval), candles);
        }
    }

    if !flushed.is_empty() {
        closed_buffer.remove_flushed(&flushed).await;
    }
}
