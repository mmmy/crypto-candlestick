use super::types::{parse_combined_stream_message, MarketEvent};
use crate::{
    domain::interval::Interval,
    engine::aggregator::{Aggregator, TradeTick},
    memory::{LatestCache, MemorySeriesStore},
    storage::sqlite::SqliteStore,
};
use futures_util::StreamExt;
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};

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
    plan: SubscriptionPlan,
    custom_aggregators: HashMap<(String, String, String), Aggregator>,
    second_aggregators: HashMap<(String, String), Aggregator>,
}

impl BinanceWorker {
    pub fn new(
        store: SqliteStore,
        latest: LatestCache,
        memory_series: MemorySeriesStore,
        symbols: Vec<String>,
        intervals: Vec<Interval>,
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
            plan,
            custom_aggregators,
            second_aggregators,
        }
    }

    pub async fn run(mut self) {
        let streams = self.plan.streams();
        if streams.is_empty() {
            return;
        }

        let url = format!(
            "wss://fstream.binance.com/stream?streams={}",
            streams.join("/")
        );

        let mut backoff_secs = 1u64;
        loop {
            match connect_async(&url).await {
                Ok((ws, _)) => {
                    backoff_secs = 1;
                    let (_, mut read) = ws.split();
                    while let Some(message) = read.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
                                {
                                    if let Ok(event) = parse_combined_stream_message(value) {
                                        self.handle_event(event).await;
                                    }
                                }
                            }
                            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                            Ok(Message::Close(_)) => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("binance connect failed: {}", err);
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

                    for (key, agg) in self.custom_aggregators.iter_mut() {
                        if key.0 == symbol && key.1 == source {
                            if agg.ingest_candle(candle.clone()).is_ok() {
                                if let Some(current) = agg.current() {
                                    self.latest.upsert(&symbol, &key.2, current).await;
                                }
                            }
                        }
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
                    let _ = self.store.upsert_candle(&symbol, &source, &candle).await;
                    self.latest.remove(&symbol, &source).await;

                    for (key, agg) in self.custom_aggregators.iter_mut() {
                        if key.0 == symbol && key.1 == source {
                            if let Ok(Some(closed)) = agg.ingest_candle(candle.clone()) {
                                let _ = self.store.upsert_candle(&symbol, &key.2, &closed).await;
                                self.latest.remove(&symbol, &key.2).await;
                            }
                            if let Some(current) = agg.current() {
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
}
