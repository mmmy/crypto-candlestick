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
    alert_last_sides: HashMap<i64, i8>,
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
            alert_last_sides: HashMap::new(),
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
                    self.evaluate_price_alerts(
                        &symbol,
                        candle.close,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await;
                    self.latest.upsert(&symbol, &source, candle.clone()).await;
                    self.refresh_custom_latest_from_open(&symbol, &source, candle)
                        .await;
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
                self.evaluate_price_alerts(&symbol, trade.price, trade.timestamp_ms)
                    .await;
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

    async fn evaluate_price_alerts(&mut self, symbol: &str, price: f64, now_ms: i64) {
        let alerts = match self
            .store
            .active_alerts_for_symbol(&symbol.to_uppercase(), now_ms)
            .await
        {
            Ok(alerts) => alerts,
            Err(err) => {
                tracing::warn!(symbol, "failed to load alerts: {}", err);
                return;
            }
        };
        for alert in alerts {
            let side = if price > alert.price {
                1
            } else if price < alert.price {
                -1
            } else {
                0
            };
            let previous = self.alert_last_sides.insert(alert.id, side).unwrap_or(0);
            let crossed = side != 0
                && previous != 0
                && side != previous
                && ((alert.direction == "cross_up" && previous < 0 && side > 0)
                    || (alert.direction == "cross_down" && previous > 0 && side < 0)
                    || (alert.direction == "cross_any"));
            if !crossed {
                continue;
            }
            if let Ok(true) = self.store.claim_alert(alert.id, now_ms).await {
                let store = self.store.clone();
                tokio::spawn(async move {
                    let body = render_alert_message(&alert.message_template, &alert, price, now_ms);
                    let result = match serde_json::from_str::<serde_json::Value>(&body) {
                        Ok(json) => {
                            let client = reqwest::Client::new();
                            let mut result = Err("webhook delivery failed".to_string());
                            for attempt in 0..3 {
                                result = client
                                    .post(&alert.webhook_url)
                                    .json(&json)
                                    .timeout(Duration::from_secs(5))
                                    .send()
                                    .await
                                    .map_err(|e| e.to_string())
                                    .and_then(|response| {
                                        if response.status().is_success() {
                                            Ok(response)
                                        } else {
                                            Err(format!("webhook returned {}", response.status()))
                                        }
                                    });
                                if result.is_ok() {
                                    break;
                                }
                                if attempt < 2 {
                                    tokio::time::sleep(Duration::from_millis(250 * (1 << attempt)))
                                        .await;
                                }
                            }
                            result
                        }
                        Err(e) => Err(format!("invalid rendered message JSON: {e}")),
                    };
                    match result {
                        Ok(_) => {
                            let _ = store.set_alert_delivery(alert.id, "success", None).await;
                        }
                        Err(err) => {
                            let _ = store
                                .set_alert_delivery(alert.id, "failed", Some(&err))
                                .await;
                        }
                    }
                });
            }
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
    ) {
        let previews = self
            .custom_aggregators
            .iter()
            .filter(|(key, _)| key.0 == symbol && key.1 == source)
            .filter_map(|(key, aggregator)| {
                let mut preview = aggregator.clone();
                if let Err(err) = preview.ingest_candle(open_candle.clone()) {
                    tracing::warn!(
                        symbol,
                        source,
                        target = %key.2,
                        "failed to preview custom latest: {}",
                        err
                    );
                    return None;
                }

                preview.current().map(|mut current| {
                    current.is_closed = false;
                    (key.2.clone(), current)
                })
            })
            .collect::<Vec<_>>();

        for (target, current) in previews {
            self.latest.upsert(symbol, &target, current).await;
        }
    }
}

fn render_alert_message(
    template: &str,
    alert: &crate::storage::sqlite::Alert,
    price: f64,
    now_ms: i64,
) -> String {
    let mut rendered = template.to_string();
    for (key, value) in [
        ("{{ticker}}", alert.symbol.clone()),
        ("{{symbol}}", alert.symbol.clone()),
        ("{{exchange}}", "BINANCE".to_string()),
        ("{{interval}}", alert.interval.clone()),
        ("{{price}}", price.to_string()),
        ("{{close}}", price.to_string()),
        ("{{alertId}}", alert.id.to_string()),
        ("{{time}}", now_ms.to_string()),
    ] {
        rendered = rendered.replace(key, &value);
    }
    rendered
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

    let series = grouped.len();
    let rows = grouped.values().map(Vec::len).sum::<usize>();
    if let Err(err) = store.upsert_candle_groups(&grouped).await {
        tracing::warn!(series, rows, "failed to flush closed kline buffer: {}", err);
    } else {
        closed_buffer.remove_flushed(&grouped).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn custom_open_preview_uses_unflushed_closed_base_candles() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let closed_buffer = ClosedKlineBuffer::default();
        let mut worker = BinanceWorker::new(
            store.clone(),
            latest.clone(),
            MemorySeriesStore::default(),
            closed_buffer.clone(),
            RuntimeHealth::default(),
            vec!["BTCUSDT".to_string()],
            vec![
                Interval::parse("5").unwrap(),
                Interval::parse("10").unwrap(),
            ],
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker
            .handle_event(MarketEvent::ClosedKline {
                symbol: "BTCUSDT".to_string(),
                interval: "5".to_string(),
                candle: Candle {
                    open_time: 0,
                    close_time: 299_999,
                    open: 100.0,
                    high: 105.0,
                    low: 99.0,
                    close: 104.0,
                    volume: 1.0,
                    quote_volume: 102.0,
                    trade_count: 2,
                    is_closed: true,
                },
            })
            .await;

        assert!(store
            .query_klines("BTCUSDT", "5", None, None, 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            closed_buffer
                .query("BTCUSDT", "5", None, None, 10)
                .await
                .len(),
            1
        );

        worker
            .handle_event(MarketEvent::OpenKline {
                symbol: "BTCUSDT".to_string(),
                interval: "5".to_string(),
                candle: Candle {
                    open_time: 300_000,
                    close_time: 599_999,
                    open: 104.0,
                    high: 112.0,
                    low: 103.0,
                    close: 111.0,
                    volume: 2.0,
                    quote_volume: 216.0,
                    trade_count: 3,
                    is_closed: false,
                },
            })
            .await;

        let preview = latest.get("BTCUSDT", "10").await.unwrap();
        assert_eq!(preview.open_time, 0);
        assert_eq!(preview.close_time, 599_999);
        assert_eq!(preview.open, 100.0);
        assert_eq!(preview.high, 112.0);
        assert_eq!(preview.low, 99.0);
        assert_eq!(preview.close, 111.0);
        assert_eq!(preview.volume, 3.0);
        assert_eq!(preview.quote_volume, 318.0);
        assert_eq!(preview.trade_count, 5);
        assert!(!preview.is_closed);

        worker
            .handle_event(MarketEvent::OpenKline {
                symbol: "BTCUSDT".to_string(),
                interval: "5".to_string(),
                candle: Candle {
                    open_time: 300_000,
                    close_time: 599_999,
                    open: 104.0,
                    high: 114.0,
                    low: 102.0,
                    close: 113.0,
                    volume: 2.5,
                    quote_volume: 270.0,
                    trade_count: 4,
                    is_closed: false,
                },
            })
            .await;

        let updated_preview = latest.get("BTCUSDT", "10").await.unwrap();
        assert_eq!(updated_preview.open, 100.0);
        assert_eq!(updated_preview.high, 114.0);
        assert_eq!(updated_preview.low, 99.0);
        assert_eq!(updated_preview.close, 113.0);
        assert_eq!(updated_preview.volume, 3.5);
        assert_eq!(updated_preview.quote_volume, 372.0);
        assert_eq!(updated_preview.trade_count, 6);
        assert!(!updated_preview.is_closed);
    }
}
