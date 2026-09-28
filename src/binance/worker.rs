use super::types::{parse_combined_stream_message, MarketEvent};
use crate::{
    binance::rest::{
        fetch_aggregate_trade_gap, refresh_startup_kline_tail, sync_native_klines, AggregateTrade,
        RestError,
    },
    config::{RealtimeSource, SymbolSubscription},
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
    subscriptions: Vec<SymbolSubscription>,
}

impl SubscriptionPlan {
    pub fn new(symbols: Vec<String>, intervals: Vec<Interval>) -> Self {
        Self::from_subscriptions(
            symbols
                .into_iter()
                .map(|symbol| {
                    SymbolSubscription::new(symbol, intervals.clone(), RealtimeSource::Auto)
                })
                .collect(),
        )
    }

    pub fn from_subscriptions(subscriptions: Vec<SymbolSubscription>) -> Self {
        Self {
            subscriptions: subscriptions
                .into_iter()
                .filter(|subscription| {
                    !subscription.symbol.is_empty() && !subscription.intervals.is_empty()
                })
                .collect(),
        }
    }

    pub fn streams(&self) -> Vec<String> {
        let mut streams = BTreeSet::new();

        for subscription in &self.subscriptions {
            let stream_symbol = subscription.symbol.to_lowercase();
            match subscription.resolved_source() {
                RealtimeSource::Trade => {
                    streams.insert(format!("{stream_symbol}@aggTrade"));
                }
                RealtimeSource::Kline1m => {
                    streams.insert(format!("{stream_symbol}@kline_1m"));
                }
                RealtimeSource::Auto => unreachable!("auto source is always resolved"),
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

        for subscription in &self.subscriptions {
            for interval in &subscription.intervals {
                if let Some(base) = interval.aggregation_base() {
                    targets.insert((
                        subscription.symbol.clone(),
                        base.canonical(),
                        interval.canonical(),
                    ));
                }
            }
        }

        targets.into_iter().collect()
    }

    pub fn kline_sources(&self) -> Vec<KlineSource> {
        let mut seen = BTreeSet::new();
        let mut sources = Vec::new();

        for subscription in &self.subscriptions {
            for interval in &subscription.intervals {
                if interval.as_millis() < 60_000 {
                    continue;
                }

                let source_interval = interval.aggregation_base().unwrap_or(*interval);
                if let Some(binance_interval) = source_interval.binance_interval() {
                    let key = (subscription.symbol.clone(), source_interval.canonical());
                    if seen.insert(key.clone()) {
                        sources.push(KlineSource {
                            symbol: subscription.symbol.clone(),
                            canonical_interval: key.1,
                            binance_interval,
                            interval: source_interval,
                        });
                    }
                }
            }

            if subscription.resolved_source() == RealtimeSource::Kline1m {
                let key = (subscription.symbol.clone(), "1".to_string());
                if seen.insert(key.clone()) {
                    sources.push(KlineSource {
                        symbol: subscription.symbol.clone(),
                        canonical_interval: key.1,
                        binance_interval: "1m",
                        interval: Interval::Minutes(1),
                    });
                }

                if subscription
                    .intervals
                    .iter()
                    .any(|interval| interval.as_millis() > Interval::Days(1).as_millis())
                {
                    let key = (subscription.symbol.clone(), "D".to_string());
                    if seen.insert(key.clone()) {
                        sources.push(KlineSource {
                            symbol: subscription.symbol.clone(),
                            canonical_interval: key.1,
                            binance_interval: "1d",
                            interval: Interval::Days(1),
                        });
                    }
                }
            }
        }

        sources
    }

    pub fn realtime_kline_targets(&self) -> Vec<(String, String, String)> {
        let mut targets = BTreeSet::new();

        for subscription in &self.subscriptions {
            if subscription.resolved_source() != RealtimeSource::Kline1m {
                continue;
            }
            for interval in &subscription.intervals {
                if interval.as_millis() > 60_000 {
                    targets.insert((
                        subscription.symbol.clone(),
                        "1".to_string(),
                        interval.canonical(),
                    ));
                }
            }
        }

        targets.into_iter().collect()
    }

    fn trade_targets(&self) -> Vec<(String, String)> {
        let mut targets = BTreeSet::new();

        for subscription in &self.subscriptions {
            if subscription.resolved_source() != RealtimeSource::Trade {
                continue;
            }
            for interval in &subscription.intervals {
                targets.insert((subscription.symbol.clone(), interval.canonical()));
            }
        }

        targets.into_iter().collect()
    }

    pub fn configured_targets(&self) -> Vec<(String, String)> {
        let mut targets = BTreeSet::new();
        for subscription in &self.subscriptions {
            for interval in &subscription.intervals {
                targets.insert((subscription.symbol.clone(), interval.canonical()));
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
    kline_aggregators: HashMap<(String, String, String), Aggregator>,
    trade_aggregators: HashMap<(String, String), Aggregator>,
    trade_cursors: HashMap<String, AggregateTrade>,
    trade_recovery: HashMap<String, HashMap<(String, String), Aggregator>>,
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
        plan: SubscriptionPlan,
        sync_lookback_bars: u32,
        catch_up_on_first_connect: bool,
        flush_max_rows: usize,
        flush_lock: FlushLock,
    ) -> Self {
        let mut kline_aggregators = HashMap::new();
        let mut trade_aggregators = HashMap::new();

        for (symbol, base, target) in plan.realtime_kline_targets() {
            if let Ok(interval) = Interval::parse(&target) {
                kline_aggregators.insert((symbol, base, target), Aggregator::new(interval));
            }
        }

        for (symbol, target) in plan.trade_targets() {
            if let Ok(interval) = Interval::parse(&target) {
                trade_aggregators.insert((symbol, target), Aggregator::new(interval));
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
            kline_aggregators,
            trade_aggregators,
            trade_cursors: HashMap::new(),
            trade_recovery: HashMap::new(),
            alert_last_sides: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let streams = self.plan.streams();
        if streams.is_empty() {
            return;
        }

        if let Err(err) = self.load_symbol_bucket_anchors().await {
            tracing::warn!("failed to load symbol bucket anchors: {}", err);
        }
        if let Err(err) = self.seed_kline_aggregators().await {
            tracing::warn!("failed to seed kline aggregators: {}", err);
        }
        if let Err(err) = self.seed_trade_aggregators().await {
            tracing::warn!("failed to seed trade aggregators: {}", err);
        }

        let url = self.plan.stream_url();

        let mut backoff_secs = 1u64;
        let mut should_catch_up_on_connect = self.catch_up_on_first_connect;
        loop {
            match connect_async(&url).await {
                Ok((ws, _)) => {
                    tracing::info!("connected to Binance websocket");
                    if should_catch_up_on_connect {
                        if let Err(err) = self.recover_on_connect().await {
                            tracing::warn!("websocket reconnect kline catch-up failed: {}", err);
                            self.runtime_health.mark_disconnected(err.to_string()).await;
                            sleep(Duration::from_secs(backoff_secs)).await;
                            backoff_secs = (backoff_secs * 2).min(30);
                            continue;
                        }
                    }
                    if self.trade_recovery.is_empty() {
                        self.runtime_health.mark_connected().await;
                        backoff_secs = 1;
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
                                    let trade_id = value["data"]["a"].as_i64();
                                    if let Ok(event) = parse_combined_stream_message(value) {
                                        if let MarketEvent::AggTrade { symbol, trade } = event {
                                            let result = match trade_id {
                                                Some(id) if id >= 0 => {
                                                    self.handle_aggregate_trade(
                                                        &symbol,
                                                        AggregateTrade { id, tick: trade },
                                                    )
                                                    .await
                                                }
                                                _ => Err(RestError::InvalidPayload),
                                            };
                                            if let Err(err) = result {
                                                tracing::warn!(
                                                    symbol,
                                                    "trade recovery failed: {}",
                                                    err
                                                );
                                                self.runtime_health
                                                    .mark_reconnecting(err.to_string())
                                                    .await;
                                                break;
                                            }
                                            if self.trade_recovery.is_empty() {
                                                self.runtime_health.mark_connected().await;
                                                backoff_secs = 1;
                                            }
                                        } else {
                                            self.handle_event(event).await;
                                        }
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

            self.invalidate_trade_aggregators().await;
            should_catch_up_on_connect = true;
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }

    async fn recover_on_connect(&mut self) -> Result<(), RestError> {
        flush_closed_buffer(&self.store, &self.closed_buffer, &self.flush_lock).await;
        refresh_startup_kline_tail(&self.store, &self.plan).await?;
        sync_native_klines(&self.store, &self.plan, self.sync_lookback_bars).await?;
        self.load_symbol_bucket_anchors().await?;
        self.reset_kline_aggregators();
        self.seed_kline_aggregators().await?;
        // Cursor-backed symbols need exact ID replay; live REST candles would
        // overlap the queued WebSocket trades and count their volume twice.
        self.seed_trade_aggregators().await?;
        Ok(())
    }

    async fn invalidate_trade_aggregators(&mut self) {
        let symbols = self
            .plan
            .trade_targets()
            .into_iter()
            .map(|(symbol, _)| symbol)
            .collect::<BTreeSet<_>>();
        for symbol in symbols {
            self.begin_trade_recovery(&symbol).await;
        }
        self.alert_last_sides.clear();
    }

    async fn begin_trade_recovery(&mut self, symbol: &str) {
        if self.trade_cursors.contains_key(symbol) && !self.trade_recovery.contains_key(symbol) {
            let prefix = self
                .trade_aggregators
                .iter()
                .filter(|(key, _)| key.0 == symbol)
                .map(|(key, agg)| (key.clone(), agg.clone()))
                .collect();
            self.trade_recovery.insert(symbol.to_string(), prefix);
        }
        for (key, agg) in &mut self.trade_aggregators {
            if key.0 == symbol {
                agg.reset();
                self.latest.remove(symbol, &key.1).await;
                if Interval::parse(&key.1)
                    .map(|i| i.as_millis() < 60_000)
                    .unwrap_or(false)
                {
                    self.memory_series.clear(symbol, &key.1).await;
                }
            }
        }
    }

    async fn handle_aggregate_trade(
        &mut self,
        symbol: &str,
        trade: AggregateTrade,
    ) -> Result<(), RestError> {
        if let Some(previous) = self.trade_cursors.get(symbol).copied() {
            if trade.id <= previous.id {
                return Ok(()); // WebSocket messages may overlap already replayed trades.
            }
            if trade.id != previous.id + 1 && !self.trade_recovery.contains_key(symbol) {
                self.begin_trade_recovery(symbol).await;
                self.runtime_health
                    .mark_disconnected("aggregate trade ID gap")
                    .await;
            }
            if self.trade_recovery.contains_key(symbol) {
                let missing =
                    fetch_aggregate_trade_gap(symbol, previous.id + 1, trade.id - 1).await?;
                self.complete_trade_recovery(symbol, &missing, trade)
                    .await?;
                return Ok(());
            }
        }
        self.handle_event(MarketEvent::AggTrade {
            symbol: symbol.to_string(),
            trade: trade.tick,
        })
        .await;
        self.trade_cursors.insert(symbol.to_string(), trade);
        Ok(())
    }

    async fn complete_trade_recovery(
        &mut self,
        symbol: &str,
        missing: &[AggregateTrade],
        live: AggregateTrade,
    ) -> Result<(), RestError> {
        let previous = self
            .trade_cursors
            .get(symbol)
            .ok_or(RestError::InvalidPayload)?;
        if live.id <= previous.id || missing.len() as i64 != live.id - previous.id - 1 {
            return Err(RestError::InvalidAggregateTradeGap(previous.id + 1));
        }
        let mut expected_id = previous.id + 1;
        let mut previous_time = previous.tick.timestamp_ms;
        for trade in missing.iter().chain(std::iter::once(&live)) {
            if trade.id != expected_id || trade.tick.timestamp_ms < previous_time {
                return Err(RestError::InvalidAggregateTradeGap(expected_id));
            }
            previous_time = trade.tick.timestamp_ms;
            if trade.id != live.id {
                expected_id += 1;
            }
        }
        let prefix = self
            .trade_recovery
            .remove(symbol)
            .ok_or(RestError::InvalidPayload)?;
        self.trade_aggregators.extend(prefix);
        // Establish a fresh price-alert baseline instead of notifying on replay.
        for trade in missing.iter().chain(std::iter::once(&live)) {
            self.apply_trade(symbol, trade.tick).await;
        }
        self.trade_cursors.insert(symbol.to_string(), live);
        self.alert_last_sides.clear();
        self.evaluate_price_alerts(symbol, live.tick.price, live.tick.timestamp_ms)
            .await;
        tracing::info!(
            symbol,
            replayed_trades = missing.len(),
            "trade aggregation recovered"
        );
        Ok(())
    }

    async fn handle_event(&mut self, event: MarketEvent) {
        match event {
            MarketEvent::OpenKline {
                symbol: _,
                interval: _,
                candle: _,
            } => {}
            MarketEvent::ClosedKline {
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
                    self.buffer_closed_candle(&symbol, &source, candle.clone())
                        .await;
                    self.latest.remove(&symbol, &source).await;

                    for (key, agg) in self.kline_aggregators.iter_mut() {
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
                self.apply_trade(&symbol, trade).await;
            }
            MarketEvent::Ignored => {}
        }
    }

    async fn apply_trade(&mut self, symbol: &str, trade: TradeTick) {
        for (key, agg) in self.trade_aggregators.iter_mut() {
            if key.0 == symbol {
                if let Ok(Some(closed)) = agg.ingest_trade(TradeTick {
                    timestamp_ms: trade.timestamp_ms,
                    price: trade.price,
                    quantity: trade.quantity,
                }) {
                    if Interval::parse(&key.1)
                        .map(|interval| interval.as_millis() < 60_000)
                        .unwrap_or(false)
                    {
                        self.memory_series
                            .push_closed(&symbol, &key.1, closed)
                            .await;
                    } else {
                        let rows = self.closed_buffer.upsert(&symbol, &key.1, closed).await;
                        if rows >= self.flush_max_rows {
                            flush_closed_buffer(&self.store, &self.closed_buffer, &self.flush_lock)
                                .await;
                        }
                    }
                    self.latest.remove(&symbol, &key.1).await;
                }
                if let Some(current) = agg.current() {
                    self.latest.upsert(&symbol, &key.1, current).await;
                }
            }
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
            let crossed_direction = if side > previous {
                "cross_up"
            } else {
                "cross_down"
            };
            if let Ok(true) = self
                .store
                .claim_alert_with_event(alert.id, now_ms, price, crossed_direction)
                .await
            {
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

    async fn seed_kline_aggregators(&mut self) -> Result<(), sqlx::Error> {
        for (key, agg) in self.kline_aggregators.iter_mut() {
            self.latest.remove(&key.0, &key.2).await;
            let Ok(target_interval) = Interval::parse(&key.2) else {
                continue;
            };
            let now_ms = chrono::Utc::now().timestamp_millis();
            let start_time = agg.bucket_start_ms(now_ms);
            let mut minute_start = start_time;

            if target_interval.as_millis() > Interval::Days(1).as_millis() {
                let daily_rows = self
                    .store
                    .query_klines(&key.0, "D", Some(start_time), None, 32)
                    .await?;
                for row in daily_rows {
                    if row.candle.close_time < now_ms {
                        minute_start = minute_start.max(row.candle.close_time + 1);
                        let _ = agg.ingest_candle(row.candle);
                    }
                }
            }

            let seed_limit = ((now_ms - minute_start).max(0) / 60_000 + 1) as u32;
            let minute_rows = self
                .store
                .query_klines(&key.0, &key.1, Some(minute_start), None, seed_limit.max(1))
                .await?;
            for row in minute_rows {
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

    async fn seed_trade_aggregators(&mut self) -> Result<(), sqlx::Error> {
        for (key, agg) in self.trade_aggregators.iter_mut() {
            if self.trade_recovery.contains_key(&key.0) {
                continue;
            }
            agg.reset();
            self.latest.remove(&key.0, &key.1).await;
            let Ok(target_interval) = Interval::parse(&key.1) else {
                continue;
            };
            if target_interval.as_millis() < 60_000 {
                continue;
            }

            let base_interval = target_interval
                .aggregation_base()
                .unwrap_or(target_interval);
            let base = base_interval.canonical();
            let now_ms = chrono::Utc::now().timestamp_millis();
            let bucket_start = agg.bucket_start_ms(now_ms);
            let seed_limit =
                ((now_ms - bucket_start).max(0) / base_interval.as_millis() as i64 + 1) as u32;
            let rows = self
                .store
                .query_klines(
                    &key.0,
                    &base,
                    Some(bucket_start),
                    Some(now_ms),
                    seed_limit.max(1),
                )
                .await?;

            for row in rows {
                let _ = agg.ingest_candle(row.candle);
            }

            if let Some(mut current) = agg.current() {
                current.is_closed = false;
                self.latest.upsert(&key.0, &key.1, current).await;
            }
        }

        Ok(())
    }

    async fn load_symbol_bucket_anchors(&mut self) -> Result<(), sqlx::Error> {
        for (key, agg) in self.kline_aggregators.iter_mut() {
            let Ok(interval) = Interval::parse(&key.2) else {
                continue;
            };
            if let Some(anchor_ms) =
                stored_bucket_anchor(&self.store, &key.0, &key.2, interval).await?
            {
                agg.set_bucket_anchor_ms(anchor_ms);
            }
        }

        for (key, agg) in self.trade_aggregators.iter_mut() {
            let Ok(interval) = Interval::parse(&key.1) else {
                continue;
            };
            if let Some(anchor_ms) =
                stored_bucket_anchor(&self.store, &key.0, &key.1, interval).await?
            {
                agg.set_bucket_anchor_ms(anchor_ms);
            }
        }

        Ok(())
    }

    fn reset_kline_aggregators(&mut self) {
        for aggregator in self.kline_aggregators.values_mut() {
            aggregator.reset();
        }
    }
}

async fn stored_bucket_anchor(
    store: &SqliteStore,
    symbol: &str,
    interval_name: &str,
    interval: Interval,
) -> Result<Option<i64>, sqlx::Error> {
    if !interval.uses_symbol_specific_binance_alignment() {
        return Ok(None);
    }

    let rows = store
        .query_klines(symbol, interval_name, None, None, 32)
        .await?;
    let open_times = rows
        .into_iter()
        .map(|row| row.candle.open_time)
        .collect::<Vec<_>>();

    Ok(interval.infer_bucket_anchor_ms(&open_times))
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
    use crate::config::{RealtimeSource, SymbolSubscription};

    async fn recovery_worker() -> BinanceWorker {
        BinanceWorker::new(
            SqliteStore::connect("sqlite::memory:").await.unwrap(),
            LatestCache::default(),
            MemorySeriesStore::default(),
            ClosedKlineBuffer::default(),
            RuntimeHealth::default(),
            SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
                "BTCUSDT",
                vec![Interval::Seconds(15), Interval::Minutes(1)],
                RealtimeSource::Trade,
            )]),
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        )
    }

    fn aggregate_trade(id: i64, time: i64, price: f64, quantity: f64) -> AggregateTrade {
        AggregateTrade {
            id,
            tick: TradeTick::new(time, price, quantity),
        }
    }

    #[tokio::test]
    async fn disconnect_removes_live_candles_and_seconds_history() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 1_000, 100.0, 2.0))
            .await
            .unwrap();
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(11, 16_000, 101.0, 3.0))
            .await
            .unwrap();
        assert_eq!(
            worker
                .memory_series
                .query("BTCUSDT", "15S", None, None, 10)
                .await
                .len(),
            1
        );
        worker.invalidate_trade_aggregators().await;
        assert!(worker.latest.get("BTCUSDT", "1").await.is_none());
        assert!(worker.latest.get("BTCUSDT", "15S").await.is_none());
        assert!(worker
            .memory_series
            .query("BTCUSDT", "15S", None, None, 10)
            .await
            .is_empty());
        assert!(worker
            .trade_aggregators
            .values()
            .all(|agg| agg.current().is_none()));
        assert!(worker.trade_recovery.contains_key("BTCUSDT"));
        // Repeated failed connections must retain the original trusted prefix.
        worker.invalidate_trade_aggregators().await;
        assert_eq!(
            worker.trade_recovery["BTCUSDT"][&("BTCUSDT".into(), "1".into())]
                .current()
                .unwrap()
                .volume,
            5.0
        );
    }

    #[tokio::test]
    async fn reconnect_replays_missing_trades_and_deduplicates_live_overlap() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 1_000, 100.0, 2.0))
            .await
            .unwrap();
        worker.invalidate_trade_aggregators().await;
        // Simulate a REST-repaired minute already present in the database.
        let repaired = Candle {
            open_time: 0,
            close_time: 59_999,
            open: 100.0,
            high: 150.0,
            low: 80.0,
            close: 80.0,
            volume: 9.0,
            quote_volume: 970.0,
            trade_count: 3,
            is_closed: true,
        };
        worker
            .store
            .upsert_candle("BTCUSDT", "1", &repaired)
            .await
            .unwrap();
        let missing = vec![
            aggregate_trade(11, 30_000, 150.0, 3.0),
            aggregate_trade(12, 55_000, 80.0, 4.0),
        ];
        let live = aggregate_trade(13, 61_000, 101.0, 5.0);
        worker
            .complete_trade_recovery("BTCUSDT", &missing, live)
            .await
            .unwrap();
        let rows = worker
            .closed_buffer
            .query("BTCUSDT", "1", None, None, 10)
            .await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].candle, repaired);
        worker
            .handle_aggregate_trade("BTCUSDT", live)
            .await
            .unwrap();
        assert_eq!(worker.latest.get("BTCUSDT", "1").await.unwrap().volume, 5.0);
        flush_closed_buffer(&worker.store, &worker.closed_buffer, &worker.flush_lock).await;
        assert_eq!(
            worker
                .store
                .query_klines("BTCUSDT", "1", Some(0), Some(0), 1)
                .await
                .unwrap()[0]
                .candle,
            repaired
        );
        assert!(!worker.trade_recovery.contains_key("BTCUSDT"));
    }

    #[tokio::test]
    async fn incomplete_replay_does_not_publish_or_modify_a_partial_bucket() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 1_000, 100.0, 2.0))
            .await
            .unwrap();
        worker.invalidate_trade_aggregators().await;
        let incomplete = vec![aggregate_trade(12, 30_000, 150.0, 3.0)];
        assert!(worker
            .complete_trade_recovery(
                "BTCUSDT",
                &incomplete,
                aggregate_trade(13, 61_000, 101.0, 5.0)
            )
            .await
            .is_err());
        assert!(worker.latest.get("BTCUSDT", "1").await.is_none());
        assert!(worker
            .closed_buffer
            .query("BTCUSDT", "1", None, None, 10)
            .await
            .is_empty());
        assert_eq!(worker.trade_cursors["BTCUSDT"].id, 10);
        assert!(worker.trade_recovery.contains_key("BTCUSDT"));
    }

    #[tokio::test]
    async fn replay_uses_ids_to_preserve_distinct_trades_in_the_same_millisecond() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 1_000, 100.0, 2.0))
            .await
            .unwrap();
        worker.invalidate_trade_aggregators().await;
        let missing = vec![aggregate_trade(11, 1_000, 110.0, 3.0)];
        worker
            .complete_trade_recovery("BTCUSDT", &missing, aggregate_trade(12, 1_000, 90.0, 4.0))
            .await
            .unwrap();
        let current = worker.latest.get("BTCUSDT", "1").await.unwrap();
        assert_eq!(
            (current.high, current.low, current.volume),
            (110.0, 90.0, 9.0)
        );
        assert_eq!(current.trade_count, 3);
    }

    #[tokio::test]
    async fn trade_seed_is_idempotent_and_clears_a_stale_bucket() {
        let mut worker = recovery_worker().await;
        let now = chrono::Utc::now().timestamp_millis();
        let start = Interval::Minutes(1).bucket_start_ms(now);
        let current = Candle {
            open_time: start,
            close_time: start + 59_999,
            open: 100.0,
            high: 120.0,
            low: 90.0,
            close: 110.0,
            volume: 10.0,
            quote_volume: 1_000.0,
            trade_count: 10,
            is_closed: false,
        };
        worker
            .store
            .upsert_candle("BTCUSDT", "1", &current)
            .await
            .unwrap();
        worker
            .apply_trade("BTCUSDT", TradeTick::new(start - 1_000, 999.0, 999.0))
            .await;
        worker.seed_trade_aggregators().await.unwrap();
        worker.seed_trade_aggregators().await.unwrap();
        assert_eq!(worker.latest.get("BTCUSDT", "1").await.unwrap(), current);
        assert!(worker
            .closed_buffer
            .query("BTCUSDT", "1", None, None, 10)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn closed_one_minute_klines_refresh_higher_interval_once_per_minute() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let closed_buffer = ClosedKlineBuffer::default();
        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![Interval::parse("5").unwrap()],
            RealtimeSource::Auto,
        )]);
        let mut worker = BinanceWorker::new(
            store.clone(),
            latest.clone(),
            MemorySeriesStore::default(),
            closed_buffer.clone(),
            RuntimeHealth::default(),
            plan,
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker
            .handle_event(MarketEvent::OpenKline {
                symbol: "BTCUSDT".to_string(),
                interval: "1".to_string(),
                candle: Candle {
                    open_time: 0,
                    close_time: 59_999,
                    open: 1.0,
                    high: 999.0,
                    low: 1.0,
                    close: 999.0,
                    volume: 999.0,
                    quote_volume: 999.0,
                    trade_count: 999,
                    is_closed: false,
                },
            })
            .await;
        assert!(latest.get("BTCUSDT", "5").await.is_none());

        let bucket_start = Interval::parse("5")
            .unwrap()
            .bucket_start_ms(chrono::Utc::now().timestamp_millis());
        for index in 0..2 {
            let open_time = bucket_start + index * 60_000;
            worker
                .handle_event(MarketEvent::ClosedKline {
                    symbol: "BTCUSDT".to_string(),
                    interval: "1".to_string(),
                    candle: Candle {
                        open_time,
                        close_time: open_time + 59_999,
                        open: 100.0 + index as f64,
                        high: 102.0 + index as f64,
                        low: 99.0,
                        close: 101.0 + index as f64,
                        volume: 10.0,
                        quote_volume: 1_000.0,
                        trade_count: 10,
                        is_closed: true,
                    },
                })
                .await;
        }

        assert!(store
            .query_klines("BTCUSDT", "5", None, None, 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            closed_buffer
                .query("BTCUSDT", "1", None, None, 10)
                .await
                .len(),
            2
        );

        let preview = latest.get("BTCUSDT", "5").await.unwrap();
        assert_eq!(preview.open_time, bucket_start);
        assert_eq!(preview.close_time, bucket_start + 299_999);
        assert_eq!(preview.open, 100.0);
        assert_eq!(preview.high, 103.0);
        assert_eq!(preview.low, 99.0);
        assert_eq!(preview.close, 102.0);
        assert_eq!(preview.volume, 20.0);
        assert_eq!(preview.quote_volume, 2_000.0);
        assert_eq!(preview.trade_count, 20);
        assert!(!preview.is_closed);
    }

    #[tokio::test]
    async fn trade_source_builds_and_buffers_minute_klines() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let closed_buffer = ClosedKlineBuffer::default();
        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![
                Interval::parse("15S").unwrap(),
                Interval::parse("1").unwrap(),
            ],
            RealtimeSource::Auto,
        )]);
        let mut worker = BinanceWorker::new(
            store,
            latest.clone(),
            MemorySeriesStore::default(),
            closed_buffer.clone(),
            RuntimeHealth::default(),
            plan,
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker
            .handle_event(MarketEvent::AggTrade {
                symbol: "BTCUSDT".to_string(),
                trade: TradeTick::new(1_000, 100.0, 2.0),
            })
            .await;
        worker
            .handle_event(MarketEvent::AggTrade {
                symbol: "BTCUSDT".to_string(),
                trade: TradeTick::new(61_000, 101.0, 3.0),
            })
            .await;

        let closed = closed_buffer.query("BTCUSDT", "1", None, None, 10).await;
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].candle.open_time, 0);
        assert_eq!(closed[0].candle.close, 100.0);
        assert_eq!(closed[0].candle.volume, 2.0);

        let current = latest.get("BTCUSDT", "1").await.unwrap();
        assert_eq!(current.open_time, 60_000);
        assert_eq!(current.close, 101.0);
        assert_eq!(current.volume, 3.0);
        assert!(!current.is_closed);
    }

    #[tokio::test]
    async fn trade_source_seeds_current_long_interval_before_live_trades() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let closed_buffer = ClosedKlineBuffer::default();
        let interval = Interval::parse("720").unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let bucket_start = interval.bucket_start_ms(now_ms);
        let bucket_end = bucket_start + interval.as_millis() as i64 - 1;

        store
            .upsert_candle(
                "XAUUSDT",
                "720",
                &Candle {
                    open_time: bucket_start,
                    close_time: bucket_end,
                    open: 4_448.52,
                    high: 4_514.81,
                    low: 4_442.95,
                    close: 4_497.70,
                    volume: 100.0,
                    quote_volume: 449_770.0,
                    trade_count: 10,
                    is_closed: false,
                },
            )
            .await
            .unwrap();

        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "XAUUSDT",
            vec![Interval::parse("10S").unwrap(), interval],
            RealtimeSource::Auto,
        )]);
        let mut worker = BinanceWorker::new(
            store,
            latest.clone(),
            MemorySeriesStore::default(),
            closed_buffer.clone(),
            RuntimeHealth::default(),
            plan,
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker.seed_trade_aggregators().await.unwrap();
        worker
            .handle_event(MarketEvent::AggTrade {
                symbol: "XAUUSDT".to_string(),
                trade: TradeTick::new(now_ms, 4_497.73, 1.0),
            })
            .await;

        let current = latest.get("XAUUSDT", "720").await.unwrap();
        assert_eq!(current.open, 4_448.52);
        assert_eq!(current.high, 4_514.81);
        assert_eq!(current.low, 4_442.95);
        assert_eq!(current.close, 4_497.73);
        assert_eq!(current.volume, 101.0);

        worker
            .handle_event(MarketEvent::AggTrade {
                symbol: "XAUUSDT".to_string(),
                trade: TradeTick::new(bucket_end + 1, 4_500.0, 2.0),
            })
            .await;

        let closed = closed_buffer.query("XAUUSDT", "720", None, None, 10).await;
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].candle.open, 4_448.52);
        assert_eq!(closed[0].candle.high, 4_514.81);
        assert_eq!(closed[0].candle.low, 4_442.95);
    }

    #[tokio::test]
    async fn trade_source_uses_stored_symbol_anchor_for_three_day_kline() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let interval = Interval::parse("3D").unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let local_bucket_start = interval.bucket_start_ms(now_ms);
        let symbol_anchor = local_bucket_start + Interval::Days(1).as_millis() as i64;
        let current_open = interval.bucket_start_ms_with_anchor(now_ms, symbol_anchor);

        for index in -2..=0 {
            let open_time = current_open + i64::from(index) * interval.as_millis() as i64;
            store
                .upsert_candle(
                    "BTCUSDT",
                    "3D",
                    &Candle {
                        open_time,
                        close_time: open_time + interval.as_millis() as i64 - 1,
                        open: 100.0 + f64::from(index),
                        high: 110.0,
                        low: 90.0,
                        close: 105.0,
                        volume: 10.0,
                        quote_volume: 1_000.0,
                        trade_count: 10,
                        is_closed: index < 0,
                    },
                )
                .await
                .unwrap();
        }

        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "BTCUSDT",
            vec![Interval::parse("10S").unwrap(), interval],
            RealtimeSource::Auto,
        )]);
        let mut worker = BinanceWorker::new(
            store,
            latest.clone(),
            MemorySeriesStore::default(),
            ClosedKlineBuffer::default(),
            RuntimeHealth::default(),
            plan,
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker.load_symbol_bucket_anchors().await.unwrap();
        worker.seed_trade_aggregators().await.unwrap();
        worker
            .handle_event(MarketEvent::AggTrade {
                symbol: "BTCUSDT".to_string(),
                trade: TradeTick::new(now_ms, 106.0, 1.0),
            })
            .await;

        let current = latest.get("BTCUSDT", "3D").await.unwrap();
        assert_ne!(current_open, local_bucket_start);
        assert_eq!(current.open_time, current_open);
        assert_eq!(current.open, 100.0);
        assert_eq!(current.close, 106.0);
    }

    #[tokio::test]
    async fn kline_source_uses_stored_symbol_anchor_for_three_day_kline() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let latest = LatestCache::default();
        let interval = Interval::parse("3D").unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let local_bucket_start = interval.bucket_start_ms(now_ms);
        let symbol_anchor = local_bucket_start + 2 * Interval::Days(1).as_millis() as i64;
        let current_open = interval.bucket_start_ms_with_anchor(now_ms, symbol_anchor);

        for index in -2..=0 {
            let open_time = current_open + i64::from(index) * interval.as_millis() as i64;
            store
                .upsert_candle(
                    "QQQUSDT",
                    "3D",
                    &Candle {
                        open_time,
                        close_time: open_time + interval.as_millis() as i64 - 1,
                        open: 100.0,
                        high: 110.0,
                        low: 90.0,
                        close: 105.0,
                        volume: 10.0,
                        quote_volume: 1_000.0,
                        trade_count: 10,
                        is_closed: index < 0,
                    },
                )
                .await
                .unwrap();
        }

        let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
            "QQQUSDT",
            vec![Interval::parse("1").unwrap(), interval],
            RealtimeSource::Auto,
        )]);
        let mut worker = BinanceWorker::new(
            store,
            latest.clone(),
            MemorySeriesStore::default(),
            ClosedKlineBuffer::default(),
            RuntimeHealth::default(),
            plan,
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        );

        worker.load_symbol_bucket_anchors().await.unwrap();
        worker
            .handle_event(MarketEvent::ClosedKline {
                symbol: "QQQUSDT".to_string(),
                interval: "1".to_string(),
                candle: Candle {
                    open_time: now_ms.div_euclid(60_000) * 60_000,
                    close_time: now_ms.div_euclid(60_000) * 60_000 + 59_999,
                    open: 106.0,
                    high: 107.0,
                    low: 105.0,
                    close: 106.5,
                    volume: 1.0,
                    quote_volume: 106.5,
                    trade_count: 1,
                    is_closed: true,
                },
            })
            .await;

        let current = latest.get("QQQUSDT", "3D").await.unwrap();
        assert_ne!(current_open, local_bucket_start);
        assert_eq!(current.open_time, current_open);
        assert_eq!(current.open, 106.0);
    }
}
