use super::types::{parse_combined_stream_message, MarketEvent};
use crate::{
    binance::rest::{
        fetch_aggregate_trade_gap, refresh_startup_kline_tail, refresh_trade_bootstrap_history,
        sync_native_klines, AggregateTrade, RestError,
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
const BINANCE_USDM_REST_BASE: &str = "https://fapi.binance.com";

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

            // Internal closed prefixes bootstrap Trade sources at an exact
            // minute boundary. These sources are not publicly configured
            // targets and must remain hidden by the HTTP handlers.
            if subscription.resolved_source() == RealtimeSource::Trade
                && subscription
                    .intervals
                    .iter()
                    .any(|interval| interval.as_millis() >= 60_000)
            {
                for seed_interval in [Interval::Minutes(1), Interval::Days(1)] {
                    if seed_interval == Interval::Days(1)
                        && !subscription
                            .intervals
                            .iter()
                            .any(|interval| interval.as_millis() > seed_interval.as_millis())
                    {
                        continue;
                    }
                    let key = (subscription.symbol.clone(), seed_interval.canonical());
                    if seen.insert(key.clone()) {
                        sources.push(KlineSource {
                            symbol: key.0,
                            canonical_interval: key.1,
                            binance_interval: seed_interval
                                .binance_interval()
                                .expect("native seed interval"),
                            interval: seed_interval,
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
    trade_min_complete_open: HashMap<(String, String), i64>,
    kline_initialized: BTreeSet<String>,
    kline_cursors: HashMap<String, (i64, i64, bool)>,
    // The permanent aggregators contain closed minutes only. Open-minute
    // updates are cumulative replacements applied to a clone for each preview.
    kline_prefix_next: HashMap<(String, String, String), i64>,
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
            trade_min_complete_open: HashMap::new(),
            kline_initialized: BTreeSet::new(),
            kline_cursors: HashMap::new(),
            kline_prefix_next: HashMap::new(),
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
                                                    let aggregate =
                                                        AggregateTrade { id, tick: trade };
                                                    if self.trade_cursors.contains_key(&symbol) {
                                                        self.handle_aggregate_trade(
                                                            &symbol, aggregate,
                                                        )
                                                        .await
                                                    } else {
                                                        self.bootstrap_trade_from_rest(
                                                            &symbol, aggregate,
                                                        )
                                                        .await
                                                    }
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
                                            if let MarketEvent::OpenKline {
                                                symbol, candle, ..
                                            }
                                            | MarketEvent::ClosedKline {
                                                symbol, candle, ..
                                            } = &event
                                            {
                                                if self
                                                    .kline_needs_bootstrap(symbol, candle.open_time)
                                                {
                                                    if let Err(err) = self
                                                        .bootstrap_kline_history(
                                                            symbol,
                                                            candle.open_time,
                                                        )
                                                        .await
                                                    {
                                                        tracing::warn!(
                                                            symbol,
                                                            "minute kline recovery failed: {err}"
                                                        );
                                                        self.runtime_health
                                                            .mark_reconnecting(err.to_string())
                                                            .await;
                                                        break;
                                                    }
                                                }
                                            }
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
        let market_lock = self.latest.market_update_lock();
        let _market_guard = market_lock.write().await;
        for (symbol, _) in self.plan.trade_targets() {
            self.latest.mark_recovering(&symbol).await;
        }
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
        let market_lock = self.latest.market_update_lock();
        let _guard = market_lock.write().await;
        for subscription in &self.plan.subscriptions {
            if subscription.resolved_source() == RealtimeSource::Kline1m {
                self.latest.mark_recovering(&subscription.symbol).await;
                for interval in &subscription.intervals {
                    self.latest
                        .remove(&subscription.symbol, &interval.canonical())
                        .await;
                }
            }
        }
        self.kline_initialized.clear();
        self.kline_cursors.clear();
        self.kline_prefix_next.clear();
        self.reset_kline_aggregators();
        self.alert_last_sides.clear();
    }

    async fn begin_trade_recovery(&mut self, symbol: &str) {
        let market_lock = self.latest.market_update_lock();
        let _market_guard = market_lock.write().await;
        self.latest.mark_recovering(symbol).await;
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
                if let Ok(interval) = Interval::parse(&key.1) {
                    if interval.as_millis() < 60_000 {
                        // The open second bucket crossed a transport outage.
                        // Do not publish or finalize its partial replacement.
                        if let Some(previous) = self.trade_cursors.get(symbol) {
                            self.trade_min_complete_open.insert(
                                key.clone(),
                                agg.bucket_start_ms(previous.tick.timestamp_ms)
                                    + interval.as_millis() as i64,
                            );
                        }
                    }
                }
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
        if trade.id < 0
            || trade.tick.timestamp_ms < 0
            || !trade.tick.price.is_finite()
            || trade.tick.price <= 0.0
            || !trade.tick.quantity.is_finite()
            || trade.tick.quantity <= 0.0
        {
            return Err(RestError::InvalidPayload);
        }
        if let Some(previous) = self.trade_cursors.get(symbol).copied() {
            if trade.id <= previous.id {
                return Ok(()); // WebSocket messages may overlap already replayed trades.
            }
            if trade.tick.timestamp_ms < previous.tick.timestamp_ms {
                self.begin_trade_recovery(symbol).await;
                return Err(RestError::InvalidAggregateTradeGap(trade.id));
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

    async fn bootstrap_trade_from_rest(
        &mut self,
        symbol: &str,
        live: AggregateTrade,
    ) -> Result<(), RestError> {
        {
            let market_lock = self.latest.market_update_lock();
            let _guard = market_lock.write().await;
            self.latest.mark_recovering(symbol).await;
        }
        let client = reqwest::Client::new();
        {
            let market_lock = self.latest.market_update_lock();
            let _guard = market_lock.write().await;
            refresh_trade_bootstrap_history(
                &self.store,
                &self.plan,
                symbol,
                Interval::Minutes(1).bucket_start_ms(live.tick.timestamp_ms),
                self.sync_lookback_bars,
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    symbol,
                    stage = "closed_history",
                    market_event_time_ms = live.tick.timestamp_ms,
                    "trade bootstrap stage failed: {error}"
                );
                error
            })?;
        }
        let first_id = fetch_first_minute_trade_id(&client, BINANCE_USDM_REST_BASE, symbol, live)
            .await
            .map_err(|error| {
                tracing::warn!(
                    symbol,
                    stage = "minute_first_id",
                    market_event_time_ms = live.tick.timestamp_ms,
                    "trade bootstrap stage failed: {error}"
                );
                error
            })?;
        // One predecessor proves the minute boundary even if a server returns
        // a truncated time-range response. The remaining range is the exact
        // closed-prefix -> first WebSocket ID handoff.
        let mut trades =
            fetch_aggregate_trade_gap(symbol, first_id.saturating_sub(1), live.id - 1).await?;
        let preceding = if first_id > 0 && !trades.is_empty() {
            Some(trades.remove(0))
        } else {
            None
        };
        self.complete_trade_bootstrap(symbol, preceding, &trades, live)
            .await
    }

    async fn complete_trade_bootstrap(
        &mut self,
        symbol: &str,
        preceding: Option<AggregateTrade>,
        missing: &[AggregateTrade],
        live: AggregateTrade,
    ) -> Result<(), RestError> {
        let minute_start = Interval::Minutes(1).bucket_start_ms(live.tick.timestamp_ms);
        let first_id = missing.first().map_or(live.id, |trade| trade.id);
        if first_id < 0 || live.id < first_id || live.id - first_id != missing.len() as i64 {
            return Err(RestError::InvalidAggregateTradeGap(first_id));
        }
        if first_id > 0
            && !preceding.is_some_and(|trade| {
                trade.id == first_id - 1 && trade.tick.timestamp_ms < minute_start
            })
        {
            return Err(RestError::InvalidAggregateTradeGap(first_id));
        }
        let mut previous_time = minute_start;
        for (expected_id, trade) in (first_id..).zip(missing.iter().chain(std::iter::once(&live))) {
            if trade.id != expected_id
                || trade.tick.timestamp_ms < previous_time
                || trade.tick.timestamp_ms > live.tick.timestamp_ms
                || !trade.tick.price.is_finite()
                || trade.tick.price <= 0.0
                || !trade.tick.quantity.is_finite()
                || trade.tick.quantity <= 0.0
            {
                return Err(RestError::InvalidAggregateTradeGap(expected_id));
            }
            previous_time = trade.tick.timestamp_ms;
        }

        let market_lock = self.latest.market_update_lock();
        let _guard = market_lock.write().await;
        self.latest.mark_recovering(symbol).await;
        let keys = self
            .trade_aggregators
            .keys()
            .filter(|key| key.0 == symbol)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let interval = Interval::parse(&key.1).map_err(|_| RestError::InvalidPayload)?;
            let bucket_start = self.trade_aggregators[&key].bucket_start_ms(live.tick.timestamp_ms);
            let prefix = if interval.as_millis() >= 60_000 {
                self.closed_trade_prefix(symbol, bucket_start, minute_start, interval)
                    .await?
            } else {
                None
            };
            let agg = self
                .trade_aggregators
                .get_mut(&key)
                .expect("configured aggregator");
            agg.reset();
            self.latest.remove(symbol, &key.1).await;
            self.trade_min_complete_open.remove(&key);
            if interval.as_millis() < 60_000 {
                self.memory_series.clear(symbol, &key.1).await;
            }
            if let Some(prefix) = prefix {
                for candle in prefix {
                    agg.ingest_candle(candle)
                        .map_err(|_| RestError::InvalidPayload)?;
                }
            } else {
                // Unverified histories and the startup second bucket remain
                // unavailable until an entirely observed new bucket begins.
                self.trade_min_complete_open
                    .insert(key, bucket_start + interval.as_millis() as i64);
            }
        }
        for trade in missing.iter().chain(std::iter::once(&live)) {
            self.apply_trade_inner(symbol, trade.tick, true).await;
        }
        self.trade_cursors.insert(symbol.to_string(), live);
        self.publish_trade_snapshot(symbol, live.tick.timestamp_ms)
            .await;
        drop(_guard);
        self.alert_last_sides.clear();
        self.evaluate_price_alerts(symbol, live.tick.price, live.tick.timestamp_ms)
            .await;
        tracing::info!(
            symbol,
            replayed_trades = missing.len(),
            "trade source bootstrapped at an exact ID boundary"
        );
        Ok(())
    }

    async fn closed_trade_prefix(
        &self,
        symbol: &str,
        bucket_start: i64,
        minute_start: i64,
        interval: Interval,
    ) -> Result<Option<Vec<Candle>>, sqlx::Error> {
        let mut prefix = Vec::new();
        let mut next_open = bucket_start;
        if interval.as_millis() > Interval::Days(1).as_millis() {
            let daily_end = Interval::Days(1).bucket_start_ms(minute_start);
            let days = self
                .closed_prefix_rows(symbol, "D", bucket_start, daily_end - 1, 32)
                .await?;
            for candle in days {
                if !candle.is_closed
                    || candle.open_time != next_open
                    || candle.close_time != next_open + 86_400_000 - 1
                {
                    return Ok(None);
                }
                next_open += 86_400_000;
                prefix.push(candle);
            }
            if next_open != daily_end {
                return Ok(None);
            }
        }
        let limit = ((minute_start - next_open).max(0) / 60_000) as u32;
        if limit > 0 {
            let rows = self
                .closed_prefix_rows(symbol, "1", next_open, minute_start - 1, limit)
                .await?;
            for candle in rows {
                if !candle.is_closed
                    || candle.open_time != next_open
                    || candle.close_time != next_open + 60_000 - 1
                {
                    return Ok(None);
                }
                next_open += 60_000;
                prefix.push(candle);
            }
        }
        Ok((next_open == minute_start).then_some(prefix))
    }

    async fn closed_prefix_rows(
        &self,
        symbol: &str,
        interval: &str,
        start: i64,
        end: i64,
        limit: u32,
    ) -> Result<Vec<Candle>, sqlx::Error> {
        if start > end {
            return Ok(Vec::new());
        }
        let buffered = self
            .closed_buffer
            .query(symbol, interval, Some(start), Some(end), limit)
            .await;
        let stored = self
            .store
            .query_klines(symbol, interval, Some(start), Some(end), limit)
            .await?;
        let mut by_time = std::collections::BTreeMap::new();
        for row in stored.into_iter().chain(buffered) {
            by_time.insert(row.candle.open_time, row.candle);
        }
        Ok(by_time.into_values().collect())
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
        let market_lock = self.latest.market_update_lock();
        let _market_guard = market_lock.write().await;
        let prefix = self
            .trade_recovery
            .remove(symbol)
            .ok_or(RestError::InvalidPayload)?;
        // Minute histories can be replayed exactly from the saved prefix.
        // Seconds were explicitly invalidated and must warm up anew.
        self.trade_aggregators
            .extend(prefix.into_iter().filter(|(key, _)| {
                Interval::parse(&key.1)
                    .map(|interval| interval.as_millis() >= 60_000)
                    .unwrap_or(false)
            }));
        // Establish a fresh price-alert baseline instead of notifying on replay.
        for trade in missing.iter().chain(std::iter::once(&live)) {
            self.apply_trade_inner(symbol, trade.tick, true).await;
        }
        self.trade_cursors.insert(symbol.to_string(), live);
        self.publish_trade_snapshot(symbol, live.tick.timestamp_ms)
            .await;
        drop(_market_guard);
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

    fn kline_needs_bootstrap(&self, symbol: &str, minute_open: i64) -> bool {
        if !self.kline_initialized.contains(symbol) {
            return true;
        }
        self.kline_cursors
            .get(symbol)
            .is_some_and(|(previous_open, _, closed)| {
                minute_open > *previous_open && (minute_open != previous_open + 60_000 || !closed)
            })
    }

    async fn bootstrap_kline_history(
        &mut self,
        symbol: &str,
        minute_open: i64,
    ) -> Result<(), RestError> {
        {
            let market_lock = self.latest.market_update_lock();
            let _guard = market_lock.write().await;
            self.latest.mark_recovering(symbol).await;
        }
        // Startup REST synchronization can precede the first live event by
        // minutes. Repair closed history at the incoming minute boundary;
        // a REST open candle is never used as a current-minute replacement.
        flush_closed_buffer(&self.store, &self.closed_buffer, &self.flush_lock).await;
        refresh_trade_bootstrap_history(
            &self.store,
            &self.plan,
            symbol,
            minute_open,
            self.sync_lookback_bars,
        )
        .await?;
        let market_lock = self.latest.market_update_lock();
        let _guard = market_lock.write().await;
        self.load_symbol_bucket_anchors().await?;
        self.seed_kline_symbol(symbol, minute_open).await?;
        self.kline_cursors.remove(symbol);
        self.kline_initialized.insert(symbol.to_string());
        tracing::info!(
            symbol,
            "minute kline source bootstrapped at a closed-history boundary"
        );
        Ok(())
    }

    async fn seed_kline_symbol(
        &mut self,
        symbol: &str,
        minute_open: i64,
    ) -> Result<(), sqlx::Error> {
        let keys = self
            .kline_aggregators
            .keys()
            .filter(|key| key.0 == symbol)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let interval = Interval::parse(&key.2).expect("configured period");
            let start = self.kline_aggregators[&key].bucket_start_ms(minute_open);
            let prefix = self
                .closed_trade_prefix(symbol, start, minute_open, interval)
                .await?;
            let agg = self
                .kline_aggregators
                .get_mut(&key)
                .expect("configured aggregator");
            agg.reset();
            self.kline_prefix_next.remove(&key);
            self.latest.remove(symbol, &key.2).await;
            if let Some(prefix) = prefix {
                for candle in prefix {
                    let _ = agg.ingest_candle(candle);
                }
                self.kline_prefix_next.insert(key, minute_open);
            }
        }
        Ok(())
    }

    fn accept_kline_event(&mut self, symbol: &str, candle: &Candle, event_time_ms: i64) -> bool {
        if candle.open_time < 0
            || candle.open_time % 60_000 != 0
            || candle.close_time != candle.open_time + 59_999
            || event_time_ms < candle.open_time
        {
            return false;
        }
        if self
            .kline_cursors
            .get(symbol)
            .is_some_and(|(open, event, closed)| {
                candle.open_time < *open
                    || event_time_ms < *event
                    || (candle.open_time == *open && *closed)
            })
        {
            return false;
        }
        self.kline_cursors.insert(
            symbol.to_string(),
            (candle.open_time, event_time_ms, candle.is_closed),
        );
        true
    }

    async fn publish_kline_snapshot(
        &self,
        symbol: &str,
        minute: Option<&Candle>,
        event_time_ms: i64,
    ) {
        let mut candles = HashMap::new();
        for (configured_symbol, target) in self.plan.configured_targets() {
            if configured_symbol != symbol {
                continue;
            }
            let current = if target == "1" {
                minute.cloned()
            } else {
                let key = (symbol.to_string(), "1".to_string(), target.clone());
                self.kline_aggregators.get(&key).and_then(|prefix| {
                    let mut preview = prefix.clone();
                    if let Some(minute) = minute {
                        if self.kline_prefix_next.get(&key) != Some(&minute.open_time) {
                            if prefix.bucket_start_ms(minute.open_time) != minute.open_time {
                                return None;
                            }
                            preview.reset();
                        }
                        preview.ingest_candle(minute.clone()).ok()?;
                    } else if !self.kline_prefix_next.contains_key(&key) {
                        return None;
                    }
                    preview.current()
                })
            };
            if let Some(mut current) = current {
                current.is_closed = false;
                self.latest.upsert(symbol, &target, current.clone()).await;
                candles.insert(target, current);
            } else {
                self.latest.remove(symbol, &target).await;
            }
        }
        // One accepted event, one atomic set of configured dynamic periods.
        self.latest
            .publish_live_symbol(
                symbol,
                candles,
                event_time_ms,
                chrono::Utc::now().timestamp_millis(),
            )
            .await;
    }

    async fn handle_event(&mut self, event: MarketEvent) {
        let market_lock = self.latest.market_update_lock();
        let _market_guard = market_lock.write().await;
        match event {
            MarketEvent::OpenKline {
                symbol,
                interval,
                event_time_ms,
                candle,
            } => {
                if interval == "1" && self.accept_kline_event(&symbol, &candle, event_time_ms) {
                    self.publish_kline_snapshot(&symbol, Some(&candle), event_time_ms)
                        .await;
                }
            }
            MarketEvent::ClosedKline {
                symbol,
                interval,
                event_time_ms,
                candle,
            } => {
                if interval != "1" || !self.accept_kline_event(&symbol, &candle, event_time_ms) {
                    return;
                }
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
                            let bucket_start = agg.bucket_start_ms(candle.open_time);
                            if self.kline_prefix_next.get(key) != Some(&candle.open_time) {
                                if bucket_start != candle.open_time {
                                    self.kline_prefix_next.remove(key);
                                    agg.reset();
                                    self.latest.remove(&symbol, &key.2).await;
                                    continue;
                                }
                                agg.reset();
                            }
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
                            self.kline_prefix_next
                                .insert(key.clone(), candle.close_time + 1);
                        }
                    }
                }
                self.publish_kline_snapshot(&symbol, None, event_time_ms)
                    .await;
            }
            MarketEvent::AggTrade { symbol, trade } => {
                if self
                    .latest
                    .live_market_event_time_ms(&symbol)
                    .await
                    .is_some_and(|event_time_ms| trade.timestamp_ms < event_time_ms)
                {
                    return;
                }
                self.evaluate_price_alerts(&symbol, trade.price, trade.timestamp_ms)
                    .await;
                self.apply_trade(&symbol, trade).await;
                self.publish_trade_snapshot(&symbol, trade.timestamp_ms)
                    .await;
            }
            MarketEvent::Ignored => {}
        }
    }

    async fn apply_trade(&mut self, symbol: &str, trade: TradeTick) {
        self.apply_trade_inner(symbol, trade, false).await;
    }

    async fn publish_trade_snapshot(&self, symbol: &str, market_event_time_ms: i64) {
        let candles = self
            .trade_aggregators
            .iter()
            .filter(|(key, _)| key.0 == symbol)
            .filter_map(|(key, aggregator)| {
                aggregator.current().map(|candle| (key.1.clone(), candle))
            })
            .collect::<HashMap<_, _>>();
        if !candles.is_empty() {
            self.latest
                .publish_live_symbol(
                    symbol,
                    candles,
                    market_event_time_ms,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await;
        }
    }

    async fn apply_trade_inner(&mut self, symbol: &str, trade: TradeTick, recovering: bool) {
        for (key, agg) in self.trade_aggregators.iter_mut() {
            if key.0 == symbol {
                if self
                    .trade_min_complete_open
                    .get(key)
                    .is_some_and(|min_open| agg.bucket_start_ms(trade.timestamp_ms) < *min_open)
                {
                    continue;
                }
                if let Ok(Some(mut closed)) = agg.ingest_trade(TradeTick {
                    timestamp_ms: trade.timestamp_ms,
                    price: trade.price,
                    quantity: trade.quantity,
                }) {
                    if Interval::parse(&key.1)
                        .map(|interval| interval.as_millis() < 60_000)
                        .unwrap_or(false)
                    {
                        self.memory_series.push_closed(symbol, &key.1, closed).await;
                    } else {
                        if recovering {
                            // Replay may finalize an old, initially incomplete
                            // prefix. A REST-repaired closed candle is complete
                            // and must not be replaced with that partial bar.
                            if let Ok(rows) = self
                                .store
                                .query_klines(
                                    symbol,
                                    &key.1,
                                    Some(closed.open_time),
                                    Some(closed.open_time),
                                    1,
                                )
                                .await
                            {
                                if let Some(repaired) =
                                    rows.into_iter().find(|row| row.candle.is_closed)
                                {
                                    closed = repaired.candle;
                                }
                            }
                        }
                        let rows = self.closed_buffer.upsert(symbol, &key.1, closed).await;
                        if rows >= self.flush_max_rows {
                            flush_closed_buffer(&self.store, &self.closed_buffer, &self.flush_lock)
                                .await;
                        }
                    }
                    self.latest.remove(symbol, &key.1).await;
                }
                if let Some(current) = agg.current() {
                    self.latest.upsert(symbol, &key.1, current).await;
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
        let minute_open =
            Interval::Minutes(1).bucket_start_ms(chrono::Utc::now().timestamp_millis());
        let symbols = self
            .plan
            .subscriptions
            .iter()
            .filter(|subscription| subscription.resolved_source() == RealtimeSource::Kline1m)
            .map(|subscription| subscription.symbol.clone())
            .collect::<Vec<_>>();
        for symbol in symbols {
            self.seed_kline_symbol(&symbol, minute_open).await?;
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

async fn fetch_first_minute_trade_id(
    client: &reqwest::Client,
    base_url: &str,
    symbol: &str,
    live: AggregateTrade,
) -> Result<i64, RestError> {
    let minute_start = Interval::Minutes(1).bucket_start_ms(live.tick.timestamp_ms);
    let payload = client
        .get(format!("{base_url}/fapi/v1/aggTrades"))
        .query(&[
            ("symbol", symbol.to_string()),
            ("startTime", minute_start.to_string()),
            ("endTime", live.tick.timestamp_ms.to_string()),
            ("limit", "1".to_string()),
        ])
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    let first = payload
        .as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| {
            tracing::warn!(
                symbol,
                minute_start,
                end_time = live.tick.timestamp_ms,
                "bootstrap first-trade lookup returned no array row"
            );
            RestError::InvalidPayload
        })?;
    let id = first["a"].as_i64().ok_or(RestError::InvalidPayload)?;
    let time = first["T"].as_i64().ok_or(RestError::InvalidPayload)?;
    if id < 0 || id > live.id || time < minute_start || time > live.tick.timestamp_ms {
        tracing::warn!(
            symbol,
            minute_start,
            end_time = live.tick.timestamp_ms,
            first_id = id,
            first_time = time,
            live_id = live.id,
            "bootstrap first trade lies outside the exact WebSocket boundary"
        );
        return Err(RestError::InvalidPayload);
    }
    Ok(id)
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

    async fn bootstrap_worker(intervals: &[&str]) -> BinanceWorker {
        BinanceWorker::new(
            SqliteStore::connect("sqlite::memory:").await.unwrap(),
            LatestCache::default(),
            MemorySeriesStore::default(),
            ClosedKlineBuffer::default(),
            RuntimeHealth::default(),
            SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
                "BTCUSDT",
                intervals
                    .iter()
                    .map(|value| Interval::parse(value).unwrap())
                    .collect(),
                RealtimeSource::Trade,
            )]),
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        )
    }

    fn seed_candle(open: i64, interval: Interval, volume: f64, is_closed: bool) -> Candle {
        Candle {
            open_time: open,
            close_time: open + interval.as_millis() as i64 - 1,
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 100.0,
            volume,
            quote_volume: volume * 100.0,
            trade_count: 5,
            is_closed,
        }
    }

    #[tokio::test]
    async fn bootstrap_uses_only_closed_prefix_and_exact_ids_for_day_and_seconds() {
        let mut worker = bootstrap_worker(&["15S", "1", "5", "D", "2D"]).await;
        let day = 86_400_000;
        worker
            .store
            .upsert_candle(
                "BTCUSDT",
                "D",
                &seed_candle(0, Interval::Days(1), 50.0, true),
            )
            .await
            .unwrap();
        worker
            .store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &seed_candle(day, Interval::Minutes(1), 10.0, true),
            )
            .await
            .unwrap();
        // REST dynamic buckets contain overlapping future trades and cannot
        // provide the handoff point. They must be completely disregarded.
        for (name, interval) in [
            ("1", Interval::Minutes(1)),
            ("5", Interval::Minutes(5)),
            ("D", Interval::Days(1)),
        ] {
            let start = interval.bucket_start_ms(day + 61_000);
            let mut dynamic = seed_candle(start, interval, 999.0, false);
            dynamic.high = 999.0;
            // Keep the genuinely closed minute prefix at the previous open.
            worker
                .store
                .upsert_candle("BTCUSDT", name, &dynamic)
                .await
                .unwrap();
        }
        let preceding = aggregate_trade(10, day + 59_000, 100.0, 1.0);
        let missing = [aggregate_trade(11, day + 60_100, 101.0, 1.0)];
        let live = aggregate_trade(12, day + 61_000, 102.0, 2.0);
        worker
            .complete_trade_bootstrap("BTCUSDT", Some(preceding), &missing, live)
            .await
            .unwrap();
        let snapshot = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(!snapshot.recovering);
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(snapshot.candles["1"].volume, 3.0);
        assert_eq!(snapshot.candles["5"].volume, 13.0);
        assert_eq!(snapshot.candles["D"].volume, 13.0);
        assert_eq!(snapshot.candles["2D"].volume, 63.0);
        assert_eq!(snapshot.candles["2D"].high, 110.0);
        assert!(!snapshot.candles.contains_key("15S"));
        worker
            .handle_aggregate_trade("BTCUSDT", live)
            .await
            .unwrap();
        assert_eq!(
            worker
                .latest
                .live_snapshot("BTCUSDT")
                .await
                .unwrap()
                .sequence,
            1
        );
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(13, day + 75_000, 103.0, 1.0))
            .await
            .unwrap();
        let next = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert_eq!(next.candles["15S"].open_time, day + 75_000);
        assert_eq!(next.candles["15S"].volume, 1.0);
        assert!(worker
            .memory_series
            .query("BTCUSDT", "15S", None, None, 10)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn bootstrap_missing_closed_prefix_keeps_that_period_unknown() {
        let mut worker = bootstrap_worker(&["1", "5"]).await;
        // A missing initial minute makes the 5m prefix unverifiable.
        worker
            .store
            .upsert_candle(
                "BTCUSDT",
                "1",
                &seed_candle(60_000, Interval::Minutes(1), 10.0, true),
            )
            .await
            .unwrap();
        worker
            .complete_trade_bootstrap(
                "BTCUSDT",
                Some(aggregate_trade(10, 119_000, 100.0, 1.0)),
                &[aggregate_trade(11, 120_100, 101.0, 1.0)],
                aggregate_trade(12, 121_000, 102.0, 2.0),
            )
            .await
            .unwrap();
        let snapshot = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(snapshot.candles.contains_key("1"));
        assert!(!snapshot.candles.contains_key("5"));
        assert!(worker.latest.get("BTCUSDT", "5").await.is_none());
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(13, 300_000, 103.0, 1.0))
            .await
            .unwrap();
        assert_eq!(
            worker
                .latest
                .live_snapshot("BTCUSDT")
                .await
                .unwrap()
                .candles["5"]
                .volume,
            1.0
        );
        assert!(worker
            .closed_buffer
            .query("BTCUSDT", "5", None, None, 10)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn bootstrap_requires_a_proven_first_trade_boundary() {
        let mut worker = bootstrap_worker(&["1", "5"]).await;
        let missing = [aggregate_trade(11, 60_100, 101.0, 1.0)];
        let live = aggregate_trade(12, 61_000, 102.0, 2.0);
        // An earlier trade in the same minute reveals a truncated prefix.
        assert!(worker
            .complete_trade_bootstrap(
                "BTCUSDT",
                Some(aggregate_trade(10, 60_001, 100.0, 1.0)),
                &missing,
                live
            )
            .await
            .is_err());
        assert!(worker.latest.live_snapshot("BTCUSDT").await.is_none());
        assert!(worker.trade_cursors.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_boundary_request_is_bounded_and_rejects_wrong_time() {
        use axum::{extract::Query, routing::get, Json, Router};
        let router = Router::new().route(
            "/fapi/v1/aggTrades",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query["symbol"], "BTCUSDT");
                assert_eq!(query["startTime"], "60000");
                assert_eq!(query["endTime"], "61000");
                assert_eq!(query["limit"], "1");
                assert!(!query.contains_key("fromId"));
                Json(serde_json::json!([{"a": 10, "T": 60001, "p": "100", "q": "1"}]))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let id = fetch_first_minute_trade_id(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "BTCUSDT",
            aggregate_trade(12, 61_000, 102.0, 2.0),
        )
        .await
        .unwrap();
        task.abort();
        assert_eq!(id, 10);

        let router = Router::new().route(
            "/fapi/v1/aggTrades",
            get(|| async { Json(serde_json::json!([{"a": 10, "T": 59999}])) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let result = fetch_first_minute_trade_id(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "BTCUSDT",
            aggregate_trade(12, 61_000, 102.0, 2.0),
        )
        .await;
        task.abort();
        assert!(matches!(result, Err(RestError::InvalidPayload)));
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
        let recovering = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(recovering.recovering);
        assert!(recovering.candles.is_empty());
        assert_eq!(recovering.market_event_time_ms, 16_000);
        assert_eq!(recovering.generation, 2);
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
            worker
                .latest
                .live_snapshot("BTCUSDT")
                .await
                .unwrap()
                .generation,
            2
        );
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
        let snapshot = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(!snapshot.recovering);
        assert_eq!(snapshot.sequence, 2); // Replay publishes exactly once.
        assert_eq!(snapshot.generation, 2);
        assert_eq!(snapshot.market_event_time_ms, 61_000);
        assert_eq!(snapshot.candles["1"].close, 101.0);
        assert_eq!(snapshot.candles["15S"].close, 101.0);
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
        assert_eq!(
            worker
                .latest
                .live_snapshot("BTCUSDT")
                .await
                .unwrap()
                .sequence,
            snapshot.sequence
        );
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
        assert!(worker.latest.get("BTCUSDT", "15S").await.is_none());
        let snapshot = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(!snapshot.candles.contains_key("15S"));
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(13, 15_000, 105.0, 1.0))
            .await
            .unwrap();
        assert_eq!(
            worker.latest.get("BTCUSDT", "15S").await.unwrap().volume,
            1.0
        );
        assert!(worker
            .memory_series
            .query("BTCUSDT", "15S", None, None, 10)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn dynamic_symbol_snapshot_contains_the_same_trade_in_every_interval() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 1_000, 100.0, 2.0))
            .await
            .unwrap();
        let first = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert_eq!(first.candles.len(), 2);
        assert_eq!(first.sequence, 1);
        assert_eq!(first.generation, 1);
        for candle in first.candles.values() {
            assert_eq!(candle.close, 100.0);
            assert!(!candle.is_closed);
            assert!(candle.close_time > first.market_event_time_ms);
        }
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(11, 1_000, 105.0, 3.0))
            .await
            .unwrap();
        let second = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert_eq!(second.sequence, 2);
        for candle in second.candles.values() {
            assert_eq!(candle.close, 105.0);
            assert_eq!(candle.volume, 5.0);
        }
        assert!(first.candles.values().all(|candle| candle.close == 100.0));
    }

    #[tokio::test]
    async fn forward_id_with_regressing_market_time_is_not_fresh_data() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 16_000, 100.0, 2.0))
            .await
            .unwrap();
        assert!(worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(11, 15_000, 105.0, 3.0))
            .await
            .is_err());
        let snapshot = worker.latest.live_snapshot("BTCUSDT").await.unwrap();
        assert!(snapshot.recovering);
        assert_eq!(snapshot.market_event_time_ms, 16_000);
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(worker.trade_cursors["BTCUSDT"].id, 10);
    }

    #[tokio::test]
    async fn replay_keeps_complete_rest_repair_over_partial_saved_prefix() {
        let mut worker = recovery_worker().await;
        worker
            .handle_aggregate_trade("BTCUSDT", aggregate_trade(10, 30_000, 100.0, 2.0))
            .await
            .unwrap();
        worker.invalidate_trade_aggregators().await;
        let repaired = Candle {
            open_time: 0,
            close_time: 59_999,
            open: 80.0,
            high: 150.0,
            low: 70.0,
            close: 150.0,
            volume: 50.0,
            quote_volume: 5_000.0,
            trade_count: 20,
            is_closed: true,
        };
        worker
            .store
            .upsert_candle("BTCUSDT", "1", &repaired)
            .await
            .unwrap();
        worker
            .complete_trade_recovery(
                "BTCUSDT",
                &[aggregate_trade(11, 55_000, 150.0, 3.0)],
                aggregate_trade(12, 61_000, 110.0, 4.0),
            )
            .await
            .unwrap();
        let closed = worker
            .closed_buffer
            .query("BTCUSDT", "1", None, None, 10)
            .await;
        assert_eq!(closed[0].candle, repaired);
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
    async fn minute_klines_update_dynamic_preview_before_close() {
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
                event_time_ms: 1_000,
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
        assert_eq!(latest.get("BTCUSDT", "5").await.unwrap().close, 999.0);
        assert!(closed_buffer
            .query("BTCUSDT", "5", None, None, 10)
            .await
            .is_empty());

        let bucket_start = Interval::parse("5")
            .unwrap()
            .bucket_start_ms(chrono::Utc::now().timestamp_millis());
        for index in 0..2 {
            let open_time = bucket_start + index * 60_000;
            worker
                .handle_event(MarketEvent::ClosedKline {
                    symbol: "BTCUSDT".to_string(),
                    interval: "1".to_string(),
                    event_time_ms: open_time + 59_999,
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

    async fn minute_worker(intervals: &[&str]) -> BinanceWorker {
        BinanceWorker::new(
            SqliteStore::connect("sqlite::memory:").await.unwrap(),
            LatestCache::default(),
            MemorySeriesStore::default(),
            ClosedKlineBuffer::default(),
            RuntimeHealth::default(),
            SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
                "KORUUSDT",
                intervals
                    .iter()
                    .map(|value| Interval::parse(value).unwrap())
                    .collect(),
                RealtimeSource::Kline1m,
            )]),
            1_500,
            false,
            usize::MAX,
            Arc::new(Mutex::new(())),
        )
    }

    fn minute_candle(open_time: i64, volume: f64, closed: bool) -> Candle {
        Candle {
            open_time,
            close_time: open_time + 59_999,
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 101.0,
            volume,
            quote_volume: volume * 101.0,
            trade_count: volume as u64,
            is_closed: closed,
        }
    }

    async fn minute_event(worker: &mut BinanceWorker, candle: Candle, event_time_ms: i64) {
        let event = if candle.is_closed {
            MarketEvent::ClosedKline {
                symbol: "KORUUSDT".into(),
                interval: "1".into(),
                event_time_ms,
                candle,
            }
        } else {
            MarketEvent::OpenKline {
                symbol: "KORUUSDT".into(),
                interval: "1".into(),
                event_time_ms,
                candle,
            }
        };
        worker.handle_event(event).await;
    }

    #[tokio::test]
    async fn cumulative_open_minutes_replace_preview_and_closed_minutes_are_deduplicated() {
        let mut worker = minute_worker(&["1", "5", "15"]).await;
        worker
            .store
            .upsert_candle("KORUUSDT", "1", &minute_candle(0, 10.0, true))
            .await
            .unwrap();
        worker
            .closed_buffer
            .upsert("KORUUSDT", "1", minute_candle(60_000, 10.0, true))
            .await;
        worker.seed_kline_symbol("KORUUSDT", 120_000).await.unwrap();
        minute_event(&mut worker, minute_candle(120_000, 10.0, false), 121_000).await;
        minute_event(&mut worker, minute_candle(120_000, 15.0, false), 122_000).await;
        let snapshot = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert_eq!(snapshot.market_event_time_ms, 122_000);
        assert_eq!(snapshot.candles.len(), 3);
        assert_eq!(snapshot.candles["1"].volume, 15.0);
        for period in ["5", "15"] {
            assert_eq!(snapshot.candles[period].volume, 35.0);
            assert_eq!(snapshot.candles[period].quote_volume, 35.0 * 101.0);
            assert_eq!(snapshot.candles[period].trade_count, 35);
            assert!(!snapshot.candles[period].is_closed);
        }
        // A delayed older update cannot replace the new cumulative minute.
        minute_event(&mut worker, minute_candle(120_000, 11.0, false), 121_500).await;
        assert_eq!(
            worker
                .latest
                .live_snapshot("KORUUSDT")
                .await
                .unwrap()
                .sequence,
            snapshot.sequence
        );
        minute_event(&mut worker, minute_candle(120_000, 18.0, true), 179_999).await;
        let closed = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert!(!closed.candles.contains_key("1"));
        assert_eq!(closed.candles["5"].volume, 38.0);
        minute_event(&mut worker, minute_candle(120_000, 18.0, true), 180_001).await;
        minute_event(&mut worker, minute_candle(120_000, 99.0, false), 180_002).await;
        assert_eq!(
            worker
                .latest
                .live_snapshot("KORUUSDT")
                .await
                .unwrap()
                .sequence,
            closed.sequence
        );
        minute_event(&mut worker, minute_candle(180_000, 7.0, false), 181_000).await;
        assert_eq!(
            worker
                .latest
                .live_snapshot("KORUUSDT")
                .await
                .unwrap()
                .candles["5"]
                .volume,
            45.0
        );
    }

    #[tokio::test]
    async fn minute_source_finalizes_target_at_its_last_minute_and_starts_next_dynamic_bucket() {
        let mut worker = minute_worker(&["1", "5"]).await;
        for index in 0..5 {
            let candle = minute_candle(index * 60_000, 10.0, true);
            let event_time = candle.close_time;
            minute_event(&mut worker, candle, event_time).await;
        }
        let rows = worker
            .closed_buffer
            .query("KORUUSDT", "5", None, None, 10)
            .await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].candle.volume, 50.0);
        assert_eq!(rows[0].candle.close_time, 299_999);
        assert!(rows[0].candle.is_closed);
        assert!(!worker
            .latest
            .live_snapshot("KORUUSDT")
            .await
            .unwrap()
            .candles
            .contains_key("5"));
        minute_event(&mut worker, minute_candle(300_000, 2.0, false), 301_000).await;
        let current = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert_eq!(current.candles["5"].open_time, 300_000);
        assert_eq!(current.candles["5"].volume, 2.0);
    }

    #[tokio::test]
    async fn missing_or_partial_minute_prefix_cannot_publish_a_partial_long_candle() {
        let mut worker = minute_worker(&["1", "5"]).await;
        worker
            .store
            .upsert_candle("KORUUSDT", "1", &minute_candle(0, 10.0, true))
            .await
            .unwrap();
        worker
            .store
            .upsert_candle("KORUUSDT", "1", &minute_candle(60_000, 99.0, false))
            .await
            .unwrap();
        worker.seed_kline_symbol("KORUUSDT", 120_000).await.unwrap();
        minute_event(&mut worker, minute_candle(120_000, 1.0, false), 121_000).await;
        let snapshot = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert!(snapshot.candles.contains_key("1"));
        assert!(!snapshot.candles.contains_key("5"));
        assert!(worker
            .closed_buffer
            .query("KORUUSDT", "5", None, None, 10)
            .await
            .is_empty());
        minute_event(&mut worker, minute_candle(300_000, 2.0, false), 301_000).await;
        assert_eq!(
            worker
                .latest
                .live_snapshot("KORUUSDT")
                .await
                .unwrap()
                .candles["5"]
                .volume,
            2.0
        );
    }

    #[tokio::test]
    async fn minute_source_multiday_prefix_uses_daily_then_buffered_minutes() {
        let mut worker = minute_worker(&["1", "10D"]).await;
        let target = Interval::Days(10);
        // The three fixture days must already be closed on every day of a 10D
        // bucket; SQLite correctly rejects future rows as closed history.
        let bucket = target.bucket_start_ms(chrono::Utc::now().timestamp_millis())
            - target.as_millis() as i64;
        let day = 86_400_000;
        for index in 0..3 {
            let mut candle = minute_candle(bucket + index * day, 100.0, true);
            candle.close_time = candle.open_time + day - 1;
            worker
                .store
                .upsert_candle("KORUUSDT", "D", &candle)
                .await
                .unwrap();
        }
        let minute_open = bucket + 3 * day + 120_000;
        for index in 0..2 {
            worker
                .closed_buffer
                .upsert(
                    "KORUUSDT",
                    "1",
                    minute_candle(bucket + 3 * day + index * 60_000, 10.0, true),
                )
                .await;
        }
        worker
            .seed_kline_symbol("KORUUSDT", minute_open)
            .await
            .unwrap();
        minute_event(
            &mut worker,
            minute_candle(minute_open, 5.0, false),
            minute_open + 1_000,
        )
        .await;
        let current = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert_eq!(current.candles["10D"].open_time, bucket);
        assert_eq!(current.candles["10D"].volume, 325.0);
    }

    #[tokio::test]
    async fn minute_source_disconnect_invalidates_generation_and_requires_history_bootstrap() {
        let mut worker = minute_worker(&["1", "5"]).await;
        minute_event(&mut worker, minute_candle(0, 10.0, false), 1_000).await;
        worker.kline_initialized.insert("KORUUSDT".into());
        let before = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        worker.invalidate_trade_aggregators().await;
        let recovering = worker.latest.live_snapshot("KORUUSDT").await.unwrap();
        assert!(recovering.recovering);
        assert!(recovering.candles.is_empty());
        assert_eq!(recovering.generation, before.generation + 1);
        assert!(worker.kline_needs_bootstrap("KORUUSDT", 0));
        assert!(worker.latest.get("KORUUSDT", "5").await.is_none());
    }

    #[tokio::test]
    async fn minute_source_missing_final_or_whole_minutes_requires_history_repair() {
        let mut worker = minute_worker(&["1", "5"]).await;
        worker.kline_initialized.insert("KORUUSDT".into());
        worker
            .kline_cursors
            .insert("KORUUSDT".into(), (0, 1_000, false));
        assert!(!worker.kline_needs_bootstrap("KORUUSDT", 0));
        assert!(worker.kline_needs_bootstrap("KORUUSDT", 60_000));
        worker
            .kline_cursors
            .insert("KORUUSDT".into(), (0, 59_999, true));
        assert!(!worker.kline_needs_bootstrap("KORUUSDT", 60_000));
        assert!(worker.kline_needs_bootstrap("KORUUSDT", 120_000));
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
                event_time_ms: current_open + 59_999,
                candle: Candle {
                    open_time: current_open,
                    close_time: current_open + 59_999,
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
