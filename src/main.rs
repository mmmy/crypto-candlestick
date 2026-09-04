use crypto_candlestick::binance::{
    rest::{refresh_startup_kline_tail, sync_native_klines},
    worker::{flush_closed_buffer, BinanceWorker, FlushLock, SubscriptionPlan},
};
use crypto_candlestick::config::AppConfig;
use crypto_candlestick::http::{router, AppState, HealthTarget};
use crypto_candlestick::logging;
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load()?;
    let _log_guard = logging::init(&config.log_dir, &config.log_level)?;
    tracing::info!(log_dir = %config.log_dir, log_level = %config.log_level, "logging initialized");

    for subscription in &config.subscriptions {
        let intervals = subscription
            .intervals
            .iter()
            .map(|interval| interval.canonical())
            .collect::<Vec<_>>()
            .join(",");
        tracing::info!(
            symbol = %subscription.symbol,
            configured_source = %subscription.source,
            resolved_source = %subscription.resolved_source(),
            intervals,
            "configured market subscription"
        );
    }

    let plan = SubscriptionPlan::from_subscriptions(config.subscriptions.clone());

    let store =
        SqliteStore::connect_with_retention(&config.database_url, config.retention_bars).await?;
    let latest = LatestCache::default();
    let memory_series = MemorySeriesStore::new(config.retention_bars as usize);
    let closed_buffer = ClosedKlineBuffer::default();
    let flush_lock: FlushLock = Arc::new(Mutex::new(()));
    let runtime_health = RuntimeHealth::default();
    let health_targets = plan
        .configured_targets()
        .into_iter()
        .map(|(symbol, interval)| HealthTarget { symbol, interval })
        .collect::<Vec<_>>();

    if !plan.streams().is_empty() {
        let mut initial_sync_completed = false;
        if config.sync_on_start {
            tracing::info!("syncing native Binance klines before websocket startup");
            if let Err(err) = sync_native_klines(&store, &plan, config.sync_lookback_bars).await {
                tracing::warn!("startup sync failed: {}", err);
            } else if let Err(err) = refresh_startup_kline_tail(&store, &plan).await {
                tracing::warn!("startup tail refresh failed: {}", err);
            } else {
                initial_sync_completed = true;
            }
        }

        let worker = BinanceWorker::new(
            store.clone(),
            latest.clone(),
            memory_series.clone(),
            closed_buffer.clone(),
            runtime_health.clone(),
            plan,
            config.sync_lookback_bars,
            !initial_sync_completed,
            config.realtime_flush_max_rows.max(1),
            flush_lock.clone(),
        );
        tokio::spawn(async move {
            worker.run().await;
        });
    }

    let flush_store = store.clone();
    let flush_buffer = closed_buffer.clone();
    let interval_flush_lock = flush_lock.clone();
    let flush_interval_secs = config.realtime_flush_interval_secs.max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(flush_interval_secs));
        loop {
            interval.tick().await;
            flush_closed_buffer(&flush_store, &flush_buffer, &interval_flush_lock).await;
        }
    });

    let app = router(AppState {
        store: store.clone(),
        latest,
        memory_series,
        closed_buffer: closed_buffer.clone(),
        health_targets,
        runtime_health,
    });

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("listening on {}", config.bind_addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(store, closed_buffer, flush_lock))
        .await?;

    Ok(())
}

async fn shutdown_signal(
    store: SqliteStore,
    closed_buffer: ClosedKlineBuffer,
    flush_lock: FlushLock,
) {
    if tokio::signal::ctrl_c().await.is_ok() {
        flush_closed_buffer(&store, &closed_buffer, &flush_lock).await;
    }
}
