use crypto_candlestick::binance::{rest::sync_native_klines, worker::BinanceWorker};
use crypto_candlestick::config::AppConfig;
use crypto_candlestick::http::{router, AppState, HealthTarget};
use crypto_candlestick::memory::{LatestCache, MemorySeriesStore};
use crypto_candlestick::storage::sqlite::SqliteStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load();

    let store =
        SqliteStore::connect_with_retention(&config.database_url, config.retention_bars).await?;
    let latest = LatestCache::default();
    let memory_series = MemorySeriesStore::new(config.retention_bars as usize);
    let health_targets = config
        .symbols
        .iter()
        .flat_map(|symbol| {
            config.intervals.iter().map(move |interval| HealthTarget {
                symbol: symbol.to_uppercase(),
                interval: interval.canonical(),
            })
        })
        .collect::<Vec<_>>();

    if !config.symbols.is_empty() && !config.intervals.is_empty() {
        if config.sync_on_start {
            tracing::info!("syncing native Binance klines before websocket startup");
            if let Err(err) = sync_native_klines(
                &store,
                config.symbols.clone(),
                config.intervals.clone(),
                config.sync_lookback_bars,
            )
            .await
            {
                tracing::warn!("startup sync failed: {}", err);
            }
        }

        let worker = BinanceWorker::new(
            store.clone(),
            latest.clone(),
            memory_series.clone(),
            config.symbols,
            config.intervals,
        );
        tokio::spawn(async move {
            worker.run().await;
        });
    }

    let app = router(AppState {
        store,
        latest,
        memory_series,
        health_targets,
    });

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
