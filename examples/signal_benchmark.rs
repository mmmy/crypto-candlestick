//! Synthetic signal benchmark. Uses an in-memory database; no network or Webhooks.
use crypto_candlestick::config::AppConfig;
use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::http::{AppState, HealthTarget};
use crypto_candlestick::memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore};
use crypto_candlestick::runtime_health::RuntimeHealth;
use crypto_candlestick::signals::service::{now_ms, SignalService};
use crypto_candlestick::storage::sqlite::SqliteStore;
use std::{collections::HashMap, time::Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let market = AppConfig::load()?;
    let targets = market
        .subscriptions
        .iter()
        .flat_map(|symbol| {
            symbol.intervals.iter().map(move |interval| HealthTarget {
                symbol: symbol.symbol.clone(),
                interval: interval.canonical(),
            })
        })
        .collect::<Vec<_>>();
    let data = AppState {
        store: SqliteStore::connect("sqlite::memory:").await?,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        runtime_health: RuntimeHealth::default(),
        health_targets: targets.clone(),
    };
    // Fixed past simulation time keeps DB's wall-clock closed-row gate deterministic.
    let time = now_ms() - 60_000;
    for subscription in &market.subscriptions {
        for interval in &subscription.intervals {
            let duration = interval.as_millis() as i64;
            let open = interval.bucket_start_ms(time);
            let history = (1..=499)
                .rev()
                .map(|index| candle(open - index * duration, duration, 100.0, true))
                .collect::<Vec<_>>();
            if duration < 60_000 {
                for row in history {
                    data.memory_series
                        .push_closed(&subscription.symbol, &interval.canonical(), row)
                        .await;
                }
            } else {
                data.store
                    .upsert_candles(&subscription.symbol, &interval.canonical(), &history)
                    .await?;
            }
        }
    }
    let path = std::env::temp_dir().join(format!(
        "signal-benchmark-{}-{}.toml",
        std::process::id(),
        now_ms()
    ));
    std::fs::write(&path, "enabled=true\nevaluation_interval_secs=5\n")?;
    let service = SignalService::new(data.clone(), path.clone());
    let loaded = service.reload().await;
    let _ = std::fs::remove_file(&path);
    loaded?;
    let publish = |price: f64| {
        let data = data.clone();
        let subscriptions = market.subscriptions.clone();
        async move {
            let lock = data.latest.market_update_lock();
            let _write = lock.write().await;
            for subscription in subscriptions {
                let current = subscription
                    .intervals
                    .iter()
                    .map(|interval| {
                        (
                            interval.canonical(),
                            candle(
                                interval.bucket_start_ms(time),
                                interval.as_millis() as i64,
                                price,
                                false,
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                data.latest
                    .publish_live_symbol(&subscription.symbol, current, time, time)
                    .await;
            }
        }
    };
    publish(120.0).await;
    let start = Instant::now();
    service.sample_once_at(time).await;
    let cold = start.elapsed().as_secs_f64() * 1000.0;
    let mut updated = Vec::new();
    for index in 0..20 {
        publish(120.0 + index as f64 / 10.0).await;
        let start = Instant::now();
        service.sample_once_at(time).await;
        updated.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let start = Instant::now();
    service.sample_once_at(time).await;
    let unchanged = start.elapsed().as_secs_f64() * 1000.0;
    updated.sort_by(f64::total_cmp);
    let snapshot = service.snapshot_at(time).await;
    println!(
        "{} symbols, {} configured periods; fixed 500-bar windows",
        market.subscriptions.len(),
        targets.len()
    );
    println!("cold input+calculation: {cold:.2} ms; changed-snapshot P95: {:.2} ms; unchanged snapshot: {unchanged:.2} ms",updated[18]);
    println!(
        "synthetic result: {}; no live feed, production DB or notification requests",
        snapshot.status
    );
    Ok(())
}

fn candle(open: i64, duration: i64, price: f64, closed: bool) -> Candle {
    Candle {
        open_time: open,
        close_time: open + duration - 1,
        open: price,
        high: price + 1.0,
        low: price - 1.0,
        close: price,
        volume: 10.0,
        quote_volume: price * 10.0,
        trade_count: 10,
        is_closed: closed,
    }
}
