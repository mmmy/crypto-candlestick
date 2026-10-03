use crypto_candlestick::{domain::candle::Candle, memory::LatestCache};
use std::collections::HashMap;

fn dynamic_candle() -> Candle {
    Candle {
        open_time: 60_000,
        close_time: 119_999,
        open: 100.0,
        high: 105.0,
        low: 99.0,
        close: 104.0,
        volume: 2.0,
        quote_volume: 208.0,
        trade_count: 2,
        is_closed: false,
    }
}

#[tokio::test]
async fn snapshot_metadata_is_per_symbol_and_recovery_is_idempotent() {
    let cache = LatestCache::default();
    let lock = cache.market_update_lock();
    let _guard = lock.write().await;
    cache
        .publish_live_symbol(
            "btcusdt",
            HashMap::from([("1".to_string(), dynamic_candle())]),
            61_000,
            61_100,
        )
        .await;
    cache
        .publish_live_symbol(
            "ETHUSDT",
            HashMap::from([("1".to_string(), dynamic_candle())]),
            62_000,
            62_100,
        )
        .await;
    let before = cache.live_snapshot("BTCUSDT").await.unwrap();
    cache.mark_recovering("btcusdt").await;
    cache.mark_recovering("BTCUSDT").await;
    let recovery = cache.live_snapshot("BTCUSDT").await.unwrap();
    assert!(recovery.recovering);
    assert_eq!(recovery.generation, before.generation + 1);
    assert_eq!(recovery.sequence, before.sequence);
    assert_eq!(recovery.received_at_ms, before.received_at_ms);
    assert!(recovery.candles.is_empty());
    assert!(!cache.live_snapshot("ETHUSDT").await.unwrap().recovering);
    cache
        .publish_live_symbol(
            "BTCUSDT",
            HashMap::from([("1".to_string(), dynamic_candle())]),
            63_000,
            63_100,
        )
        .await;
    let restored = cache.live_snapshot("btcusdt").await.unwrap();
    assert!(!restored.recovering);
    assert_eq!(restored.generation, recovery.generation);
    assert_eq!(restored.sequence, before.sequence + 1);
    // An owned earlier snapshot remains immutable after subsequent updates.
    assert_eq!(before.market_event_time_ms, 61_000);
    assert_eq!(before.candles.len(), 1);
}

#[tokio::test]
async fn legacy_candle_cache_does_not_claim_atomic_live_trade_support() {
    let cache = LatestCache::default();
    cache.upsert("BTCUSDT", "1", dynamic_candle()).await;
    assert!(cache.get("BTCUSDT", "1").await.is_some());
    assert!(cache.live_snapshot("BTCUSDT").await.is_none());
}

#[test]
fn bootstrap_history_sources_do_not_become_public_targets() {
    use crypto_candlestick::{
        binance::worker::SubscriptionPlan,
        config::{RealtimeSource, SymbolSubscription},
        domain::interval::Interval,
    };
    let plan = SubscriptionPlan::from_subscriptions(vec![SymbolSubscription::new(
        "BTCUSDT",
        vec![Interval::Minutes(60), Interval::Weeks(1)],
        RealtimeSource::Trade,
    )]);
    let history = plan.kline_sources();
    assert!(history
        .iter()
        .any(|source| source.canonical_interval == "1"));
    assert!(history
        .iter()
        .any(|source| source.canonical_interval == "D"));
    let public = plan.configured_targets();
    assert!(!public
        .iter()
        .any(|(_, interval)| interval == "1" || interval == "D"));
    assert_eq!(public.len(), 2);
}

#[tokio::test]
async fn recovery_watch_notifies_all_subscribers_once_per_generation() {
    let cache = LatestCache::default();
    let mut first = cache.recovery_changes();
    let mut second = cache.clone().recovery_changes();
    assert_eq!(*first.borrow(), 0);
    assert!(!first.has_changed().unwrap());
    cache.mark_recovering("BTCUSDT").await;
    assert!(first.has_changed().unwrap());
    assert!(second.has_changed().unwrap());
    first.changed().await.unwrap();
    second.changed().await.unwrap();
    assert_eq!(*first.borrow(), 1);
    assert_eq!(*second.borrow(), 1);
    assert!(cache.live_snapshot("BTCUSDT").await.unwrap().recovering);
    cache.mark_recovering("btcusdt").await;
    assert!(!first.has_changed().unwrap());
    assert!(!second.has_changed().unwrap());
    cache
        .publish_live_symbol("BTCUSDT", HashMap::new(), 61_000, 61_100)
        .await;
    assert!(!first.has_changed().unwrap());
    cache.mark_recovering("BTCUSDT").await;
    first.changed().await.unwrap();
    second.changed().await.unwrap();
    assert_eq!(*first.borrow(), 2);
    assert_eq!(*second.borrow(), 2);
    cache.mark_recovering("ETHUSDT").await;
    assert!(first.has_changed().unwrap());
    assert!(second.has_changed().unwrap());
    assert_eq!(*first.borrow_and_update(), 3);
    assert_eq!(*second.borrow_and_update(), 3);
}

#[tokio::test]
async fn recovery_watch_retains_changes_without_an_active_receiver() {
    let cache = LatestCache::default();
    cache.mark_recovering("BTCUSDT").await;
    let mut late = cache.recovery_changes();
    assert_eq!(*late.borrow_and_update(), 1);
    assert!(!late.has_changed().unwrap());
    cache.mark_recovering("ETHUSDT").await;
    assert!(late.has_changed().unwrap());
    assert_eq!(*late.borrow_and_update(), 2);
}
