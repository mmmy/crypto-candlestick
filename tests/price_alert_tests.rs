use crypto_candlestick::{
    price_alerts::{start_delivery, CreateRequest, Error, PatchRequest, PriceAlert},
    storage::sqlite::SqliteStore,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

async fn fixture(url: &str) -> SqliteStore {
    let store = SqliteStore::connect_price_alert_fixture(url).await.unwrap();
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("0.01"), None)
        .await
        .unwrap();
    store
}

#[tokio::test]
async fn decimal_ticks_round_coordinates_and_compare_fractional_trend_lines_exactly() {
    let store = fixture("sqlite::memory:").await;
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("0.10000000"), None)
        .await
        .unwrap();
    let a = create(&store, create_value("decimal-horizontal", 0.15)).await;
    assert_eq!(a.geometry.first.price, 0.2);
    assert_eq!(a.geometry.second.price, 0.2);
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.1, 100)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.2, 101)
        .await
        .unwrap();
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].trigger_price,
        0.2
    );
    let mut value = create_value("decimal-trend", 0.2);
    value["geometry"] = json!({"kind":"trend_segment","first":{"timeMs":102,"price":0.2},"second":{"timeMs":104,"price":0.4},"extend":"both"});
    let a = create(&store, value).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.1, 102)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.3, 103)
        .await
        .unwrap();
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].direction,
        "cross_up"
    );
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("0.01"), None)
        .await
        .unwrap();
    let mut value = create_value("fractional-line", 0.2);
    value["geometry"] = json!({"kind":"trend_segment","first":{"timeMs":104,"price":0.2},"second":{"timeMs":106,"price":0.21},"extend":"both"});
    let a = create(&store, value).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.2, 105)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 0.21, 106)
        .await
        .unwrap();
    assert_eq!(store.price_alert_events(a.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn metadata_failure_suspends_v2_and_recovery_establishes_fresh_baseline() {
    let store = SqliteStore::connect_price_alert_fixture("sqlite::memory:")
        .await
        .unwrap();
    let initial = store.price_alert_market("BTCUSDT").await.unwrap();
    assert_eq!(initial.status, "loading");
    assert_eq!(initial.tick_size, None);
    let active = create_value("no-metadata", 100.0);
    assert!(matches!(
        store
            .create_price_alert(
                serde_json::from_value(active.clone()).unwrap(),
                &active.to_string()
            )
            .await,
        Err(Error::MetadataUnavailable(_))
    ));
    let mut disabled = active;
    disabled["status"] = json!("disabled");
    disabled["mutationId"] = json!("disabled-no-metadata");
    let a = create(&store, disabled).await;
    assert_eq!(a.status, "disabled");
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("0.01"), None)
        .await
        .unwrap();
    let a = patch(
        &store,
        a.id,
        json!({"mutationId":"metadata-arm","expectedRevision":1,"rearm":true}),
    )
    .await
    .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
        .await
        .unwrap();
    store
        .set_price_alert_metadata_fixture("BTCUSDT", None, Some("isolated exchange outage"))
        .await
        .unwrap();
    assert_eq!(
        store.get_price_alert(a.id).await.unwrap().data_status,
        "unavailable"
    );
    assert_eq!(
        store
            .price_alert_market("BTCUSDT")
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("isolated exchange outage")
    );
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, 11)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("0.01"), None)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, 12)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 100.0, 13)
        .await
        .unwrap();
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].direction,
        "cross_down"
    );
}

#[tokio::test]
async fn tick_ordinals_are_bounded_and_invalid_sample_resets_only_v2_baseline() {
    let store = fixture("sqlite::memory:").await;
    store
        .set_price_alert_metadata_fixture("BTCUSDT", Some("1"), None)
        .await
        .unwrap();
    let max = (1_u64 << 53) as f64;
    let too_large = create_value("over-bound", max + 2.0);
    assert!(matches!(
        store
            .create_price_alert(
                serde_json::from_value(too_large.clone()).unwrap(),
                &too_large.to_string()
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let a = create(&store, create_value("large-line", max - 2.0)).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", max - 4.0, 10)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", max + 2.0, 11)
        .await
        .unwrap(); // invalid ordinal cannot synthesize crossing
    store
        .evaluate_drawing_alerts("BTCUSDT", max, 12)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", max - 2.0, 13)
        .await
        .unwrap();
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].direction,
        "cross_down"
    );
    let ordinary = SqliteStore::connect("sqlite::memory:").await.unwrap();
    assert!(ordinary
        .set_price_alert_metadata_fixture("BTCUSDT", Some("1"), None)
        .await
        .is_err());
}

fn create_value(id: &str, price: f64) -> Value {
    json!({"mutationId":id,"symbol":"BTCUSDT","interval":"60","status":"active","name":"crossing",
        "geometry":{"kind":"horizontal_segment","first":{"timeMs":1,"price":price},"second":{"timeMs":2,"price":price},"extend":"both"},
        "webhookUrl":"http://127.0.0.1:1/isolated","messageTemplate":"{\"symbol\":\"{{ticker}}\",\"price\":\"{{close}}\",\"period\":\"{{interval}}\",\"noConfirm\":false}"})
}
async fn create(store: &SqliteStore, value: Value) -> PriceAlert {
    store
        .create_price_alert(
            serde_json::from_value::<CreateRequest>(value.clone()).unwrap(),
            &value.to_string(),
        )
        .await
        .unwrap()
}
async fn patch(store: &SqliteStore, id: i64, value: Value) -> Result<PriceAlert, Error> {
    store
        .patch_price_alert(
            id,
            serde_json::from_value::<PatchRequest>(value.clone()).unwrap(),
            &value.to_string(),
        )
        .await
}
#[tokio::test]
async fn touch_cross_once_keeps_tv_json_types_and_observed_direction() {
    let store = fixture("sqlite::memory:").await;
    let a = create(&store, create_value("create-touch", 100.0)).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 99.0, 10)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 100.0, 11)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 101.0, 12)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 98.0, 13)
        .await
        .unwrap();
    let events = store.price_alert_events(a.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].direction, "cross_up");
    assert_eq!(events[0].line_price, 100.0);
    assert_eq!(events[0].payload["symbol"], "BTCUSDT");
    assert_eq!(events[0].payload["price"], "100");
    assert_eq!(events[0].payload["period"], "60");
    assert_eq!(events[0].payload["noConfirm"], false);
    assert_eq!(store.get_price_alert(a.id).await.unwrap().revision, 2);
}

#[tokio::test]
async fn ticker_preserves_backend_symbol_without_using_tv_alias() {
    let store = fixture("sqlite::memory:").await;
    store
        .set_price_alert_metadata_fixture("SOXLUSDT", Some("0.01"), None)
        .await
        .unwrap();
    let mut request = create_value("original-backend-ticker", 168.5);
    request["symbol"] = json!("SOXLUSDT");
    request["tvSymbol"] = json!("SOXLUSDT.P");
    request["interval"] = json!("30");
    request["messageTemplate"] = json!(json!({
        "des": "{{interval}}底部合约多{{ticker}}下穿{{close}}",
        "exchange": "BINANCE",
        "name": "PRE-LONG",
        "noConfirm": false,
        "period": "{{interval}}",
        "price": "{{close}}",
        "side": "BUY",
        "symbol": "{{ticker}}"
    })
    .to_string());
    let alert = create(&store, request).await;
    assert_eq!(alert.tv_symbol, "SOXLUSDT.P");
    store
        .evaluate_drawing_alerts("SOXLUSDT", 169.0, 10)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("SOXLUSDT", 168.34, 11)
        .await
        .unwrap();
    let events = store.price_alert_events(alert.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].payload,
        json!({
            "des": "30底部合约多SOXLUSDT下穿168.34",
            "exchange": "BINANCE",
            "name": "PRE-LONG",
            "noConfirm": false,
            "period": "30",
            "price": "168.34",
            "side": "BUY",
            "symbol": "SOXLUSDT"
        })
    );
}
#[tokio::test]
async fn move_has_fresh_baseline_triggered_rearms_and_appearance_preserves_it() {
    let store = fixture("sqlite::memory:").await;
    let a = create(&store, create_value("create-move", 100.0)).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 99.0, 10)
        .await
        .unwrap();
    let mut g = a.geometry.clone();
    g.first.price = 110.0;
    g.second.price = 110.0;
    let moved = patch(
        &store,
        a.id,
        json!({"mutationId":"move","expectedRevision":1,"geometry":g}),
    )
    .await
    .unwrap();
    assert_eq!(moved.arm_generation, 2);
    store
        .evaluate_drawing_alerts("BTCUSDT", 105.0, 11)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    let styled = patch(
        &store,
        a.id,
        json!({"mutationId":"style","expectedRevision":2,"label":"BUY","color":"#ff0011"}),
    )
    .await
    .unwrap();
    assert_eq!(styled.arm_generation, 2);
    store
        .evaluate_drawing_alerts("BTCUSDT", 111.0, 12)
        .await
        .unwrap();
    let fired = store.get_price_alert(a.id).await.unwrap();
    assert_eq!(fired.status, "triggered");
    let mut g = fired.geometry.clone();
    g.first.price = 120.0;
    g.second.price = 120.0;
    let rearmed = patch(
        &store,
        a.id,
        json!({"mutationId":"after-trigger","expectedRevision":fired.revision,"geometry":g}),
    )
    .await
    .unwrap();
    assert_eq!(rearmed.status, "active");
    assert_eq!(rearmed.arm_generation, 3);
    assert_eq!(rearmed.triggered_at, None);
    store
        .evaluate_drawing_alerts("BTCUSDT", 121.0, 13)
        .await
        .unwrap(); // first sample is baseline
    assert_eq!(store.price_alert_events(a.id).await.unwrap().len(), 1);
    store
        .evaluate_drawing_alerts("BTCUSDT", 119.0, 14)
        .await
        .unwrap();
    assert_eq!(store.price_alert_events(a.id).await.unwrap().len(), 2);
    let page = store.price_alert_events_page(a.id, 1, None).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].arm_generation, 3);
    let older = store
        .price_alert_events_page(a.id, 1, Some(page[0].arm_generation))
        .await
        .unwrap();
    assert_eq!(older.len(), 1);
    assert_eq!(older[0].arm_generation, 2);
    assert!(store.price_alert_events_page(a.id, 0, None).await.is_err());
}
#[tokio::test]
async fn paused_move_stays_paused_expired_move_stays_expired_and_null_expiry_rearms() {
    let store = fixture("sqlite::memory:").await;
    let mut value = create_value("disabled", 100.0);
    value["status"] = json!("disabled");
    value["messageTemplate"] = json!("");
    value["webhookUrl"] = json!("");
    let a = create(&store, value).await;
    let mut g = a.geometry.clone();
    g.first.price = 101.0;
    g.second.price = 101.0;
    let paused = patch(
        &store,
        a.id,
        json!({"mutationId":"paused-move","expectedRevision":1,"geometry":g}),
    )
    .await
    .unwrap();
    assert_eq!(paused.status, "disabled");
    assert_eq!(paused.arm_generation, 1);
    let mut value = create_value("expired", 100.0);
    value["expiresAt"] = json!(1);
    let a = create(&store, value).await;
    assert_eq!(a.status, "expired");
    let moved = patch(
        &store,
        a.id,
        json!({"mutationId":"expired-move","expectedRevision":1,"geometry":paused.geometry}),
    )
    .await
    .unwrap();
    assert_eq!(moved.status, "expired");
    assert_eq!(moved.arm_generation, 1);
    assert!(matches!(
        patch(
            &store,
            a.id,
            json!({"mutationId":"bad-rearm","expectedRevision":2,"rearm":true})
        )
        .await,
        Err(Error::Invalid(_))
    ));
    let active = patch(
        &store,
        a.id,
        json!({"mutationId":"clear-expiry","expectedRevision":2,"expiresAt":null}),
    )
    .await
    .unwrap();
    assert_eq!(active.expires_at, None);
    assert_eq!(active.status, "active");
    assert_eq!(active.arm_generation, 2);
    let disabled=patch(&store,a.id,json!({"mutationId":"disable-while-move","expectedRevision":3,"geometry":a.geometry,"status":"disabled"})).await.unwrap();
    assert_eq!(disabled.status, "disabled");
    assert_eq!(disabled.arm_generation, 2);
}
#[tokio::test]
async fn finite_segment_enters_without_synthetic_cross_and_trend_uses_each_sample_line() {
    let store = fixture("sqlite::memory:").await;
    let mut value = create_value("finite", 100.0);
    value["geometry"]["first"]["timeMs"] = json!(100);
    value["geometry"]["second"]["timeMs"] = json!(200);
    value["geometry"]["extend"] = json!("none");
    let a = create(&store, value).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 90)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, 100)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 100.0, 101)
        .await
        .unwrap();
    assert_eq!(store.price_alert_events(a.id).await.unwrap().len(), 1);
    let mut value = create_value("trend", 100.0);
    value["geometry"] = json!({"kind":"trend_segment","first":{"timeMs":200,"price":120.0},"second":{"timeMs":100,"price":100.0},"extend":"both"});
    let a = create(&store, value).await;
    assert_eq!(a.geometry.first.time_ms, 100);
    store
        .evaluate_drawing_alerts("BTCUSDT", 111.0, 150)
        .await
        .unwrap(); // above 110
    store
        .evaluate_drawing_alerts("BTCUSDT", 111.0, 160)
        .await
        .unwrap(); // below 112
    let events = store.price_alert_events(a.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].direction, "cross_down");
    assert_eq!(events[0].line_price, 112.0);
}
#[tokio::test]
async fn recovery_and_out_of_order_data_never_replay_crossings() {
    let store = fixture("sqlite::memory:").await;
    let a = create(&store, create_value("recovery", 100.0)).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
        .await
        .unwrap();
    store.reset_price_alert_baselines(Some("BTCUSDT")).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, 20)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 19)
        .await
        .unwrap();
    assert!(store.price_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 100.0, 21)
        .await
        .unwrap();
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].direction,
        "cross_down"
    );
}
#[tokio::test]
async fn mutations_recover_exact_response_conflicting_reuse_and_cas_are_rejected() {
    let store = fixture("sqlite::memory:").await;
    let value = create_value("idempotent", 100.0);
    let a = create(&store, value.clone()).await;
    assert_eq!(create(&store, value).await, a);
    assert_eq!(store.list_price_alerts().await.unwrap().len(), 1);
    let mut changed = create_value("idempotent", 101.0);
    assert!(matches!(
        store
            .create_price_alert(
                serde_json::from_value(changed.clone()).unwrap(),
                &changed.to_string()
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let first = json!({"mutationId":"cas-first","expectedRevision":1,"label":"one"});
    let second = json!({"mutationId":"cas-second","expectedRevision":1,"label":"two"});
    let (one, two) = tokio::join!(
        patch(&store, a.id, first.clone()),
        patch(&store, a.id, second)
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    assert_eq!(patch(&store, a.id, first).await.unwrap().revision, 2);
    let deleted = store
        .delete_price_alert(a.id, 2, "delete-cas")
        .await
        .unwrap();
    assert_eq!(
        store
            .delete_price_alert(a.id, 2, "delete-cas")
            .await
            .unwrap(),
        deleted
    );
    assert_eq!(
        store.price_alert_mutation("delete-cas").await.unwrap(),
        deleted
    );
    changed["mutationId"] = json!("missing-old");
    assert!(matches!(
        store.get_price_alert(a.id).await,
        Err(Error::NotFound)
    ));
}
#[tokio::test]
async fn persistent_outbox_survives_restart_and_old_receipt_cannot_pollute_rearm() {
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    type Seen = Arc<Mutex<Vec<(String, String, Value)>>>;
    async fn receive(
        State(seen): State<Seen>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        seen.lock().unwrap().push((
            headers["x-guaili-event-id"].to_str().unwrap().into(),
            headers["x-guaili-event-time"].to_str().unwrap().into(),
            body,
        ));
        Json(json!({"accepted":true}))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/isolated", post(receive))
        .with_state(seen.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let file = std::env::temp_dir().join(format!(
        "guaili-alert-fixture-{}-{}.db",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let url = format!(
        "sqlite://{}?mode=rwc",
        file.to_string_lossy().replace('\\', "/")
    );
    let store = fixture(&url).await;
    let mut value = create_value("restart", 100.0);
    value["webhookUrl"] = json!(format!("http://{addr}/isolated"));
    let a = create(&store, value).await;
    let now = chrono::Utc::now().timestamp_millis();
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, now)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, now + 1)
        .await
        .unwrap();
    let fired = store.get_price_alert(a.id).await.unwrap();
    let prior = store.price_alert_events(a.id).await.unwrap()[0].clone();
    let rearmed = patch(
        &store,
        a.id,
        json!({"mutationId":"restart-rearm","expectedRevision":fired.revision,"rearm":true}),
    )
    .await
    .unwrap();
    drop(store);
    let store = fixture(&url).await;
    let delivery = start_delivery(store.clone()).unwrap();
    for _ in 0..80 {
        if store.price_alert_events(a.id).await.unwrap()[0].delivery_status == "success" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        store.price_alert_events(a.id).await.unwrap()[0].delivery_status,
        "success"
    );
    let current = store.get_price_alert(a.id).await.unwrap();
    assert_eq!(current.arm_generation, rearmed.arm_generation);
    assert_eq!(current.status, "active");
    assert_eq!(current.delivery_status, None);
    {
        let data = seen.lock().unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].0, prior.id);
        assert_eq!(data[0].1, (now + 1).to_string());
        assert_eq!(data[0].2, prior.payload);
    }
    delivery.stop().await;
    server.abort();
    drop(store);
    // Only isolated, explicitly created fixtures are removed.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", file.display()));
    }
}
#[tokio::test]
async fn legacy_cache_reset_persistent_clears_and_event_receipts_remain_compatible() {
    use crypto_candlestick::storage::sqlite::Alert;
    let store = fixture("sqlite::memory:").await;
    let mut a = store
        .insert_alert(&Alert {
            id: 0,
            symbol: "BTCUSDT".into(),
            interval: "60".into(),
            price: 100.0,
            direction: "cross_any".into(),
            status: "active".into(),
            expires_at: Some(i64::MAX),
            webhook_url: "http://127.0.0.1:1/isolated".into(),
            message_template: "{\"symbol\":\"{{ticker}}\"}".into(),
            created_at: 1,
            updated_at: 1,
            triggered_at: None,
            delivery_status: None,
            delivery_error: None,
        })
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
        .await
        .unwrap();
    a.price = 200.0;
    a.updated_at = 2;
    a.expires_at = None;
    store.update_alert(&a).await.unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 220.0, 11)
        .await
        .unwrap();
    assert!(store.list_alert_events(a.id).await.unwrap().is_empty());
    store
        .evaluate_drawing_alerts("BTCUSDT", 200.0, 12)
        .await
        .unwrap();
    let old = store.list_alert_events(a.id).await.unwrap()[0].clone();
    a.updated_at = 13;
    a.status = "active".into();
    a.triggered_at = None;
    a.delivery_status = None;
    a.delivery_error = None;
    store.update_alert(&a).await.unwrap();
    let saved = store.get_alert(a.id).await.unwrap().unwrap();
    assert_eq!(saved.expires_at, None);
    assert_eq!(saved.triggered_at, None);
    assert_eq!(saved.delivery_status, None);
    store
        .evaluate_drawing_alerts("BTCUSDT", 190.0, 14)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 210.0, 15)
        .await
        .unwrap();
    store
        .set_alert_event_delivery(old.id, "failed", Some("old generation"))
        .await
        .unwrap();
    let events = store.list_alert_events(a.id).await.unwrap();
    assert_eq!(events[1].delivery_status.as_deref(), Some("failed"));
    assert_eq!(events[0].delivery_status.as_deref(), Some("pending"));
    assert_eq!(
        store
            .get_alert(a.id)
            .await
            .unwrap()
            .unwrap()
            .delivery_status
            .as_deref(),
        Some("pending")
    );
}

#[tokio::test]
async fn deleted_drawings_preserve_cancelled_trigger_history() {
    let store = fixture("sqlite::memory:").await;
    let a = create(&store, create_value("delete-history", 100.0)).await;
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, 10)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, 11)
        .await
        .unwrap();
    let a = store.get_price_alert(a.id).await.unwrap();
    store
        .delete_price_alert(a.id, a.revision, "delete-history-commit")
        .await
        .unwrap();
    let events = store.price_alert_events(a.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].delivery_status, "cancelled");
}

#[tokio::test]
async fn pooled_delivery_is_bounded_and_shutdown_cancels_in_flight_requests() {
    use axum::{extract::State, routing::post, Router};
    type Counts = (Arc<AtomicUsize>, Arc<AtomicUsize>);
    async fn receive(State((active, maximum)): State<Counts>) -> &'static str {
        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
        maximum.fetch_max(count, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3)).await;
        active.fetch_sub(1, Ordering::SeqCst);
        "{}"
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/isolated", post(receive))
        .with_state((active.clone(), maximum.clone()));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let store = fixture("sqlite::memory:").await;
    for n in 0..16 {
        let mut value = create_value(&format!("bound-{n}"), 100.0);
        value["webhookUrl"] = json!(format!("http://{address}/isolated"));
        create(&store, value).await;
    }
    let now = chrono::Utc::now().timestamp_millis();
    store
        .evaluate_drawing_alerts("BTCUSDT", 90.0, now)
        .await
        .unwrap();
    store
        .evaluate_drawing_alerts("BTCUSDT", 110.0, now + 1)
        .await
        .unwrap();
    let delivery = start_delivery(store.clone()).unwrap();
    for _ in 0..80 {
        if active.load(Ordering::SeqCst) == 8 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 8);
    let start = Instant::now();
    delivery.stop().await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(store
        .price_alert_events(1)
        .await
        .unwrap()
        .iter()
        .all(|e| e.delivery_status == "pending"));
    server.abort();
}

#[tokio::test]
async fn http_v2_contract_supports_drawings_filters_null_expiry_and_recovery() {
    use crypto_candlestick::{
        http::{router, AppState, HealthTarget},
        memory::{ClosedKlineBuffer, LatestCache, MemorySeriesStore},
        runtime_health::RuntimeHealth,
    };
    let store = fixture("sqlite::memory:").await;
    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        closed_buffer: ClosedKlineBuffer::default(),
        runtime_health: RuntimeHealth::default(),
        health_targets: vec![HealthTarget {
            symbol: "BTCUSDT".into(),
            interval: "60".into(),
        }],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/api/price-alerts");
    let market: Value = client
        .get(format!("{base}/market?symbol=BTCUSDT"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        market,
        json!({"symbol":"BTCUSDT","tvSymbol":"BTCUSDT.P","tickSize":"0.01","status":"ready","reason":null})
    );
    assert_eq!(
        client
            .get(format!("{base}/market?symbol=ETHUSDT"))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let mut value = create_value("http-create", 100.0);
    value["status"] = json!("disabled");
    value["webhookUrl"] = json!("");
    value["messageTemplate"] = json!("");
    let response = client.post(&base).json(&value).send().await.unwrap();
    assert_eq!(response.status(), 201);
    let a: PriceAlert = response.json().await.unwrap();
    assert_eq!(a.status, "disabled");
    let replay: Value = client
        .get(format!("{base}/mutations/http-create"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay["id"], a.id);
    let filtered: Value = client
        .get(format!("{base}?symbol=ETHUSDT"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered, json!([]));
    assert_eq!(
        client
            .patch(format!("{base}/{}", a.id))
            .json(&json!({"mutationId":"http-stale","expectedRevision":0,"label":"bad"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        client
            .delete(format!(
                "{base}/{}?mutationId=http-delete&expectedRevision=1",
                a.id
            ))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    server.abort();
}
#[tokio::test]
#[ignore = "Release benchmark; run explicitly with cargo test --release --test price_alert_tests release_index_benchmark -- --ignored --nocapture"]
async fn release_index_benchmark() {
    let store = fixture("sqlite::memory:").await;
    let count = 1000;
    for n in 0..count {
        create(
            &store,
            create_value(&format!("bench-{n}"), 1000.0 + n as f64),
        )
        .await;
    }
    store
        .evaluate_drawing_alerts("BTCUSDT", 900.0, 1000)
        .await
        .unwrap();
    let samples = 10_000;
    let start = Instant::now();
    for n in 0..samples {
        store
            .evaluate_drawing_alerts("BTCUSDT", 900.0 + (n % 2) as f64, 1001 + n)
            .await
            .unwrap();
    }
    let memory = start.elapsed();
    // Identical rule count, price and no-event scenario. The v1 former worker
    // queried SQLite and decoded every active rule on each accepted tick.
    let old_store = fixture("sqlite::memory:").await;
    for n in 0..count {
        old_store
            .insert_alert(&crypto_candlestick::storage::sqlite::Alert {
                id: 0,
                symbol: "BTCUSDT".into(),
                interval: "60".into(),
                price: 1000.0 + n as f64,
                direction: "cross_any".into(),
                status: "active".into(),
                expires_at: None,
                webhook_url: "http://127.0.0.1:1/isolated".into(),
                message_template: "{}".into(),
                created_at: 0,
                updated_at: 0,
                triggered_at: None,
                delivery_status: None,
                delivery_error: None,
            })
            .await
            .unwrap();
    }
    let start = Instant::now();
    let mut sides = std::collections::HashMap::new();
    for n in 0..samples {
        let price = 900.0 + (n % 2) as f64;
        for alert in old_store
            .active_alerts_for_symbol("BTCUSDT", 1001 + n)
            .await
            .unwrap()
        {
            sides.insert(alert.id, if price > alert.price { 1 } else { -1 });
        }
    }
    let sqlite = start.elapsed();
    println!("Release identical horizontal once/both rules={count}, samples={samples}, no crossings: indexed memory {:?} ({:.3} us/tick); former SQLite {:?} ({:.3} us/tick); speedup {:.1}x; memory-only isolated SQLite one writer; no networking or outbox work",memory,memory.as_secs_f64()*1e6/samples as f64,sqlite,sqlite.as_secs_f64()*1e6/samples as f64,sqlite.as_secs_f64()/memory.as_secs_f64());
    assert_eq!(store.price_alert_events(1).await.unwrap().len(), 0);
}
