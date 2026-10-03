use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use crypto_candlestick::signals::{
    config::{SignalConfig, WecomAlertConfig},
    delivery::{send, DeliveryJob, DeliveryManager},
    model::{
        Availability, IntervalEvidence, SignalDirection, SignalKind, SignalRun, SignalStructure,
    },
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn alert(id: &str, url: &str) -> WecomAlertConfig {
    WecomAlertConfig {
        id: id.into(),
        name: id.into(),
        webhook_url: url.into(),
        symbols: vec!["BTCUSDT".into()],
        min_signal_interval: "15".into(),
        kinds: vec![
            SignalKind::Extreme,
            SignalKind::Compression,
            SignalKind::Conflict,
        ],
        cooldown_secs: 300,
    }
}

fn config() -> SignalConfig {
    SignalConfig {
        enabled: true,
        symbols: vec!["BTCUSDT".into()],
        wecom_alerts: vec![alert("btc", "http://127.0.0.1:12345/mock?key=TEST_SECRET")],
        ..SignalConfig::default()
    }
}

fn signal(id: &str, anchor: &str, kind: SignalKind) -> SignalStructure {
    SignalStructure {
        id: id.into(),
        kind,
        direction: SignalDirection::Positive,
        runs: vec![SignalRun {
            direction: SignalDirection::Positive,
            intervals: vec![
                "3".into(),
                "5".into(),
                "8".into(),
                "10".into(),
                anchor.into(),
            ],
            min_abs_value: 10,
            max_abs_value: 13,
            mean_abs_value: 11.0,
            max_abs_guaili: None,
            mean_abs_guaili: None,
        }],
        level_count: 5,
        total_level_count: 5,
        anchor_interval: anchor.into(),
        first_observed_at: Some(0),
        formed_at: Some(0),
        last_changed_at: Some(0),
    }
}

fn results(
    signals: Vec<SignalStructure>,
    complete: bool,
) -> Vec<(String, Vec<SignalStructure>, bool)> {
    vec![("BTCUSDT".into(), signals, complete)]
}

fn evidence(missing: Option<&str>) -> Vec<IntervalEvidence> {
    let mut points = ["3", "5", "8", "10", "15"]
        .iter()
        .map(|interval| IntervalEvidence {
            interval: (*interval).into(),
            availability: if missing == Some(*interval) {
                Availability::Missing
            } else {
                Availability::Ready
            },
            ..IntervalEvidence::default()
        })
        .collect::<Vec<_>>();
    points.push(IntervalEvidence {
        interval: "W".into(),
        availability: Availability::WarmingUp,
        ..IntervalEvidence::default()
    });
    points
}

#[test]
fn partial_long_warmup_does_not_block_short_alert() {
    let config = config();
    let mut manager = DeliveryManager::new();
    let baseline = vec![("BTCUSDT".into(), vec![], evidence(None))];
    assert!(manager
        .prepare_with_evidence(&config, &baseline, 0)
        .is_empty());
    let current = vec![(
        "BTCUSDT".into(),
        vec![signal("short", "15", SignalKind::Extreme)],
        evidence(None),
    )];
    assert_eq!(
        manager.prepare_with_evidence(&config, &current, 5000).len(),
        1
    );
    assert!(manager
        .prepare_with_evidence(&config, &current, 10_000)
        .is_empty());
}

#[test]
fn affected_missing_keeps_match_until_known_end() {
    let mut config = config();
    config.wecom_alerts[0].cooldown_secs = 0;
    let mut manager = DeliveryManager::new();
    let active = vec![(
        "BTCUSDT".into(),
        vec![signal("same", "15", SignalKind::Extreme)],
        evidence(None),
    )];
    assert!(manager
        .prepare_with_evidence(&config, &active, 0)
        .is_empty());
    let unknown = vec![("BTCUSDT".into(), vec![], evidence(Some("5")))];
    assert!(manager
        .prepare_with_evidence(&config, &unknown, 5000)
        .is_empty());
    assert!(manager
        .prepare_with_evidence(&config, &active, 10_000)
        .is_empty());
    let ended = vec![("BTCUSDT".into(), vec![], evidence(None))];
    manager.prepare_with_evidence(&config, &ended, 15_000);
    assert_eq!(
        manager
            .prepare_with_evidence(&config, &active, 20_000)
            .len(),
        1
    );
}

#[test]
fn all_unknown_does_not_establish_evidence_baseline() {
    let config = config();
    let mut manager = DeliveryManager::new();
    let unknown = vec![(
        "BTCUSDT".into(),
        vec![],
        vec![IntervalEvidence {
            interval: "15".into(),
            availability: Availability::WarmingUp,
            ..IntervalEvidence::default()
        }],
    )];
    assert!(manager
        .prepare_with_evidence(&config, &unknown, 0)
        .is_empty());
    let active = vec![(
        "BTCUSDT".into(),
        vec![signal("first", "15", SignalKind::Extreme)],
        evidence(None),
    )];
    assert!(manager
        .prepare_with_evidence(&config, &active, 5000)
        .is_empty());
}

#[test]
fn baseline_minimum_crossing_and_continuation_do_not_spam() {
    let config = config();
    let mut manager = DeliveryManager::new();
    assert!(manager
        .prepare(
            &config,
            &results(vec![signal("one", "8", SignalKind::Extreme)], true),
            0
        )
        .is_empty());
    let eligible = results(vec![signal("one", "15", SignalKind::Extreme)], true);
    assert_eq!(manager.prepare(&config, &eligible, 5000).len(), 1);
    assert!(manager.prepare(&config, &eligible, 10_000).is_empty());
    let expanded = results(vec![signal("one", "60", SignalKind::Extreme)], true);
    assert!(manager.prepare(&config, &expanded, 15_000).is_empty());
}

#[test]
fn startup_existing_signals_and_unknown_results_establish_safe_baseline() {
    let config = config();
    let eligible = results(vec![signal("one", "15", SignalKind::Extreme)], true);
    let mut manager = DeliveryManager::new();
    assert!(manager
        .prepare(&config, &results(vec![], false), 0)
        .is_empty());
    assert!(manager.prepare(&config, &eligible, 5000).is_empty());
    assert!(manager
        .prepare(&config, &results(vec![], false), 10_000)
        .is_empty());
    assert!(manager.prepare(&config, &eligible, 15_000).is_empty());
    manager.prepare(&config, &results(vec![], true), 20_000);
    assert_eq!(
        manager
            .prepare(
                &config,
                &results(vec![signal("two", "15", SignalKind::Extreme)], true),
                25_000
            )
            .len(),
        1
    );
}

#[test]
fn scans_all_candidates_and_deduplicates_same_target() {
    let mut config = config();
    config
        .wecom_alerts
        .push(alert("overlap", &config.wecom_alerts[0].webhook_url));
    let mut manager = DeliveryManager::new();
    manager.prepare(&config, &results(vec![], true), 0);
    let jobs = manager.prepare(
        &config,
        &results(
            vec![
                signal("too-small", "8", SignalKind::Extreme),
                signal("compression", "15", SignalKind::Compression),
            ],
            true,
        ),
        5000,
    );
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].message.contains("多周期近均线"));
    assert!(!format!("{:?}", jobs[0]).contains("TEST_SECRET"));
}

#[test]
fn separate_targets_receive_same_event_and_cooldown_blocks_reformation() {
    let mut config = config();
    config
        .wecom_alerts
        .push(alert("other-target", "http://127.0.0.1:12346/mock"));
    let mut manager = DeliveryManager::new();
    manager.prepare(&config, &results(vec![], true), 0);
    assert_eq!(
        manager
            .prepare(
                &config,
                &results(vec![signal("one", "15", SignalKind::Extreme)], true),
                5000
            )
            .len(),
        2
    );
    manager.prepare(&config, &results(vec![], true), 10_000);
    assert!(manager
        .prepare(
            &config,
            &results(vec![signal("two", "15", SignalKind::Extreme)], true),
            15_000
        )
        .is_empty());
    manager.prepare(&config, &results(vec![], true), 310_000);
    assert_eq!(
        manager
            .prepare(
                &config,
                &results(vec![signal("three", "15", SignalKind::Extreme)], true),
                315_000
            )
            .len(),
        2
    );
}

#[test]
fn disabled_or_reconfigured_manager_does_not_send_old_state() {
    let mut config = config();
    let mut manager = DeliveryManager::new();
    manager.prepare(&config, &results(vec![], true), 0);
    let eligible = results(vec![signal("one", "15", SignalKind::Extreme)], true);
    let job = manager.prepare(&config, &eligible, 5000).remove(0);
    config.wecom_alerts[0].name = "changed".into();
    assert!(manager.prepare(&config, &eligible, 10_000).is_empty());
    manager.report_result(&job, &Ok(()), 10_000);
    assert_eq!(manager.health().successful, 0);
    config.enabled = false;
    assert!(manager.prepare(&config, &eligible, 15_000).is_empty());
}

#[test]
fn delivery_health_history_is_bounded() {
    let mut config = config();
    config.wecom_alerts[0].cooldown_secs = 0;
    let mut manager = DeliveryManager::new();
    manager.prepare(&config, &results(vec![], true), 0);
    for index in 0..40 {
        manager.prepare(&config, &results(vec![], true), index * 10_000);
        let job = manager
            .prepare(
                &config,
                &results(
                    vec![signal(&index.to_string(), "15", SignalKind::Extreme)],
                    true,
                ),
                index * 10_000 + 5000,
            )
            .remove(0);
        manager.report_result(&job, &Err("mock failure".into()), index * 10_000 + 5001);
    }
    let health = manager.health();
    assert_eq!(health.failed, 40);
    assert_eq!(health.recent.len(), 32);
    assert!(!serde_json::to_string(&health)
        .unwrap()
        .contains("TEST_SECRET"));
}

async fn mock_server(
    fail_first: usize,
    error_code: i64,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = (calls.clone(), fail_first, error_code);
    let router = Router::new()
        .route(
            "/mock",
            post(
                |State((calls, fail_first, code)): State<(Arc<AtomicUsize>, usize, i64)>,
                 Json(body): Json<serde_json::Value>| async move {
                    assert_eq!(body["msgtype"], "text");
                    assert!(body["text"]["content"]
                        .as_str()
                        .unwrap()
                        .contains("BTCUSDT"));
                    let index = calls.fetch_add(1, Ordering::SeqCst);
                    if index < fail_first {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({"errcode": -1})),
                        )
                    } else {
                        (
                            StatusCode::OK,
                            Json(serde_json::json!({"errcode": code, "errmsg": "TEST_SECRET"})),
                        )
                    }
                },
            ),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/mock?key=TEST_SECRET",
        listener.local_addr().unwrap()
    );
    let handle = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, calls, handle)
}

fn prepare_job(url: &str) -> DeliveryJob {
    let mut config = config();
    config.wecom_alerts[0].webhook_url = url.into();
    let mut manager = DeliveryManager::new();
    manager.prepare(&config, &results(vec![], true), 0);
    manager
        .prepare(
            &config,
            &results(vec![signal("one", "15", SignalKind::Extreme)], true),
            5000,
        )
        .remove(0)
}

#[tokio::test]
async fn sender_retries_http_failure_and_requires_wecom_success() {
    let (url, calls, handle) = mock_server(2, 0).await;
    assert!(send(&prepare_job(&url)).await.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    handle.abort();

    let (url, calls, handle) = mock_server(0, 93000).await;
    let error = send(&prepare_job(&url)).await.unwrap_err();
    assert!(error.contains("93000"));
    assert!(!error.contains("TEST_SECRET"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    handle.abort();
}

#[tokio::test]
async fn sender_retries_at_most_three_times() {
    let (url, calls, handle) = mock_server(100, 0).await;
    assert!(send(&prepare_job(&url)).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    handle.abort();
}
