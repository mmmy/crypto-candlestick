use crypto_candlestick::signals::config::RuleConfig;
use crypto_candlestick::signals::detector::{detect_structures, primary_signal, same_coverage};
use crypto_candlestick::signals::model::{
    Availability, IntervalEvidence, SignalDirection, SignalKind,
};

const PERIODS: &[&str] = &[
    "10S", "15S", "30S", "45S", "1", "2", "3", "4", "5", "8", "10", "15", "20", "30", "45", "60",
    "90", "120", "180", "240", "360", "480", "720", "D", "2D", "3D", "4D", "W", "10D",
];

fn evidence(values: &[i32]) -> Vec<IntervalEvidence> {
    values
        .iter()
        .zip(PERIODS)
        .map(|(&value, &interval)| IntervalEvidence {
            interval: interval.to_string(),
            availability: Availability::Ready,
            value: Some(value),
            guaili: Some(f64::from(value) / 10.0),
            history_count: 60,
            is_closed: Some(false),
            ..IntervalEvidence::default()
        })
        .collect()
}

#[test]
fn thresholds_are_inclusive_and_runs_are_maximal() {
    let rules = RuleConfig::default();
    assert!(detect_structures(&evidence(&[10; 4]), &rules).is_empty());
    let positives = detect_structures(&evidence(&[10; 7]), &rules);
    assert_eq!(positives.len(), 1);
    assert_eq!(positives[0].level_count, 7);
    assert_eq!(positives[0].runs[0].direction, SignalDirection::Positive);
    assert_eq!(positives[0].runs[0].min_abs_value, 10);
    assert_eq!(positives[0].anchor_interval, "3");
    let negatives = detect_structures(&evidence(&[-10; 5]), &rules);
    assert_eq!(negatives[0].direction, SignalDirection::Negative);
    assert!(detect_structures(&evidence(&[9; 5]), &rules).is_empty());
    assert_eq!(
        detect_structures(&evidence(&[-2; 5]), &rules)[0].kind,
        SignalKind::Compression
    );
    assert!(detect_structures(&evidence(&[3; 5]), &rules).is_empty());
}

#[test]
fn all_unavailable_periods_break_adjacency() {
    for availability in [
        Availability::Filtered,
        Availability::Missing,
        Availability::WarmingUp,
        Availability::Stale,
        Availability::Gap,
        Availability::Recovering,
        Availability::Invalid,
    ] {
        let mut items = evidence(&[11; 9]);
        items[4].availability = availability;
        assert!(
            detect_structures(&items, &RuleConfig::default()).is_empty(),
            "{availability:?} must separate the two four-level runs"
        );
    }
    assert!(!Availability::Filtered.is_unknown());
    assert!(!Availability::Ready.is_unknown());
    assert!(Availability::Missing.is_unknown());
}

#[test]
fn current_dynamic_candles_are_allowed() {
    let items = evidence(&[15; 5]);
    assert!(items.iter().all(|item| item.is_closed == Some(false)));
    let signals = detect_structures(&items, &RuleConfig::default());
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].kind, SignalKind::Extreme);
    assert!(signals[0].first_observed_at.is_none());
    assert!(signals[0].formed_at.is_none());
}

#[test]
fn true_duration_orders_week_before_ten_days() {
    let mut items: Vec<_> = ["10D", "4D", "2D", "W", "3D", "D"]
        .into_iter()
        .map(|interval| IntervalEvidence {
            interval: interval.to_string(),
            availability: Availability::Ready,
            value: Some(10),
            ..IntervalEvidence::default()
        })
        .collect();
    items.reverse();
    let signals = detect_structures(&items, &RuleConfig::default());
    assert_eq!(
        signals[0].runs[0].intervals,
        ["D", "2D", "3D", "4D", "W", "10D"]
    );
    assert_eq!(signals[0].anchor_interval, "10D");
}

#[test]
fn all_zero_compression_precedes_all_two_when_counts_match() {
    let mut signals = detect_structures(
        &evidence(&[0, 0, 0, 0, 0, 3, 2, 2, 2, 2, 2]),
        &RuleConfig::default(),
    );
    assert_eq!(signals.len(), 2);
    for (index, signal) in signals.iter_mut().enumerate() {
        signal.id = format!("compression-{index}");
    }
    assert_eq!(primary_signal(&signals), Some("compression-0".to_string()));
}

#[test]
fn conflict_pairs_and_separate_extreme_and_near_zero_runs_are_all_kept() {
    let signals = detect_structures(
        &evidence(&[
            10, 10, 10, 10, 10, -10, -10, -10, -10, -10, 0, 0, 0, 0, 0, 11, 11, 11, 11, 11,
        ]),
        &RuleConfig::default(),
    );
    assert_eq!(
        signals
            .iter()
            .filter(|s| s.kind == SignalKind::Extreme)
            .count(),
        3
    );
    assert_eq!(
        signals
            .iter()
            .filter(|s| s.kind == SignalKind::Conflict)
            .count(),
        2
    );
    assert_eq!(signals.len(), 6);
    assert_eq!(signals[0].direction, SignalDirection::Positive);
    assert_eq!(signals[1].direction, SignalDirection::Negative);
    assert_eq!(signals[0].total_level_count, 10);
    assert_eq!(signals[0].level_count, 5);
    let mut identified = signals;
    for (index, signal) in identified.iter_mut().enumerate() {
        signal.id = index.to_string();
    }
    // Equal coverage selects the conflict reaching the largest period.
    assert_eq!(primary_signal(&identified), Some("1".to_string()));
}

#[test]
fn a_conflict_requires_both_runs_to_reach_minimum_levels() {
    let signals = detect_structures(
        &evidence(&[10, 10, 10, 10, 10, -10, -10, -10, -10]),
        &RuleConfig::default(),
    );
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].kind, SignalKind::Extreme);
}

#[test]
fn primary_extreme_prefers_more_levels_then_minimum_strength() {
    let mut signals = detect_structures(
        &evidence(&[10, 10, 10, 10, 10, 3, 15, 15, 15, 15, 15]),
        &RuleConfig::default(),
    );
    signals[0].id = "weak".to_string();
    signals[1].id = "strong".to_string();
    assert_eq!(primary_signal(&signals), Some("strong".to_string()));
    signals[0].level_count = 6;
    assert_eq!(primary_signal(&signals), Some("weak".to_string()));
}

#[test]
fn duplicates_do_not_inflate_level_count_and_ambiguous_duplicates_break_runs() {
    let mut items = evidence(&[10; 4]);
    items.push(items[0].clone());
    assert!(detect_structures(&items, &RuleConfig::default()).is_empty());
    let mut items = evidence(&[10; 5]);
    let mut duplicate = items[2].clone();
    duplicate.value = Some(-10);
    items.push(duplicate);
    assert!(detect_structures(&items, &RuleConfig::default()).is_empty());
}

#[test]
fn invalid_numbers_and_missing_values_are_barriers() {
    let mut items = evidence(&[10; 5]);
    items[2].guaili = Some(f64::NAN);
    assert!(detect_structures(&items, &RuleConfig::default()).is_empty());
    items[2].guaili = Some(1.0);
    items[2].atr14 = Some(0.0);
    assert!(detect_structures(&items, &RuleConfig::default()).is_empty());
    items[2].atr14 = Some(1.0);
    items[2].value = None;
    assert!(detect_structures(&items, &RuleConfig::default()).is_empty());
}

#[test]
fn configurable_rules_and_statistics_preserve_integer_units() {
    let rules = RuleConfig {
        extreme_threshold: 20,
        compression_band: 1,
        minimum_levels: 3,
        ..RuleConfig::default()
    };
    let signals = detect_structures(&evidence(&[20, 30, 40]), &rules);
    assert_eq!(signals[0].runs[0].min_abs_value, 20);
    assert_eq!(signals[0].runs[0].max_abs_value, 40);
    assert_eq!(signals[0].runs[0].mean_abs_value, 30.0);
    assert!(detect_structures(&evidence(&[10; 5]), &rules).is_empty());
    assert!(detect_structures(&evidence(&[2; 5]), &rules).is_empty());
    assert_eq!(
        detect_structures(&evidence(&[1; 3]), &rules)[0].kind,
        SignalKind::Compression
    );
}

#[test]
fn coverage_ignores_numeric_changes_and_json_uses_documented_names() {
    let old = detect_structures(&evidence(&[10; 5]), &RuleConfig::default());
    let updated = detect_structures(&evidence(&[20; 5]), &RuleConfig::default());
    assert!(same_coverage(&old[0], &updated[0]));
    let expanded = detect_structures(&evidence(&[20; 6]), &RuleConfig::default());
    assert!(!same_coverage(&old[0], &expanded[0]));
    let json = serde_json::to_value(&old[0]).unwrap();
    assert_eq!(json["kind"], "extreme");
    assert_eq!(json["direction"], "positive");
    assert_eq!(json["anchorInterval"], "1");
    assert_eq!(json["totalLevelCount"], 5);
    assert!(json["formedAt"].is_null());
    assert_eq!(
        serde_json::to_value(Availability::WarmingUp).unwrap(),
        "warming_up"
    );
}

#[test]
fn shared_legacy_parity_fixture_detects_all_dynamic_structures_without_crossing_barriers() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/signal-structure-parity.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let mut evidence: Vec<IntervalEvidence> =
            serde_json::from_value(case["evidence"].clone()).unwrap();
        for item in &mut evidence {
            item.is_closed = Some(false);
        }
        // Input order must not influence configured-period adjacency.
        evidence.reverse();
        let mut detected = detect_structures(&evidence, &RuleConfig::default());
        let signature = |signal: &crypto_candlestick::signals::model::SignalStructure| {
            let kind = serde_json::to_value(signal.kind)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string();
            let runs = signal
                .runs
                .iter()
                .map(|run| {
                    let direction = serde_json::to_value(run.direction)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string();
                    format!("{direction}[{}]", run.intervals.join(","))
                })
                .collect::<Vec<_>>()
                .join(">");
            format!("{kind}:{runs}")
        };
        let mut actual: Vec<_> = detected.iter().map(&signature).collect();
        actual.sort();
        let mut expected: Vec<String> =
            serde_json::from_value(case["expectedAll"].clone()).unwrap();
        expected.sort();
        assert_eq!(actual, expected, "{}", case["name"]);
        for signal in &mut detected {
            signal.id = signature(signal);
            for run in &signal.runs {
                let raw: Vec<_> = run
                    .intervals
                    .iter()
                    .map(|interval| {
                        evidence
                            .iter()
                            .find(|item| &item.interval == interval)
                            .unwrap()
                            .guaili
                            .unwrap()
                            .abs()
                    })
                    .collect();
                assert_eq!(
                    run.max_abs_guaili,
                    raw.iter().copied().max_by(f64::total_cmp)
                );
                assert_eq!(
                    run.mean_abs_guaili,
                    Some(raw.iter().sum::<f64>() / raw.len() as f64)
                );
            }
        }
        // A primary compression uses the same unrounded max/mean tie-breaks.
        let visible: Vec<String> = serde_json::from_value(case["expectedWidget"].clone()).unwrap();
        if visible.len() == 1 {
            assert_eq!(
                primary_signal(&detected),
                visible.first().cloned(),
                "{} primary",
                case["name"]
            );
        }
    }
}
