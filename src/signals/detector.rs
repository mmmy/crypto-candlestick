use std::cmp::Ordering;

use crate::domain::interval::Interval;
use crate::signals::config::RuleConfig;
use crate::signals::model::{
    Availability, IntervalEvidence, SignalDirection, SignalKind, SignalRun, SignalStructure,
};

/// Detect maximal runs in the *complete configured period set*. Callers must
/// retain missing, warming, stale and filtered periods in `evidence`, since they
/// break adjacency. Both current dynamic and already closed candles are valid.
/// All qualifying runs and all opposite-sign run pairs are returned; there is
/// no notification-specific filtering here.
pub fn detect_structures(
    evidence: &[IntervalEvidence],
    rules: &RuleConfig,
) -> Vec<SignalStructure> {
    if rules.minimum_levels == 0 || rules.extreme_threshold <= 0 || rules.compression_band < 0 {
        return Vec::new();
    }

    let mut ordered = Vec::with_capacity(evidence.len());
    for item in evidence {
        // An unrecognized period makes the configured adjacency ambiguous.
        let Ok(interval) = Interval::parse(&item.interval) else {
            return Vec::new();
        };
        ordered.push(OrderedEvidence {
            interval: interval.canonical(),
            duration: interval.as_millis(),
            value: eligible_value(item),
            guaili: item
                .guaili
                .or_else(|| item.value.map(|value| f64::from(value) / 10.0)),
        });
    }
    ordered.sort_by_key(|item| item.duration);

    // A repeated alias (D / 1D, for example) must not count as another level.
    // Contradictory duplicate observations are a barrier instead of choosing
    // one arbitrarily and potentially creating a false structure.
    let mut unique: Vec<OrderedEvidence> = Vec::with_capacity(ordered.len());
    for item in ordered {
        if let Some(previous) = unique.last_mut() {
            if previous.duration == item.duration {
                if previous.value != item.value || previous.guaili != item.guaili {
                    previous.value = None;
                }
                continue;
            }
        }
        unique.push(item);
    }

    let extremes = build_runs(&unique, rules.minimum_levels, |value| {
        if value >= rules.extreme_threshold {
            Some(SignalDirection::Positive)
        } else if value <= -rules.extreme_threshold {
            Some(SignalDirection::Negative)
        } else {
            None
        }
    });
    let compressions = build_runs(&unique, rules.minimum_levels, |value| {
        (value.saturating_abs() <= rules.compression_band).then_some(SignalDirection::Neutral)
    });

    let mut result = Vec::new();
    for (first_index, first) in extremes.iter().enumerate() {
        for second in &extremes[first_index + 1..] {
            if first.direction != second.direction {
                result.push(structure(
                    SignalKind::Conflict,
                    vec![first.clone(), second.clone()],
                ));
            }
        }
    }
    result.extend(
        extremes
            .into_iter()
            .map(|run| structure(SignalKind::Extreme, vec![run])),
    );
    result.extend(
        compressions
            .into_iter()
            .map(|run| structure(SignalKind::Compression, vec![run])),
    );
    result
}

/// Select a structure for a compact widget while keeping all structures in the
/// API response. The returned ID is assigned by the runtime before this call.
pub fn primary_signal(signals: &[SignalStructure]) -> Option<String> {
    signals
        .iter()
        .max_by(|left, right| compare_structures(left, right))
        .map(|signal| signal.id.clone())
}

/// Statistics may change every sample without changing the structure's period
/// coverage. This comparison is useful for deciding whether its shape changed.
pub fn same_coverage(left: &SignalStructure, right: &SignalStructure) -> bool {
    left.kind == right.kind
        && left.direction == right.direction
        && left.runs.len() == right.runs.len()
        && left
            .runs
            .iter()
            .zip(&right.runs)
            .all(|(a, b)| a.direction == b.direction && a.intervals == b.intervals)
}

struct OrderedEvidence {
    interval: String,
    duration: u64,
    value: Option<i32>,
    guaili: Option<f64>,
}

fn eligible_value(item: &IntervalEvidence) -> Option<i32> {
    if item.availability != Availability::Ready
        || [item.guaili, item.ma, item.atr_rank]
            .into_iter()
            .flatten()
            .any(|value| !value.is_finite())
        || item
            .atr14
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return None;
    }
    item.value
}

fn build_runs(
    evidence: &[OrderedEvidence],
    minimum_levels: usize,
    direction_at: impl Fn(i32) -> Option<SignalDirection>,
) -> Vec<SignalRun> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut current_direction = None;
    for (index, item) in evidence.iter().enumerate() {
        let direction = item.value.and_then(&direction_at);
        if direction != current_direction || direction.is_none() {
            finish_run(
                &mut result,
                &evidence[start..index],
                current_direction,
                minimum_levels,
            );
            start = index;
            current_direction = direction;
        }
    }
    finish_run(
        &mut result,
        &evidence[start..],
        current_direction,
        minimum_levels,
    );
    result
}

fn finish_run(
    result: &mut Vec<SignalRun>,
    items: &[OrderedEvidence],
    direction: Option<SignalDirection>,
    minimum_levels: usize,
) {
    let Some(direction) = direction else {
        return;
    };
    if items.len() < minimum_levels {
        return;
    }
    let values: Vec<i32> = items
        .iter()
        .map(|item| {
            item.value
                .expect("a qualifying run has values")
                .saturating_abs()
        })
        .collect();
    let guaili: Vec<f64> = items
        .iter()
        .map(|item| {
            item.guaili
                .expect("a qualifying run has finite guaili")
                .abs()
        })
        .collect();
    result.push(SignalRun {
        direction,
        intervals: items.iter().map(|item| item.interval.clone()).collect(),
        min_abs_value: *values.iter().min().expect("a qualifying run is nonempty"),
        max_abs_value: *values.iter().max().expect("a qualifying run is nonempty"),
        mean_abs_value: values.iter().map(|value| f64::from(*value)).sum::<f64>()
            / values.len() as f64,
        max_abs_guaili: guaili.iter().copied().max_by(f64::total_cmp),
        mean_abs_guaili: Some(guaili.iter().sum::<f64>() / guaili.len() as f64),
    });
}

fn structure(kind: SignalKind, runs: Vec<SignalRun>) -> SignalStructure {
    let level_count = runs
        .iter()
        .map(|run| run.intervals.len())
        .max()
        .unwrap_or(0);
    let total_level_count = runs.iter().map(|run| run.intervals.len()).sum();
    let anchor_interval = runs
        .iter()
        .filter_map(|run| run.intervals.last())
        .max_by_key(|interval| duration(interval))
        .cloned()
        .unwrap_or_default();
    SignalStructure {
        id: String::new(),
        kind,
        direction: runs[0].direction,
        runs,
        level_count,
        total_level_count,
        anchor_interval,
        first_observed_at: None,
        formed_at: None,
        last_changed_at: None,
    }
}

fn compare_structures(left: &SignalStructure, right: &SignalStructure) -> Ordering {
    let priority = |kind| match kind {
        SignalKind::Conflict => 3,
        SignalKind::Extreme => 2,
        SignalKind::Compression => 1,
    };
    priority(left.kind)
        .cmp(&priority(right.kind))
        .then_with(|| match left.kind {
            SignalKind::Conflict => left.total_level_count.cmp(&right.total_level_count),
            SignalKind::Extreme => left
                .level_count
                .cmp(&right.level_count)
                .then_with(|| left.runs[0].min_abs_value.cmp(&right.runs[0].min_abs_value)),
            SignalKind::Compression => left
                .level_count
                .cmp(&right.level_count)
                .then_with(|| {
                    max_abs_guaili(&right.runs[0]).total_cmp(&max_abs_guaili(&left.runs[0]))
                })
                .then_with(|| {
                    mean_abs_guaili(&right.runs[0]).total_cmp(&mean_abs_guaili(&left.runs[0]))
                }),
        })
        .then_with(|| duration(&left.anchor_interval).cmp(&duration(&right.anchor_interval)))
        // Stable selection for an otherwise exact tie.
        .then_with(|| right.id.cmp(&left.id))
}

fn max_abs_guaili(run: &SignalRun) -> f64 {
    run.max_abs_guaili
        .unwrap_or(f64::from(run.max_abs_value) / 10.0)
}

fn mean_abs_guaili(run: &SignalRun) -> f64 {
    run.mean_abs_guaili.unwrap_or(run.mean_abs_value / 10.0)
}

fn duration(interval: &str) -> u64 {
    Interval::parse(interval)
        .map(|value| value.as_millis())
        .unwrap_or(0)
}
