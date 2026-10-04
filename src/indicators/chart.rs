/*
 * Android channel/amplitude semantics derived from guaili Android indicator files.
 * Amplitude algorithm: Copyright (c) gouge99. SPDX-License-Identifier: MPL-2.0
 */
use crate::domain::candle::Candle;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AndroidChannel {
    pub ema20: f64,
    pub atr14: Option<f64>,
    pub upper: Option<f64>,
    pub lower: Option<f64>,
    pub close_deviation: Option<f64>,
    pub trend: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Amplitude {
    pub normalized_range: f64,
    pub threshold: Option<f64>,
    pub edge_distance: Option<f64>,
    pub weak_top: bool,
    pub weak_bottom: bool,
}

/// Separate Android chart contract: EMA starts at the first close, ATR14 at SMA(TR,14).
/// Never substitute these ATR values for matrix-v1's first-TR initialized ATR.
pub fn compute_android_chart(candles: &[Candle]) -> Vec<(AndroidChannel, Amplitude)> {
    let mut output = Vec::with_capacity(candles.len());
    let mut tr = Vec::with_capacity(candles.len());
    let mut emas = Vec::with_capacity(candles.len());
    let mut ranges = Vec::with_capacity(candles.len());
    let mut atr: Option<f64> = None;
    let mut top: Option<usize> = None;
    let mut bottom: Option<usize> = None;
    for (i, candle) in candles.iter().enumerate() {
        let range = if i == 0 {
            candle.high - candle.low
        } else {
            (candle.high - candle.low)
                .max((candle.high - candles[i - 1].close).abs())
                .max((candle.low - candles[i - 1].close).abs())
        };
        tr.push(range);
        let ema = if i == 0 {
            candle.close
        } else {
            2.0 / 21.0 * candle.close + (1.0 - 2.0 / 21.0) * emas[i - 1]
        };
        emas.push(ema);
        atr = match (i, atr) {
            (13, _) => Some(tr[..14].iter().sum::<f64>() / 14.0),
            (_, Some(previous)) => Some((previous * 13.0 + range) / 14.0),
            _ => None,
        };
        let trend = if i >= 3 && atr.is_some_and(|a| (ema - emas[i - 3]).abs() > a * 0.1) {
            if ema > emas[i - 1] && emas[i - 1] > emas[i - 2] && emas[i - 2] > emas[i - 3] {
                "long"
            } else if ema < emas[i - 1] && emas[i - 1] < emas[i - 2] && emas[i - 2] < emas[i - 3] {
                "short"
            } else {
                "neutral"
            }
        } else {
            "neutral"
        };
        let denominator = candle.high + candle.low;
        let normalized_range = if denominator == 0.0 {
            0.0
        } else {
            range / denominator * 2.0
        };
        ranges.push(normalized_range);
        let threshold = (i >= 19).then(|| {
            let mut window = ranges[i + 1 - 20..=i].to_vec();
            window.sort_by(f64::total_cmp);
            window[17] // ceil(90% * 20) - 1; ties still use strict > below.
        });
        let window = &candles[(i + 1).saturating_sub(20)..=i];
        let highest = window.iter().all(|c| candle.high >= c.high);
        let lowest = window.iter().all(|c| candle.low <= c.low);
        let large = threshold.is_some_and(|t| normalized_range > t);
        if large && candle.close > candle.open && highest {
            top = Some(i);
        }
        if large && candle.close < candle.open && lowest {
            bottom = Some(i);
        }
        let edge_distance = atr.filter(|a| *a != 0.0).map(|a| {
            if candle.low > ema {
                (candle.low - ema) / a
            } else if candle.high < ema {
                (ema - candle.high) / a
            } else {
                0.0
            }
        });
        let passes = edge_distance.is_some_and(|v| (1.0..=10.0).contains(&v));
        output.push((
            AndroidChannel {
                ema20: ema,
                atr14: atr,
                upper: atr.map(|a| ema + a),
                lower: atr.map(|a| ema - a),
                close_deviation: atr.filter(|a| *a != 0.0).map(|a| (candle.close - ema) / a),
                trend,
            },
            Amplitude {
                normalized_range,
                threshold,
                edge_distance,
                weak_top: top.is_some_and(|at| i - at < 4) && highest && passes,
                weak_bottom: bottom.is_some_and(|at| i - at < 4) && lowest && passes,
            },
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candle(i: usize, o: f64, h: f64, l: f64, c: f64) -> Candle {
        Candle {
            open_time: i as i64 * 60_000,
            close_time: i as i64 * 60_000 + 59_999,
            open: o,
            high: h,
            low: l,
            close: c,
            volume: 10.0,
            quote_volume: c * 10.0,
            trade_count: 1,
            is_closed: true,
        }
    }
    #[test]
    fn android_atr_has_exact_fourteen_bar_warmup_and_sma_seed() {
        let bars: Vec<_> = (0..14)
            .map(|i| candle(i, 100.0, 101.0 + i as f64, 99.0, 100.0))
            .collect();
        let out = compute_android_chart(&bars);
        assert!(out[..13].iter().all(|(c, _)| c.atr14.is_none()));
        assert_eq!(out[13].0.atr14, Some(8.5));
        assert_eq!(out[0].0.ema20, 100.0);
    }
    #[test]
    fn strong_top_and_following_reverse_candle_match_android_golden_case() {
        let mut bars: Vec<_> = (0..20)
            .map(|i| candle(i, 100.0, 101.0, 99.0, 100.0))
            .collect();
        bars.push(candle(20, 105.0, 112.0, 104.0, 110.0));
        bars.push(candle(21, 109.0, 113.0, 105.0, 108.0));
        let out = compute_android_chart(&bars);
        assert!(out[20].1.weak_top && out[21].1.weak_top);
        assert!(!out[20].1.weak_bottom);
        assert!(out[19].1.threshold.is_some());
        assert!(out[18].1.threshold.is_none());
        assert_ne!(out[20].0.close_deviation, out[20].1.edge_distance);
    }
    #[test]
    fn ties_and_zero_atr_do_not_create_signals_or_nonfinite_values() {
        let bars: Vec<_> = (0..25)
            .map(|i| candle(i, 100.0, 100.0, 100.0, 100.0))
            .collect();
        let out = compute_android_chart(&bars);
        assert_eq!(out[24].0.atr14, Some(0.0));
        assert!(out[24].0.close_deviation.is_none());
        assert!(out.iter().all(|(_, p)| !p.weak_top && !p.weak_bottom));
    }
}
