use crate::domain::candle::Candle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaType {
    Sma,
    Ema,
    Smma,
    Wma,
    Vwma,
}

#[derive(Debug, Clone, Copy)]
pub struct GuailiConfig {
    pub ma_length: usize,
    pub ma_type: MaType,
    pub atr_len: usize,
    pub atr_percent_len: usize,
    pub max_atr_rank: f64,
    pub slope_mul: f64,
    pub use_slope: bool,
}

impl Default for GuailiConfig {
    fn default() -> Self {
        Self {
            ma_length: 20,
            ma_type: MaType::Ema,
            atr_len: 1,
            atr_percent_len: 20,
            max_atr_rank: 100.0,
            slope_mul: 0.1,
            use_slope: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GuailiPoint {
    pub open_time: i64,
    pub close_time: i64,
    pub ma: f64,
    pub atr14: f64,
    pub atr_rank: Option<f64>,
    pub rank_filter: bool,
    pub guaili: f64,
    pub value: i32,
    pub long_trend: bool,
    pub short_trend: bool,
    pub is_closed: bool,
}

pub fn compute_guaili(candles: &[Candle], config: GuailiConfig) -> Vec<GuailiPoint> {
    if candles.is_empty() {
        return Vec::new();
    }

    let ma_values = ma_values(candles, config.ma_length.max(1), config.ma_type);
    let tr_values = true_ranges(candles);
    let atr_values = rma_values(&tr_values, config.atr_len.max(1));
    let atr14_values = rma_values(&tr_values, 14);
    let atrma_values = candles
        .iter()
        .zip(atr_values.iter())
        .map(|(candle, atr)| atr / (candle.high + candle.low) * 2.0)
        .collect::<Vec<_>>();
    let atr_ranks = percent_ranks(&atrma_values, config.atr_percent_len.max(1));

    let mut points = Vec::with_capacity(candles.len());
    for index in 0..candles.len() {
        let candle = &candles[index];
        let ma = ma_values[index];
        let atr14 = atr14_values[index];
        let prev_atr14 = index
            .checked_sub(1)
            .map(|prev| atr14_values[prev])
            .unwrap_or(atr14);
        let mut guaili = 0.0;

        if candle.low > ma && candle.high > ma && prev_atr14 != 0.0 {
            guaili = (candle.low - ma) / prev_atr14;
        } else if candle.high < ma && candle.low < ma && prev_atr14 != 0.0 {
            guaili = -((ma - candle.high) / prev_atr14);
        }

        let is_up = index > 0 && ma > ma_values[index - 1];
        let is_down = index > 0 && ma < ma_values[index - 1];
        let previous_up = index >= 2 && ma_values[index - 1] > ma_values[index - 2];
        let previous_down = index >= 2 && ma_values[index - 1] < ma_values[index - 2];
        let previous2_up = index >= 3 && ma_values[index - 2] > ma_values[index - 3];
        let previous2_down = index >= 3 && ma_values[index - 2] < ma_values[index - 3];
        let slope = index
            .checked_sub(3)
            .map(|prev| (ma - ma_values[prev]).abs())
            .unwrap_or(0.0);
        let trend_strength_ok = slope > atr14 * config.slope_mul;
        let slope_filter = !config.use_slope || trend_strength_ok;

        points.push(GuailiPoint {
            open_time: candle.open_time,
            close_time: candle.close_time,
            ma,
            atr14,
            atr_rank: atr_ranks[index],
            rank_filter: atr_ranks[index]
                .map(|rank| rank <= config.max_atr_rank)
                .unwrap_or(false),
            guaili,
            value: (guaili * 10.0) as i32,
            long_trend: is_up && previous_up && previous2_up && slope_filter,
            short_trend: is_down && previous_down && previous2_down && slope_filter,
            is_closed: candle.is_closed,
        });
    }

    points
}

fn ma_values(candles: &[Candle], length: usize, ma_type: MaType) -> Vec<f64> {
    match ma_type {
        MaType::Sma => sma_values(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            length,
        ),
        MaType::Ema => ema_values(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            length,
        ),
        MaType::Smma => rma_values(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            length,
        ),
        MaType::Wma => wma_values(
            &candles
                .iter()
                .map(|candle| candle.close)
                .collect::<Vec<_>>(),
            length,
        ),
        MaType::Vwma => vwma_values(candles, length),
    }
}

fn sma_values(values: &[f64], length: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let start = (index + 1).saturating_sub(length);
            mean(&values[start..=index])
        })
        .collect()
}

fn ema_values(values: &[f64], length: usize) -> Vec<f64> {
    let alpha = 2.0 / (length as f64 + 1.0);
    let mut result = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        if index == 0 {
            result.push(*value);
        } else {
            result.push(alpha * value + (1.0 - alpha) * result[index - 1]);
        }
    }
    result
}

fn rma_values(values: &[f64], length: usize) -> Vec<f64> {
    let mut result = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        if index == 0 {
            result.push(*value);
        } else {
            result.push((result[index - 1] * (length as f64 - 1.0) + value) / length as f64);
        }
    }
    result
}

fn wma_values(values: &[f64], length: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let start = (index + 1).saturating_sub(length);
            let window = &values[start..=index];
            let mut weighted_sum = 0.0;
            let mut weight_sum = 0.0;
            for (offset, value) in window.iter().enumerate() {
                let weight = (offset + 1) as f64;
                weighted_sum += value * weight;
                weight_sum += weight;
            }
            weighted_sum / weight_sum
        })
        .collect()
}

fn vwma_values(candles: &[Candle], length: usize) -> Vec<f64> {
    candles
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let start = (index + 1).saturating_sub(length);
            let window = &candles[start..=index];
            let volume_sum = window.iter().map(|candle| candle.volume).sum::<f64>();
            if volume_sum == 0.0 {
                return mean(&window.iter().map(|candle| candle.close).collect::<Vec<_>>());
            }
            window
                .iter()
                .map(|candle| candle.close * candle.volume)
                .sum::<f64>()
                / volume_sum
        })
        .collect()
}

fn true_ranges(candles: &[Candle]) -> Vec<f64> {
    candles
        .iter()
        .enumerate()
        .map(|(index, candle)| {
            if index == 0 {
                candle.high - candle.low
            } else {
                let previous_close = candles[index - 1].close;
                (candle.high - candle.low)
                    .max((candle.high - previous_close).abs())
                    .max((candle.low - previous_close).abs())
            }
        })
        .collect()
}

fn percent_ranks(values: &[f64], length: usize) -> Vec<Option<f64>> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if index + 1 < length {
                return None;
            }
            let start = index + 1 - length;
            let count_less_or_equal = values[start..=index]
                .iter()
                .filter(|item| **item <= *value)
                .count();
            Some((count_less_or_equal as f64 - 1.0) / (length as f64 - 1.0) * 100.0)
        })
        .collect()
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}
