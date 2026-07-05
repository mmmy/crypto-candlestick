use crypto_candlestick::domain::candle::Candle;
use crypto_candlestick::indicators::guaili::{compute_guaili, GuailiConfig, MaType};

fn candle(index: i64, high: f64, low: f64, close: f64) -> Candle {
    Candle {
        open_time: index * 60_000,
        close_time: index * 60_000 + 59_999,
        open: close,
        high,
        low,
        close,
        volume: 1.0,
        quote_volume: close,
        trade_count: 1,
        is_closed: true,
    }
}

#[test]
fn computes_positive_guaili_from_distance_above_ema() {
    let candles = (0..20)
        .map(|index| {
            let close = 100.0 + index as f64 * 10.0;
            candle(index, close + 1.0, close - 1.0, close)
        })
        .collect::<Vec<_>>();

    let values = compute_guaili(
        &candles,
        GuailiConfig {
            ma_length: 3,
            ma_type: MaType::Ema,
            atr_len: 1,
            atr_percent_len: 20,
            max_atr_rank: 100.0,
            slope_mul: 0.1,
            use_slope: true,
        },
    );

    let latest = values.last().expect("expected latest guaili value");
    assert!((latest.ma - 280.000_019_073_486_3).abs() < 0.000_001);
    assert!((latest.atr14 - 8.798_424_795_031_17).abs() < 0.000_001);
    assert!((latest.guaili - 1.042_983_536_760_88).abs() < 0.000_001);
    assert_eq!(latest.value, 10);
    assert!(latest.rank_filter);
    assert!(latest.long_trend);
    assert!(!latest.short_trend);
}

#[test]
fn computes_negative_guaili_from_distance_below_ema() {
    let candles = (0..20)
        .map(|index| {
            let close = 300.0 - index as f64 * 10.0;
            candle(index, close + 1.0, close - 1.0, close)
        })
        .collect::<Vec<_>>();

    let values = compute_guaili(
        &candles,
        GuailiConfig {
            ma_length: 3,
            ma_type: MaType::Ema,
            atr_len: 1,
            atr_percent_len: 20,
            max_atr_rank: 100.0,
            slope_mul: 0.1,
            use_slope: true,
        },
    );

    let latest = values.last().expect("expected latest guaili value");
    assert!((latest.ma - 119.999_980_926_513_67).abs() < 0.000_001);
    assert!((latest.guaili + 1.042_983_536_760_88).abs() < 0.000_001);
    assert_eq!(latest.value, -10);
    assert!(!latest.long_trend);
    assert!(latest.short_trend);
}
