use crate::domain::{candle::Candle, interval::Interval};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeTick {
    pub timestamp_ms: i64,
    pub price: f64,
    pub quantity: f64,
}

impl TradeTick {
    pub fn new(timestamp_ms: i64, price: f64, quantity: f64) -> Self {
        Self {
            timestamp_ms,
            price,
            quantity,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AggregationError {
    #[error("out-of-order input for interval")]
    OutOfOrder,
}

#[derive(Debug, Clone)]
pub struct Aggregator {
    interval: Interval,
    bucket_anchor_ms: Option<i64>,
    current: Option<Candle>,
}

impl Aggregator {
    pub fn new(interval: Interval) -> Self {
        Self {
            interval,
            bucket_anchor_ms: None,
            current: None,
        }
    }

    pub fn set_bucket_anchor_ms(&mut self, anchor_ms: i64) {
        self.bucket_anchor_ms = Some(anchor_ms);
    }

    pub fn bucket_start_ms(&self, timestamp_ms: i64) -> i64 {
        self.bucket_anchor_ms
            .map(|anchor_ms| {
                self.interval
                    .bucket_start_ms_with_anchor(timestamp_ms, anchor_ms)
            })
            .unwrap_or_else(|| self.interval.bucket_start_ms(timestamp_ms))
    }

    pub fn reset(&mut self) {
        self.current = None;
    }

    pub fn ingest_trade(&mut self, tick: TradeTick) -> Result<Option<Candle>, AggregationError> {
        self.ingest_candle(Candle::from_trade(
            tick.timestamp_ms,
            tick.price,
            tick.quantity,
        ))
    }

    pub fn ingest_candle(&mut self, mut input: Candle) -> Result<Option<Candle>, AggregationError> {
        let bucket_start = self.bucket_start_ms(input.open_time);
        let bucket_end = bucket_start + self.interval.as_millis() as i64 - 1;
        input.open_time = bucket_start;
        input.close_time = bucket_end.max(input.close_time);

        match self.current.take() {
            None => {
                self.current = Some(input);
                Ok(None)
            }
            Some(mut current) => {
                if bucket_start < current.open_time {
                    self.current = Some(current);
                    return Err(AggregationError::OutOfOrder);
                }

                if bucket_start == current.open_time {
                    current.merge_from(&input);
                    self.current = Some(current);
                    Ok(None)
                } else {
                    let finalized = current.finalize();
                    self.current = Some(input);
                    Ok(Some(finalized))
                }
            }
        }
    }

    pub fn flush(&mut self) -> Option<Candle> {
        self.current.take().map(Candle::finalize)
    }

    pub fn current(&self) -> Option<Candle> {
        self.current.clone()
    }
}
