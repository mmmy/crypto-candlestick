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
    current: Option<Candle>,
}

impl Aggregator {
    pub fn new(interval: Interval) -> Self {
        Self {
            interval,
            current: None,
        }
    }

    pub fn ingest_trade(&mut self, tick: TradeTick) -> Result<Option<Candle>, AggregationError> {
        self.ingest_candle(Candle::from_trade(
            tick.timestamp_ms,
            tick.price,
            tick.quantity,
        ))
    }

    pub fn ingest_candle(&mut self, mut input: Candle) -> Result<Option<Candle>, AggregationError> {
        let bucket_start = self.interval.bucket_start_ms(input.open_time);
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
