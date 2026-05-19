use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
    pub is_closed: bool,
}

impl Candle {
    pub fn from_trade(timestamp_ms: i64, price: f64, quantity: f64) -> Self {
        Self {
            open_time: timestamp_ms,
            close_time: timestamp_ms,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: quantity,
            quote_volume: price * quantity,
            trade_count: 1,
            is_closed: false,
        }
    }

    pub fn merge_from(&mut self, other: &Self) {
        self.high = self.high.max(other.high);
        self.low = self.low.min(other.low);
        self.close = other.close;
        self.close_time = self.close_time.max(other.close_time);
        self.volume += other.volume;
        self.quote_volume += other.quote_volume;
        self.trade_count += other.trade_count;
        self.is_closed = other.is_closed;
    }

    pub fn finalize(mut self) -> Self {
        self.is_closed = true;
        self
    }
}
