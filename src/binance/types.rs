use crate::{domain::candle::Candle, engine::aggregator::TradeTick};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    OpenKline {
        symbol: String,
        interval: String,
        candle: Candle,
    },
    ClosedKline {
        symbol: String,
        interval: String,
        candle: Candle,
    },
    AggTrade {
        symbol: String,
        trade: TradeTick,
    },
    Ignored,
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceParseError {
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid number: {0}")]
    Number(#[from] std::num::ParseFloatError),
    #[error("unsupported stream event")]
    Unsupported,
}

#[derive(Debug, Deserialize)]
struct CombinedStreamMessage {
    #[serde(rename = "stream")]
    _stream: String,
    data: StreamEvent,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
enum StreamEvent {
    #[serde(rename = "kline")]
    Kline(KlineEvent),
    #[serde(rename = "aggTrade")]
    AggTrade(AggTradeEvent),
}

#[derive(Debug, Deserialize)]
struct KlineEvent {
    #[serde(rename = "E")]
    _event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: RawKline,
}

#[derive(Debug, Deserialize)]
struct RawKline {
    #[serde(rename = "t")]
    open_time: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(rename = "s")]
    _symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trade_count: u64,
    #[serde(rename = "x")]
    is_closed: bool,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "V")]
    _taker_buy_base_volume: String,
    #[serde(rename = "Q")]
    _taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    _ignore: String,
}

#[derive(Debug, Deserialize)]
struct AggTradeEvent {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time: i64,
}

pub fn parse_combined_stream_message(
    raw: serde_json::Value,
) -> Result<MarketEvent, BinanceParseError> {
    let message: CombinedStreamMessage = serde_json::from_value(raw)?;
    match message.data {
        StreamEvent::Kline(event) => {
            let k = event.kline;
            let event = if k.is_closed {
                MarketEvent::ClosedKline {
                    symbol: event.symbol,
                    interval: k.interval,
                    candle: Candle {
                        open_time: k.open_time,
                        close_time: k.close_time,
                        open: parse_f64(&k.open)?,
                        high: parse_f64(&k.high)?,
                        low: parse_f64(&k.low)?,
                        close: parse_f64(&k.close)?,
                        volume: parse_f64(&k.volume)?,
                        quote_volume: parse_f64(&k.quote_volume)?,
                        trade_count: k.trade_count,
                        is_closed: true,
                    },
                }
            } else {
                MarketEvent::OpenKline {
                    symbol: event.symbol,
                    interval: k.interval,
                    candle: Candle {
                        open_time: k.open_time,
                        close_time: k.close_time,
                        open: parse_f64(&k.open)?,
                        high: parse_f64(&k.high)?,
                        low: parse_f64(&k.low)?,
                        close: parse_f64(&k.close)?,
                        volume: parse_f64(&k.volume)?,
                        quote_volume: parse_f64(&k.quote_volume)?,
                        trade_count: k.trade_count,
                        is_closed: false,
                    },
                }
            };
            Ok(event)
        }
        StreamEvent::AggTrade(event) => Ok(MarketEvent::AggTrade {
            symbol: event.symbol,
            trade: TradeTick {
                timestamp_ms: event.trade_time,
                price: parse_f64(&event.price)?,
                quantity: parse_f64(&event.quantity)?,
            },
        }),
    }
}

fn parse_f64(raw: &str) -> Result<f64, BinanceParseError> {
    Ok(raw.parse::<f64>()?)
}
