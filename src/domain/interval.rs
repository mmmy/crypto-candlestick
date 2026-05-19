use chrono::{Datelike, TimeZone, Utc};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Interval {
    Seconds(u32),
    Minutes(u32),
    Days(u32),
    Weeks(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalParseError {
    pub input: String,
}

impl fmt::Display for IntervalParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid interval: {}", self.input)
    }
}

impl std::error::Error for IntervalParseError {}

impl Interval {
    pub fn parse(input: &str) -> Result<Self, IntervalParseError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(IntervalParseError {
                input: input.to_owned(),
            });
        }

        if trimmed.eq_ignore_ascii_case("D") {
            return Ok(Self::Days(1));
        }

        if trimmed.eq_ignore_ascii_case("W") {
            return Ok(Self::Weeks(1));
        }

        let last = trimmed.chars().last().unwrap();
        if last.is_ascii_alphabetic() {
            let number = &trimmed[..trimmed.len() - last.len_utf8()];
            let value = parse_positive_u32(number, input)?;
            return match last {
                's' | 'S' => Ok(Self::Seconds(value)),
                'm' | 'M' => Ok(Self::Minutes(value)),
                'd' | 'D' => Ok(Self::Days(value)),
                'h' | 'H' => Ok(Self::Minutes(value.saturating_mul(60))),
                'w' | 'W' => Ok(Self::Weeks(value)),
                _ => Err(IntervalParseError {
                    input: input.to_owned(),
                }),
            };
        }

        let value = parse_positive_u32(trimmed, input)?;
        Ok(Self::Minutes(value))
    }

    pub fn as_millis(&self) -> u64 {
        match self {
            Self::Seconds(v) => *v as u64 * 1_000,
            Self::Minutes(v) => *v as u64 * 60_000,
            Self::Days(v) => *v as u64 * 86_400_000,
            Self::Weeks(v) => *v as u64 * 604_800_000,
        }
    }

    pub fn bucket_start_ms(&self, timestamp_ms: i64) -> i64 {
        let interval_ms = self.as_millis() as i64;
        match self {
            Self::Weeks(_) => {
                let dt = Utc
                    .timestamp_millis_opt(timestamp_ms)
                    .single()
                    .unwrap_or_else(|| {
                        Utc.timestamp_millis_opt(0)
                            .single()
                            .expect("unix epoch exists")
                    });
                let weekday = dt.weekday().num_days_from_monday() as i64;
                let start_of_day = dt.date_naive().and_hms_opt(0, 0, 0).unwrap();
                let midnight_ms = Utc.from_utc_datetime(&start_of_day).timestamp_millis();
                midnight_ms - weekday * 86_400_000
            }
            _ => timestamp_ms.div_euclid(interval_ms) * interval_ms,
        }
    }

    pub fn canonical(&self) -> String {
        match self {
            Self::Seconds(v) => format!("{v}S"),
            Self::Minutes(v) => v.to_string(),
            Self::Days(v) => {
                if *v == 1 {
                    "D".to_string()
                } else {
                    format!("{v}D")
                }
            }
            Self::Weeks(v) => {
                if *v == 1 {
                    "W".to_string()
                } else {
                    format!("{v}W")
                }
            }
        }
    }

    pub fn binance_interval(&self) -> Option<&'static str> {
        match self {
            Self::Minutes(1) => Some("1m"),
            Self::Minutes(3) => Some("3m"),
            Self::Minutes(5) => Some("5m"),
            Self::Minutes(15) => Some("15m"),
            Self::Minutes(30) => Some("30m"),
            Self::Minutes(60) => Some("1h"),
            Self::Minutes(120) => Some("2h"),
            Self::Minutes(240) => Some("4h"),
            Self::Minutes(360) => Some("6h"),
            Self::Minutes(480) => Some("8h"),
            Self::Minutes(720) => Some("12h"),
            Self::Days(1) => Some("1d"),
            Self::Days(3) => Some("3d"),
            Self::Weeks(1) => Some("1w"),
            _ => None,
        }
    }

    pub fn aggregation_base(&self) -> Option<Self> {
        if self.binance_interval().is_some() {
            return None;
        }

        match self {
            Self::Minutes(_) => Some(Self::Minutes(1)),
            Self::Days(_) => Some(Self::Days(1)),
            _ => None,
        }
    }
}

fn parse_positive_u32(raw: &str, input: &str) -> Result<u32, IntervalParseError> {
    match raw.parse::<u32>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(IntervalParseError {
            input: input.to_owned(),
        }),
    }
}
