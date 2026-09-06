use chrono::{Datelike, TimeZone, Utc};
use std::{collections::BTreeMap, fmt};

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
            let interval = match last {
                's' | 'S' => Self::Seconds(value),
                'd' | 'D' => Self::Days(value),
                'w' | 'W' => Self::Weeks(value),
                _ => Err(IntervalParseError {
                    input: input.to_owned(),
                })?,
            };
            return validate_supported(interval, input);
        }

        let value = parse_positive_u32(trimmed, input)?;
        validate_supported(Self::Minutes(value), input)
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

    pub fn bucket_start_ms_with_anchor(&self, timestamp_ms: i64, anchor_ms: i64) -> i64 {
        let interval_ms = self.as_millis() as i64;
        anchor_ms + (timestamp_ms - anchor_ms).div_euclid(interval_ms) * interval_ms
    }

    pub fn uses_symbol_specific_binance_alignment(&self) -> bool {
        matches!(self, Self::Days(days) if *days > 1) && self.binance_interval().is_some()
    }

    pub fn infer_bucket_anchor_ms(&self, open_times: &[i64]) -> Option<i64> {
        let interval_ms = self.as_millis() as i64;
        let mut phase_counts = BTreeMap::new();
        for open_time in open_times {
            *phase_counts
                .entry(open_time.rem_euclid(interval_ms))
                .or_insert(0usize) += 1;
        }

        phase_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(phase, _)| phase)
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

    pub fn from_binance_interval(input: &str) -> Option<Self> {
        match input {
            "1m" => Some(Self::Minutes(1)),
            "3m" => Some(Self::Minutes(3)),
            "5m" => Some(Self::Minutes(5)),
            "15m" => Some(Self::Minutes(15)),
            "30m" => Some(Self::Minutes(30)),
            "1h" => Some(Self::Minutes(60)),
            "2h" => Some(Self::Minutes(120)),
            "4h" => Some(Self::Minutes(240)),
            "6h" => Some(Self::Minutes(360)),
            "8h" => Some(Self::Minutes(480)),
            "12h" => Some(Self::Minutes(720)),
            "1d" => Some(Self::Days(1)),
            "3d" => Some(Self::Days(3)),
            "1w" => Some(Self::Weeks(1)),
            _ => None,
        }
    }

    pub fn aggregation_base(&self) -> Option<Self> {
        if self.binance_interval().is_some() {
            return None;
        }

        match self {
            Self::Minutes(minutes) => native_minute_bases()
                .iter()
                .rev()
                .copied()
                .find(|base| *base < *minutes && minutes % base == 0)
                .map(Self::Minutes),
            Self::Days(_) => Some(Self::Days(1)),
            _ => None,
        }
    }
}

fn validate_supported(interval: Interval, input: &str) -> Result<Interval, IntervalParseError> {
    if is_supported(interval) {
        Ok(interval)
    } else {
        Err(IntervalParseError {
            input: input.to_owned(),
        })
    }
}

fn is_supported(interval: Interval) -> bool {
    match interval {
        Interval::Seconds(value) => [10, 15, 30, 45].contains(&value),
        Interval::Minutes(value) => supported_minutes().contains(&value),
        Interval::Days(value) => [1, 2, 3, 4, 10].contains(&value),
        Interval::Weeks(value) => value == 1,
    }
}

fn supported_minutes() -> &'static [u32] {
    &[
        1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720,
    ]
}

fn native_minute_bases() -> &'static [u32] {
    &[1, 3, 5, 15, 30, 60, 120, 240, 360, 480, 720]
}

fn parse_positive_u32(raw: &str, input: &str) -> Result<u32, IntervalParseError> {
    match raw.parse::<u32>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(IntervalParseError {
            input: input.to_owned(),
        }),
    }
}
