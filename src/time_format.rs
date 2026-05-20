use chrono::{Local, SecondsFormat, TimeZone};

pub fn format_timestamp_ms(timestamp_ms: i64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| timestamp_ms.to_string())
}
