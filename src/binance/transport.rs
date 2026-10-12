//! One process-wide REST budget per origin, shared by history, recovery and metadata.
use super::rest::RestError;
use reqwest::{header::HeaderMap, RequestBuilder, Response, StatusCode};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{
    sync::Mutex as AsyncMutex,
    time::{sleep_until, Instant},
};

const WEIGHT_BUDGET: u32 = 600;
const WEIGHT_SPACING: Duration = Duration::from_millis(100);
const MAX_ATTEMPTS: u32 = 5;
const UNKNOWN_BAN: Duration = Duration::from_secs(3 * 24 * 3600);

#[derive(Debug)]
struct Gate {
    next_request: Instant,
    strikes: u32,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            next_request: Instant::now(),
            strikes: 0,
        }
    }
}
type SharedGate = Arc<AsyncMutex<Gate>>;

fn gate_for(origin: &str) -> SharedGate {
    static GATES: OnceLock<Mutex<HashMap<String, SharedGate>>> = OnceLock::new();
    GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(origin.to_owned())
        .or_default()
        .clone()
}

pub(crate) fn client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new).clone()
}

pub(crate) fn kline_weight(limit: u32) -> u32 {
    match limit {
        0..=99 => 1,
        100..=499 => 2,
        500..=1000 => 5,
        _ => 10,
    }
}

fn retry_after(headers: &HeaderMap, now_ms: i64) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds.min(30 * 24 * 3600)));
    }
    let deadline = chrono::DateTime::parse_from_rfc2822(value)
        .ok()?
        .timestamp_millis();
    Some(Duration::from_millis(
        deadline.saturating_sub(now_ms).max(0) as u64,
    ))
}

fn ban_delay(message: &str, now_ms: i64) -> Option<Duration> {
    let suffix = message.split_once("banned until ")?.1;
    let deadline: i64 = suffix
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some(Duration::from_millis(
        deadline.saturating_sub(now_ms).max(0) as u64,
    ))
}

fn minute_delay(now_ms: i64) -> Duration {
    Duration::from_millis((60_000 - now_ms.rem_euclid(60_000) + 1000) as u64)
}

fn fallback_delay(status: StatusCode, strikes: u32) -> Duration {
    if status.as_u16() == 418 {
        UNKNOWN_BAN
    } else {
        Duration::from_secs((60_u64 << strikes.min(4)).min(900))
    }
}

/// Only idempotent GET requests are supplied here. The mutex covers the request
/// and response headers, so a concurrent caller cannot race past a new cooldown.
pub(crate) async fn send(request: RequestBuilder, weight: u32) -> Result<Response, RestError> {
    let url = request
        .try_clone()
        .ok_or(RestError::NonReplayableRequest)?
        .build()?
        .url()
        .clone();
    let shared = gate_for(&url.origin().ascii_serialization());
    for attempt in 1..=MAX_ATTEMPTS {
        let mut gate = shared.lock().await;
        sleep_until(gate.next_request).await;
        gate.next_request = Instant::now() + WEIGHT_SPACING * weight;
        let mut response = request
            .try_clone()
            .ok_or(RestError::NonReplayableRequest)?
            .send()
            .await?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let used_weight = response
            .headers()
            .get("x-mbx-used-weight-1m")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<u32>().ok());
        if used_weight.is_some_and(|used| used.saturating_add(weight) >= WEIGHT_BUDGET) {
            let delay = minute_delay(now_ms);
            gate.next_request = gate.next_request.max(Instant::now() + delay);
            tracing::info!(
                used_weight,
                budget = WEIGHT_BUDGET,
                wait_ms = delay.as_millis() as u64,
                "Binance REST weight budget reached; waiting for next minute"
            );
        }
        let status = response.status();
        if status != StatusCode::TOO_MANY_REQUESTS && status.as_u16() != 418 {
            let response = response.error_for_status()?;
            gate.strikes = 0;
            return Ok(response);
        }
        let header_delay = retry_after(response.headers(), now_ms);
        let fallback = fallback_delay(status, gate.strikes);
        let jitter = Duration::from_millis(250 + (now_ms.unsigned_abs() % 250));
        let previous_deadline = gate.next_request;
        // Reserve cooldown immediately: a timeout, cancellation or malformed
        // error body must not let another task send into an active IP limit.
        gate.next_request =
            previous_deadline.max(Instant::now() + header_delay.unwrap_or(fallback) + jitter);
        gate.strikes = gate.strikes.saturating_add(1);
        // Consume a bounded error body for Binance's code/message and ban deadline.
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            let available = 8192_usize.saturating_sub(bytes.len());
            bytes.extend_from_slice(&chunk[..chunk.len().min(available)]);
            if bytes.len() == 8192 {
                break;
            }
        }
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        let code = payload["code"].as_i64();
        let message = payload["msg"]
            .as_str()
            .unwrap_or("Binance rate limit response")
            .to_owned();
        let body_delay = ban_delay(&message, now_ms);
        let delay = match (header_delay, body_delay) {
            (Some(header), Some(body)) => header.max(body),
            (Some(delay), None) | (None, Some(delay)) => delay,
            (None, None) => fallback,
        }
        .min(Duration::from_secs(30 * 24 * 3600));
        // Small jitter also ensures Retry-After: 0 does not cause a hot retry loop.
        gate.next_request = previous_deadline.max(Instant::now() + delay + jitter);
        tracing::warn!(%url, status = status.as_u16(), code, %message, used_weight,
            attempt, max_attempts = MAX_ATTEMPTS, wait_ms = (delay + jitter).as_millis() as u64,
            "Binance REST rate limited; shared cooldown before retrying this request");
        if attempt == MAX_ATTEMPTS || status.as_u16() == 418 {
            return Err(RestError::RateLimited {
                status: status.as_u16(),
                code,
                message,
            });
        }
        // Release between attempts. Other REST users must obey the same deadline.
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::OriginalUri, http::StatusCode as HttpStatus, response::IntoResponse, routing::get,
        Json, Router,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, timeout};

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (url, task)
    }

    #[test]
    fn weights_and_cooldown_deadlines_match_binance_rules() {
        for (limit, weight) in [
            (3, 1),
            (99, 1),
            (100, 2),
            (499, 2),
            (500, 5),
            (1000, 5),
            (1500, 10),
        ] {
            assert_eq!(kline_weight(limit), weight);
        }
        assert_eq!(
            fallback_delay(StatusCode::TOO_MANY_REQUESTS, 0).as_secs(),
            60
        );
        assert_eq!(
            fallback_delay(StatusCode::TOO_MANY_REQUESTS, 1).as_secs(),
            120
        );
        assert_eq!(
            fallback_delay(StatusCode::TOO_MANY_REQUESTS, 99).as_secs(),
            900
        );
        assert_eq!(fallback_delay(StatusCode::IM_A_TEAPOT, 0), UNKNOWN_BAN);
        assert_eq!(minute_delay(59_999).as_millis(), 1001);
        assert_eq!(minute_delay(60_000).as_millis(), 61_000);
        let now = 1_700_000_000_000;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "2".parse().unwrap());
        assert_eq!(retry_after(&headers, now).unwrap().as_secs(), 2);
        headers.insert(
            "retry-after",
            "Tue, 14 Nov 2023 22:13:25 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after(&headers, now).unwrap().as_secs(), 5);
        headers.insert("retry-after", "invalid".parse().unwrap());
        assert!(retry_after(&headers, now).is_none());
        assert_eq!(
            ban_delay("IP banned until 1700000003000. Please use websocket", now)
                .unwrap()
                .as_secs(),
            3
        );
    }

    #[tokio::test]
    async fn cooldown_is_shared_by_different_clients_and_endpoints() {
        let count = Arc::new(AtomicUsize::new(0));
        let requests = count.clone();
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
            let count = requests.clone();
            let events = events.clone();
            async move {
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                events
                    .send((uri.path().to_owned(), Instant::now()))
                    .unwrap();
                if attempt == 0 {
                    (
                        HttpStatus::TOO_MANY_REQUESTS,
                        [("retry-after", "1")],
                        Json(serde_json::json!({"code": -1003, "msg": "Too many requests"})),
                    )
                        .into_response()
                } else {
                    Json(serde_json::json!([])).into_response()
                }
            }
        }));
        let (url, server) = serve(app).await;
        let first_url = format!("{url}/fapi/v1/klines");
        let first =
            tokio::spawn(async move { send(reqwest::Client::new().get(first_url), 1).await });
        let (_, limited_at) = observed.recv().await.unwrap();
        let other_url = format!("{url}/fapi/v1/exchangeInfo");
        let other =
            tokio::spawn(async move { send(reqwest::Client::new().get(other_url), 1).await });
        sleep(Duration::from_millis(200)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "another client must not bypass IP cooldown"
        );
        timeout(Duration::from_secs(4), async {
            first.await.unwrap().unwrap();
            other.await.unwrap().unwrap();
        })
        .await
        .unwrap();
        for _ in 0..2 {
            let (_, at) = observed.recv().await.unwrap();
            assert!(at.duration_since(limited_at) >= Duration::from_secs(1));
        }
        assert_eq!(count.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn missing_retry_after_reserves_sixty_seconds_even_if_task_is_cancelled() {
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let (url, server) = serve(Router::new().fallback(get(move || {
            let events = events.clone();
            async move {
                events.send(()).unwrap();
                (
                    HttpStatus::TOO_MANY_REQUESTS,
                    Json(serde_json::json!({"code": -1003})),
                )
            }
        })))
        .await;
        let request_url = url.clone();
        let request =
            tokio::spawn(async move { send(reqwest::Client::new().get(request_url), 1).await });
        observed.recv().await.unwrap();
        sleep(Duration::from_millis(50)).await;
        request.abort();
        let _ = request.await;
        let gate = gate_for(
            &reqwest::Url::parse(&url)
                .unwrap()
                .origin()
                .ascii_serialization(),
        );
        assert!(
            gate.lock()
                .await
                .next_request
                .duration_since(Instant::now())
                >= Duration::from_secs(59)
        );
        server.abort();
    }

    #[tokio::test]
    async fn weight_header_pauses_next_request_and_normal_400_is_not_retried() {
        let (url, server) = serve(Router::new().fallback(get(|| async {
            (
                [("x-mbx-used-weight-1m", "600")],
                Json(serde_json::json!([])),
            )
        })))
        .await;
        send(reqwest::Client::new().get(&url), 1).await.unwrap();
        let gate = gate_for(
            &reqwest::Url::parse(&url)
                .unwrap()
                .origin()
                .ascii_serialization(),
        );
        assert!(gate.lock().await.next_request > Instant::now() + Duration::from_millis(900));
        server.abort();

        let count = Arc::new(AtomicUsize::new(0));
        let requests = count.clone();
        let (url, server) = serve(Router::new().fallback(get(move || {
            let count = requests.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                HttpStatus::BAD_REQUEST
            }
        })))
        .await;
        let error = send(reqwest::Client::new().get(url), 1).await.unwrap_err();
        assert!(matches!(error, RestError::Http(_)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn exhausted_retry_budget_keeps_cooldown_for_next_recovery_round() {
        let count = Arc::new(AtomicUsize::new(0));
        let requests = count.clone();
        let (url, server) = serve(Router::new().fallback(get(move || {
            let count = requests.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                (
                    HttpStatus::TOO_MANY_REQUESTS,
                    [("retry-after", "0")],
                    Json(serde_json::json!({"code": -1003, "msg": "limited"})),
                )
            }
        })))
        .await;
        let error = timeout(
            Duration::from_secs(5),
            send(reqwest::Client::new().get(&url), 1),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(
            error,
            RestError::RateLimited {
                status: 429,
                code: Some(-1003),
                ..
            }
        ));
        assert_eq!(count.load(Ordering::SeqCst), 5);
        let gate = gate_for(
            &reqwest::Url::parse(&url)
                .unwrap()
                .origin()
                .ascii_serialization(),
        );
        assert!(gate.lock().await.next_request > Instant::now());
        server.abort();
    }

    #[tokio::test]
    async fn ban_returns_without_retry_and_preserves_announced_deadline() {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let (url, server) = serve(Router::new().fallback(get(move || async move {
            (HttpStatus::IM_A_TEAPOT, Json(serde_json::json!({"code": -1003, "msg": format!("IP banned until {}.", now_ms + 3000)})))
        }))).await;
        let error = send(reqwest::Client::new().get(&url), 1).await.unwrap_err();
        assert!(matches!(error, RestError::RateLimited { status: 418, .. }));
        let gate = gate_for(
            &reqwest::Url::parse(&url)
                .unwrap()
                .origin()
                .ascii_serialization(),
        );
        let remaining = gate
            .lock()
            .await
            .next_request
            .duration_since(Instant::now());
        assert!(remaining > Duration::from_secs(2) && remaining < Duration::from_secs(4));
        server.abort();
    }
}
