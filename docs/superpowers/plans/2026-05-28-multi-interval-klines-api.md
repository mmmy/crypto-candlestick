# Multi-Interval Klines API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-interval `/api/klines` contract with a required `intervals` query parameter and a unified `series` response.

**Architecture:** Keep the existing Axum route and refactor the handler's current single-interval query body into a helper that returns one `KlineSeries`. The top-level handler parses and validates the comma-separated interval list, preserves request order, calls the helper once per interval, and returns a `KlineEnvelope` containing all series.

**Tech Stack:** Rust, Axum, Serde, Tokio tests, Reqwest, SQLite store, in-memory latest/series stores.

---

## File Structure

- Modify `src/http/handlers.rs`: change `KlineQuery`, response structs, interval parsing, and query helper extraction.
- Modify `tests/http_tests.rs`: update existing `/api/klines` assertions from `interval`/`data` to `intervals`/`series`, and add validation/mixed-store coverage.
- Modify `README.md`: document `intervals`, the new `series` response, and command examples.

---

### Task 1: Add Failing HTTP Contract Tests

**Files:**
- Modify: `tests/http_tests.rs`

- [ ] **Step 1: Update persisted-row test to require `intervals` and `series`**

Replace the request and response assertions inside `klines_endpoint_returns_persisted_rows` with:

```rust
let response = reqwest::get(format!(
    "http://{addr}/api/klines?symbol=BTCUSDT&intervals=1&limit=10"
))
.await
.unwrap();

assert_eq!(response.status(), StatusCode::OK);
let body: serde_json::Value = response.json().await.unwrap();
assert_eq!(body["symbol"], "BTCUSDT");
assert_eq!(body["intervals"], serde_json::json!(["1"]));
assert_eq!(body["limit"], 10);
assert_eq!(body["closedOnly"], false);
assert_eq!(body["timezone"], "Asia/Shanghai");
assert!(body["serverTime"].as_i64().unwrap() > 0);
assert_eq!(body["series"].as_array().unwrap().len(), 1);
assert_eq!(body["series"][0]["interval"], "1");
assert_eq!(body["series"][0]["startTime"], "1970-01-01T08:00:01.000+08:00");
assert_eq!(body["series"][0]["endTime"], "1970-01-01T08:00:01.000+08:00");
assert_eq!(body["series"][0]["count"], 1);
assert_eq!(body["series"][0]["data"].as_array().unwrap().len(), 1);
assert_eq!(
    body["series"][0]["data"][0]["candle"]["openTime"],
    "1970-01-01T08:00:01.000+08:00"
);
assert_eq!(
    body["series"][0]["data"][0]["candle"]["closeTime"],
    "1970-01-01T08:00:01.999+08:00"
);
```

- [ ] **Step 2: Update single-interval behavior tests to use `series` paths**

In the remaining `/api/klines` tests, change URLs from `interval=` to `intervals=` and update JSON paths:

```rust
// defaults limit test
"http://{addr}/api/klines?symbol=BTCUSDT&intervals=1"

// latest open candle test
"http://{addr}/api/klines?symbol=BTCUSDT&intervals=1&limit=10"
assert_eq!(body["series"][0]["data"].as_array().unwrap().len(), 2);
assert_eq!(
    body["series"][0]["data"][1]["candle"]["openTime"],
    "1970-01-01T08:01:00.000+08:00"
);
assert_eq!(body["series"][0]["data"][1]["candle"]["isClosed"], false);

// closedOnly test
"http://{addr}/api/klines?symbol=BTCUSDT&intervals=1&limit=10&closedOnly=true"
assert_eq!(body["closedOnly"], true);
assert_eq!(body["series"][0]["count"], 1);
assert_eq!(body["series"][0]["data"].as_array().unwrap().len(), 1);
assert_eq!(body["series"][0]["data"][0]["candle"]["isClosed"], true);

// contiguous rows test
"http://{addr}/api/klines?symbol=BTCUSDT&intervals=1&limit=10"
assert_eq!(body["series"][0]["data"].as_array().unwrap().len(), 2);

// second interval memory test
"http://{addr}/api/klines?symbol=BTCUSDT&intervals=15S&limit=10"
assert_eq!(body["series"][0]["data"].as_array().unwrap().len(), 2);
```

- [ ] **Step 3: Add a multi-interval mixed-store test**

Add this test near the existing kline endpoint tests:

```rust
#[tokio::test]
async fn klines_endpoint_returns_multiple_intervals_in_request_order() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let latest = LatestCache::default();
    let memory_series = MemorySeriesStore::default();

    store
        .upsert_candle(
            "BTCUSDT",
            "1",
            &Candle {
                open_time: 60_000,
                close_time: 119_999,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 12.5,
                quote_volume: 1_250.0,
                trade_count: 3,
                is_closed: true,
            },
        )
        .await
        .unwrap();

    memory_series
        .push_closed(
            "BTCUSDT",
            "15S",
            Candle {
                open_time: 15_000,
                close_time: 29_999,
                open: 99.0,
                high: 100.0,
                low: 98.0,
                close: 99.5,
                volume: 5.0,
                quote_volume: 497.5,
                trade_count: 2,
                is_closed: true,
            },
        )
        .await;

    let app = router(AppState {
        store,
        latest,
        memory_series,
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&intervals=15S,1&limit=10"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["intervals"], serde_json::json!(["15S", "1"]));
    assert_eq!(body["series"].as_array().unwrap().len(), 2);
    assert_eq!(body["series"][0]["interval"], "15S");
    assert_eq!(body["series"][0]["data"][0]["interval"], "15S");
    assert_eq!(body["series"][1]["interval"], "1");
    assert_eq!(body["series"][1]["data"][0]["interval"], "1");

    server.abort();
}
```

- [ ] **Step 4: Add rejection tests for old and invalid parameters**

Add these tests:

```rust
#[tokio::test]
async fn klines_endpoint_rejects_missing_intervals() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&interval=1"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("missing intervals"));

    server.abort();
}

#[tokio::test]
async fn klines_endpoint_rejects_empty_interval_item() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let app = router(AppState {
        store,
        latest: LatestCache::default(),
        memory_series: MemorySeriesStore::default(),
        health_targets: Vec::new(),
        runtime_health: RuntimeHealth::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let response = reqwest::get(format!(
        "http://{addr}/api/klines?symbol=BTCUSDT&intervals=1,,5"
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("empty interval"));

    server.abort();
}
```

- [ ] **Step 5: Run focused tests and verify they fail**

Run:

```powershell
cargo test --test http_tests klines_endpoint_ -- --nocapture
```

Expected: compile failures or assertion failures because `KlineQuery` still requires `interval` and `KlineEnvelope` has no `series`.

---

### Task 2: Implement Multi-Interval Handler

**Files:**
- Modify: `src/http/handlers.rs`

- [ ] **Step 1: Replace query and response structs**

Replace the existing `KlineQuery` and `KlineEnvelope` definitions with:

```rust
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineQuery {
    pub symbol: String,
    pub intervals: Option<String>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: Option<u32>,
    pub closed_only: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineSeries {
    pub interval: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub count: usize,
    pub data: Vec<KlineResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KlineEnvelope {
    pub symbol: String,
    pub intervals: Vec<String>,
    pub limit: u32,
    pub closed_only: bool,
    pub timezone: &'static str,
    pub server_time: i64,
    pub series: Vec<KlineSeries>,
}
```

- [ ] **Step 2: Add interval-list parser**

Add this helper before `pub async fn klines`:

```rust
fn parse_intervals(input: Option<&str>) -> Result<Vec<Interval>, (axum::http::StatusCode, String)> {
    let input = input
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "missing intervals".to_string(),
            )
        })?;

    input
        .split(',')
        .map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return Err((
                    axum::http::StatusCode::BAD_REQUEST,
                    "empty interval in intervals".to_string(),
                ));
            }

            Interval::parse(item)
                .map_err(|err| (axum::http::StatusCode::BAD_REQUEST, err.to_string()))
        })
        .collect()
}
```

- [ ] **Step 3: Extract one-interval query helper**

Move the body of the existing single-interval handler into:

```rust
async fn query_kline_series(
    state: &AppState,
    symbol: &str,
    interval: Interval,
    start_time: Option<i64>,
    end_time: Option<i64>,
    limit: u32,
    closed_only: bool,
) -> Result<KlineSeries, (axum::http::StatusCode, String)> {
    let canonical_interval = interval.canonical();
    let query_limit = if closed_only {
        limit.saturating_add(1)
    } else {
        limit
    };
    let mut rows = if interval.as_millis() < 60_000 {
        state
            .memory_series
            .query(symbol, &canonical_interval, start_time, end_time, query_limit)
            .await
    } else {
        state
            .store
            .query_klines(symbol, &canonical_interval, start_time, end_time, query_limit)
            .await
            .map_err(|err| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )
            })?
    };

    if !closed_only {
        if let Some(latest) = state
            .latest
            .get(symbol, &canonical_interval)
            .await
            .filter(|candle| {
                start_time
                    .map(|start| candle.open_time >= start)
                    .unwrap_or(true)
                    && end_time
                        .map(|end| candle.open_time <= end)
                        .unwrap_or(true)
            })
        {
            let latest_row = StoredKline {
                symbol: symbol.to_uppercase(),
                interval: canonical_interval.clone(),
                candle: latest,
            };
            if let Some(last) = rows.last_mut() {
                if last.candle.open_time == latest_row.candle.open_time {
                    *last = latest_row;
                } else if last.candle.open_time < latest_row.candle.open_time {
                    rows.push(latest_row);
                }
            } else {
                rows.push(latest_row);
            }
        }
    }

    if closed_only {
        rows.retain(|row| row.candle.is_closed);
    }

    if rows.len() > limit as usize {
        let start = rows.len() - limit as usize;
        rows = rows.split_off(start);
    }

    keep_latest_contiguous_rows(&mut rows, interval.as_millis() as i64);

    let start_time = rows
        .first()
        .map(|row| format_timestamp_ms(row.candle.open_time));
    let end_time = rows
        .last()
        .map(|row| format_timestamp_ms(row.candle.open_time));
    let data: Vec<KlineResponse> = rows.into_iter().map(KlineResponse::from).collect();

    Ok(KlineSeries {
        interval: canonical_interval,
        start_time,
        end_time,
        count: data.len(),
        data,
    })
}
```

- [ ] **Step 4: Rewrite the handler wrapper**

Replace `pub async fn klines` with:

```rust
pub async fn klines(
    State(state): State<AppState>,
    Query(query): Query<KlineQuery>,
) -> Result<Json<KlineEnvelope>, (axum::http::StatusCode, String)> {
    let intervals = parse_intervals(query.intervals.as_deref())?;
    let canonical_intervals = intervals
        .iter()
        .map(Interval::canonical)
        .collect::<Vec<_>>();
    let limit = query.limit.unwrap_or(200);
    let closed_only = query.closed_only.unwrap_or(false);
    let mut series = Vec::with_capacity(intervals.len());

    for interval in intervals {
        series.push(
            query_kline_series(
                &state,
                &query.symbol,
                interval,
                query.start_time,
                query.end_time,
                limit,
                closed_only,
            )
            .await?,
        );
    }

    Ok(Json(KlineEnvelope {
        symbol: query.symbol.to_uppercase(),
        intervals: canonical_intervals,
        limit,
        closed_only,
        timezone: "Asia/Shanghai",
        server_time: Local::now().timestamp_millis(),
        series,
    }))
}
```

- [ ] **Step 5: Run focused HTTP tests**

Run:

```powershell
cargo test --test http_tests klines_endpoint_ -- --nocapture
```

Expected: all `/api/klines` tests pass.

- [ ] **Step 6: Commit handler and test changes**

Run:

```powershell
git add src/http/handlers.rs tests/http_tests.rs
git commit -m "feat: support multi-interval kline queries"
```

---

### Task 3: Update README Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the query example**

Change:

```http
GET /api/klines?symbol=BTCUSDT&interval=1&limit=1000
```

to:

```http
GET /api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=1000
```

- [ ] **Step 2: Update the parameter table**

Replace the `interval` row with:

```markdown
| `intervals` | 是 | 逗号分隔的周期列表，例如 `1,5,15`；可选值：`15S`, `30S`, `45S`, `1`, `2`, `3`, `4`, `5`, `8`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120`, `180`, `240`, `360`, `480`, `720`, `D`, `2D`, `3D`, `4D`, `10D`, `W` |
```

- [ ] **Step 3: Update the response example**

Replace the old top-level `interval`, `startTime`, `endTime`, `count`, and `data` shape with:

```json
{
  "symbol": "BTCUSDT",
  "intervals": ["1", "5", "15"],
  "limit": 1000,
  "closedOnly": false,
  "timezone": "Asia/Shanghai",
  "serverTime": 1780000000000,
  "series": [
    {
      "interval": "1",
      "startTime": "2024-03-10T00:00:00.000+08:00",
      "endTime": "2024-03-10T00:00:00.000+08:00",
      "count": 1,
      "data": [
        {
          "symbol": "BTCUSDT",
          "interval": "1",
          "candle": {
            "openTime": "2024-03-10T00:00:00.000+08:00",
            "closeTime": "2024-03-10T00:00:59.999+08:00",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 12.5,
            "quoteVolume": 1250.0,
            "tradeCount": 42,
            "isClosed": true
          }
        }
      ]
    }
  ]
}
```

- [ ] **Step 4: Update command examples**

Change the PowerShell and curl examples to:

```powershell
Invoke-RestMethod "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=10"
```

```bash
curl "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=10"
```

- [ ] **Step 5: Run README search verification**

Run:

```powershell
rg -n "api/klines\\?symbol=BTCUSDT&interval=" README.md
```

Expected: no matches.

- [ ] **Step 6: Commit documentation**

Run:

```powershell
git add README.md
git commit -m "docs: update klines query examples"
```

---

### Task 4: Final Verification

**Files:**
- Verify: `src/http/handlers.rs`
- Verify: `tests/http_tests.rs`
- Verify: `README.md`

- [ ] **Step 1: Format code**

Run:

```powershell
cargo fmt
```

Expected: command exits successfully.

- [ ] **Step 2: Run full test suite**

Run:

```powershell
cargo test
```

Expected: all tests pass.

- [ ] **Step 3: Inspect final diff**

Run:

```powershell
git status --short
git diff --stat HEAD
```

Expected: only intended files have uncommitted changes, or no changes if all task commits were made.

- [ ] **Step 4: Summarize result**

Report:

- `/api/klines` now requires `intervals`.
- Responses use top-level `series`.
- Old `interval` requests return `400`.
- Full verification command results.
