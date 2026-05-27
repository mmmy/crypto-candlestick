# Gap-Only Kline Catch-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill missing recently closed native Binance klines on startup and after WebSocket reconnects while requesting only missing time ranges.

**Architecture:** `src/binance/rest.rs` owns the gap-window calculation, missing-range detection, and Binance REST paging with `startTime` and `endTime`. `src/main.rs` keeps startup sync behavior, and `src/binance/worker.rs` receives `sync_lookback_bars` so every successful WebSocket connection can trigger the same catch-up path before live events continue. Custom intervals are rebuilt after native gaps are filled.

**Tech Stack:** Rust, Tokio, Reqwest, SQLx SQLite, Binance USD-M Futures REST/WebSocket, existing integration tests.

---

## File Structure

- Modify `src/binance/rest.rs`: add `MissingKlineRange`, gap detection helpers, bounded REST fetching with `endTime`, and rewrite `sync_native_klines` to scan a closed lookback window.
- Modify `src/binance/worker.rs`: store `sync_lookback_bars` in `BinanceWorker`, pass it from `new`, and run catch-up after each successful WebSocket connection.
- Modify `src/main.rs`: pass `config.sync_lookback_bars` into `BinanceWorker::new`.
- Modify `tests/rest_tests.rs`: add unit tests for gap range detection and catch-up request planning behavior that can run without Binance network access.
- Modify `tests/subscription_plan_tests.rs` only if constructor call sites need adjustment; prefer avoiding test churn by preserving `SubscriptionPlan` APIs.

### Task 1: Add Gap Detection Unit Tests

**Files:**
- Modify: `tests/rest_tests.rs`
- Modify: `src/binance/rest.rs`

- [ ] **Step 1: Write failing tests for missing range detection**

Append these tests to `tests/rest_tests.rs`:

```rust
use crypto_candlestick::binance::rest::{detect_missing_kline_ranges, MissingKlineRange};

#[test]
fn detects_single_missing_kline_range() {
    let ranges = detect_missing_kline_ranges(
        0,
        4 * 60_000,
        60_000,
        &[0, 60_000, 180_000, 240_000],
    );

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 120_000,
            end_open_time: 120_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn merges_consecutive_missing_klines_into_one_range() {
    let ranges = detect_missing_kline_ranges(0, 5 * 60_000, 60_000, &[0, 60_000, 300_000]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 120_000,
            end_open_time: 240_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn detects_tail_lag_as_missing_range() {
    let ranges = detect_missing_kline_ranges(0, 4 * 60_000, 60_000, &[0, 60_000, 120_000]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 180_000,
            end_open_time: 240_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn empty_window_fetches_whole_closed_lookback_range() {
    let ranges = detect_missing_kline_ranges(0, 2 * 60_000, 60_000, &[]);

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 0,
            end_open_time: 120_000,
            interval_ms: 60_000,
        }]
    );
}

#[test]
fn complete_window_has_no_missing_ranges() {
    let ranges = detect_missing_kline_ranges(0, 2 * 60_000, 60_000, &[0, 60_000, 120_000]);

    assert!(ranges.is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test --test rest_tests detect
```

Expected: compile fails because `detect_missing_kline_ranges` and `MissingKlineRange` do not exist.

- [ ] **Step 3: Add minimal public range type and detection helper**

In `src/binance/rest.rs`, add this after the constants:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingKlineRange {
    pub start_open_time: i64,
    pub end_open_time: i64,
    pub interval_ms: i64,
}

impl MissingKlineRange {
    fn missing_count(&self) -> u32 {
        ((self.end_open_time - self.start_open_time) / self.interval_ms + 1) as u32
    }
}

pub fn detect_missing_kline_ranges(
    window_start_open_time: i64,
    window_end_open_time: i64,
    interval_ms: i64,
    existing_open_times: &[i64],
) -> Vec<MissingKlineRange> {
    let existing = existing_open_times
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut ranges = Vec::new();
    let mut current_start = None;
    let mut expected = window_start_open_time;

    while expected <= window_end_open_time {
        if existing.contains(&expected) {
            if let Some(start) = current_start.take() {
                ranges.push(MissingKlineRange {
                    start_open_time: start,
                    end_open_time: expected - interval_ms,
                    interval_ms,
                });
            }
        } else if current_start.is_none() {
            current_start = Some(expected);
        }

        expected += interval_ms;
    }

    if let Some(start) = current_start {
        ranges.push(MissingKlineRange {
            start_open_time: start,
            end_open_time: window_end_open_time,
            interval_ms,
        });
    }

    ranges
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test --test rest_tests detect
```

Expected: all five new detection tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/binance/rest.rs tests/rest_tests.rs
git commit -m "test: cover kline gap range detection"
```

### Task 2: Add Closed Lookback Window Calculation Tests

**Files:**
- Modify: `tests/rest_tests.rs`
- Modify: `src/binance/rest.rs`

- [ ] **Step 1: Write failing tests for closed lookback windows**

Append these tests to `tests/rest_tests.rs`:

```rust
use crypto_candlestick::binance::rest::closed_lookback_window;

#[test]
fn closed_lookback_window_ends_at_latest_closed_bucket() {
    let interval = Interval::parse("5").unwrap();
    let now_ms = 1779868323456; // 2026-05-27 16:52:03.456 +08:00

    let (start, end) = closed_lookback_window(&interval, 3, now_ms);

    assert_eq!(start, 1779867600000); // 15:40 +08:00
    assert_eq!(end, 1779868200000); // 15:50 +08:00
}

#[test]
fn closed_lookback_window_uses_one_bar_minimum() {
    let interval = Interval::parse("1").unwrap();
    let now_ms = 1779868323456;

    let (start, end) = closed_lookback_window(&interval, 0, now_ms);

    assert_eq!(start, end);
    assert_eq!(end, 1779868260000); // 16:51 +08:00
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test --test rest_tests closed_lookback_window
```

Expected: compile fails because `closed_lookback_window` does not exist.

- [ ] **Step 3: Add closed lookback helper**

In `src/binance/rest.rs`, add:

```rust
pub fn closed_lookback_window(interval: &Interval, lookback_bars: u32, now_ms: i64) -> (i64, i64) {
    let interval_ms = interval.as_millis() as i64;
    let current_bucket_start = interval.bucket_start_ms(now_ms);
    let latest_closed_open_time = current_bucket_start - interval_ms;
    let bar_count = i64::from(lookback_bars.max(1));
    let start_open_time = latest_closed_open_time - (bar_count - 1) * interval_ms;

    (start_open_time, latest_closed_open_time)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test --test rest_tests closed_lookback_window
```

Expected: both closed-window tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/binance/rest.rs tests/rest_tests.rs
git commit -m "feat: calculate closed kline lookback windows"
```

### Task 3: Add REST Page Planning With startTime and endTime

**Files:**
- Modify: `tests/rest_tests.rs`
- Modify: `src/binance/rest.rs`

- [ ] **Step 1: Write failing tests for bounded request pages**

Append these tests to `tests/rest_tests.rs`:

```rust
use crypto_candlestick::binance::rest::{plan_rest_kline_pages, RestKlinePage};

#[test]
fn plans_single_rest_page_for_small_gap() {
    let pages = plan_rest_kline_pages(&MissingKlineRange {
        start_open_time: 1779867600000,
        end_open_time: 1779867600000,
        interval_ms: 300_000,
    });

    assert_eq!(
        pages,
        vec![RestKlinePage {
            start_time: 1779867600000,
            end_time: 1779867899999,
            limit: 1,
        }]
    );
}

#[test]
fn splits_large_gap_without_requesting_outside_range() {
    let pages = plan_rest_kline_pages(&MissingKlineRange {
        start_open_time: 0,
        end_open_time: 1500 * 60_000,
        interval_ms: 60_000,
    });

    assert_eq!(pages.len(), 2);
    assert_eq!(
        pages[0],
        RestKlinePage {
            start_time: 0,
            end_time: 1499 * 60_000 + 59_999,
            limit: 1500,
        }
    );
    assert_eq!(
        pages[1],
        RestKlinePage {
            start_time: 1500 * 60_000,
            end_time: 1500 * 60_000 + 59_999,
            limit: 1,
        }
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test --test rest_tests plan
```

Expected: compile fails because `plan_rest_kline_pages` and `RestKlinePage` do not exist.

- [ ] **Step 3: Add REST page planner**

In `src/binance/rest.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestKlinePage {
    pub start_time: i64,
    pub end_time: i64,
    pub limit: u32,
}

pub fn plan_rest_kline_pages(range: &MissingKlineRange) -> Vec<RestKlinePage> {
    let mut pages = Vec::new();
    let mut next_start = range.start_open_time;
    let mut remaining = range.missing_count();

    while remaining > 0 {
        let limit = remaining.min(MAX_KLINE_LIMIT);
        let page_end_open_time = next_start + (i64::from(limit) - 1) * range.interval_ms;
        pages.push(RestKlinePage {
            start_time: next_start,
            end_time: page_end_open_time + range.interval_ms - 1,
            limit,
        });
        next_start = page_end_open_time + range.interval_ms;
        remaining -= limit;
    }

    pages
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test --test rest_tests plan
```

Expected: both REST page planner tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/binance/rest.rs tests/rest_tests.rs
git commit -m "feat: plan bounded Binance kline requests"
```

### Task 4: Rewrite Startup Sync To Fill Only Missing Ranges

**Files:**
- Modify: `src/binance/rest.rs`
- Modify: `tests/rest_tests.rs`

- [ ] **Step 1: Write a storage-level test for missing window detection**

Append this async test to `tests/rest_tests.rs`:

```rust
use crypto_candlestick::binance::rest::missing_ranges_for_source;
use crypto_candlestick::binance::worker::KlineSource;

#[tokio::test]
async fn finds_only_missing_ranges_from_sqlite_window() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let source = KlineSource {
        symbol: "BTCUSDT".to_string(),
        canonical_interval: "5".to_string(),
        binance_interval: "5m",
        interval: Interval::parse("5").unwrap(),
    };

    for open_time in [0, 300_000, 900_000, 1_200_000] {
        store
            .upsert_candle(
                "BTCUSDT",
                "5",
                &Candle {
                    open_time,
                    close_time: open_time + 299_999,
                    open: 100.0,
                    high: 101.0,
                    low: 99.0,
                    close: 100.5,
                    volume: 1.0,
                    quote_volume: 100.0,
                    trade_count: 1,
                    is_closed: true,
                },
            )
            .await
            .unwrap();
    }

    let ranges = missing_ranges_for_source(&store, &source, 0, 1_200_000)
        .await
        .unwrap();

    assert_eq!(
        ranges,
        vec![MissingKlineRange {
            start_open_time: 600_000,
            end_open_time: 600_000,
            interval_ms: 300_000,
        }]
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test --test rest_tests finds_only_missing_ranges_from_sqlite_window
```

Expected: compile fails because `missing_ranges_for_source` does not exist.

- [ ] **Step 3: Add source-window gap query helper**

In `src/binance/rest.rs`, add:

```rust
pub async fn missing_ranges_for_source(
    store: &SqliteStore,
    source: &crate::binance::worker::KlineSource,
    window_start_open_time: i64,
    window_end_open_time: i64,
) -> Result<Vec<MissingKlineRange>, RestError> {
    let interval_ms = source.interval.as_millis() as i64;
    let expected_bars =
        ((window_end_open_time - window_start_open_time) / interval_ms + 1) as u32;
    let rows = store
        .query_klines(
            &source.symbol,
            &source.canonical_interval,
            Some(window_start_open_time),
            Some(window_end_open_time),
            expected_bars,
        )
        .await?;
    let existing_open_times = rows
        .into_iter()
        .map(|row| row.candle.open_time)
        .collect::<Vec<_>>();

    Ok(detect_missing_kline_ranges(
        window_start_open_time,
        window_end_open_time,
        interval_ms,
        &existing_open_times,
    ))
}
```

- [ ] **Step 4: Update `fetch_klines_page` to include `endTime`**

Replace the existing `fetch_klines_page` signature and query block with:

```rust
async fn fetch_klines_page(
    client: &reqwest::Client,
    symbol: &str,
    interval: &str,
    start_time: i64,
    end_time: i64,
    limit: u32,
) -> Result<Vec<Candle>, RestError> {
    let payload = client
        .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/klines"))
        .query(&[
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
            ("limit", limit.min(MAX_KLINE_LIMIT).to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    parse_rest_klines(payload)
}
```

- [ ] **Step 5: Rewrite `sync_native_klines` to use missing ranges**

Replace the body of the `for source in plan.kline_sources()` loop in `sync_native_klines` with:

```rust
let now_ms = chrono::Utc::now().timestamp_millis();
let (window_start, window_end) =
    closed_lookback_window(&source.interval, lookback_bars, now_ms);
let ranges = missing_ranges_for_source(store, &source, window_start, window_end).await?;

for range in ranges {
    for page in plan_rest_kline_pages(&range) {
        let candles = fetch_klines_page(
            &client,
            &source.symbol,
            source.binance_interval,
            page.start_time,
            page.end_time,
            page.limit,
        )
        .await?;

        for candle in &candles {
            store
                .upsert_candle(&source.symbol, &source.canonical_interval, candle)
                .await?;
        }

        sleep(Duration::from_millis(120)).await;
    }
}
```

Keep the existing `rebuild_custom_klines(store, &plan, DEFAULT_REBUILD_LIMIT).await?;` after the loop.

- [ ] **Step 6: Run rest tests**

Run:

```powershell
cargo test --test rest_tests
```

Expected: all `rest_tests` pass.

- [ ] **Step 7: Commit**

```powershell
git add src/binance/rest.rs tests/rest_tests.rs
git commit -m "feat: sync only missing kline ranges"
```

### Task 5: Trigger Catch-Up After WebSocket Reconnect

**Files:**
- Modify: `src/binance/worker.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Add `sync_lookback_bars` to `BinanceWorker`**

In `src/binance/worker.rs`, import the sync function:

```rust
use crate::{
    binance::rest::sync_native_klines,
    domain::interval::Interval,
    engine::aggregator::{Aggregator, TradeTick},
    memory::{LatestCache, MemorySeriesStore},
    runtime_health::{RuntimeHealth, WS_IDLE_TIMEOUT},
    storage::sqlite::SqliteStore,
};
```

Add a field to `BinanceWorker`:

```rust
sync_lookback_bars: u32,
```

Add the constructor parameter after `intervals`:

```rust
sync_lookback_bars: u32,
```

Store it in `Self`:

```rust
sync_lookback_bars,
```

- [ ] **Step 2: Pass config from main**

In `src/main.rs`, update the `BinanceWorker::new` call:

```rust
let worker = BinanceWorker::new(
    store.clone(),
    latest.clone(),
    memory_series.clone(),
    runtime_health.clone(),
    config.symbols,
    config.intervals,
    config.sync_lookback_bars,
);
```

- [ ] **Step 3: Add reconnect catch-up after connection succeeds**

In `src/binance/worker.rs`, inside the `Ok((ws, _)) => {` block, immediately after `backoff_secs = 1;`, add:

```rust
if let Err(err) = sync_native_klines(
    &self.store,
    self.plan.symbols.clone(),
    self.plan.intervals.clone(),
    self.sync_lookback_bars,
)
.await
{
    tracing::warn!("websocket reconnect kline catch-up failed: {}", err);
}
```

This code can access `symbols` and `intervals` because it is inside the same module as `SubscriptionPlan`.

- [ ] **Step 4: Run compile check**

Run:

```powershell
cargo test --no-run
```

Expected: all tests compile.

- [ ] **Step 5: Run focused tests**

Run:

```powershell
cargo test --test rest_tests --test subscription_plan_tests
```

Expected: both test suites pass.

- [ ] **Step 6: Commit**

```powershell
git add src/binance/worker.rs src/main.rs
git commit -m "feat: catch up klines after websocket reconnect"
```

### Task 6: Final Verification

**Files:**
- Verify: entire workspace

- [ ] **Step 1: Run formatting**

Run:

```powershell
cargo fmt
```

Expected: command exits successfully.

- [ ] **Step 2: Run full tests**

Run:

```powershell
cargo test
```

Expected: all tests pass.

- [ ] **Step 3: Check working tree**

Run:

```powershell
git status --short
```

Expected: only pre-existing unrelated user changes remain, or the tree is clean if those were handled outside this plan.

- [ ] **Step 4: Commit formatting adjustments if any**

If `cargo fmt` changed files after the previous commits, run:

```powershell
git add src/binance/rest.rs src/binance/worker.rs src/main.rs tests/rest_tests.rs
git commit -m "style: format kline catch-up changes"
```

Expected: commit is created only if formatting produced a diff.
