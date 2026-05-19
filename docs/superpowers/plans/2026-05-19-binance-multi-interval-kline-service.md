# Binance Multi-Interval Kline Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust HTTP service that ingests Binance USD-M Futures WebSocket market data, stores klines in SQLite, and serves native plus custom intervals including second-level and multi-day candles.

**Architecture:** A Tokio background runtime consumes Binance `kline_1m` and `aggTrade` streams, routes events into interval-specific aggregators, and persists finalized candles into SQLite. Axum serves HTTP endpoints for health, subscription management, and kline queries. A small in-memory cache keeps the latest open candle per symbol/interval so reads stay fast while SQLite provides durable history.

**Tech Stack:** Rust, Tokio, Axum, tokio-tungstenite, SQLx (SQLite), Serde, Chrono, Tracing.

---

### Task 1: Core interval parsing and candle math

**Files:**
- Create: `src/domain/interval.rs`
- Create: `src/domain/candle.rs`
- Create: `tests/interval_tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
use crypto_candlestick::domain::interval::Interval;

#[test]
fn parses_custom_and_native_intervals() {
    assert_eq!(Interval::parse("15S").unwrap().as_millis(), 15_000);
    assert_eq!(Interval::parse("45").unwrap().as_millis(), 2_700_000);
    assert_eq!(Interval::parse("4D").unwrap().as_millis(), 345_600_000);
    assert_eq!(Interval::parse("W").unwrap().as_millis(), 604_800_000);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test interval_tests -- --nocapture`
Expected: fail because `Interval` is not implemented yet.

- [ ] **Step 3: Write minimal implementation**

```rust
pub enum Interval { /* ... */ }
pub fn parse_interval(input: &str) -> Result<Interval, String> { /* ... */ }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test interval_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/domain/interval.rs src/domain/candle.rs tests/interval_tests.rs
git commit -m "feat: add interval parsing and candle math"
```

### Task 2: In-memory aggregation engine

**Files:**
- Create: `src/engine/aggregator.rs`
- Create: `src/engine/store.rs`
- Create: `tests/aggregator_tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
use crypto_candlestick::domain::interval::Interval;
use crypto_candlestick::engine::aggregator::{Aggregator, TradeTick};

#[test]
fn rolls_seconds_into_correct_bucket() {
    let mut agg = Aggregator::new(Interval::parse("15S").unwrap());
    let first = agg.ingest_trade(TradeTick::new(1_000, 100.0, 2.0)).unwrap();
    assert!(first.is_none());
    let closed = agg.ingest_trade(TradeTick::new(16_000, 101.0, 1.0)).unwrap();
    assert!(closed.is_some());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test aggregator_tests -- --nocapture`
Expected: fail because aggregator is missing.

- [ ] **Step 3: Write minimal implementation**

```rust
pub struct Aggregator { /* ... */ }
pub struct TradeTick { /* ... */ }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test aggregator_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/aggregator.rs src/engine/store.rs tests/aggregator_tests.rs
git commit -m "feat: add candle aggregation engine"
```

### Task 3: SQLite storage and query layer

**Files:**
- Create: `src/storage/sqlite.rs`
- Create: `tests/storage_tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn persists_and_queries_klines() {
    // create in-memory sqlite, insert a candle, query by symbol/interval
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test storage_tests -- --nocapture`
Expected: fail because storage is missing.

- [ ] **Step 3: Write minimal implementation**

```rust
pub struct SqliteStore { /* ... */ }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test storage_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/storage/sqlite.rs tests/storage_tests.rs
git commit -m "feat: add sqlite persistence"
```

### Task 4: HTTP API and Binance ingestion wiring

**Files:**
- Create: `src/http/routes.rs`
- Create: `src/http/handlers.rs`
- Create: `src/binance/ws.rs`
- Create: `src/main.rs`
- Create: `tests/http_tests.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn health_endpoint_returns_ok() {
    // start app, call /api/health, expect 200 OK
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test http_tests -- --nocapture`
Expected: fail because HTTP app is missing.

- [ ] **Step 3: Write minimal implementation**

```rust
pub fn router(/* ... */) -> axum::Router { /* ... */ }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test http_tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/http/routes.rs src/http/handlers.rs src/binance/ws.rs src/main.rs tests/http_tests.rs
git commit -m "feat: add http api and binance ingestion"
```

