# Gap-Only Kline Catch-Up Design

## Goal

Use `SYNC_LOOKBACK_BARS` as a recent-window consistency check, not just as the initial-history size. On startup and after each successful WebSocket reconnect, the service should scan recent native Binance kline rows, detect missing closed candles, and request only those missing ranges from Binance REST.

## Behavior

- Startup catch-up runs before the WebSocket worker begins, as it does today.
- Reconnect catch-up runs after a WebSocket connection is established again.
- For each native Binance kline source, the service scans the most recent `SYNC_LOOKBACK_BARS` expected bars ending at the latest fully closed bucket.
- If the local database has no rows in that window, the service fetches the whole closed lookback window.
- If rows exist, the service computes missing open-time ranges and fetches only those gaps.
- Tail lag is treated as a gap. If the latest local candle is older than the latest closed bucket, the missing tail range is fetched.
- Current open candles are not persisted by REST catch-up; they remain handled by WebSocket/latest-cache behavior.

## Binance REST Requests

Each REST request must include both `startTime` and `endTime`, plus an appropriate `limit`. Consecutive missing bars are merged into a single range. For example, if `BTCUSDT` `5m` is missing only `2026-05-27 15:40:00 +08:00`, the request should cover exactly that closed candle:

```text
/fapi/v1/klines?symbol=BTCUSDT&interval=5m&startTime=<15:40 open ms>&endTime=<15:44:59.999 close ms>&limit=1
```

If a range exceeds Binance's page limit, the implementation pages inside that missing range without requesting data outside it.

## Custom Intervals

After native gaps are filled, custom intervals are rebuilt from stored base rows. This keeps derived intervals such as `10`, `90`, `2D`, and `10D` consistent with the repaired native data.

## Health Semantics

The existing `latestLagIntervals` check remains focused on freshness. The `consecutiveBarsFromLatest` field continues to expose recent continuity. A later change can make continuity failures affect `ok`, but this catch-up change does not need to alter monitoring semantics.

## Testing

- Unit-test gap detection for single gaps, consecutive gaps, no gaps, empty local data, and tail lag.
- Test REST request construction includes `startTime`, `endTime`, and a limit matching the missing range.
- Test startup catch-up fills a known missing bar without refetching already-present neighboring rows.
- Test reconnect invokes the same catch-up path after a successful connection.
