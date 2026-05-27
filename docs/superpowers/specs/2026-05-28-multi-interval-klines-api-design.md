# Multi-Interval Klines API Design

## Goal

Change `GET /api/klines` from a single-interval query to a multi-interval query. The endpoint should require `intervals`, return all requested intervals in one response, and use one consistent response shape for both one interval and many intervals.

## API Contract

Request:

```http
GET /api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=200
```

Query parameters:

- `symbol`: required trading pair, for example `BTCUSDT`.
- `intervals`: required comma-separated interval list. Each item uses the existing internal interval format such as `15S`, `1`, `5`, `D`, or `W`.
- `startTime`: optional open-time lower bound in milliseconds.
- `endTime`: optional open-time upper bound in milliseconds.
- `limit`: optional maximum number of rows per interval, default `200`.
- `closedOnly`: optional boolean, default `false`.

The old `interval` parameter is removed. Requests without a valid `intervals` value return `400`.

Response:

```json
{
  "symbol": "BTCUSDT",
  "intervals": ["1", "5", "15"],
  "limit": 200,
  "closedOnly": false,
  "timezone": "Asia/Shanghai",
  "serverTime": 1780000000000,
  "series": [
    {
      "interval": "1",
      "startTime": "2024-03-10T00:00:00.000+08:00",
      "endTime": "2024-03-10T00:10:00.000+08:00",
      "count": 11,
      "data": []
    }
  ]
}
```

Each `series[]` item contains the same candle rows currently returned by `data[]`, including `symbol`, `interval`, and `candle`.

## Behavior

- Preserve the requested interval order in the response.
- Canonicalize each interval using the existing `Interval` parser before querying.
- Reject the entire request with `400` if any interval is invalid.
- Apply `startTime`, `endTime`, `limit`, and `closedOnly` independently to each interval.
- Query second-level intervals from `MemorySeriesStore`; query minute-or-larger intervals from SQLite.
- Append the latest open candle from `LatestCache` independently for each interval when `closedOnly=false`.
- Trim each interval's result to the latest contiguous rows using that interval's duration.

## Implementation Shape

Refactor the current single-interval logic into a helper that accepts one parsed interval and returns one response series. The handler parses `intervals`, calls the helper for each interval, and wraps the resulting series in the new envelope.

This keeps the storage and latest-candle behavior centralized, avoids duplicating the current query logic, and makes the handler easy to test.

## Error Handling

- Missing `intervals`: `400` with a clear message.
- Empty `intervals` or empty items such as `1,,5`: `400`.
- Invalid interval item: reuse the existing interval parse error and return `400`.
- Storage errors: keep returning `500`.

## Tests

Add or update HTTP tests for:

- A multi-interval request returns one `series` item per interval.
- A one-interval request still uses the new `series` shape.
- `interval` without `intervals` is rejected.
- Invalid or empty interval entries are rejected.
- Mixed second-level and minute-level intervals read from their existing stores and preserve request order.
- `closedOnly`, latest open candle inclusion, and contiguous trimming still work per interval.

## Documentation

Update the README query section and examples to use `intervals` and the new `series` response.
