# crypto-candlestick

一个基于 Rust/Axum 的 Binance U 本位合约 K 线采集服务。服务会通过 Binance WebSocket 订阅实时行情，启动时可用 REST 接口补齐历史 K 线，将分钟级及以上周期持久化到 SQLite，并提供 HTTP 接口查询 K 线和健康状态。

## 功能特性

- 订阅 Binance Futures 实时 kline 与 aggTrade 数据。
- 支持原生 Binance 周期，也支持由基础周期聚合出的自定义周期。
- 分钟级、日线、周线 K 线写入 SQLite；秒级 K 线保存在内存中。
- 查询结果会合并当前未收盘 K 线，便于前端展示实时蜡烛图。
- 启动时可按配置同步历史 K 线，并重建自定义聚合周期。
- WebSocket 断开后自动退避重连，长时间无消息会主动重连。
- 提供基础健康检查、WebSocket 状态和逐交易对/周期的深度健康检查。

## 支持的周期

配置和查询接口使用项目内部的周期格式：

| 类型 | 格式 | 示例 |
| --- | --- | --- |
| 秒级 | `{N}S` | `15S`, `30S`, `45S` |
| 分钟级 | 数字分钟 | `1`, `2`, `3`, `5`, `15`, `60`, `240` |
| 日线 | `D` 或 `{N}D` | `D`, `2D`, `3D`, `4D`, `10D` |
| 周线 | `W` | `W` |

当前支持的分钟周期为 `1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720`。

## 环境要求

- Rust stable toolchain
- 可访问 Binance Futures REST 和 WebSocket API 的网络环境

## 快速开始

复制示例配置：

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux/macOS:

```bash
cp .env.example .env
```

按需修改 `.env` 后启动服务：

Windows PowerShell:

```powershell
cargo run
```

Linux/macOS:

```bash
cargo run
```

默认监听地址为 `127.0.0.1:3000`。首次启动时，如果 `SYNC_ON_START=true`，服务会先从 Binance REST 拉取一段历史 K 线，再连接 WebSocket 进入实时更新。

## 实时性与故障处理

服务通过 Binance combined WebSocket stream 接收实时数据。连接成功后会记录 WebSocket 状态；收到行情文本消息、Ping 或 Pong 时会刷新最近消息时间。

断线或读取失败时，后台 worker 会写入 warn 日志，更新 `/api/health/deep` 中的 WebSocket 状态，并按 `1s, 2s, 4s ... 30s` 的退避节奏重连。重连成功后会使用同一个 stream URL 重新订阅。

如果连接没有显式断开，但 60 秒没有收到任何 WebSocket 消息，worker 会判定为空闲超时，主动断开当前读取循环并重连。此时深度健康检查中的 `websocket.ok` 会变为 `false`，`reason` 为 `websocket message stream is stale`。

深度健康检查还会检查每个交易对/周期的最新 K 线是否落后：分钟级及以上周期允许最多落后 2 根，秒级周期允许最多落后 4 根。超过阈值时，对应序列的 `ok=false`，`reason` 为 `latest candle is stale`，整体 `ok` 也会变为 `false`。

> 注意：当前 REST 补历史只在启动时执行。WebSocket 断线期间的分钟级及以上 K 线，如果需要在重连后立即补齐，可在后续增加重连后的 REST catch-up 任务。

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite://candles.db` | SQLite 数据库地址 |
| `BIND_ADDR` | `127.0.0.1:3000` | HTTP 服务监听地址 |
| `BINANCE_SYMBOLS` | 空 | 逗号分隔的交易对，例如 `BTCUSDT,ETHUSDT` |
| `BINANCE_INTERVALS` | 空 | 逗号分隔的周期列表，例如 `15S,1,5,D,W` |
| `RETENTION_BARS` | `5000` | 每个交易对/周期最多保留的已存储 K 线数量，设为 `0` 表示不裁剪 |
| `SYNC_ON_START` | `true` | 启动时是否同步历史 K 线 |
| `SYNC_LOOKBACK_BARS` | `1500` | 没有本地历史时，启动同步回看的 K 线数量 |

示例：

```env
DATABASE_URL=sqlite://candles.db
BIND_ADDR=127.0.0.1:3000
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT
BINANCE_INTERVALS=15S,30S,1,5,15,60,D,W
RETENTION_BARS=5000
SYNC_ON_START=true
SYNC_LOOKBACK_BARS=1500
```

## HTTP 接口

### 健康检查

```http
GET /api/health
```

响应示例：

```json
{
  "ok": true
}
```

### 深度健康检查

```http
GET /api/health/deep
```

按配置中的 `BINANCE_SYMBOLS` 和 `BINANCE_INTERVALS` 检查各序列最新 K 线及连续性。

响应示例：

```json
{
  "ok": true,
  "websocket": {
    "connected": true,
    "lastMessageAt": "2026-05-19T20:00:30.123+08:00",
    "lastMessageAgoMs": 1200,
    "reconnectCount": 0,
    "lastError": null,
    "ok": true,
    "reason": null
  },
  "series": [
    {
      "symbol": "BTCUSDT",
      "interval": "1",
      "latestOpenTime": "2026-05-19T20:00:00.000+08:00",
      "latestLagIntervals": 0,
      "consecutiveBarsFromLatest": 42,
      "checkedBars": 5000,
      "source": "sqlite",
      "ok": true,
      "reason": null
    }
  ]
}
```

适合用于监控告警的字段：

| 字段 | 说明 |
| --- | --- |
| `ok` | WebSocket 和所有配置序列均健康时为 `true` |
| `websocket.connected` | 当前 WebSocket 是否处于连接状态 |
| `websocket.lastMessageAgoMs` | 距离最近一次 WebSocket 消息的毫秒数 |
| `websocket.reconnectCount` | 本进程启动后的重连次数 |
| `websocket.lastError` | 最近一次连接、读取或超时错误 |
| `series[].latestLagIntervals` | 最新 K 线距离当前时间桶落后的周期数 |
| `series[].consecutiveBarsFromLatest` | 从最新 K 线向前连续存在的 K 线数量 |
| `series[].ok` | 该交易对/周期是否健康 |
| `series[].reason` | 不健康时的原因 |

### 查询 K 线

```http
GET /api/klines?symbol=BTCUSDT&interval=1&limit=1000
```

查询参数：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `symbol` | 是 | 交易对，例如 `BTCUSDT` |
| `interval` | 是 | 周期，可选值：`15S`, `30S`, `45S`, `1`, `2`, `3`, `4`, `5`, `8`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120`, `180`, `240`, `360`, `480`, `720`, `D`, `2D`, `3D`, `4D`, `10D`, `W` |
| `startTime` | 否 | 起始 open time，毫秒时间戳 |
| `endTime` | 否 | 结束 open time，毫秒时间戳 |
| `limit` | 否 | 返回数量，默认 `1000` |

响应示例：

```json
[
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
```

命令行示例：

Windows PowerShell:

```powershell
Invoke-RestMethod "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&interval=1&limit=10"
```

Linux/macOS:

```bash
curl "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&interval=1&limit=10"
```

## 数据存储

SQLite 表名为 `klines`，主键为 `(symbol, interval, open_time)`。服务会在启动时自动创建表，并启用 WAL。分钟级及以上周期会持久化到 SQLite；`15S`、`30S`、`45S` 等秒级周期来自 aggTrade 聚合，当前保存在内存中，适合实时展示但不会跨进程保留。

数据库文件、WAL 文件和 `.env` 已在 `.gitignore` 中忽略。

## 开发与测试

运行测试：

Windows PowerShell:

```powershell
cargo test
```

Linux/macOS:

```bash
cargo test
```

格式化代码：

Windows PowerShell:

```powershell
cargo fmt
```

Linux/macOS:

```bash
cargo fmt
```

运行静态检查：

Windows PowerShell:

```powershell
cargo clippy
```

Linux/macOS:

```bash
cargo clippy
```

## 项目结构

```text
src/
  binance/   Binance REST/WebSocket 解析、订阅与同步逻辑
  domain/    K 线和周期等领域类型
  engine/    K 线聚合器
  http/      Axum 路由和接口处理器
  storage/   SQLite 存储层
  memory.rs  当前 K 线缓存和秒级序列内存存储
tests/       集成测试和模块行为测试
```
