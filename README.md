# crypto-candlestick

一个基于 Rust/Axum 的 Binance U 本位合约 K 线采集服务。服务会通过 Binance WebSocket 订阅实时行情，启动时可用 REST 接口补齐历史 K 线，将分钟级及以上周期持久化到 SQLite，并提供 HTTP 接口查询 K 线和健康状态。

## 功能特性

- 订阅 Binance Futures 实时 kline 与 aggTrade 数据。
- 每个交易对可独立配置周期和实时数据源，或按最小周期自动选择。
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
| 秒级 | `{N}S` | `10S`, `15S`, `30S`, `45S` |
| 分钟级 | 数字分钟 | `1`, `2`, `3`, `5`, `15`, `60`, `240` |
| 日线 | `D` 或 `{N}D` | `D`, `2D`, `3D`, `4D`, `10D` |
| 周线 | `W` | `W` |

当前支持的分钟周期为 `1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720`。

## 环境要求

- Rust stable toolchain
- 可访问 Binance Futures REST 和 WebSocket API 的网络环境

## 快速开始

按需修改项目根目录下的 `config.toml`，然后启动服务：

Windows PowerShell:

```powershell
cargo run
```

Linux/macOS:

```bash
cargo run
```

程序固定读取当前工作目录下的 `config.toml`；文件缺失、字段拼写错误或配置值无效时会拒绝启动。首次启动时，如果 `binance.sync_on_start=true`，服务会先从 Binance REST 拉取一段历史 K 线，再连接 WebSocket 进入实时更新。

## 实时性与故障处理

服务通过 Binance combined WebSocket stream 接收实时数据。连接成功后会记录 WebSocket 状态；收到行情文本消息、Ping 或 Pong 时会刷新最近消息时间。

每个交易对支持三种实时数据源：

| 数据源 | 行为 |
| --- | --- |
| `auto` | 最小周期至少为 1 分钟时选择 `kline_1m`；包含秒级周期时选择 `trade` |
| `trade` | 仅订阅一个 `aggTrade` 流，由逐笔成交生成该交易对的全部配置周期 |
| `kline_1m` | 仅订阅一个 1m K 线流，只处理已收盘 1m K，并据此生成全部配置周期 |

`kline_1m` 模式下，高级别动态 K 每分钟更新一次。币安仍会发送未收盘的 1m K 消息，但服务不会用这些消息更新 K 线或检查价格警报。显式使用 `kline_1m` 时不能配置秒级周期。

断线或读取失败时，后台 worker 会写入 warn 日志，更新 `/api/health/deep` 中的 WebSocket 状态，并按 `1s, 2s, 4s ... 30s` 的退避节奏重连。重连成功后会补齐缺失的分钟级及以上 K 线、重建聚合状态，再使用同一个 stream URL 继续订阅。程序启动时还会通过 REST 强制刷新每种基础周期最近 2 根已收盘 K 和当前 K，再从数据库恢复正在形成的聚合 K；这不会增加稳态 WebSocket 数据流。

如果连接没有显式断开，但 60 秒没有收到任何 WebSocket 消息，worker 会判定为空闲超时，主动断开当前读取循环并重连。此时深度健康检查中的 `websocket.ok` 会变为 `false`，`reason` 为 `websocket message stream is stale`。

深度健康检查还会检查每个交易对/周期的最新 K 线是否落后：分钟级及以上周期允许最多落后 2 根，秒级周期允许最多落后 4 根。超过阈值时，对应序列的 `ok=false`，`reason` 为 `latest candle is stale`，整体 `ok` 也会变为 `false`。

## 配置项

所有配置都来自 `config.toml`，不读取 `.env` 或业务环境变量。配置字段如下：

| 字段 | 说明 |
| --- | --- |
| `server.bind_addr` | HTTP 服务监听地址 |
| `database.url` | SQLite 数据库地址 |
| `database.retention_bars` | 每个交易对/周期保留的 K 线数量；`0` 表示不裁剪 |
| `binance.sync_on_start` | 启动时是否同步历史 K 线 |
| `binance.sync_lookback_bars` | 没有本地历史时同步回看的 K 线数量 |
| `binance.symbols[].symbol` | 交易对名称 |
| `binance.symbols[].intervals` | 该交易对启用的周期数组 |
| `binance.symbols[].source` | `auto`、`trade` 或 `kline_1m`；省略时为 `auto` |
| `realtime.flush_interval_secs` | 实时收盘 K 线批量写库的最长等待时间 |
| `realtime.flush_max_rows` | 缓存达到该行数时提前写库 |
| `logging.dir` | 日志目录 |
| `logging.level` | 日志过滤级别，例如 `info` 或 `debug` |

示例：

```toml
[server]
bind_addr = "127.0.0.1:3000"

[database]
url = "sqlite://candles.db?mode=rwc"
retention_bars = 5000

[binance]
sync_on_start = true
sync_lookback_bars = 1500

[[binance.symbols]]
symbol = "BTCUSDT"
intervals = ["10S", "15S", "1", "5", "15"]
source = "auto"

[[binance.symbols]]
symbol = "ETHUSDT"
intervals = ["15", "60", "240"]
source = "auto"

[realtime]
flush_interval_secs = 300
flush_max_rows = 1000

[logging]
dir = "logs"
level = "info"
```

## 日志

服务默认同时输出控制台日志和按天滚动的文件日志。文件写入 `logging.dir` 指定的目录，文件名形如 `crypto-candlestick.log.YYYY-MM-DD`。通过 `logging.level` 控制日志级别；日常使用建议设为 `info`，排查问题时可临时改为 `debug`。

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

按 `config.toml` 中的 `binance.symbols` 检查各序列最新 K 线及连续性。

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
GET /api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=1000
```

查询参数：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `symbol` | 是 | 交易对，例如 `BTCUSDT` |
| `intervals` | 是 | 逗号分隔的周期列表，例如 `1,5,15`；可选值：`10S`, `15S`, `30S`, `45S`, `1`, `2`, `3`, `4`, `5`, `8`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120`, `180`, `240`, `360`, `480`, `720`, `D`, `2D`, `3D`, `4D`, `10D`, `W` |
| `startTime` | 否 | 起始 open time，毫秒时间戳 |
| `endTime` | 否 | 结束 open time，毫秒时间戳 |
| `limit` | 否 | 返回数量，默认 `200` |
| `closedOnly` | 否 | 设为 `true` 时只返回已收线 K 线，默认 `false` |

响应示例：

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

命令行示例：

Windows PowerShell:

```powershell
Invoke-RestMethod "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=10"
```

Linux/macOS:

```bash
curl "http://127.0.0.1:3000/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=10"
```

### 查询 guaili 指标

```http
GET /api/indicators/guaili?symbols=BTCUSDT,ETHUSDT&intervals=1,5,15&limit=200
```

该接口按请求实时计算 Pine 脚本中的 `guaili` 派生值，不落库。它复用 K 线查询的数据视图：分钟级及以上周期来自 SQLite + 实时已收线缓冲，秒级周期来自内存序列；`closedOnly=false` 时会合并当前未收线 K 线。

查询参数：

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `symbols` | 是 | - | 逗号分隔的交易对列表，例如 `BTCUSDT,ETHUSDT`；单品种也使用 `symbols=BTCUSDT` |
| `intervals` | 是 | - | 逗号分隔的周期列表，例如 `1,5,15` |
| `startTime` | 否 | - | 起始 open time，毫秒时间戳 |
| `endTime` | 否 | - | 结束 open time，毫秒时间戳 |
| `limit` | 否 | `200` | 每个周期最多返回的指标点数量 |
| `calcLimit` | 否 | `500` | 每个周期最多用于计算的 K 线数量，会自动不小于 `limit`、`maLength`、`atrPercentLen` 和 `15` |
| `closedOnly` | 否 | `false` | 设为 `true` 时只使用已收线 K 线 |
| `maLength` | 否 | `20` | 均线长度 |
| `maType` | 否 | `EMA` | 可选 `SMA`, `EMA`, `SMMA (RMA)`, `RMA`, `WMA`, `VWMA` |
| `atrLen` | 否 | `1` | Pine 脚本中 ATR 小 K 过滤使用的 ATR 长度 |
| `atrPercentLen` | 否 | `20` | ATR percent rank 窗口长度 |
| `maxAtrRank` | 否 | `100` | `rankFilter` 阈值 |
| `slopeMul` | 否 | `0.1` | 趋势斜率过滤倍数 |
| `useSlope` | 否 | `true` | 是否启用斜率过滤 |

响应中每个周期包含 `data` 和 `latest`。`value` 对应 Pine 脚本中的 `int(guaili * 10)`。如果只想看最新值，可以传 `limit=1`；接口仍会用 `calcLimit` 指定的历史 K 线计算 MA/ATR，再只返回最新 1 个指标点。

响应示例：

```json
{
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "intervals": ["1"],
  "limit": 200,
  "calcLimit": 500,
  "closedOnly": false,
  "config": {
    "maLength": 20,
    "maType": "EMA",
    "atrLen": 1,
    "atrPercentLen": 20,
    "maxAtrRank": 100.0,
    "slopeMul": 0.1,
    "useSlope": true
  },
  "timezone": "Asia/Shanghai",
  "serverTime": 1780000000000,
  "results": [
    {
      "symbol": "BTCUSDT",
      "series": [
        {
          "interval": "1",
          "startTime": "2024-03-10T00:00:00.000+08:00",
          "endTime": "2024-03-10T00:19:00.000+08:00",
          "count": 20,
          "latest": {
            "openTime": "2024-03-10T00:19:00.000+08:00",
            "closeTime": "2024-03-10T00:19:59.999+08:00",
            "ma": 100.0,
            "atr14": 10.0,
            "atrRank": 50.0,
            "rankFilter": true,
            "guaili": 1.2,
            "value": 12,
            "longTrend": true,
            "shortTrend": false,
            "isClosed": false
          },
          "data": []
        }
      ]
    }
  ]
}
```

多品种、多级别乖离信号的历史验证、阈值建议和数据质量限制，见
[乖离多级别信号验证记录](docs/guaili-multi-interval-signal-validation.md)。

### 价格警报

警报只能创建在已配置的交易对与周期组合上。`trade` 模式使用逐笔成交价检查；`kline_1m` 模式使用每分钟收盘价检查，因此分钟内穿越后又收回的情况不会触发。价格严格从警戒线一侧穿到另一侧时触发；等于警戒线不触发。警报为一次性触发，到期后保持原 `expiresAt` 但不再触发。Webhook 失败仍会标记为已触发，并记录投递失败状态，后台最多重试 3 次。

```http
POST   /api/alerts
GET    /api/alerts
GET    /api/alerts/:id
GET    /api/alerts/:id/events
PATCH  /api/alerts/:id
DELETE /api/alerts/:id
```

创建示例：

```json
{
  "symbol": "BTCUSDT",
  "interval": "1",
  "price": 100000,
  "direction": "cross_down",
  "expiresAt": 1788266400000,
  "webhookUrl": "https://example.com/webhook",
  "messageTemplate": "{\"des\":\"{{interval}}底部合约多{{ticker}}下穿{{close}}\",\"symbol\":\"{{ticker}}\",\"price\":\"{{close}}\"}"
}
```

`direction` 支持 `cross_up`、`cross_down`、`cross_any`。模板支持 `{{ticker}}`、`{{symbol}}`、`{{exchange}}`、`{{interval}}`、`{{price}}`、`{{close}}`、`{{alertId}}`、`{{time}}`。重新设置已触发或禁用的警报时，PATCH 传入 `status: "active"`。

触发记录按警报 ID 保存，`GET /api/alerts/:id/events` 返回该警报的触发价格、方向和 Webhook 投递结果；删除警报时同步删除其触发记录。

## 数据存储

SQLite 表名为 `klines`，主键为 `(symbol, interval, open_time)`。服务会在启动时自动创建表，并启用 WAL。分钟级及以上周期会持久化到 SQLite；`10S`、`15S`、`30S`、`45S` 等秒级周期来自 aggTrade 聚合，当前保存在内存中，适合实时展示但不会跨进程保留。

数据库文件、WAL 文件和旧的 `.env` 文件已在 `.gitignore` 中忽略；程序只读取 `config.toml`。

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
