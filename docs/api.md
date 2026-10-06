# HTTP API 接入契约

本文描述当前代码提供的接口。服务用途和部署见 [README](../README.md)，指标计算规则见 [guaili 算法与业务语义](guaili.md)。路由以 [`src/http/routes.rs`](../src/http/routes.rs) 为准；行情参数和响应见 [`src/http/handlers.rs`](../src/http/handlers.rs)，动态信号接口见 [`src/http/signals.rs`](../src/http/signals.rs)。

## 通用约定

- 仓库当前 `config.toml` 的地址为 `http://127.0.0.1:3005`，部署时以 `server.bind_addr` 为准。
- 成功响应为 JSON，字段采用 `camelCase`；删除成功为 HTTP 204，无响应体。带 JSON 正文的 POST/PATCH 请求使用 `Content-Type: application/json`；信号重载无需请求体。
- 当前未实现身份认证、API 版本前缀、CORS 中间件或面向调用方的 WebSocket/SSE 推送。普通行情与指标由调用方轮询 HTTP；价格警报触发后可投递 Webhook。
- 交易对请统一使用大写且不带空格，例如 `BTCUSDT`。指标接口的 `symbols` 会去空格并转大写；K 线接口各数据源的大小写处理不完全一致，不应依赖小写输入。
- 多值参数用逗号分隔。行情/指标的 `intervals`、`symbols` 保留请求顺序，不排序、不去重；空元素（如 `1,,5`）返回 400。动态信号接口的 `symbols` 去重并保留首次出现顺序。
- 只公开配置中启用的交易对/周期。K 线与单周期指标的合法但未配置组合返回空序列（HTTP 200），不会按请求创建订阅或从 Binance 现拉历史；非法周期返回 400。动态信号查询未启用的品种返回 400。
- K 线和指标的 `data` 按 `openTime` 升序排列；`limit` 是每条序列的上限，不是整个响应的总条数。无数据时 `count=0`、`startTime=endTime=null`、`data=[]`；指标另有 `latest=null`。
- `startTime`、`endTime` 都筛选 **K 线开盘时间**，包含边界；不筛选收盘时间。响应中序列的同名字段表示实际返回首尾数据的开盘时间。

### 时间格式

| 位置 | 类型 / 单位 |
| --- | --- |
| 查询参数 `startTime`、`endTime` | Unix 毫秒整数 |
| K 线 / 指标顶层 `serverTime` | Unix 毫秒整数 |
| K 线 / 指标点 `openTime`、`closeTime` 和序列 `startTime`、`endTime` | RFC 3339 字符串，带毫秒与时区偏移 |
| 健康摘要 `serverTime`、深度检查 `latestOpenTime`、`lastMessageAt` | 同上；后两者可能为 `null` |
| 警报及事件的 `expiresAt`、`createdAt`、`updatedAt`、`triggeredAt` | Unix 毫秒整数；可空字段见下文 |
| 动态信号接口的全部时间字段（含证据点 `openTime/closeTime`） | Unix 毫秒整数；不可用或尚未确认时为 `null` |

**当前时间格式限制：** K 线和指标响应的 `timezone` 固定为 `Asia/Shanghai`，但时间字符串由服务进程的本地时区格式化。部署在其他时区时，该标签与字符串偏移可能不一致；调用方应解析字符串自带的偏移，不要再手动加 8 小时。希望统一为北京时间时，应将运行环境时区配置为 Asia/Shanghai。

时间显示不改变 K 线分桶。周线按 UTC 周一对齐；普通周期按 Unix 时间桶对齐；原生 `3D` 会从对应交易对的历史数据推断分桶相位。不要按本地午夜重新解释响应的 `openTime`。

### 支持的周期

| 类型 | 可用值 |
| --- | --- |
| 秒 | `10S`, `15S`, `30S`, `45S` |
| 分钟 | `1`, `2`, `3`, `4`, `5`, `8`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120`, `180`, `240`, `360`, `480`, `720` |
| 日 | `D`, `2D`, `3D`, `4D`, `10D` |
| 周 | `W` |

`1D`、`1W` 分别规范化为 `D`、`W`，秒/日/周后缀不区分大小写。分钟直接写数字，不使用 Binance 的 `1m`、`1h` 等格式。能解析的周期不一定已在当前部署中配置。

## 接口总览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/health` | 检查 HTTP 服务是否能响应 |
| GET | `/api/health/summary` | 汇总行情健康情况 |
| GET | `/api/health/deep` | 查看 WebSocket 及各序列详情 |
| GET | `/api/klines` | 一个交易对、多个周期的 OHLCV |
| GET | `/api/indicators/guaili` | 多个交易对、多个周期的乖离与趋势状态 |
| GET | `/api/charts/guaili` | 同一输入的 OHLC、Android 图表指标与矩阵指标快照 |
| GET | `/api/signals` | 最近的动态多周期信号采样及数据质量 |
| POST | `/api/signals/reload` | 原子重载独立 `signals.toml` 配置 |
| POST | `/api/alerts` | 创建一次性价格穿越警报 |
| GET | `/api/alerts` | 列出全部警报 |
| GET | `/api/alerts/{id}` | 查询警报 |
| PATCH | `/api/alerts/{id}` | 修改、禁用或重新启用警报 |
| DELETE | `/api/alerts/{id}` | 删除警报及其事件 |
| GET | `/api/alerts/{id}/events` | 查看触发与投递记录 |

健康接口还提供 `/health`、`/health/summary`、`/health/deep` 三个等价别名。`{id}` 替换为警报整数 ID。

## 查询 K 线

```http
GET /api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=200&closedOnly=true
```

| 参数 | 必填 | 默认 | 含义 |
| --- | --- | --- | --- |
| `symbol` | 是 | — | 一个已配置交易对 |
| `intervals` | 是 | — | 逗号分隔的周期；单周期也使用复数参数名 |
| `startTime` | 否 | — | 最小开盘时间，Unix 毫秒，含边界 |
| `endTime` | 否 | — | 最大开盘时间，Unix 毫秒，含边界 |
| `limit` | 否 | `200` | 每个周期最多返回多少根，非负整数；`0` 返回空序列 |
| `closedOnly` | 否 | `false` | `true` 仅返回已收盘数据；`false` 可包含当前动态 K 线 |

单周期请求 `symbol=BTCUSDT&intervals=1&limit=1&closedOnly=true` 的响应结构示例（数值仅作字段示意）：

```json
{
  "symbol": "BTCUSDT",
  "intervals": ["1"],
  "limit": 1,
  "closedOnly": true,
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

| 字段 | 业务含义 |
| --- | --- |
| `open/high/low/close` | 桶内开盘、最高、最低和最后价格，按交易对报价单位计价 |
| `volume` | 基础资产成交数量 |
| `quoteVolume` | 报价资产成交额；成交聚合时累加 `price × quantity` |
| `tradeCount` | REST/原生 K 线为上游成交笔数；实时 `aggTrade` 聚合每个消息计 1，不能当作相同口径的原始逐笔成交数 |
| `openTime/closeTime` | K 线时间桶的起止时间；`closeTime` 通常是桶末毫秒，不表示该数据已收盘 |
| `isClosed` | 是否已收盘；`false` 的价格、成交量和指标仍可变化 |
| `count` | 当前 `data` 的实际长度，不是数据库总量 |

### 数据合并、缺口与历史查询

分钟及以上周期合并 SQLite 与已收盘内存缓冲，同一 `openTime` 优先使用缓冲值；秒级读取内存。`closedOnly=false` 时还会合并当前 K 线缓存。

合并、过滤和数量截取后，接口只保留**最后一个缺口之后的连续片段**。例如有 `00:00、00:01、00:03、00:04` 四根，只返回 `00:03、00:04`。没有补零 K 线；`count < limit` 可能是历史不足、区间限制、缺口或未配置，而不只是请求失败。

当前选取规则需要特别留意：

- 不传 `startTime`：各数据源取不晚于 `endTime` 的最近数据，再合并并保留最新 `limit` 根。
- 传入 `startTime`：SQLite 从起点向后取最早一批；内存缓冲/秒级仍取范围内最新一批；合并后再截取最新 `limit` 根。因此这不是统一的向前游标分页接口。
- 向前查询历史可不传 `startTime`，将下一页 `endTime` 设为上一页最早 `openTime` 转成毫秒后减 1。跨缺口会分段返回；受保留数量限制，不能保证完整历史。
- 当前没有业务层最大 `limit` 限制或总量分页元数据。调用方应控制每次请求的周期数与数量。

## 查询 guaili 指标

```http
GET /api/indicators/guaili?symbols=BTCUSDT,XAUUSDT&intervals=1,5,15&limit=1&calcLimit=500&closedOnly=true
```

每个交易对/周期单独计算，按请求现算，不保存指标结果，不自动组合跨周期信号或创建价格警报。

| 参数 | 必填 | 默认 | 含义 |
| --- | --- | --- | --- |
| `symbols` | 是 | — | 逗号分隔的交易对；单品种也使用复数参数名 |
| `intervals` | 是 | — | 逗号分隔的周期 |
| `startTime/endTime` | 否 | — | 限制参与计算的 K 线开盘时间，含边界 |
| `limit` | 否 | `200` | 每条序列最多返回的指标点数；`0` 返回空序列 |
| `calcLimit` | 否 | `500` | 最多用于计算的 K 线数量，实际采用 `max(请求值或500, limit, 请求maLength或20, 请求atrPercentLen或20, 15)` |
| `closedOnly` | 否 | `false` | 为 `true` 时只使用已收盘 K 线 |
| `maLength` | 否 | `20` | 均线长度，最小修正为 `1` |
| `maType` | 否 | `EMA` | `SMA`、`EMA`、`SMMA`、`SMMA (RMA)`、`RMA`、`WMA`、`VWMA`，不区分大小写 |
| `atrLen` | 否 | `1` | 用于波动排名的 ATR 长度，最小修正为 `1`；不改变乖离分母 ATR14 |
| `atrPercentLen` | 否 | `20` | 波动排名窗口，最小修正为 `2` |
| `maxAtrRank` | 否 | `100` | `rankFilter` 的排名上限；通常按 0–100 理解，当前不强制范围校验 |
| `slopeMul` | 否 | `0.1` | 均线斜率阈值的 ATR14 倍数，当前不强制非负校验 |
| `useSlope` | 否 | `true` | 是否启用趋势强度过滤 |

响应 `calcLimit` 和 `config` 回显实际采用的计算上限与配置；`RMA/SMMA` 回显为 `SMMA (RMA)`。`calcLimit` 是上限，不是实际历史根数；它不会自动提高到 `atrLen`，也不会为 `startTime` 之前补取预热数据。

单品种、单周期 `limit=1` 的响应结构示例（数值仅作字段示意）：

```json
{
  "symbols": ["BTCUSDT"],
  "intervals": ["1"],
  "limit": 1,
  "calcLimit": 500,
  "closedOnly": true,
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
          "startTime": "2024-03-10T00:19:00.000+08:00",
          "endTime": "2024-03-10T00:19:00.000+08:00",
          "count": 1,
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
            "isClosed": true,
            "availability": "ready",
            "reasonCode": null,
            "reason": null,
            "historyCount": 20
          },
          "data": [
            {
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
              "isClosed": true,
              "availability": "ready",
              "reasonCode": null,
              "reason": null,
              "historyCount": 20
            }
          ],
          "availability": "ready",
          "reasonCode": null,
          "reason": null
        }
      ]
    }
  ]
}
```

`latest` 始终等于 `data` 最后一项；它是请求范围内最新点，不一定是当前市场最新点。`count` 为返回点数，不能据此判断 `limit=1` 的请求是否用了足够的预热数据。

矩阵指标由后端判定可用性。每个点新增 `availability`（`ready/filtered/warming_up/invalid/missing/stale/recovering`）、`reasonCode`、`reason` 和 `historyCount`；序列级同样返回状态与原因，未配置为 `not_configured`。`historyCount` 是该点计算输入中的已收盘根数，不是响应 `count`。

公开数值及趋势/过滤字段均可为 `null`：历史未完成指标预热、分母无效或其他计算异常时不能用 0 占位。默认矩阵要求至少 20 根连续输入 K，与信号所需 60 根已收盘历史独立。`ready/filtered` 返回有效数值，真实 0 和小数向零截断形成的 0 保留；`filtered` 不清空数值。客户端将 `null` 显示为 `—`，不重复判断历史根数、EMA/ATR 预热或分母。

当前实时请求（`closedOnly=false` 且无 `endTime`）的最新点还检查实时行情：未收到行情、恢复中、过期、时间无效或动态桶缺失时，`latest` 与 `data` 最后一项同时置空指标并说明原因。单周期计算输入与质量校验使用同一份已发布动态快照，并在读取历史期间阻止行情写入，避免混用重连代次或动态桶；多周期之间仍不保证原子快照。历史查询使用 `closedOnly=true` 或明确的 `endTime`。网络/数据库等整体请求错误继续返回 HTTP 错误，不伪造一批数值 0。

| 字段 | 业务含义 |
| --- | --- |
| `ma` | 当前 K 线的收盘价均线 |
| `atr14` | 当前 K 线的 ATR14；乖离实际除以前一根 ATR14 |
| `guaili` | 整根 K 线与均线的有符号距离，以前一根 ATR14 归一化 |
| `value` | `guaili × 10` 向零截断后的整数；不是百分比 |
| `atrRank` | 相对波动在窗口中的百分位排名；历史不足时为 `null` |
| `rankFilter` | 有效点排名不大于 `maxAtrRank`；不可用点为 `null` |
| `longTrend/shortTrend` | 均线连续三次上升/下降，并满足可选斜率过滤 |
| `isClosed` | 该点对应 K 线是否已收盘 |

**有效点的这些字段独立返回。** `rankFilter=false` 不会将 `guaili/value` 清零，也不会强制趋势字段变成 `false`。正乖离只说明 K 线位于均线上方，负乖离只说明位于下方；自动交易或多周期策略须由调用方定义。完整公式、初始化和边界示例见 [算法说明](guaili.md)。

## 健康检查

### 存活检查

`GET /api/health` 固定返回 `{"ok":true}`。它表示 HTTP 处理器可用，不检查数据库、上游行情或指标历史长度。

### 摘要检查

`GET /api/health/summary` 示例：

```json
{
  "ok": true,
  "websocketOk": true,
  "totalSeries": 2,
  "okSeries": 2,
  "badSeries": 0,
  "symbols": [{"symbol": "BTCUSDT", "total": 2, "ok": 2, "bad": 0}],
  "reasons": [],
  "serverTime": "2026-05-19T20:00:30.123+08:00"
}
```

`totalSeries/okSeries/badSeries` 是配置序列总数/健康数/异常数；`symbols` 按交易对汇总；`reasons` 中每项为 `{"reason":"latest candle is stale","count":1}`，只汇总序列原因，不包含 WebSocket 原因。此接口仍执行深度扫描，只缩减响应体。

### 深度检查

`GET /api/health/deep` 示例：

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
      "latestOpenTime": "2026-05-19T19:59:00.000+08:00",
      "latestLagIntervals": 1,
      "consecutiveBarsFromLatest": 42,
      "checkedBars": 42,
      "source": "sqlite+buffer",
      "ok": true,
      "reason": null
    }
  ]
}
```

- 整体 `ok` 是 `websocket.ok` 与全部序列 `ok` 的合取。不健康时通常仍为 HTTP 200，监控必须检查响应体。
- `lastMessageAgoMs` 为距最近消息的毫秒数；文本行情、Ping、Pong 都能刷新时间，不代表每个交易对都已更新。没有消息时两个消息时间字段为 `null`。
- `reconnectCount` 为本进程累计重连计数；`lastError` 是记录的连接/读取错误，可为空。
- WebSocket 断开时 `reason="websocket is disconnected"`；仍标记连接但消息超过 60 秒未更新时为 `websocket message stream is stale`。空闲超时导致断开后，可转为前者。尚未启用检查时 `ok` 可为 `true`，不能据此推断已收到行情。
- `series` 列出所有配置目标，可用于核对订阅组合；每条最多扫描 5000 根。`source` 为 `memory`（秒级）或 `sqlite+buffer`（分钟及以上）。
- `latestLagIntervals` 是最新开盘时间落后当前桶的周期数；秒级最多允许 4 个周期，其他周期最多允许 2 个。无数据时它和 `latestOpenTime` 为 `null`。
- `checkedBars` 是本次实际扫描条数；`consecutiveBarsFromLatest` 是从最新记录向前连续的根数。
- 序列无记录时 `reason="no closed candles"`；超出滞后阈值时为 `latest candle is stale`。当前实现扫描 SQLite/缓冲/秒级内存，不合并当前 K 线缓存，且没有再按 `isClosed` 过滤 SQLite 记录。
- 健康判定只要求连续尾段非空且不陈旧。较早历史有缺口、EMA/ATR 预热不足时仍可能 `ok=true`，不能把健康检查作为历史完整性或指标稳定性的证明。

## 动态信号与企业微信警报

这是与原有一次性价格穿越警报独立的功能。引擎以 `signals.toml` 的频率（默认 5 秒）读取每个品种的当前动态 K 快照、复用连续已收盘历史计算指标，再识别多周期结构。HTTP 查询只读内存结果，不增加计算或数据库查询，也不改变监控品种。算法细节见 [动态多周期结构](guaili.md#动态多周期结构)。

### 查询当前结果

```http
GET /api/signals
GET /api/signals?symbols=BTCUSDT,XAUUSDT
```

仅支持可选 `symbols`。省略时返回信号配置中的全部品种；提供时按逗号拆分、去空格、转大写、去重并按首次出现顺序返回，正常计算状态按筛选后返回品种的数据质量汇总，其他未查询品种的预热不影响该响应状态。空元素、未知参数或未在信号计算配置中启用的品种返回 400。未启用或首轮采样尚未完成时 `results=[]`；HTTP 200 本身不代表信号有效。

| 顶层字段 | 语义 |
| --- | --- |
| `enabled` / `status` | 是否启用及整体状态，见状态表 |
| `configHash` | 当前计算参数指纹；修改企业微信配置不改变此值，不包含 Webhook |
| `indicatorConfig` | 实际参与计算的 `maType` 与 `maLength`，用于显示服务器的均线名称 |
| `ruleConfig` | 当前生效规则：`extremeThreshold`、`compressionBand`、`minimumLevels`、`minHistoryBars`，含义见下文 |
| `qualityConfig` | 当前生效质量时限：`maxMarketAgeMs` 与 `maxResultAgeMs`，均为毫秒 |
| `ruleVersion` | 当前为 `live-v1` |
| `candleMode` / `evaluationMode` | 固定 `live` / `sampled_live`，表示定时采样动态 K |
| `evaluationIntervalMs` | 配置的采样间隔，毫秒 |
| `serverTime` | 此次 HTTP 响应时刻，Unix 毫秒 |
| `runId` | 运行实例标识；重启后变化，客户端不能跨实例沿用旧信号 ID |
| `snapshotVersion` | 同一实例内的采样发布版本，重载清空结果时也可增长；不是完整响应内容版本，见下文 |
| `evaluatedAt` / `computeDurationMs` | 最近采样时刻 / 该轮耗时；尚未采样时为 `null` / `0` |
| `configError` | 初始配置错误的脱敏说明，正常为 `null` |
| `delivery` | 内存投递统计、脱敏错误及最近最多 32 条结果 |
| `results` | 各品种结构和逐周期数据质量 |

`ruleConfig.extremeThreshold` 是正负共振的显示整数绝对值阈值（默认 `10`），`compressionBand` 是近均线显示整数绝对值上限（默认 `2`）；整数来自原始乖离乘 10 后向零截断，不应将 `2` 等同于原始值精确 `0.2`。`minimumLevels` 是单段所需相邻配置周期数（默认 `5`），`minHistoryBars` 是每周期所需连续已收盘历史数（默认 `60`），不包含当前动态 K。`qualityConfig.maxMarketAgeMs` 同时限制行情事件和接收时间，`maxResultAgeMs` 限制最近采样年龄。客户端应读取实际值，不能根据 `evaluationIntervalMs` 推定这些时限。

元数据与结果属于同一份生效配置。启用、关闭、等待首次采样时均返回；首次配置加载失败时返回安全的默认关闭配置元数据，而非未通过验证的文件内容。成功计算配置重载先替换元数据并清空旧结果，失败重载保留原元数据和结果。这里不返回企业微信地址、凭证或订阅详情。

| 整体 `status` | 语义 |
| --- | --- |
| `disabled` | 总开关关闭或可选配置文件缺失；计算和信号发送停止，结果为空 |
| `warming_up` | 等待首次采样，或所有品种均在预热/恢复 |
| `ready` | 所有配置周期数据可判断；仍可能没有信号，ATR rank 过滤不等于数据故障 |
| `degraded` | 部分周期无法判断、行情陈旧，或采样结果已过期；其他有效区间可有信号 |
| `config_error` | 启动时文件无效；信号关闭，行情采集继续运行 |

每项 `results[]` 包含：

| 字段 | 语义 |
| --- | --- |
| `symbol` | 品种 |
| `dataStatus` | `ready/warming_up/recovering/stale/degraded`；逐周期原因见下方证据 |
| `sampledAt` | 本轮采样时刻，Unix 毫秒 |
| `marketSequence` / `generation` | 行情输入版本 / 恢复代次；不可用时为 `null` |
| `lastMarketEventTime` | 此品种最后行情事件的 Unix 毫秒时间 |
| `primarySignal` | 小组件可优先展示的结构 ID，无结构时 `null` |
| `signals` | 全部满足规则的结构，企业微信会扫描全部结构 |
| `perIntervalQuality` | 完整配置周期集合的证据，按真实时长排序 |
| `missingIntervals` | 目前无法可靠判断的周期列表，包括预热、过期、缺口等 |

`signals[]` 的结构字段：`id`、`kind`（`extreme/compression/conflict`）、`direction`（`positive/negative/neutral`）、`runs[]`、`levelCount`、`totalLevelCount`、`anchorInterval`、`firstObservedAt`、`formedAt`、`lastChangedAt`。`anchorInterval` 是结构最大周期；`levelCount` 是最长单段周期数，分歧的 `totalLevelCount` 为两段总数。分歧方向表示短周期段：`positive` 为短正长负，`negative` 为短负长正。

`runs[]` 包含 `direction`、完整 `intervals`、`minAbsValue/maxAbsValue/meanAbsValue`，这些统计量单位为显示整数 `value` 的绝对值。另返回可为空的 `maxAbsGuaili/meanAbsGuaili`，单位为原始浮点 `guaili` 的绝对值，近均线段排序使用它们保留截断前精度。ID 随同向同类结构的周期重叠继承；`firstObservedAt` 是首次采样观察时间，`formedAt=null` 表示初次建立基线时已经存在，不能据此推断真实形成时间。`lastChangedAt` 只在周期覆盖形状变化时更新，不代表每次数值变化。

每项 `perIntervalQuality[]` 返回 `interval`、`availability`、`reason`、可选的 `reasonCode`、`value`、原始 `guaili`、`ma`、当前根 `atr14`、`atrRank`、`longTrend/shortTrend`、`historyCount`、`openTime/closeTime`、`marketEventTime`、`isClosed`。这里时间为 Unix 毫秒，区别于旧指标接口的 RFC 3339。信号引擎使用当前动态 K，因此有效证据的 `isClosed=false`。

| `availability` | 是否参与结构 / 含义 |
| --- | --- |
| `ready` | 参与；动态 K、连续历史、时效和指标有效，ATR rank 通过 |
| `filtered` | 不参与；数据可判断，但 ATR rank 未通过 |
| `warming_up` | 不参与；动态成交快照或连续历史尚不足 |
| `missing` | 不参与；当前动态 K 缺失或时间桶不覆盖当前行情 |
| `stale` | 不参与；行情或最近结果超时 |
| `gap` | 不参与；历史尾部不能与动态 K 连续衔接 |
| `recovering` | 不参与；上游断流或聚合状态正在恢复 |
| `invalid` | 不参与；数据、时间、波动分母或取数无效 |

`reason` 保留面向人的说明，客户端不应解析该英文句子的字面内容。`reasonCode` 是稳定机器原因，响应始终包含该字段；当前有效或 ATR 过滤证据通常为 `null`。按当前契约解析原因码，不提供旧响应缺字段或未知码的兼容解码。

| `reasonCode` | 含义 |
| --- | --- |
| `sampling_stale` | 最近采样结果已过期，或采样时间异常 |
| `market_stale` | 行情事件或接收时间已过期 |
| `market_recovering` | 上游恢复中，或采样后行情代次发生改变 |
| `waiting_market` | 尚未取得动态成交快照 |
| `dynamic_missing` / `dynamic_time_mismatch` | 动态 K 缺失 / 动态桶不覆盖当前行情时间 |
| `insufficient_history` / `history_gap` | 连续已收盘历史不足 / 历史尾部与动态 K 不连续 |
| `history_unavailable` | 历史读取失败 |
| `market_time_invalid` | 行情事件或接收时间异常地处于未来 |
| `invalid_data` | K 线价格、成交量等输入无效 |
| `indicator_missing` / `indicator_invalid` | 指标结果缺失 / 指标波动分母或结果无效 |
| `indicator_warming_up` | 波动排名尚未可用 |

所有不参与周期都会打断相邻区间，不能删掉后再拼接。未知数据不会被解释成信号结束。长周期预热不阻断短周期有效结构；某旧结构的参与周期未知时，运行状态保留其身份用于恢复去重，但查询只返回当前有效的结构。

默认行情时效上限 30 秒、结果时效上限 15 秒。请求时发现最近采样超时会返回 `degraded`，清空 `signals/primarySignal`，将证据标为 `stale` 并将数值/趋势字段置空；恢复或行情过期的查询失效也同样置空，保留原因与时间。处理只作用于响应副本，已固定的历史采样证据保持不变。`serverTime` 不等于行情时间，不同品种不保证同一行情事件时刻。成交稀疏品种也会在超过行情时效后暂时隐藏，不能把未更新的旧价格当成实时行情。

GET 对采样时效、包含有效数值证据品种的当前行情恢复/时效再次校验，即使没有活动信号结构也会清空失效数值；这些质量变化及结构清除不发布新的 `snapshotVersion`。因此同一 `runId + snapshotVersion` 的两次响应可能拥有不同的 `status`、`dataStatus`、`signals`、`availability` 和 `reasonCode`。客户端不能仅因版本相同跳过响应，离线后还须按服务器时间及实际质量时限停止把旧结构展示为当前有效信号。无结构的缓存证据也不能当作持续实时更新的行情值。

最小关闭响应示例：

```json
{
  "enabled": false,
  "status": "disabled",
  "configHash": "0000000000000000",
  "indicatorConfig": { "maType": "EMA", "maLength": 20 },
  "ruleConfig": { "extremeThreshold": 10, "compressionBand": 2, "minimumLevels": 5, "minHistoryBars": 60 },
  "qualityConfig": { "maxMarketAgeMs": 30000, "maxResultAgeMs": 15000 },
  "ruleVersion": "live-v1",
  "candleMode": "live",
  "evaluationMode": "sampled_live",
  "evaluationIntervalMs": 5000,
  "serverTime": 1790000000000,
  "runId": "1790000000000-1234-1",
  "snapshotVersion": 1,
  "evaluatedAt": null,
  "computeDurationMs": 0,
  "configError": null,
  "delivery": {
    "queued": 0,
    "successful": 0,
    "failed": 0,
    "dropped": 0,
    "lastError": null,
    "lastAttemptAt": null,
    "lastSuccessAt": null,
    "recent": []
  },
  "results": []
}
```

指纹、实例 ID 和时间是示意值。`delivery.queued` 是累计生成任务数，不是当前队列长度；`successful/failed` 统计完成的投递任务，`dropped` 统计超容量、重载失效或过期等丢弃任务。每项 `recent` 只含 `alertId`、脱敏 `targetId`、`symbol`、`attemptedAt`、`successful`、`error`，不返回 Webhook。

### 重载配置

```http
POST /api/signals/reload
```

无需请求体，读取服务当前工作目录的 `signals.toml`。先完整解析、校验，再一次性替换运行配置；失败返回 400 脱敏纯文本，保留旧配置和结果。配置文件缺失按关闭配置处理。改动计算参数或开关会清空旧结果并重新采样，改变企业微信订阅（包括 `message_format`）会重建通知基线。仅改变消息格式保留计算结果和 `configHash`，不触发指标重算。重载成功不会补发旧信号；旧配置尚未发送的任务作废，发送中的请求取消后不继续重试。

成功响应示例：

```json
{
  "enabled": true,
  "status": "warming_up",
  "configHash": "0000000000000000",
  "evaluationIntervalMs": 5000,
  "alertCount": 0
}
```

`status` 是重载后的当前结果状态，若只改订阅可以继续保持 `ready`。接口没有认证，部署层应管理重载入口的访问范围。

### 企业微信订阅与投递

每段 `[[wecom_alerts]]` 配置一个目标和订阅，字段见 [配置示例](../signals.example.toml)。品种和类型筛选应用于完整结构集合，最小级别按 `anchorInterval` 的真实时长判断，含相等：`3–15` 分钟结构满足最低 `15`，`1–8` 不满足；分歧按较长一段的最大周期判断。筛选不裁剪计算周期，也不限制 HTTP 结果。

进程首次观察、订阅变化及断流恢复后建立通知基线，已有结构不立即发送。后续信号首次符合订阅（包括同一 ID 从 8 分钟扩展到 15 分钟并跨过订阅下限）才发送。持续符合的同轮信号不重复；同一事件被多条订阅选中且 Webhook 相同只生成一个任务，消息格式和名称采用配置文件中首条实际生成任务的订阅，不同目标各生成任务。默认 300 秒冷却按目标/品种/信号类型与方向共享；冷却内新匹配被抑制，不会冷却结束后集中补发，需退出后再次符合。`cooldown_secs=0` 关闭冷却。

每条订阅可配置 `message_format`，只接受小写 `"detailed"`（默认）或 `"compact"`。不配置时保留原有详细消息：订阅名称、品种、类型、方向、完整周期列表、最大级别、覆盖级数、完整北京时间采样时间和观察说明。无效值或类型按配置错误处理，重载返回 400 并保留旧配置。

简要模式发送无主动换行的固定正文，示例：

```text
🔴⬆️ BTCUSDT 上方乖离共振｜⏱️10s–2m·6级｜15:58:05
🟢⬇️ BTCUSDT 下方乖离共振｜⏱️2m–10m·5级｜21:12:04
🟡 BTCUSDT 多周期近均线｜⏱️1m–15m·5级｜15:58:05
🔀 BTCUSDT 长短周期分歧·短正长负｜⏱️10s–2m / 5m–30m｜15:58:05
```

简要正文新增信号标识：`🔴⬆️` 上方共振、`🟢⬇️` 下方共振、`🟡` 近均线、`🔀` 分歧；周期范围前增加 `⏱️`。读取或匹配消息正文的调用方应适配这些前缀，HTTP 信号字段不变。`5级` 表示覆盖 5 个周期，范围右端表示该段最大周期；emoji 只辅助识别指标状态，不表示买卖指令或强度评级。详细正文格式保持原样。

范围是每段实际参与周期的首尾，不代表包含所有标准周期；完整周期列表仍在 HTTP `runs[].intervals` 中提供。`s/m/d/w` 分别表示秒/分钟/日/周，单周期只显示一个带单位的值。共振/近均线显示覆盖级数，分歧显示短、长两段范围及方向。简要模式省去订阅名称、完整日期、独立最大级别、完整列表和说明文字；时间仍为动态 K 的采样时刻，按北京时间（UTC+8）显示 `HH:mm:ss`。客户端可能自动折行；两种格式都仅表示指标状态，供观察。

发送在独立任务中进行，不阻塞采样；队列容量 256，每轮最多 256 个任务，超过容量或超过结果时效的旧任务丢弃。两种格式均使用企业微信 `text` JSON，无可执行模板。每次超时 3 秒，最多总计 3 次尝试，重试间隔 250ms、500ms；HTTP 2xx 且响应 JSON `errcode=0` 才算成功。网络错误、HTTP 5xx/429、企业微信 `-1/45009` 可重试，其他返回错误直接记失败；响应错误正文不回显。

结果、基线、冷却、队列和投递状态仅在内存保存，不增加 SQLite 表，不提供持久事件查询或退出后恢复发送。关闭总开关停止这套信号计算和发送；原有 `/api/alerts` 价格穿越功能仍按原配置运行。

## 价格警报与 Webhook

价格警报监测某交易对是否穿过固定价位，与 guaili 指标相互独立。

### 创建

```http
POST /api/alerts
Content-Type: application/json
```

```json
{
  "symbol": "BTCUSDT",
  "interval": "1",
  "price": 100000,
  "direction": "cross_down",
  "expiresAt": null,
  "webhookUrl": "https://example.com/webhook",
  "messageTemplate": "{\"symbol\":\"{{ticker}}\",\"price\":\"{{close}}\",\"alertId\":\"{{alertId}}\",\"time\":\"{{time}}\"}",
  "status": "active"
}
```

`symbol/interval/price/direction/webhookUrl/messageTemplate` 必填；`expiresAt` 可省略或为 `null`（不过期），`status` 默认 `active`，也可设 `disabled`。消息模板只接受 `messageTemplate` 字段。

- 组合必须已配置；`price` 必须为正有限数值；方向仅支持 `cross_up`、`cross_down`、`cross_any`。
- `webhookUrl` 必须以 `http://` 或 `https://` 开头。
- `messageTemplate` 是**内容为合法 JSON 的字符串**，不是直接传入 JSON 对象。占位符应放在 JSON 字符串内，保证替换前后均为合法 JSON。
- 同一交易对、周期、价位已存在警报时返回 409；即使已有警报已禁用、已触发或已过期，也占用该价位。不同方向不会绕过此限制。

创建成功返回 HTTP 201 与完整 Alert 对象：

```json
{
  "id": 1,
  "symbol": "BTCUSDT",
  "interval": "1",
  "price": 100000.0,
  "direction": "cross_down",
  "status": "active",
  "expiresAt": null,
  "webhookUrl": "https://example.com/webhook",
  "messageTemplate": "{\"symbol\":\"{{ticker}}\",\"price\":\"{{close}}\",\"alertId\":\"{{alertId}}\",\"time\":\"{{time}}\"}",
  "createdAt": 1780000000000,
  "updatedAt": 1780000000000,
  "triggeredAt": null,
  "deliveryStatus": null,
  "deliveryError": null
}
```

`status` 可读值为 `active/disabled/triggered`，没有单独的 `expired` 状态。`expiresAt` 不为空且已到期的警报停止参与判断，但列表中的 `status` 不自动修改。`triggeredAt` 为触发时间；`deliveryStatus` 为 `null/success/failed`，`deliveryError` 为失败原因或 `null`。`null` 本身不能证明投递成功。

### 触发规则

`trade` 数据源每收到一条有效实时 aggTrade 检查成交价；`kline_1m` 每收到一条有效未收盘或已收盘 1m K 更新检查该更新的 close。**警报的 `interval` 是归属/模板信息，不改变价格采样频率**；即使警报属于 `60`，也不等待 60 分钟收盘。

从已观察的一侧达到或跨越警戒线时触发：

- `cross_up`：上一次有效侧别 `< price`，本次价格 `>= price`。
- `cross_down`：上一次有效侧别 `> price`，本次价格 `<= price`。
- `cross_any`：上述任意一种；事件记录实际发生的方向。

第一次观察只建立基线；首次价格等于阈值也不触发。阈值为 100 时，`99 → 100 → 101` 在 100 触发向上穿越；`99 → 101` 同样触发。方向不匹配的触碰保留有效侧别。基线在重启、重连、历史恢复及配置修改后重新建立，历史补齐不补发穿越。`kline_1m` 更新频率由上游提供，更新之间出现并回撤的瞬时成交仍可能漏报；需要逐成交语义时配置 `source="trade"`。

### 投递与模板

触发状态、事件、投递任务在同一 SQLite 事务提交，再通过共享 HTTP 客户端异步发送 POST JSON。v1/v2 合计最多 8 个并发请求，总共最多 3 次尝试，每次超时 5 秒，失败退避 1 秒、2 秒；任意 2xx 表示接收成功。尝试次数在发请求前持久化，进程重启后恢复待投递任务，超过 30 天不再投递。旧事件回执只能修改对应事件，并且仅在它仍属于当前触发时更新警报摘要。关机取消在途请求，尚未确认的任务保持待处理。

| 占位符 | 替换内容 |
| --- | --- |
| `{{ticker}}` / `{{symbol}}` | 交易对 |
| `{{exchange}}` | 固定 `BINANCE` |
| `{{interval}}` | 警报所属周期 |
| `{{price}}` / `{{close}}` | 本次触发时观察到的价格，可能与警戒价不同 |
| `{{alertId}}` | 警报 ID |
| `{{time}}` | 本次触发时间，Unix 毫秒 |

Webhook 没有额外的固定外层包装或自动签名，正文完全由模板决定。附带 `X-Guaili-Event-Id` 全局持久事件编号和 `X-Guaili-Event-Time` 触发 Unix 毫秒；重试使用同一编号，接收端必须按编号去重。HTTP 2xx 仅表示接口接收，不代表交易完成。v1 的 `{{ticker}}` 保持原交易对；v2 使用明确的 `tvSymbol`。

### 查询、修改与删除

| 请求 | 成功响应 |
| --- | --- |
| `GET /api/alerts` | 200，Alert 数组，按 ID 降序；没有过滤或分页参数 |
| `GET /api/alerts/{id}` | 200，单个 Alert |
| `PATCH /api/alerts/{id}` | 200，更新后的 Alert；只传需要修改的创建字段 |
| `DELETE /api/alerts/{id}` | 204，同时删除该警报的所有事件 |
| `GET /api/alerts/{id}/events` | 200，事件数组，按事件 ID 降序；无事件为 `[]` |

PATCH 示例：`{"status":"disabled"}` 禁用；`{"status":"active"}` 重新启用，允许下次穿越再次触发。PATCH 不能主动设置 `triggered`。如果原警报已过期，还需将 `expiresAt` 更新为未来毫秒时间。

PATCH `expiresAt:null` 会持久清除已有到期时间；重新启用会持久清空 `triggeredAt/deliveryStatus/deliveryError`。价格、品种、周期和重新启用的修改立即更新内存规则并重设穿越基线，下一次实时观察只建立新基线。

事件响应示例：

```json
[
  {
    "id": 1,
    "alertId": 1,
    "triggeredAt": 1780000001000,
    "triggerPrice": 99990.0,
    "direction": "cross_down",
    "deliveryStatus": "success",
    "deliveryError": null
  }
]
```

## 错误处理

业务错误通常返回**纯文本**，目前没有统一 JSON 错误对象。先判断 HTTP 状态，再按内容类型读取正文。

| HTTP 状态 | 常见原因 / 响应文本 |
| --- | --- |
| 400 | 缺少参数、非法查询类型、`missing intervals`、`missing symbols`、`empty interval in intervals`、`invalid interval: ...`、`unsupported maType: ...`、警报字段校验失败、信号查询品种/参数无效或重载配置校验失败 |
| 404 | 警报不存在：`alert not found`；或请求路径不存在 |
| 409 | 警报价位重复：`an alert already exists at this price line` |
| 415 / 422 | JSON 请求 Content-Type 不正确，或 JSON 字段类型/必填字段不匹配等框架提取错误；JSON 语法错误也可能为 400 |
| 500 | 数据库操作失败等内部错误 |

非法列表元素或数据库失败会使整个请求失败，不返回部分成功的多周期结果。K 线与单周期指标的合法但未配置组合仍返回 200 空序列；信号查询未启用的品种返回 400。健康状态为 `false` 或信号质量异常也不能仅靠状态码识别。

## 同源图表快照

`GET /api/charts/guaili?symbol=BTCUSDT&interval=1&limit=300&calcLimit=500&closedOnly=false`

这是新增代码接口，部署包含该版本的服务后可用；旧 K 线与指标接口保持原契约。图表调用方使用这一条请求读取 OHLC 与指标，不能将 `/api/klines` 和 `/api/indicators/guaili` 两次请求当作同一采样。

| 参数 | 说明 |
| --- | --- |
| symbol / interval | 必填单品种、单周期；只接受后端配置组合，未配置返回400 |
| limit | 输出最多多少根，默认300，范围1–2000 |
| calcLimit | 计算输入最多多少根，默认500，实际至少limit和20，最多2000 |
| closedOnly | 默认false，允许动态末根；true只读取已收盘点 |

矩阵计算固定复用当前 `GuailiConfig::default()`，`matrixConfig`回显配置；本接口不接受自定义MA参数。Android图表口径固定EMA20、SMA(TR,14)初始化的ATR14、波幅20根90% nearest-rank。指标区别详见 guaili.md。

响应包括 `symbol`、规范化`interval`、`candleMode`、`snapshotId`、`serverTime/capturedAt`（Unix毫秒）、`source`、`indicatorContracts`、`matrixConfig`、`calcLimit`、`actualCalcBars/actualCalcStartTimeMs`、`indicatorCoverageStartTimeMs/indicatorCoverageEndTimeMs`、`dataQuality/reasons`、`nextBefore`及`bars`。

`bars[]`按开盘时间升序，每根包含数字毫秒的`openTimeMs/closeTimeMs`、OHLCV、quoteVolume、tradeCount、isClosed，以及以下具名组：

- `androidChannel`：ema20、atr14、上下轨、`closeDeviation`和当前通道趋势；ATR前13根为空。
- `amplitude`：normalizedRange、threshold、`edgeDistance`、weakTop/weakBottom；阈值前19根为空。
- `matrix`（`indicatorContracts.matrix=matrix-v2`）：ma、当前及前atr14、`rawGuaili/value`、atrRank/rankFilter、当前及前根趋势，以及 `availability/reasonCode/reason`。`rawGuaili/value` 在未预热、分母无效或最新实时行情过期时为空，诊断用均线/ATR 字段保留；有效的 0 保留。`closedOnly=true` 的已收盘历史指标不因实时行情过期而置空。矩阵仍使用前ATR分母，未改变原算法。仅提供 `matrix-v2` 契约。

两套指标都由响应OHLC所属同一冻结计算输入生成。actualCalcStartTime可能早于输出首根，不能拿输出300根重新计算就声称与500根计算结果一致。source若存在，提供运行标识、行情sequence/generation、事件时间和接收时间；它描述捕获的动态输入，不代表整个数据库历史的事务版本。历史本身没有版本来源时source可为空。

复制内存后进行数据库读取，转储前缓冲副本用于合并。若动态桶或恢复代次改变，有限重试；恢复中返回503，不能输出混合来源。无效OHLC或缺口裁剪到有效连续尾段并在reasons中说明。算术溢出返回422。dataQuality可为ready、warming_up、missing、stale、degraded或unverified；HTTP200不等于所有指标已预热或源行情持续更新。

nextBefore为输出首根openTimeMs−1，可供旧 `/api/klines` 的endTime向过去分页；本图表接口暂不接受历史游标，也不承诺无限秒级历史。客户端只替换当前响应的指标覆盖，历史OHLC页不得拼接不同计算窗口的旧指标线。

## 绘图绑定价格警报 v2

桌面端使用 /api/price-alerts，参见 [v2 详细 HTTP 契约与触发语义](price-alerts-v2.md)。原有 /api/alerts 保持 Android HTTP 字段兼容，并共享新的内存规则缓存和持久投递队列。
