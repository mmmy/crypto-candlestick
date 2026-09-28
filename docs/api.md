# HTTP API 接入契约

本文描述当前代码提供的接口。服务用途和部署见 [README](../README.md)，指标计算规则见 [guaili 算法与业务语义](guaili.md)。路由以 [`src/http/routes.rs`](../src/http/routes.rs) 为准，参数和响应以 [`src/http/handlers.rs`](../src/http/handlers.rs) 为准。

## 通用约定

- 仓库当前 `config.toml` 的地址为 `http://127.0.0.1:3005`，部署时以 `server.bind_addr` 为准。
- 成功响应为 JSON，字段采用 `camelCase`；删除成功为 HTTP 204，无响应体。POST/PATCH 请求使用 `Content-Type: application/json`。
- 当前未实现身份认证、API 版本前缀、CORS 中间件或面向调用方的 WebSocket/SSE 推送。普通行情与指标由调用方轮询 HTTP；价格警报触发后可投递 Webhook。
- 交易对请统一使用大写且不带空格，例如 `BTCUSDT`。指标接口的 `symbols` 会去空格并转大写；K 线接口各数据源的大小写处理不完全一致，不应依赖小写输入。
- 多值参数用逗号分隔。`intervals`、`symbols` 保留请求顺序，不排序、不去重；空元素（如 `1,,5`）返回 400。
- 只公开配置中启用的交易对/周期。合法但未配置的组合返回空序列（HTTP 200），不会按请求创建订阅或从 Binance 现拉历史；非法周期返回 400。
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
            "isClosed": true
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
              "isClosed": true
            }
          ]
        }
      ]
    }
  ]
}
```

`latest` 始终等于 `data` 最后一项；它是请求范围内最新点，不一定是当前市场最新点。`count` 为返回点数，不能据此判断 `limit=1` 的请求是否用了足够的预热数据。

| 字段 | 业务含义 |
| --- | --- |
| `ma` | 当前 K 线的收盘价均线 |
| `atr14` | 当前 K 线的 ATR14；乖离实际除以前一根 ATR14 |
| `guaili` | 整根 K 线与均线的有符号距离，以前一根 ATR14 归一化 |
| `value` | `guaili × 10` 向零截断后的整数；不是百分比 |
| `atrRank` | 相对波动在窗口中的百分位排名；历史不足时为 `null` |
| `rankFilter` | 排名存在且不大于 `maxAtrRank` |
| `longTrend/shortTrend` | 均线连续三次上升/下降，并满足可选斜率过滤 |
| `isClosed` | 该点对应 K 线是否已收盘 |

**这些字段独立返回。** `rankFilter=false` 不会将 `guaili/value` 清零，也不会强制趋势字段变成 `false`。正乖离只说明 K 线位于均线上方，负乖离只说明位于下方；自动交易或多周期策略须由调用方定义。完整公式、初始化和边界示例见 [算法说明](guaili.md)。

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

`symbol/interval/price/direction/webhookUrl/messageTemplate` 必填；`expiresAt` 可省略或为 `null`（不过期），`status` 默认 `active`，也可设 `disabled`。`message` 是 `messageTemplate` 的输入别名。

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

`trade` 数据源每收到一条 aggTrade 检查成交价；`kline_1m` 每收到一根已收盘 1m K 检查收盘价。**警报的 `interval` 是归属/模板信息，不改变价格采样频率**；即使警报属于 `60`，也不等待 60 分钟收盘。

两次相邻有效检查都必须严格位于警戒线两侧：

- `cross_up`：上一次价格 `< price`，本次价格 `> price`。
- `cross_down`：上一次价格 `> price`，本次价格 `< price`。
- `cross_any`：上述任意一种；事件记录实际发生的方向。

第一次观察只建立基线。等于警戒价会将当前侧别记为 0，不触发；例如阈值为 100 时，`99 → 100 → 101` 不触发，而 `99 → 101` 触发向上穿越。侧别基线仅在进程内保存，重启后重新建立；历史补齐不补发过去的穿越事件。`kline_1m` 无法发现分钟内穿越后收回的价格路径。

### 投递与模板

触发时先将警报标记为 `triggered` 并写入事件，再异步发送 HTTP POST JSON。当前最多**总共尝试 3 次**（首次 + 2 次重试），每次超时 5 秒，失败重试间隔为 250ms、500ms；任意 2xx 算成功。失败不会恢复 `active`，进程退出也没有持久化投递任务自动恢复。

| 占位符 | 替换内容 |
| --- | --- |
| `{{ticker}}` / `{{symbol}}` | 交易对 |
| `{{exchange}}` | 固定 `BINANCE` |
| `{{interval}}` | 警报所属周期 |
| `{{price}}` / `{{close}}` | 本次触发时观察到的价格，可能与警戒价不同 |
| `{{alertId}}` | 警报 ID |
| `{{time}}` | 本次触发时间，Unix 毫秒 |

Webhook 没有额外的固定外层包装或自动签名，正文完全由模板决定。重试可能重复投递同一事件，接收方可在模板中加入 `alertId` 和 `time` 作为去重依据。

### 查询、修改与删除

| 请求 | 成功响应 |
| --- | --- |
| `GET /api/alerts` | 200，Alert 数组，按 ID 降序；没有过滤或分页参数 |
| `GET /api/alerts/{id}` | 200，单个 Alert |
| `PATCH /api/alerts/{id}` | 200，更新后的 Alert；只传需要修改的创建字段 |
| `DELETE /api/alerts/{id}` | 204，同时删除该警报的所有事件 |
| `GET /api/alerts/{id}/events` | 200，事件数组，按事件 ID 降序；无事件为 `[]` |

PATCH 示例：`{"status":"disabled"}` 禁用；`{"status":"active"}` 重新启用，允许下次穿越再次触发。PATCH 不能主动设置 `triggered`。如果原警报已过期，还需将 `expiresAt` 更新为未来毫秒时间。

当前修改接口有三个实现限制，调用方需据此处理：

1. PATCH `expiresAt:null` 目前会被当作未提供，不能清除已有到期时间；可改成未来时间，或删除并新建无到期警报（新建会更换 ID，删除会清除历史事件）。
2. 重新启用时，PATCH 响应中的 `triggeredAt/deliveryStatus/deliveryError` 会清空，但数据库更新未持久化这些清空值，随后 GET 仍可能返回旧值；使用事件接口区分历史触发记录。
3. 修改价格/交易对或重新启用不会重置进程内该 ID 的穿越基线，不能保证首次检查一定按新配置重新建立基线。

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
| 400 | 缺少参数、非法查询类型、`missing intervals`、`missing symbols`、`empty interval in intervals`、`invalid interval: ...`、`unsupported maType: ...`、警报字段校验失败 |
| 404 | 警报不存在：`alert not found`；或请求路径不存在 |
| 409 | 警报价位重复：`an alert already exists at this price line` |
| 415 / 422 | JSON 请求 Content-Type 不正确，或 JSON 字段类型/必填字段不匹配等框架提取错误；JSON 语法错误也可能为 400 |
| 500 | 数据库操作失败等内部错误 |

非法列表元素或数据库失败会使整个请求失败，不返回部分成功的多周期结果。合法但未配置的组合仍返回 200 空序列；健康状态为 `false` 也不能仅靠状态码识别。
