# crypto-candlestick

一个基于 Rust/Axum 的 Binance U 本位合约行情后端，供图表、策略程序、监控程序等通过 HTTP 调用。服务负责采集和聚合 K 线、计算 guaili 指标与多周期动态信号、发送企业微信信号警报、监测价格穿越并发送 Webhook，以及报告行情健康状态。

服务从 Binance WebSocket 接收实时行情，通过 REST 补齐分钟及以上历史，并使用 SQLite 和内存缓存提供查询。多周期信号使用当前动态 K，默认每 5 秒计算一次，查询读取已有内存结果。当前没有面向调用方的行情 WebSocket/SSE 推送或交易下单；信号表达指标状态，供观察。

## 文档导航

| 文档 | 读者与内容 |
| --- | --- |
| 本 README | 服务能力、接入示例、配置、启动和运维 |
| [HTTP API 接入契约](docs/api.md) | 调用方：全部路由、参数、返回字段、时间、错误和警报规则 |
| [guaili 算法与业务语义](docs/guaili.md) | 指标使用者：公式、正负值/零值、趋势、过滤、预热和多周期使用边界 |
| [多级别信号验证记录](docs/guaili-multi-interval-signal-validation.md) | 历史研究：特定样本的观察，不能替代当前动态采样规则及验证 |
| [AGENTS.md](AGENTS.md) | 代码维护助手：修改接口/算法时同步文档、验证行为与兼容性 |

接口和业务含义在 README/docs 中维护。`AGENTS.md` 专门保存代码助手的仓库工作约定，采用该标准文件名；相关机制见 [OpenAI 官方说明](https://learn.chatgpt.com/docs/agent-configuration/agents-md)。

## 功能特性

- 订阅 Binance Futures 实时 kline 与 aggTrade 数据。
- 每个交易对可独立配置周期和实时数据源，或按最小周期自动选择。
- 支持原生 Binance 周期，也支持由基础周期聚合出的自定义周期。
- 分钟级、日线、周线 K 线写入 SQLite；秒级 K 线保存在内存中。
- 查询可合并当前未收盘 K 线；`closedOnly=true` 用于只读取已收盘数据。
- 按请求计算 guaili、波动排名与趋势状态；提供独立的一次性价格穿越警报。
- 常驻多周期动态信号引擎：识别上方/下方乖离共振、多周期近均线、长短周期分歧，提供内存查询接口。
- 独立 `signals.toml` 配置信号开关、5 秒采样、品种和多个企业微信警报；支持详细/简要消息、原子重载、订阅筛选、去重、冷却与有界重试。
- 多周期查询仅公开已配置组合，遇到历史缺口只返回最新连续片段。
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

在项目根目录执行（PowerShell / Linux / macOS）：

```sh
cargo run
```

仓库当前配置监听 `127.0.0.1:3005`；本文调用示例使用该地址，部署时以 `server.bind_addr` 为准。

程序固定读取当前工作目录下的 `config.toml`；文件缺失、字段拼写错误或配置值无效时会拒绝启动。另读取独立的 `signals.toml`：缺失时信号功能关闭，初始配置错误时信号状态为 `config_error`，行情服务继续运行。可复制 `signals.example.toml` 建立本地配置；示例启用计算，未配置企业微信发送。首次启动时，如果 `binance.sync_on_start=true`，服务会先从 Binance REST 拉取一段历史 K 线，再连接 WebSocket 进入实时更新。

## 实时性与故障处理

服务通过 Binance combined WebSocket stream 接收实时数据。连接成功后会记录 WebSocket 状态；收到行情文本消息、Ping 或 Pong 时会刷新最近消息时间。

每个交易对支持三种实时数据源：

| 数据源 | 行为 |
| --- | --- |
| `auto` | 最小周期至少为 1 分钟时选择 `kline_1m`；包含秒级周期时选择 `trade` |
| `trade` | 仅订阅一个 `aggTrade` 流，由逐笔成交生成该交易对的全部配置周期 |
| `kline_1m` | 仅订阅一个 1m K 线流，以最新动态分钟更新预览、已收盘分钟更新历史，生成全部配置周期 |

`kline_1m` 模式随币安未收盘 1m K 消息更新各级别动态 K；同一分钟的累计 OHLCV 替换预览，不重复累计。已收盘分钟单独进入历史聚合，原有价格穿越警报仍只检查 1m 收盘价。显式使用 `kline_1m` 时不能配置秒级周期。

断线或读取失败时，后台 worker 会写入 warn 日志，更新 `/api/health/deep` 中的 WebSocket 状态，并按 `1s, 2s, 4s ... 30s` 的退避节奏重连。重连成功后会补齐缺失的分钟级及以上 K 线、重建聚合状态，再使用同一个 stream URL 继续订阅。程序启动时还会通过 REST 强制刷新每种基础周期最近 2 根已收盘 K 和当前 K，再从数据库恢复正在形成的聚合 K；币安原生 3D 周期会从各交易对的历史 K 推断其独立分桶相位。这些恢复步骤不会增加稳态 WebSocket 数据流。

如果连接没有显式断开，但 60 秒没有收到任何 WebSocket 消息，worker 会判定为空闲超时，主动断开当前读取循环并重连。在仍标记连接但消息已陈旧时，`reason` 为 `websocket message stream is stale`；断开后为 `websocket is disconnected`，两者的 `websocket.ok` 都为 `false`。

深度健康检查还会检查每个交易对/周期的最新 K 线是否落后：分钟级及以上周期允许最多落后 2 根，秒级周期允许最多落后 4 根。超过阈值时，对应序列的 `ok=false`，`reason` 为 `latest candle is stale`，整体 `ok` 也会变为 `false`。

## 配置项

行情配置来自 `config.toml`，信号和企业微信订阅来自 `signals.toml`，不读取 `.env` 或业务环境变量。行情配置字段如下：

| 字段 | 说明 |
| --- | --- |
| `server.bind_addr` | HTTP 服务监听地址 |
| `database.url` | SQLite 数据库地址 |
| `database.retention_bars` | 每个交易对/周期的历史保留数量；SQLite 中 `0` 表示不裁剪，但当前秒级内存会按 0 条保留，使用秒级时需设为正数 |
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
bind_addr = "127.0.0.1:3005"

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
symbol = "XAUUSDT"
intervals = ["15", "60", "240"]
source = "auto"

[realtime]
flush_interval_secs = 300
flush_max_rows = 1000

[logging]
dir = "logs"
level = "info"
```

## 动态信号与企业微信配置

`signals.toml` 与行情配置分开维护，可参考 [signals.example.toml](signals.example.toml)。只有一个总开关 `enabled`，关闭后同时停止信号计算及关联企业微信警报，查询返回 `disabled`、空结果；行情采集及原有价格警报继续运行。

`GET /api/signals` 返回当前实际生效的 `indicatorConfig`、`ruleConfig` 和 `qualityConfig`，包括规则整数阈值、最少级数、连续历史要求以及毫秒质量时限。逐周期证据返回可为空的 `reasonCode`，用于稳定识别预热、过期、缺口、恢复等原因，并提供 `reason` 文案。客户端应根据实际时限判断有效性，并处理同一 `snapshotVersion` 下质量失效及结构清除，不能只用采样版本去重。详见 [信号 HTTP 契约](docs/api.md#查询当前结果)。

| 配置 | 默认 / 语义 |
| --- | --- |
| `enabled` | 缺省为 `false`；示例配置为 `true` |
| `evaluation_interval_secs` | `5`；定时采样当前动态 K，行情聚合持续更新 |
| `symbols` | 空列表使用行情配置的全部品种；周期沿用品种的完整配置集合 |
| `indicator.*` | EMA20、最多 500 根输入（最多 499 根历史加当前动态 K），与现有 guaili 公式一致 |
| `rules.extreme_threshold` / `compression_band` | `10` / `2`，单位为界面整数 `value` |
| `rules.minimum_levels` / `min_history_bars` | `5` 个相邻周期 / 至少 `60` 根连续已收盘历史 |
| `quality.max_market_age_secs` | `30`；行情事件和接收时间均须及时 |
| `quality.max_result_age_secs` | `15`；超时结果不再作为当前信号，发送队列中过时任务丢弃 |
| `wecom_alerts[]` | 每段配置一个订阅；包含唯一 `id`、名称、Webhook、品种、最小信号级别、信号类型和冷却 |
| `wecom_alerts[].message_format` | 缺省为 `"detailed"`；`"compact"` 使用简要单行消息，每条订阅可分别设置 |

信号引擎支持 `trade` 和 `kline_1m` 两种数据源的每品种一致动态快照。`auto` 仍按最小配置周期选择来源，无需为了信号切换成逐笔成交。分钟 K 来源使用连续已闭合日线/分钟前缀加最新动态分钟构造各周期；首连、断线或缺少最终闭合分钟时补齐闭合历史并重新建立基线。

企业微信配置示例（将占位 Webhook 替换为实际机器人地址后使用）：

```toml
[[wecom_alerts]]
id = "btc-observation"
name = "BTC观察"
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=REPLACE_WITH_KEY"
symbols = ["BTCUSDT"]
min_signal_interval = "15"
kinds = ["extreme", "compression", "conflict"]
cooldown_secs = 300
message_format = "compact" # compact：简要单行；detailed：详细（默认）
```

`message_format` 只接受小写 `"compact"`、`"detailed"`，其他值或类型会使配置校验失败。详细模式保留订阅名称、完整周期列表、最大级别、覆盖级数、完整北京时间和观察说明。简要模式不主动换行，省去名称和说明，示例：

```text
🔴⬆️ BTCUSDT 上方乖离共振｜⏱️10s–2m·6级｜15:58:05
🟢⬇️ BTCUSDT 下方乖离共振｜⏱️2m–10m·5级｜21:12:04
🟡 BTCUSDT 多周期近均线｜⏱️1m–15m·5级｜15:58:05
🔀 BTCUSDT 长短周期分歧·短正长负｜⏱️10s–2m / 5m–30m｜15:58:05
```

简要消息以 emoji 区分信号：`🔴⬆️` 上方共振、`🟢⬇️` 下方共振、`🟡` 近均线、`🔀` 分歧；`⏱️` 标记周期范围。`5级` 表示覆盖 5 个周期，范围右端表示该段最大周期，颜色和覆盖数不表示买卖指令或信号强度。详细模式格式保持原样。

简要范围表示实际参与周期的首尾，不代表包含范围内全部标准周期；完整列表通过 `/api/signals` 查询。单位 `s/m/d/w` 分别表示秒/分钟/日/周；单个周期不显示范围。共振和近均线显示覆盖级数，分歧保留短、长两段范围及各段方向。时间为动态 K 的北京时间采样时刻（UTC+8），简要模式只显示时分秒；客户端可能按屏幕宽度自动折行。两种格式都只描述指标状态，供观察。

`min_signal_interval` 筛选候选结构的**最大周期**，不改变算法输入。例如 `3–15` 分钟共振满足最低 `15`；`1–8` 分钟不满足。分歧按长周期一端的最大周期判断。一条订阅一个 Webhook；需要多个群时增加配置段。同一事件命中多个订阅但 Webhook 相同，只发一次；消息格式和名称采用文件中首条实际生成发送任务的订阅；不同 Webhook 分别发送。

进程首次观察及断流恢复后建立基线，已有信号不会立即发送；随后首次符合品种/类型/最小级别才发。持续、扩大且已经符合的同轮信号不重复发送。冷却默认 300 秒，按接收目标、品种和信号方向/类型共同计算；冷却内被抑制的匹配不延后补发，需要以后退出再重新符合。未知数据不会当作信号结束，其他有效区间仍能形成警报。信号和最近 32 条投递状态只存在内存，不新增 SQLite 表，重启不恢复或补发旧任务。

秒级历史启动或断流恢复后需要重新积累；默认 60 根门槛下，`10S` 周期约需 10 分钟连续收盘数据，`45S` 约需 45 分钟。大级别历史不足只影响对应区间，其他有效周期可正常产生信号。5 秒采样可能遗漏两个采样时刻之间出现又消失的短暂结构。

修改文件后执行重载，配置校验失败则保留旧配置。只改 `message_format` 不重算指标或清空结果，但会按订阅变化重建通知基线，旧配置的待发任务作废，已有信号不会补发：

```powershell
Invoke-RestMethod -Method Post "http://127.0.0.1:3005/api/signals/reload"
Invoke-RestMethod "http://127.0.0.1:3005/api/signals?symbols=BTCUSDT,XAUUSDT"
```

接口字段及数据质量详见 [动态信号接口](docs/api.md#动态信号与企业微信警报)。Webhook 不通过该查询接口或配置错误回显。实际配置文件若加入真实凭据，应只保存在服务器，避免把凭据提交到版本库。

## 日志

服务默认同时输出控制台日志和按天滚动的文件日志。文件写入 `logging.dir` 指定的目录，文件名形如 `crypto-candlestick.log.YYYY-MM-DD`。通过 `logging.level` 控制日志级别；日常使用建议设为 `info`，排查问题时可临时改为 `debug`。

## HTTP 接口

完整参数、JSON 示例和错误响应见 [HTTP API 接入契约](docs/api.md)。

| 方法 | 路径 | 提供的能力 |
| --- | --- | --- |
| GET | `/api/health` | HTTP 存活检查，返回 `{"ok":true}` |
| GET | `/api/health/summary` | 汇总 WebSocket 与配置序列的健康情况 |
| GET | `/api/health/deep` | 各交易对/周期的最新数据、滞后与连续尾段详情 |
| GET | `/api/klines` | 一个交易对、多个周期的 K 线 |
| GET | `/api/indicators/guaili` | 多个交易对、多个周期的乖离、趋势和波动过滤状态 |
| GET | `/api/charts/guaili` | 单次冻结输入的 OHLC、Android 通道/波幅与矩阵指标；见 API 契约 |
| GET | `/api/signals` | 读取最近动态信号采样、全部结构和逐周期数据质量 |
| POST | `/api/signals/reload` | 校验并原子重载独立信号配置 |
| POST / GET | `/api/alerts` | 创建一次性价格警报 / 列出警报 |
| GET / PATCH / DELETE | `/api/alerts/{id}` | 查询 / 修改或启停 / 删除警报 |
| GET | `/api/alerts/{id}/events` | 查询触发价格、实际穿越方向及 Webhook 投递结果 |

健康接口另有 `/health`、`/health/summary`、`/health/deep` 等价别名。当前没有认证或 CORS 中间件，浏览器跨域接入需由部署层配置。

### 调用示例

查询 K 线时参数为单数 `symbol`；查询指标时为复数 `symbols`；周期一律使用复数 `intervals`，分钟直接写数字。以下交易对/周期应先在 `config.toml` 中启用。

PowerShell：

```powershell
# 获取图表数据，允许最后一根为动态 K 线
Invoke-RestMethod "http://127.0.0.1:3005/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=200"

# 获取各周期最新已收盘指标，使用最多 500 根历史计算
Invoke-RestMethod "http://127.0.0.1:3005/api/indicators/guaili?symbols=BTCUSDT,XAUUSDT&intervals=1,5,15&limit=1&calcLimit=500&closedOnly=true"

# 获取服务器每 5 秒计算的动态多周期信号，查询不会重新计算
Invoke-RestMethod "http://127.0.0.1:3005/api/signals?symbols=BTCUSDT,XAUUSDT"

# 检查行情是否及时，需读取响应中的 ok
Invoke-RestMethod "http://127.0.0.1:3005/api/health/summary"
```

Linux/macOS：

```bash
curl "http://127.0.0.1:3005/api/klines?symbol=BTCUSDT&intervals=1,5,15&limit=200"
curl "http://127.0.0.1:3005/api/indicators/guaili?symbols=BTCUSDT,XAUUSDT&intervals=1,5,15&limit=1&calcLimit=500&closedOnly=true"
curl "http://127.0.0.1:3005/api/health/summary"
```

### 调用方需要理解的数据语义

- K 线响应为 `series[]`，指标响应为 `results[].series[]`，都按请求顺序组织周期，序列内 `data` 按开盘时间升序。指标 `latest` 等于该序列 `data` 的最后一项。
- `limit` 是每个周期的输出上限，指标的 `calcLimit` 是计算历史上限。`limit=1` 不等于只用一根 K 线计算；实际历史可能仍不足预热。
- K 线/单周期指标未配置的组合返回 200 空序列，不自动增加订阅；动态信号查询未启用的品种返回 400。历史缺口会裁剪掉缺口以前的数据，因此返回数量可能小于 `limit`。
- `startTime/endTime` 是包含边界的开盘时间过滤条件，输入 Unix 毫秒。输出 K 线时间为带偏移的 RFC 3339 字符串；`timezone` 标签与主机时区的当前限制见 [时间格式](docs/api.md#时间格式)。
- 默认 `closedOnly=false`，当前 K 线与指标会变动；按收盘确认的业务使用 `closedOnly=true`。历史回放还需按当时的 `closeTime` 对齐，不能只看现在是否已收盘。
- 健康异常通常仍返回 HTTP 200。`/api/health` 只表示进程可响应；摘要/深度检查通过也不证明全部历史连续或指标预热充分。

### guaili 信号的业务含义

默认使用收盘价 EMA20。整根 K 线位于均线上方时，`guaili=(low-MA)/前一根ATR14`；整根位于下方时，`guaili=(high-MA)/前一根ATR14`；有效 K 线触碰或跨越均线时为 0。历史预热不足、分母无效或实时行情失效时公开指标为 `null`，状态和原因由后端返回。`value` 是 `guaili*10` 向零截断后的整数，**不是百分比乖离率**。

例如均线 100、前一根 ATR14 为 10、当前 low 为 112，则 `guaili=1.2`、`value=12`。`value=0` 也可能是微小非零乖离被截断，不能直接等同于“K 线穿过均线”或“低波动”。

`longTrend/shortTrend` 表示均线连续三次上升/下降，并通过可选的斜率过滤；`rankFilter` 表示归一化波动的排名不超过阈值。三个维度独立输出，`rankFilter=false` 不会让乖离值归零。正负乖离和趋势标记都不是后端生成的买卖指令。

全部 MA/ATR 公式、排名规则、初始化、多周期对齐和边界例子见 [guaili 算法与业务语义](docs/guaili.md)。

### 价格警报的业务含义

警报在指定交易对价格严格穿过固定价位时一次性触发，支持 `cross_up/cross_down/cross_any`。它不依赖 guaili；`trade` 模式按 aggTrade 价格检查，`kline_1m` 模式按 1m 收盘价检查。警报所属 `interval` 不改变检查频率。

触发要求相邻两次检查都位于阈值两侧，首次观察和价格等于阈值均不触发。到期后停止检查，但没有单独的 `expired` 状态。触发后先记录事件，再异步向模板指定的 Webhook 投递 JSON，总共最多尝试 3 次；投递失败也保持 `triggered`。

创建请求、模板变量、响应字段、重新启用与当前 PATCH 限制见 [价格警报接口](docs/api.md#价格警报与-webhook)。

## 数据存储

K 线存储在 SQLite 的 `klines` 表，主键为 `(symbol, interval, open_time)`；价格警报与触发事件分别存于 `alerts`、`alert_events`。服务会在启动时自动创建表，并启用 WAL。分钟级及以上周期会持久化到 SQLite；`10S`、`15S`、`30S`、`45S` 等秒级周期来自 aggTrade 聚合，当前保存在内存中，适合实时展示但不会跨进程保留。

实时已收盘 K 线先进入内存缓冲，再按时间/数量批量写库；查询可立即读到缓冲数据，但进程异常退出时未刷盘部分不具备持久化保证。

数据库文件、WAL 文件和旧的 `.env` 文件已在 `.gitignore` 中忽略；程序读取 `config.toml` 和独立的 `signals.toml`。多周期信号及其企业微信投递状态只在内存保存，不写入现有价格警报表。

## 开发与测试

```sh
cargo test
cargo fmt --check
cargo clippy
```

代码格式化使用 `cargo fmt`。接口与算法相关测试可单独运行：

```sh
cargo test --test http_tests --test guaili_tests
cargo test --test signal_config_tests --test signal_delivery_tests --test signal_detector_tests --test signal_service_tests
```

维护约定见 [AGENTS.md](AGENTS.md)。修改接口或算法时，同时更新 API 契约、算法说明及相关示例。

## 项目结构

```text
src/
  binance/          Binance REST/WebSocket 解析、订阅、恢复和警报触发
  domain/           K 线和周期类型
  engine/           K 线聚合器
  http/             Axum 路由、参数与响应
  indicators/       guaili 指标计算
  signals/          动态多周期检测、独立配置、采样缓存与企业微信投递
  storage/          SQLite K 线、警报与事件存储
  memory.rs         动态 K 线、收盘缓冲和秒级序列
  runtime_health.rs 上游连接健康状态
  time_format.rs    API 时间字符串格式化
  config.rs         配置加载与校验
  logging.rs        日志初始化
  main.rs           程序启动入口
tests/              集成测试和模块行为测试
docs/               接口契约、算法说明及历史研究/设计记录
AGENTS.md           代码维护助手的仓库约定
```
