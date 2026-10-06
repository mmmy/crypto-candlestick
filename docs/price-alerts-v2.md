# 绘图绑定价格警报 v2

桌面端使用 `/api/price-alerts`，Android 原有 `/api/alerts` 继续兼容。存储初始化仅增加新表，不重建 K 线或旧警报表。不修改现有警报 JSON 正文；v1 `{{ticker}}` 保持原交易对，v2 `{{ticker}}` 使用 `tvSymbol`，本服务 Binance USDM 默认 `BTCUSDT.P`。

## HTTP 契约

时间均为 UTC Unix 毫秒数字。成功结果直接返回对象/数组，无外层包装。错误为 HTTP 400/404/409/503/500 和文本说明。

| 请求 | 行为 |
| --- | --- |
| GET `/api/price-alerts?symbol=BTCUSDT&interval=60` | 按 ID 降序；两个过滤参数都可省略 |
| POST `/api/price-alerts` | 201 创建绘图，`mutationId` 必填；默认 `disabled` |
| GET `/api/price-alerts/{id}` | 200 当前绘图配置与状态 |
| PATCH `/api/price-alerts/{id}` | 200 原子修改，必填 `mutationId`、`expectedRevision` |
| DELETE `/api/price-alerts/{id}?mutationId=...&expectedRevision=...` | 200 `{deleted:true,id,revision}`；保留历史事件、取消未确认投递 |
| GET `/api/price-alerts/{id}/events?limit=200&beforeArmGeneration=3` | 按布防代次降序；默认 1000，上限 1000；游标可省略，删除后仍可查询 |
| GET `/api/price-alerts/market?symbol=BTCUSDT` | 真实 Binance PRICE_FILTER 最小价位及可用状态 |
| POST `/api/price-alerts/market/refresh` | 202 `{refreshQueued:true}`，通知后台重新读取元数据 |
| GET `/api/price-alerts/mutations/{mutationId}` | 200 返回该操作已持久保存的原始结果；未提交为 404 |

`mutationId` 为 1..128 个 ASCII 字母、数字、`-`、`_`、`:`。同 ID、同操作和同请求重试返回原结果；跨对象/方法或改正文复用同 ID 返回 409。版本不匹配返回 409，客户端应重新读取再决定编辑。绘图创建、配置编辑和实际触发均递增 `revision`；投递回执更新独立的摘要字段，不递增配置版本。`armGeneration` 仅在重新布防时递增，用于隔离旧触发回执。

```json
{
  "mutationId": "desktop-create-unique-uuid",
  "symbol": "BTCUSDT",
  "interval": "60",
  "tvSymbol": "BTCUSDT.P",
  "name": "穿过多",
  "geometry": {
    "kind": "horizontal_segment",
    "first": { "timeMs": 1791200000000, "price": 65000 },
    "second": { "timeMs": 1791286400000, "price": 65000 },
    "extend": "right"
  },
  "direction": "cross_any",
  "frequency": "once",
  "status": "active",
  "expiresAt": null,
  "webhookUrl": "https://receiver.example/api/web-hook/signal",
  "messageTemplate": "{\"des\":\"{{interval}}底部合约多{{ticker}}下穿{{close}}\",\"name\":\"PRE-LONG\",\"side\":\"BUY\",\"exchange\":\"BINANCE\",\"symbol\":\"{{ticker}}\",\"price\":\"{{close}}\",\"period\":\"{{interval}}\",\"noConfirm\":false}",
  "label": "做多",
  "color": "#22AB94",
  "lineWidth": 2
}
```

创建必须提供 `symbol/interval/geometry`，品种和周期必须是公开配置的订阅。其他默认：`name/label/webhookUrl/messageTemplate` 空字符串，`direction=cross_any`，`frequency=once`，`status=disabled`，`expiresAt=null`，`color=#22AB94`，`lineWidth=2`，`tvSymbol=SYMBOL.P`。绘图未启用时允许空 URL/模板；启用需要合法 HTTP(S) URL 和 JSON 对象模板。

响应包含上述配置字段（不含 `mutationId`），加上：`id/revision/armGeneration` 数字、`triggeredAt` 数字或 null、`deliveryStatus/deliveryError` 字符串或 null、`dataStatus`、`createdAt/updatedAt` 数字。

`status` 为 `active/disabled/triggered/expired`。`dataStatus` 为 `waiting_for_metadata/unavailable/waiting_for_range/range_ended/invalid_line/waiting_for_price/live/disconnected`。元数据未加载或不可用时暂停 v2 监测；拿到真实最小价位后第一次行情只建立新基线。时间范围未开始或已结束分别显示 waiting_for_range/range_ended，外推线价非正或超出整数序号范围显示 invalid_line。ready 状态不表示逐笔流质量；超过 60 秒未观察有效价格显示断流。

## 最小价位与元数据恢复

市场响应为 `{symbol,tvSymbol,tickSize,status,reason}`，tickSize 是 PRICE_FILTER 原始十进制字符串或 null，status 为 ready/loading/unavailable，reason 为字符串或 null。

后台使用与投递共享的 HTTP 连接池读取 `https://fapi.binance.com/fapi/v1/exchangeInfo`，只使用 `filters[filterType=PRICE_FILTER].tickSize`，不使用 pricePrecision 推断最小价位。启动读取一次，成功每小时刷新，失败每分钟重试；单请求 10 秒超时、响应最多 8 MiB，可随关机取消。GET 市场状态只读内存，POST refresh 仅通知单个后台任务，连续通知合并。

创建/启用/移动活跃 v2 警报必须有 ready 的真实元数据，否则 HTTP 503；暂停/过期绘图可保存，元数据缺失时不监测。不能用小数位、固定 0.01 或上次过期缓存编造备用步长。元数据失效后基线清空；恢复时重新建立首个价格基线，不把恢复前后的价格差当成穿越。元数据使已保存活跃坐标产生量化调整时，坐标、版本与布防代次同步持久更新。

官方定义：[USD-M PRICE_FILTER](https://developers.binance.com/zh-CN/docs/products/derivatives-trading-usds-futures/common-definition)、[exchangeInfo](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)。

## 几何、拖动和触发

`kind=horizontal_segment/trend_segment`；`extend=none/right/both`。端点时间必须不同且非负，价格为正且有限；水平线两端严格同价。服务器按时间升序规范化端点。`none` 在两个端点时间之间（含端点）生效；`right` 从左端点到未来；`both` 双向延伸。创建/修改时，端点价格按真实 tickSize 对齐到最近最小价位，正向半价位向上对齐，并把规范坐标返回客户端。趋势线使用端点的整数价位与时间差计算，不使用屏幕像素或自动缩放坐标。整数最小价位序号必须在 `1..=2^53`；外推结果超出正数范围或上界时暂停该样本判定。

每个样本使用自己时刻的线价计算侧别。从下方达到/越过触发上穿，从上方达到/越过触发下穿；`cross_any` 接受两者。第一次观察、有限线段首次进入范围、重启、行情恢复、重设后的第一次观察只建立基线。首次价格正好在线上也不触发。等价连续样本不会反复触发；事件记录实际发生方向。仅一次触发后变成 `triggered`。

PATCH 可以传创建的可编辑字段和可选 `rearm:true`。`expiresAt:null` 真正清除到期；省略不修改。`status` 可写 `active/disabled`，禁止直接设置触发状态。

- 修改坐标、品种、周期、方向后，`active/triggered` 自动重新布防，清空触发/投递摘要并建立新基线。
- 手动 `disabled` 的绘图移动后保持暂停；显式 `status:active` 或 `rearm:true` 可启用。
- 过期后可移动但保持过期；先把到期改为未来或 null 才能重新布防。清除/修复到期后自动重新布防，手动暂停例外。
- 名称、文字、颜色、线宽、模板和通知 URL 编辑不会重设价格基线；更改同一请求里的几何时仍按移动重设。
- 同一请求的 `status:disabled` 优先于几何变化，保证暂停动作不会被自动布防覆盖。

拖动预览不发送请求。松手仅提交一次 `{mutationId,expectedRevision,geometry}`，收到成功才标识新位置已监测。网络超时必须使用原 mutationId 和原请求重试/查询操作恢复；不能因超时认定服务器未修改。

## 消息和投递

模板为 JSON 对象。字符串值内支持 `{{ticker}}/{{symbol}}/{{exchange}}/{{interval}}/{{price}}/{{close}}/{{alertId}}/{{time}}/{{eventId}}/{{linePrice}}/{{direction}}`，未知或不完整占位符返回 400。在解析后的 JSON 字符串值中替换，保留数字、布尔、数组和嵌套对象的原始类型，替换值按 JSON 正确转义。字符串占位符仍是字符串：`"price":"{{close}}"` 最终为 `"price":"65001"`。

`ticker` 为 tvSymbol，`symbol` 为行情品种；`interval` 为创建时绑定的规范周期，切换桌面图表不修改它；`close/price` 为触发实时样本价；`linePrice` 为对应时刻线价。模板中的 BUY/SELL、PRE-LONG 等固定文本不会因实际上穿/下穿而自动改变。

事件：`id` 全局字符串（数据库持久随机命名空间＋绘图 ID＋布防代次）、`alertId/armGeneration/triggeredAt` 数字、`triggerPrice/linePrice` 数字、`direction`、`payload` 实际 JSON 正文、`deliveryStatus` (`pending/success/failed/cancelled`) 和 `deliveryError` 字符串/null。

状态、事件、outbox 在同一事务提交；数据库暂时失败时保留该样本并阻塞市场处理重试，不能悄悄丢弃穿越。规则按品种缓存，水平价格和时间边界有序索引；正常水平行情不查 SQLite，候选暂存缓冲复用。仅处于有效时间范围内的趋势线按样本计算线价。未开始或结束的水平线与趋势线从价格／趋势候选索引移除，由既有开始、结束、到期时间索引安排；进入范围时建立首个基线，跨过整个范围也不补发穿越。无效报价不会消耗时间边界的有效报价游标。

共享连接池、最多 8 个并发、每次 5 秒超时，最多总共 3 次尝试。请求头 `Content-Type:application/json`、`X-Guaili-Event-Id`、`X-Guaili-Event-Time`；无额外包装。接收方按事件 ID 持久去重，以免超时重试重复执行。发送尝试次数在请求前写入；进程重启恢复待任务，超过 30 天停止。取消服务时丢弃在途 HTTP future，任务保持待确认。删除绘图取消待处理任务，已经到达接收端的请求无法撤回。2xx 表示接收成功，不代表订单成功。

当前服务仅 Binance USDM：`trade` 为逐 aggTrade；`kline_1m` 为 Binance 实时 K 更新之间的采样，仍可能遗漏更新间穿越又回撤，逐成交需求应明确配置 trade。v1 保留原浮点 API 行为；v2 行情入站沿用现有 f64 接口，在有限/正数/序号上界校验后转换为整数最小价位，水平索引使用整数，趋势线侧别使用 checked i128 交叉乘积，保留小数最小价位之间的真实线价，不把趋势线先取整。事件的 triggerPrice 保留实际样本值，linePrice 仅作展示，浮点展示误差不参与触发判断。

## 验证

`cargo test --test price_alert_tests` 覆盖触碰、方向、一次触发、移动重设、样式保留基线、暂停与过期、显式 null、趋势线、时间范围、重连、乱序、CAS/幂等恢复、HTTP、隔离数据库重启、旧代回执以及 8 并发/取消。测试仅使用 localhost 隔离接收端。

Release 同场景对比：`cargo test --release --test price_alert_tests release_index_benchmark -- --ignored --nocapture`。固定 1000 条水平规则、10000 次无穿越样本，对比内存索引与旧逐 tick SQLite 查询/解码/侧别更新；不包含网络、触发写入和磁盘 I/O，数值只适用于所测机器与场景。
