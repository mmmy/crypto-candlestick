# guaili 算法与业务语义

本文对应 [`src/indicators/guaili.rs`](../src/indicators/guaili.rs) 的当前实现。HTTP 参数与响应见 [API 接入契约](api.md)。每个交易对、每个周期各自独立计算，不混用不同周期的 MA 或 ATR。

## 指标在表达什么

`guaili` 衡量的是**整根 K 线离均线还有多远**，距离除以前一根 ATR14，使不同价格量级的序列可按各自波动尺度观察。它不是常见的 `(close - MA) / MA × 100%` 百分比乖离率。

| 输出 | 含义 | 不能直接推断的结论 |
| --- | --- | --- |
| `guaili > 0` | 整根 K 线在当前均线上方，值越大，最近边缘距均线越远 | 不自动表示买入或即将下跌 |
| `guaili < 0` | 整根 K 线在当前均线下方 | 不自动表示卖出或即将上涨 |
| `guaili = 0` | 高低区间触碰/跨越均线，或计算分母为零 | 不等于收盘价等于均线，也不保证低波动 |
| `value` | `guaili × 10` 的整数显示值 | 不是百分比，也不保留全部精度 |
| `longTrend/shortTrend` | 均线连续同向变化且通过可选强度检查 | 是持续状态，不是一次性的进场事件 |
| `rankFilter` | 相对波动排名是否低于或等于阈值 | 不是自动应用到所有输出上的总开关 |

这里返回指标与过滤状态，未实现交易执行、多周期共振评分或自动发出买卖信号。价格穿越警报是 [独立功能](api.md#价格警报与-webhook)，不会由 guaili 自动创建。

## 1. 收盘价均线 MA

记当前 K 线为 `t`，收盘价为 `C[t]`，长度为 `L=maLength`，默认 `L=20`、`maType=EMA`。所有均线均包含当前 K 线的收盘价。

| 类型 | 当前实现 |
| --- | --- |
| SMA | 最近最多 L 个收盘价的算术平均 |
| EMA | `alpha=2/(L+1)`；`MA[t]=alpha*C[t]+(1-alpha)*MA[t-1]` |
| SMMA / RMA | `MA[t]=(MA[t-1]*(L-1)+C[t])/L` |
| WMA | 最近最多 L 个收盘价按从旧到新 `1,2,...,窗口实际长度` 加权平均 |
| VWMA | 最近最多 L 根的 `sum(close*volume)/sum(volume)`；总量为 0 时退回该窗口的算术平均 |

**初始化：** EMA/RMA 都以本次计算输入的第一个值作为种子；SMA/WMA/VWMA 在历史少于 L 根时使用已有数据，不等待完整窗口。因此第一根就能返回 `ma`，并不代表均线已经预热稳定。

## 2. TR 与两种用途的 ATR

第一根的 `TR=high-low`，后续为：

```text
TR[t] = max(high[t]-low[t], abs(high[t]-close[t-1]), abs(low[t]-close[t-1]))
ATR(n)[0] = TR[0]
ATR(n)[t] = (ATR(n)[t-1]*(n-1) + TR[t]) / n
```

- **固定 ATR14**：`ATR(14)`，用于归一化乖离和衡量趋势强度。响应的 `atr14` 是当前根数值。
- **排名用 ATR**：`ATR(atrLen)`，默认 `atrLen=1`，此时就是当前 TR。它只影响 `atrRank/rankFilter`，不改变 guaili 的 ATR14 分母。

RMA 从首值递推的初始化方式不等同于先等待 n 根、以 SMA 为种子的实现。与其他图表平台比对时，必须统一输入 K 线、时间桶、历史起点和初始化方法，不能仅凭指标名称保证完全一致。

## 3. 乖离 guaili 和整数 value

设 `Aprev=ATR14[t-1]`；输入第一根没有前值时，以当前 `ATR14[0]` 代替。

```text
如果 Aprev == 0：
    guaili[t] = 0
否则，如果 low[t] > MA[t] 且 high[t] > MA[t]：
    guaili[t] = (low[t] - MA[t]) / Aprev
否则，如果 high[t] < MA[t] 且 low[t] < MA[t]：
    guaili[t] = (high[t] - MA[t]) / Aprev
否则：
    guaili[t] = 0

value[t] = trunc_toward_zero(guaili[t] * 10)
```

使用靠近均线的 K 线边缘：上方取 `low`，下方取 `high`；高低点等于均线也归入 0。分母使用**前一根** ATR14，不能拿响应点自己的 `atr14` 直接复算当根乖离。

给定 `MA=100`、前一根 `ATR14=10`：

| low | high | guaili | value | 解释 |
| ---: | ---: | ---: | ---: | --- |
| 112 | 118 | 1.2 | 12 | 最近边缘在均线上方 1.2 个前值 ATR14 |
| 82 | 88 | -1.2 | -12 | 最近边缘在均线下方 1.2 个前值 ATR14 |
| 98 | 103 | 0 | 0 | 区间跨过均线 |
| 100 | 105 | 0 | 0 | 低点恰好触碰均线 |
| 100.5 | 103 | 0.05 | 0 | 有小幅正乖离，但整数显示被截断为 0 |
| 97 | 99.5 | -0.05 | 0 | 有小幅负乖离，但整数显示被截断为 0 |

`1.29 → value=12`，`-1.29 → value=-12`，向零截断而非四舍五入或向下取整。因而 **`value=0` 不等价于 `guaili=0`**；例如“连续多个周期 value=0”不能直接断言每根 K 线都穿过均线。判断精细阈值时应使用 `guaili`。

## 4. 相对波动排名 atrRank / rankFilter

先把排名用 ATR 按价格中点归一化：

```text
relativeATR[t] = 2 * ATR(atrLen)[t] / (high[t] + low[t])
N = atrPercentLen（HTTP 接口最小为 2）
window = 最近 N 个 relativeATR，包含当前值
K = window 中 <= 当前 relativeATR 的元素个数
atrRank[t] = (K - 1) / (N - 1) * 100
rankFilter[t] = atrRank[t] 存在 且 atrRank[t] <= maxAtrRank
```

不足 N 根时 `atrRank=null`、`rankFilter=false`；该点的其他指标仍照常返回。排名包含当前值，使用 `<=` 计数，相同值会提高排名：窗口内全部相等时排名为 100，不是 0。

默认 `maxAtrRank=100`，完整窗口正常计算出的排名都会通过。将阈值调低可选出相对波动排名较低的点，但服务不会因此删除数据点或更改其 `guaili`。

## 5. 趋势状态 longTrend / shortTrend

```text
up3   = MA[t] > MA[t-1] > MA[t-2] > MA[t-3]
down3 = MA[t] < MA[t-1] < MA[t-2] < MA[t-3]
strengthOK = abs(MA[t] - MA[t-3]) > ATR14[t] * slopeMul
slopeFilter = (useSlope == false) 或 strengthOK

longTrend[t]  = up3   且 slopeFilter
shortTrend[t] = down3 且 slopeFilter
```

“连续三次”需要 4 个均线点。相等不算上升或下降；前 3 个点两种趋势均为 `false`。这里强度阈值使用**当前根** ATR14，与乖离使用前一根 ATR14 不同。`useSlope=false` 只关闭强度阈值，仍要求连续三次同向变化。

趋势不依赖 `rankFilter`，也不要求 `guaili` 同号。趋势成立期间可以连续多根为 `true`；若调用方要捕捉“刚进入趋势”，应自行比较前后两点，定义 `false → true` 事件。

## 计算历史、动态值与多周期使用

接口先按时间范围与 `closedOnly` 取最多 `calcLimit` 根连续 K 线，计算全部点，再只返回最后 `limit` 个点。`limit=1&calcLimit=500` 用于查询最新值时仍能利用历史；实际可用历史不足 500 根时按已有根数计算。

- `startTime` 会截断计算输入，服务不会额外读取其之前的数据作为预热。EMA/RMA 从该次输入首值重新初始化；改变 `calcLimit`、时间范围或可用历史，结果可能略有变化。
- 秒级历史重启即丢失，分钟及以上历史也受保留数量和缺口裁剪影响。`calcLimit` 回显值不是实际输入根数，健康检查通过也不表示预热充分。
- `closedOnly=false` 允许动态 K 线参与，当前 MA、ATR、乖离、排名和趋势都可能随行情变化。需要按收盘确认的业务应使用 `closedOnly=true`，并核对点的时间与 `isClosed`。
- `trade` 模式随 aggTrade 更新，`kline_1m` 模式随 1m 收盘更新。轮询更频繁不会提升后者的采样频率。
- 历史回放截至时刻 T 时，必须保证参与策略的数据 `closeTime <= T`。查询的 `endTime` 只限制开盘时间；`closedOnly=true` 表示相对当前已收盘，不能单独防止回放时误用当时尚未收盘的大周期数据。
- 多周期响应依次读取各序列，没有跨周期原子快照保证；顶层 `serverTime` 不是所有指标的共同信号时间。组合信号应按每个点的时间对齐。
- 跨周期顺序按真实时长排序（`W=7D`，应在 `4D` 与 `10D` 之间），不能按字符串或接口数组顺序推断大小。

[多级别信号验证记录](guaili-multi-interval-signal-validation.md) 保存了特定历史样本中的观察与后续验证设想。该记录中的阈值、回撤统计与组合规则未变成当前 API 的自动交易或共振信号；接入契约及单周期算法以本文和实现为准。
