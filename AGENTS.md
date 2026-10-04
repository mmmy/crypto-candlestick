# 仓库维护约定

这是供其他程序调用的 Rust/Axum 行情后端。修改时需要同时考虑 HTTP 契约、数据时间语义和已有调用方。

## 文档分工

- [README.md](README.md)：服务能力、接入入口、配置和运行方式。
- [docs/api.md](docs/api.md)：HTTP 参数、响应、错误、数据可用性及价格警报语义。改动接口时核对并更新相关章节。
- [docs/guaili.md](docs/guaili.md)：指标公式、初始化、参数作用和信号边界。改动指标计算时同步更新。
- `docs/*validation*` 和 `docs/superpowers/` 保存研究记录、设计与计划，不能将其中的建议直接当作当前已实现契约。

## 修改原则

- 以当前实现和测试核实行为；发现文档与代码不符时，明确当前限制，不把期望行为写成已实现功能。
- 修改路由、字段类型、默认值、时间格式、空值、排序或算法结果时，检查对调用方的影响，并在文档和变更说明中写清。
- 保持“配置周期才公开”的边界；内部补齐用基础周期不应意外通过公开数据接口暴露。
- 涉及 K 线查询时，同时检查 SQLite、收盘缓冲、秒级内存、动态 K 线、缺口裁剪和 `closedOnly`。
- 涉及 guaili 时，明确前一根 ATR14 与当前 ATR14 的用途、整数向零截断、预热和未收盘值；不将指标状态或历史研究结论描述为自动买卖指令。
- 涉及价格警报时，同时核对 HTTP 校验、数据库状态、worker 的穿越基线、事件与 Webhook 投递；不要只依据 PATCH 返回对象推断持久化行为。

## 代码入口与验证

| 变更范围 | 主要入口 | 相关现有测试 |
| --- | --- | --- |
| HTTP 契约 | `src/http/routes.rs`, `src/http/handlers.rs` | `cargo test --test http_tests` |
| 指标 | `src/indicators/guaili.rs` | `cargo test --test guaili_tests --test http_tests` |
| 周期、聚合、数据源 | `src/domain/interval.rs`, `src/engine/aggregator.rs`, `src/binance/` | `interval_tests`, `aggregator_tests`, `subscription_plan_tests`, `rest_tests` |
| 存储和缓存 | `src/storage/sqlite.rs`, `src/memory.rs` | `storage_tests`, `latest_cache_tests` |
| 配置 | `src/config.rs`, `config.toml` | `config_tests` |

Rust 代码修改按影响范围运行测试，必要时运行 `cargo test`；格式与静态检查使用 `cargo fmt --check`、`cargo clippy`。纯文档修改核对路由/字段/公式、示例 JSON 和链接；需要确认行为时运行相关现有测试，不为文字改动添加无意义测试。

程序读取当前目录下的 `config.toml`；测试或临时验证使用隔离数据库，避免改动业务 `candles.db`。如测试确实需要重启已运行程序，可先停止确认属于当前仓库的服务进程，再启动测试程序；不要终止无关进程。原有补充原则见 [src/AGENT.md](src/AGENT.md)。

# 远程部署
ssh -p 22 root@139.180.203.107
windows使用用wsl构建, 服务器用nohup部署启动的