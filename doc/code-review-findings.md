# 代码审查结论（简要）

范围
- 验证并稳定 API/WS → MatchEngine → Outbox → Kafka → Consumer 的端到端流程。
- 修复 outbox 幂等与事务性（Transactional Outbox）、Position 事务一致性、并发处理粒度与测试隔离问题。

主要发现
- Position（持仓）层曾使用字符串/浮点进行表示和计算，存在精度与正确性风险（P1）。
- 原先的 outbox 幂等实现依赖先查后写（TOCTOU），需要改为 DB 原子插入（ON CONFLICT DO NOTHING）。
- PositionProcessor 在进程内使用全局互斥与内存幂等缓存 `processedTrades`，会影响并发并且不可靠；建议以 DB 去重为权威。
- Outbox dispatcher 的 topic 路由与 Kafka writer 缓存需按 topic 获取写入器以便正确路由和单元测试注入 mock。
- 测试隔离问题：`biz/service` 下的 `TestMain` 会在包级别初始化 Postgres，导致单元测试在没有容器时失败；需要可控开关或在 CI 中运行容器化集成测试。

已实施的改动（摘要）
- 将 `Position` 模型从字符串改为 `QuantityInNano` / `PriceInNano`（bigint），并把 `BuyPositionTx`/`SellPositionTx` 改为使用纳单位与 `math/big` 做加权均价计算，避免浮点误差与溢出。
- 引入 `WriteOutboxEntryIfNotExists`（`ON CONFLICT DO NOTHING`）实现 DB 原子幂等，并在 `PositionProcessor` 事务化路径先写 outbox 再更新持仓。
- 添加基于 DB 的幂等表 `processed_trades` 与 `ProcessedRepo`，在非事务路径优先使用 DB 去重，内存缓存作为回退。
- 将 `PositionProcessor` 的全局互斥替换为按 symbol 的细粒度锁（`sync.Map` 缓存 `*sync.Mutex`）。
- 修复测试隔离：`TestMain` 增加环境变量 `SKIP_PG_TEST_MAIN=1` 支持以便在本地运行不依赖外部 Postgres 的单测。

未决与建议（重点）
- P1：数据迁移 — 现有 production 数据需要从字符串/浮点平滑迁移到纳单位整数（见改进计划）。此项为高风险操作，需准备回滚与验证脚本。
- 将 `processed_trades` 加入主迁移（`pg.AutoMigrate`）并在 CI 中验证表创建与并发插入语义。
- 将集成测试（Postgres、Kafka）加入 CI matrix，确保 `ON CONFLICT` 与并发场景在真实 DB 上验证。

参考变更文件（示例）
- biz/model/asset.go
- biz/service/asset_service.go
- biz/service/event_processor_position.go
- biz/dal/pg/outbox_repo.go
- biz/dal/pg/processed_repo.go (新增)
- biz/model/processed_trade.go (新增)
- biz/service/order_service_test.go (TestMain 可跳过标志)

如需我接着生成迁移脚本与 CI 配置，我可以继续按计划完成。
