# 问题定位（文件与位置）

以下列出本次审查中发现的问题及对应代码位置，便于快速定位与讨论：

- 持仓数值表示（P1，正确性）
  - 文件：biz/model/asset.go
  - 问题：`Volume` / `AvgPrice` 使用字符串类型，导致在业务代码中频繁字符串<->float 转换，存在精度与表现不一致风险。

- 持仓加权均价与算术（P1）
  - 文件：biz/service/asset_service.go
  - 问题：使用 `strconv.ParseFloat` 与 float64 运算计算加权均价，可能引发舍入误差与大数溢出。

- Transactional Outbox TOCTOU（可靠性）
  - 文件：biz/dal/pg/outbox_repo.go
  - 问题：原实现存在先查后写的 TOCTOU 风险，已改为 `OnConflict{DoNothing:true}` 的原子插入。

- 内存幂等缓存与并发锁粒度（性能）
  - 文件：biz/service/event_processor_position.go
  - 问题：之前使用全局 `pp.mu sync.Mutex` 与进程内 `processedTrades`；现已改为按 symbol 的锁与 DB 去重 fallback。

- DB 去重表与仓库（新增）
  - 文件：biz/model/processed_trade.go（新增）
  - 文件：biz/dal/pg/processed_repo.go（新增）
  - 说明：提供 `WriteIfNotExists`（ON CONFLICT）与 `Delete` 用于非事务路径的幂等控制与失败回滚清理。

- Outbox dispatcher 与 Kafka writer 路由
  - 文件：biz/service/outbox_dispatcher.go（若存在）
  - 文件：biz/dal/kafka/init.go
  - 问题：按 topic 获取 per-topic writer（`GetWriter(topic)`）并支持 mock 注入以便测试；已做调整（详见变更）。

- 测试隔离问题
  - 文件：biz/service/order_service_test.go
  - 问题：包级 `TestMain` 会在测试包运行时初始化 Postgres，导致没有容器时出现连接拒绝。已新增 `SKIP_PG_TEST_MAIN=1` 支持并把部分测试移到 `service_test` 包以便隔离。

- 新增/修改测试文件（用于验证并发与事务回滚）
  - biz/dal/pg/outbox_repo_test.go
  - biz/dal/pg/outbox_repo_postgres_concurrency_test.go (integration)
  - biz/service/position_processor_tx_atomicity_test.go
