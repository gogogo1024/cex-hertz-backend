# 改进计划（分阶段、按优先级）

目标：修复数值正确性（P1）、保证事务与幂等性、提高并发吞吐、并把关键集成测试纳入 CI 校验。

阶段 A（立即 - 高优先级）
1. 数据迁移（Position 类型从 string → bigint 纳单位）【P1】
   - 方案：采用双写 + 背景迁移的零停机流程：
     1) 在 DB 中新增 `volume_bigint`、`avg_price_bigint` 列（bigint，默认 0）。
     2) 在服务中同时写入旧列（兼容）和新列（纳单位）。
     3) 编写并运行 Go backfill 工具，逐行读取旧列、解析成纳单位并写入新列，记录失败项并重试。
     4) 验证数据一致性；切换代码只读新列并移除旧列（经审计与备份后）。

2. 将 `processed_trades` 表纳入主迁移（`pg.AutoMigrate`）并在 CI 容器环境中测试并发插入（确保 `ON CONFLICT` 语义）。

3. 在 CI 中加入 Postgres/Kafka matrix：使用 `docker-compose-base.yaml` 启动 postgres/kafka，然后运行带 `//go:build integration` 的测试。

阶段 B（短期 - 中优先级）
1. 完善监控和报警：Outbox publish success/failure、retry count、dispatcher lag。
2. 移除进程内 `processedTrades`（在确认 DB 去重稳定后），或把内存缓存降级为短期缓存（TTL），减小内存依赖。

阶段 C（中期 - 低优先级）
1. 性能：评估更细粒度的锁策略（当前已按 symbol lock 实现），必要时使用分片锁或乐观并发重试。
2. 安全与审计：记录 outbox publish 的 trace-id，增强重放验证与消息去重日志。

验证与发布流程
- 在 dev 环境用 docker-compose 启动 Postgres & Kafka 并运行全量测试：

```bash
docker-compose -f docker-compose-base.yaml up -d pg zookeeper kafka
SKIP_PG_TEST_MAIN=1 go test ./... -v
# integration tests
go test ./... -tags=integration -v
```

风险与缓解
- 数据迁移可能导致短期不一致：采用双写 + backfill + 验证 + 回滚点策略。
- 生产变更须在低峰窗口发布并备份 DB 快照。

交付产物
- DB migration 脚本 / backfill 工具（Go）
- CI workflow（GitHub Actions）示例
- 运行手册和回滚步骤
