扩展变更说明 — DB 驱动的幂等策略（自动追加）

概要

本 PR 将幂等性逻辑从进程内/错误文本解析迁移到数据库原子操作（`ON CONFLICT DO NOTHING`），以减少 TOCTOU 竞态、提高多实例场景下的确定性，并把 outbox 表同时用作发布凭证与幂等标记。

主要改动（详细）

- 新增 `WriteOutboxEntryIfNotExists(tx, entry) (bool, error)`：
  - 文件：`biz/dal/pg/outbox_repo.go`
  - 实现：在事务内使用 GORM `clause.OnConflict{DoNothing:true}` 尝试插入 outbox 条目，返回 `inserted`（`RowsAffected>0`）以判断是否为首次处理。

- `PositionProcessor.handleTradeExecutedTransactional`：
  - 文件：`biz/service/event_processor_position.go`
  - 实现：在同一事务内先调用 `WriteOutboxEntryIfNotExists` 作为幂等预留：
    - 若 `inserted==false`（说明其它实例已处理）则跳过业务更新并提前返回（事务回滚或无影响）。
    - 若 `inserted==true`，则在事务内继续更新持仓并提交。这样保证了“先预约 outbox → 再做业务更新”或将 outbox 作为并发控制点的原子性。

- `DatabaseProcessor`（trade/order 写入）:
  - 文件：`biz/service/event_processor_db.go`
  - 实现：原先基于 "先查再写" 或字符串错误识别的幂等策略改为：使用 `ON CONFLICT DO NOTHING` 原子插入，依据 `RowsAffected`（==0 表示重复）判断是否已处理并跳过重复写入。

- 兼容保守处理：
  - 暂时保留 `isUniqueConstraintError` 作为备用检测（没有被主路径依赖），可在后续清理。

测试（简要）

- 已在本地运行 `go test ./biz/service -v` 并通过。

完整测试日志未包含在 PR 中（按要求移除）。
影响/兼容性

- 向后兼容：该改动通过数据库原子操作降低了因并发导致的重复写入风险，对外部接口无影响。但需注意 outbox 表的保留策略：既然 outbox 同时承担幂等标记与事件发布凭证，建议明确 outbox 条目的保留/清理策略（例如至少保留 N 天或在确认事件被消费并记录 audit 后再删除）。

后续建议

1. 将 `isUniqueConstraintError` 清理（或至少移动为测试/诊断工具）；主路径改为 `OnConflict` 后字符串匹配变得不必要。  
2. 在 CI 中增加针对不同 DB（sqlite / postgres）的集成测试，确保 `OnConflict` 行为一致。  
3. 审查 outbox 清理策略并在 README/运维文档中说明（保留期、审计、清理脚本）。

如何在本地复现变更并运行测试

```bash
# 在仓库根目录
git checkout feature/db-idempotency-onconflict
# 运行服务包测试
go test ./biz/service -v
# 若要运行所有测试（可能耗时）
# go test ./... -v
```

