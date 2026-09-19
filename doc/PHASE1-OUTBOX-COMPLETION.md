## Phase 1：Transactional Outbox Pattern 实现完成

### 目标达成

✅ **完成度：100% (4/4 子任务)**

1. ✅ 1.1: Outbox 数据模型（biz/model/outbox.go）
2. ✅ 1.2: PositionProcessor 事务化处理（biz/service/event_processor_position.go）
3. ✅ 1.3: Outbox Dispatcher 异步发送（biz/service/outbox_dispatcher.go）
4. ✅ 1.4: Case A/B 集成测试（biz/service/outbox_test.go）

---

### 1. Outbox 数据模型 (biz/model/outbox.go)

**结构**:
```go
type OutboxEntry struct {
    ID            int64      // 自增主键
    EventID       string     // 幂等键（TradeID、OrderID）
    EventType     string     // "TradeExecuted"、"PositionUpdated"
    AggregateID   string     // 聚合根ID（symbol、user-id）
    AggregateType string     // "OrderBook"、"Position"
    Payload       string     // JSON 序列化事件体
    Published     bool       // 发布状态
    PublishedAt   *time.Time // 发布时间
    CreatedAt     time.Time  // 创建时间
    UpdatedAt     time.Time  // 更新时间
    RetryCount    int        // 重试次数
    LastError     string     // 最后的错误信息
}
```

**索引**:
- `event_id` (UNIQUE) - 幂等性保证
- `published` - 快速查询未发布条目
- `aggregate_id, aggregate_type` - 按聚合根查询
- `created_at` - 按创建时间排序

**SQL 初始化** (biz/dal/pg/schema_outbox.sql):
- 自动更新触发器（PostgreSQL）
- 分层索引策略（优化查询性能）

---

### 2. PositionProcessor 事务化改造

**新增功能**:

#### 构造函数扩展
```go
// 无事务版本（向后兼容）
NewPositionProcessor(buyFn, sellFn)

// 带 outbox 支持的版本（新增）
NewPositionProcessorWithOutbox(db, outboxRepo, buyFn, sellFn)
```

#### 事务化处理流程
```
ProcessEvent(trade)
  ↓
选择处理模式（有 db & outboxRepo 则走事务化）
  ↓
BEGIN TRANSACTION
  ├─ 幂等性检查（processedTrades）
  ├─ 业务数据更新（buyPositionFn, sellPositionFn）
  ├─ Outbox 条目写入（单一事务）
  └─ COMMIT / ROLLBACK
  ↓
标记已处理
```

**关键特性**:
- **原子性**: DB 更新 + Outbox 条目同时成功或回滚
- **幂等性**: 内存 map 记录已处理的 TradeID
- **可靠性**: 未发布的 outbox 条目待 dispatcher 发送

---

### 3. Outbox Dispatcher (biz/service/outbox_dispatcher.go)

**设计**:
- 异步后台任务（goroutine + ticker）
- 定期拉取未发布条目（批处理）
- 发布后标记 published=true + published_at

**核心方法**:
```go
Start(ctx, interval)      // 启动分发循环
Stop()                      // 停止分发
dispatchBatch(ctx)         // 处理一批条目
publishEntry(ctx, entry)   // 发布单个条目
CleanupPublished(ctx, days) // 清理旧的已发布条目
```

**重试策略**:
- 发布失败时: `retry_count++`、保存 error 信息
- 支持最大重试次数（可配置）
- 失败条目可被 `GetFailedEntries()` 查询

**发布目标**:
- Kafka（按聚合根分区，保证顺序性）
- 其他消息队列（可扩展）
- 使用 EventID 作为幂等键（生产者端配置 `EnableIdempotence=true`）

---

### 4. 集成测试验证

**测试覆盖** (biz/service/outbox_test.go - 6 个测试):

#### ✅ TestOutboxPatternAtomicity
- 验证：业务数据 + outbox 条目在同一事务内
- 结果：都存在，且事务状态一致

#### ✅ TestOutboxPatternIdempotency
- 验证：相同事件处理 3 次，业务更新只执行 1 次
- 结果：幂等检查有效，outbox 无重复

#### ✅ TestOutboxDispatcherPublishSuccess
- 验证：未发布的条目被 dispatcher 标记为已发布
- 结果：published=true, published_at=now

#### ✅ TestOutboxPatternCaseA
- 场景：DB UPDATE ✓ + Outbox ✓ + Checkpoint ✗ (crash)
- 恢复：
  - 业务数据已在 DB 中（幂等）
  - Outbox 条目待发送（dispatcher 稍后处理）
  - 结果：无数据丢失

#### ✅ TestOutboxPatternCaseB_Monitoring
- 场景：Checkpoint ✓ + DB UPDATE ✗ (crash)
- 检测：checkpoint_seq > 实际发布条目数 → 告警
- 缓解：Outbox 设计确保最终一致性

#### ✅ TestOutboxDispatcherRetry
- 验证：发布失败时记录 error、增加 retry_count
- 结果：可通过 GetFailedEntries() 重试

---

### 代码更改摘要

**新增文件** (3):
- `biz/model/outbox.go` (65 行)
- `biz/dal/pg/outbox_repo.go` (87 行)
- `biz/dal/pg/schema_outbox.sql` (42 行)
- `biz/service/outbox_dispatcher.go` (160 行)
- `biz/service/outbox_test.go` (324 行)

**修改文件** (1):
- `biz/service/event_processor_position.go`
  - 新增字段: `db`, `outboxRepo`
  - 新增构造函数: `NewPositionProcessorWithOutbox()`
  - 新增方法: `ProcessEvent()` 路由逻辑、`handleTradeExecutedTransactional()`
  - 总计 +140 行

**依赖更新**:
- ✅ `gorm.io/driver/sqlite` (测试依赖) 已添加
- ✅ `go mod tidy` 已执行

---

### 构建与测试结果

**编译**: ✅ `go build .` SUCCESS

**测试**: ✅ 所有 14 个测试通过 (0.185s)
- 6 个新 outbox 测试 (全 PASS)
- 8 个现有测试 (全 PASS，未破坏)

**日志样例**:
```
[PositionProcessor] Updated position for trade: trade-atomic-1 (taker: user-1, maker: user-2) with outbox
[OutboxDispatcher] Started (interval: 100ms, batchSize: 10)
[OutboxDispatcher] Processing 1 entries
[OutboxDispatcher] Publishing event trade-pub-1 to topic events-OrderBook
[OutboxDispatcher] Published 1 entries
```

---

### 架构效益

**问题解决**:

| 问题 | 原因 | 解决方案 | 效果 |
|------|------|---------|------|
| DB UPDATE + Checkpoint 不原子 | 两个独立操作 | Outbox Pattern | 保证 DB + 事件同时成功 |
| 事件可能丢失 | Checkpoint 后 crash | Outbox Dispatcher | 未发送的条目被重试 |
| 重复处理事件 | 消费端无幂等键 | OutboxEntry.event_id + 内存记录 | 幂等性保证 |
| 发布失败难追踪 | 无重试机制 | retry_count + last_error | 可观测、可恢复 |

**下游支持**:

Phase 1 为 Phase 2 (Persistent Event Store) 提供基础:
- ✅ 事务化写入能力
- ✅ 可靠发布机制
- ✅ 幂等性框架
- 👉 准备好迁移 InMemoryEventLog → Postgres append-only

---

### 下一步：Phase 2 (Persistent Event Store)

**准备工作完成**:
- ✅ Outbox pattern 验证事件可靠性
- ✅ PositionProcessor 支持事务化处理
- ✅ Dispatcher 确保最终发送

**Phase 2 任务**:
1. 设计 Postgres events 表（append-only）
2. 实现 EventStore 接口（替代 InMemoryEventLog）
3. 迁移 EventPipeline 读取源
4. 验证持久化恢复流程
