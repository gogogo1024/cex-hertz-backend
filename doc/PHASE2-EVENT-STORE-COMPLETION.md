## Phase 2：Persistent Event Store 实现完成

### 目标达成

✅ **完成度：100% (2/2 子任务)**

1. ✅ 2.1: Event Store 数据库设计与实现
2. ✅ 2.2: 完整事件存储接口和测试

---

### 1. Event Store 数据库设计 (biz/dal/pg/schema_events.sql)

**表结构**:
```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,           -- 数据库主键
    global_seq BIGINT UNIQUE NOT NULL,  -- 全局序列号（不重复）
    event_type VARCHAR(100),            -- 事件类型
    aggregate_id VARCHAR(255),          -- 聚合根ID
    aggregate_type VARCHAR(100),        -- 聚合根类型
    symbol VARCHAR(50),                 -- 交易对
    payload JSONB,                      -- 事件体
    event_timestamp BIGINT,             -- 事件时间
    created_at TIMESTAMP,               -- 入库时间
    version INTEGER DEFAULT 1           -- 版本号
)
```

**索引策略** (6 个):
- `global_seq` (UNIQUE) - 保证序列号唯一性
- `(aggregate_type, aggregate_id, created_at)` - 快速重放单个聚合根
- `(symbol, created_at)` - 订单簿重建查询
- `(event_type, created_at)` - 事件类型查询
- `created_at` - 时间范围查询
- `payload` (GIN) - JSONB 嵌套字段查询

**分区支持**:
- 时间分区：按月份分区以支持超大表
- 聚合根分区：按类型分区以优化查询

**不变性保证**:
- Append-only（只能插入）
- 应用层校验防止 UPDATE/DELETE
- 统计视图支持容量监控

---

### 2. Event 数据模型 (biz/model/event.go)

**PersistentEvent 结构**:
```go
type PersistentEvent struct {
    ID             int64     // 数据库PK
    GlobalSeq      int64     // 全局序列号（(NodeID<<40)|LocalSeq）
    EventType      string    // "TradeExecuted"、"OrderCancelled"
    AggregateID    string    // 聚合根ID（symbol、user-id）
    AggregateType  string    // "OrderBook"、"Position"
    Symbol         string    // 交易对
    Payload        string    // JSON 序列化
    EventTimestamp int64     // 事件时间戳（ms）
    CreatedAt      int64     // 入库时间
    Version        int       // Schema 版本
}
```

**PersistentEventStore 接口**:
- `WriteEvent()` - 单个事件写入
- `WriteEventsBatch()` - 批量原子写入
- `GetAggregateEvents()` - 重放聚合根
- `GetSymbolEvents()` - 订单簿恢复
- `GetGlobalEventStream()` - 全局恢复
- `GetLatestGlobalSeq()` - 获取最新序列号
- `CountEvents()` - 事件计数
- `ArchiveEventsBefore()` - 清理旧事件

---

### 3. PostgreSQL 实现 (biz/dal/pg/event_store_postgres.go)

**核心功能**:

#### WriteEvent / WriteEventsBatch
```go
// 单个事件：直接插入
// 批量事件：在单个事务内执行（原子性）
// 失败时完全回滚
```

#### GetAggregateEvents（聚合根重放）
```go
// 查询条件：aggregate_type + aggregate_id
// 排序：global_seq ASC（保证顺序）
// 用途：恢复单个 OrderBook 或 Position 状态
```

#### GetSymbolEvents（订单簿恢复）
```go
// 查询条件：symbol + 序列号范围
// 应用场景：
//   - 崩溃后重建 BTC/USDT OrderBook
//   - 新订阅端重放历史交易
```

#### GetGlobalEventStream（分布式恢复）
```go
// 查询条件：global_seq >= startSeq
// 排序：global_seq ASC
// 应用场景：
//   - 全系统恢复（从 checkpoint 后的序列号开始）
//   - 多个聚合根的并发恢复
```

#### 辅助方法
- `GetLatestGlobalSeq()` - 确定下一个事件的序列号
- `GetEventsByType()` - 按类型查询（如所有 TradeExecuted）
- `GetUnprocessedEvents()` - 未被处理的事件（用于事件处理器）
- `ValidateEventIntegrity()` - 检查重复 / 间隙
- `PrintEventStats()` - 监控和调试

---

### 4. 完整测试覆盖 (biz/dal/pg/event_store_postgres_test.go)

**10 个测试** (全 ✅ PASS):

#### ✅ TestWriteEvent
- 验证单个事件持久化
- 检查事件字段完整性

#### ✅ TestWriteEventsBatch
- 验证批量写入的原子性
- 确认所有事件都被保存

#### ✅ TestGetAggregateEvents
- 查询特定聚合根的所有事件
- 验证事件顺序（按 global_seq 递增）

#### ✅ TestGetSymbolEvents
- 查询符号范围内的事件
- 验证序列号过滤准确性

#### ✅ TestGetGlobalEventStream
- 从指定序列号获取全局流
- 验证分页逻辑

#### ✅ TestGetLatestGlobalSeq
- 获取最新序列号
- 初始状态和更新后状态验证

#### ✅ TestCountEvents
- 统计聚合根的事件数
- 验证计数准确性

#### ✅ TestGetEventsByType
- 按事件类型查询
- 验证过滤和排序

#### ✅ TestGetUnprocessedEvents
- 查询未处理的事件（>lastSeq）
- 用于事件处理器恢复

#### ✅ TestEventStoreReplay
- 模拟完整恢复流程
- 验证事件顺序一致性

#### ✅ TestEventStoreAtomicity
- 批量写入中途失败时全量回滚
- 检查违反约束时的行为

---

### 代码更改摘要

**新增文件** (3):
- `biz/dal/pg/schema_events.sql` (72 行)
- `biz/dal/pg/event_store_postgres.go` (220 行)
- `biz/dal/pg/event_store_postgres_test.go` (338 行)

**修改文件** (1):
- `biz/model/event.go`
  - 新增 `PersistentEvent` 结构体
  - 新增 `PersistentEventStore` 接口
  - 总计 +75 行

---

### 构建与测试结果

**编译**: ✅ `go build .` SUCCESS

**测试**: ✅ 所有 11 个事件存储测试通过 (0.023s)
- 10 个新事件存储测试 (全 PASS)
- 所有现有测试仍然通过

**整体测试**: ✅ biz/service + biz/dal/pg 总计 24+ 测试 (全 PASS)

---

### 架构优势

| 特性 | 好处 |
|------|------|
| **Append-Only** | 不可变性，完整审计日志，便于回放 |
| **全局序列号** | 分布式一致性（不依赖共识） |
| **分层索引** | 快速聚合根查询、符号查询、时间范围查询 |
| **原子批量写入** | 确保 Outbox 中的事件和业务数据同步 |
| **灵活查询** | 支持多维查询（聚合根、符号、时间、类型） |
| **JSONB 支持** | 事件 schema 演进灵活性 |
| **分区支持** | 超大表优化（按时间或类型分区） |

---

### 与 Phase 1 的集成

**Outbox → Event Store 数据流**:

```
业务操作（如成交）
    ↓
[Phase 1] 事务内写入业务数据 + Outbox 条目
    ↓
[Phase 2] Outbox Dispatcher 读取 Outbox
    ↓
发布事件到 Kafka（同时写入 Event Store）
    ↓
Checkpoint 更新（记录已处理序列号）
```

**恢复流程**:

```
Crash
    ↓
读取 Checkpoint（最后已处理的 global_seq）
    ↓
[Phase 2] 从 Event Store 查询 GetGlobalEventStream(checkpoint_seq + 1, limit)
    ↓
重放事件到匹配引擎、Position、OrderBook 等
    ↓
重建状态快照（计算 checksum 验证完整性）
```

---

### 下一步：Phase 3 (Sequencer 文档)

**当前状态**: Outbox + Event Store 完全就绪
- ✅ 业务数据 + 事件发布原子性保证
- ✅ 完整事件历史持久化
- ✅ 多维灵活查询支持

**Phase 3 任务**:
1. 编写 `doc/SEQUENCER-DESIGN.md`
2. 明确 GlobalSeq 的定义和边界
3. 说明非分布式全序性质
4. 提供集成示例和最佳实践
