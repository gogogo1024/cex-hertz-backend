## Sequencer 设计与 GlobalSeq 语义

### 概述

本文档澄清分布式高并发撮合系统中的全局序列号（GlobalSeq）的设计、用途和边界条件。

---

### 1. GlobalSeq 定义

#### 1.1 计算公式

```
GlobalSeq = (NodeID << 40) | LocalSeq

其中：
  - NodeID: 节点编号，范围 [0, 255]（8 位）
  - LocalSeq: 节点内本地序列号，范围 [0, 1099511627775]（40 位）
```

#### 1.2 特性

| 特性 | 说明 |
|------|------|
| **唯一性** | 同一节点内 LocalSeq 递增，不重复 |
| **单调性** | 在同一节点内，GlobalSeq 严格递增 |
| **可解析性** | 可从 GlobalSeq 提取 NodeID 和 LocalSeq |
| **容错性** | 支持 256 个节点，每个节点 2^40 个事件（>1TB）|
| **稀疏性** | 跨节点的 GlobalSeq 可能有间隙（非连续）|

---

### 2. GlobalSeq 的用途

#### 2.1 事件唯一标识

**问题**: 分布式系统中，多个节点同时生成交易事件，如何确保不重复？

**解决方案**:
```
TradeID = f(symbol, buyer_order_id, seller_order_id)  // 业务幂等键
GlobalSeq = (NodeID << 40) | LocalSeq                  // 系统唯一性
```

**存储**:
```sql
-- events 表
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    global_seq BIGINT UNIQUE NOT NULL,  -- 确保全系统唯一
    trade_id VARCHAR(100),              -- 业务幂等键
    event_type VARCHAR(100),
    ...
);
```

**用途**:
- Event Store 中的 UNIQUE 约束
- Checkpoint 记录（"已处理到 global_seq = 12345"）
- 事件重复检测

#### 2.2 分区内排序

**问题**: OrderBook 处理需要严格的事件顺序，但交易来自不同节点。

**解决方案**: 使用 GlobalSeq 进行分区内排序

```
订单簿分区管理：
  - 每个符号（如 BTC/USDT）一个分区
  - 来自不同节点的交易混合到同一分区
  - 按 GlobalSeq 排序处理，确保顺序一致

重放流程：
  SELECT * FROM events 
  WHERE symbol = 'BTC/USDT' 
  ORDER BY global_seq ASC
  LIMIT 1000
```

**保证**:
- ✅ 同一节点内的事件顺序保持（LocalSeq 递增）
- ✅ 跨节点的事件在分区内有明确的处理顺序
- ❌ **不保证**: Node A 的事件 A 一定在 Node B 的事件 B 之前

#### 2.3 Checkpoint 管理

**定义**:
```
Checkpoint = {
  symbol: "BTC/USDT",
  global_seq: 12345,              // 最后已处理的序列号
  timestamp: 1609459200000,
  state_checksum: "abc123def456"
}
```

**用途**:
- 崩溃恢复的起点
- 避免重复处理事件
- 进度跟踪

**查询恢复**:
```go
// 从 checkpoint 恢复
checkpoint := GetCheckpoint("BTC/USDT")

// 查询未处理的事件
events := eventStore.GetGlobalEventStream(
  startSeq = checkpoint.GlobalSeq + 1,
  limit = 1000
)

// 重放事件重建状态
for _, event := range events {
  orderbook.ProcessEvent(event)
  UpdateCheckpoint(event.GlobalSeq)
}
```

---

### 3. 关键边界条件

#### 3.1 不是分布式共识

❌ **误解**: GlobalSeq 是全局总序，可用于跨节点的因果关系推导

✅ **正确**: GlobalSeq 只在分区内提供顺序保证

**反例**:
```
时间线：
  10:00:00 Node A 生成事件 E1，GlobalSeq = (1 << 40) | 100 = 1099511627876
  10:00:01 Node B 生成事件 E2，GlobalSeq = (2 << 40) | 50  = 2199023255666

E2.GlobalSeq > E1.GlobalSeq，但 E1 的发生时间早于 E2
```

**不能假设**: E2 是由 E1 的结果触发的

**因果关系应通过**: 业务逻辑约束（如 order matching）而非 GlobalSeq

#### 3.2 跨分区的序列号间隙

**问题**: Node A 处理 BTC/USDT，Node B 处理 ETH/USDT，两者的 GlobalSeq 是否连续？

**答案**: 不连续

```
事件流（按事件产生时间）：
  T1: Node A 处理 BTC/USDT 交易 → GlobalSeq = (1 << 40) | 1
  T2: Node B 处理 ETH/USDT 交易 → GlobalSeq = (2 << 40) | 1
  T3: Node A 处理 BTC/USDT 交易 → GlobalSeq = (1 << 40) | 2
  ...

全局序列号视图（按值递增）：
  (1 << 40) | 1  = 1099511627777
  (1 << 40) | 2  = 1099511627778
  ...
  (2 << 40) | 1  = 2199023255553  ← 跳跃（间隙）
```

**应对方案**:
- 使用 `event_store.GetGlobalEventStream()` 而非假设连续
- 在重放时需要处理间隙（正常情况）
- 不能用 GlobalSeq 做简单的 COUNT 统计

#### 3.3 LocalSeq 溢出

**约束**:
- LocalSeq 范围：[0, 2^40 - 1] ≈ 1TB
- 一个节点每秒处理 100 万事件 → ~1 万秒 ≈ 3 小时达到上限

**应对**:
- 定期 checkpoint 和清理（不保留超过 N 天的事件）
- 或设置告警：LocalSeq > 90% 时提示

**重启处理**:
```go
// 节点重启后需要恢复 LocalSeq
func InitSequencer(nodeID int) *Sequencer {
  maxSeq := eventStore.GetMaxLocalSeqForNode(nodeID)
  return &Sequencer{
    nodeID: nodeID,
    localSeq: maxSeq + 1,  // 继续递增
  }
}
```

---

### 4. 应用场景

#### 场景 A: 单分区恢复（推荐）

```
系统崩溃 → BTC/USDT 分区失效

恢复步骤：
1. 读取 BTC/USDT 的 Checkpoint（global_seq = X）
2. 从 Event Store 查询 WHERE symbol='BTC/USDT' AND global_seq > X
3. 按 global_seq 重放事件
4. 重建 OrderBook 状态
5. 更新 Checkpoint
```

**优点**:
- 快速（只恢复一个分区）
- 不影响其他交易对

#### 场景 B: 跨分区同步恢复

```
多个分区同时崩溃 → 需要全局恢复

恢复步骤：
1. 读取全局 Checkpoint（global_seq = Y）
2. 从 Event Store 查询 global_seq > Y（不限分区）
3. 按 global_seq 排序，逐个处理事件
4. 各分区并发处理（通过 aggregate_id/symbol 分发）
5. 所有分区完成后更新全局 Checkpoint
```

**注意**:
- 需要处理序列号间隙（正常）
- 各分区独立维护 Checkpoint

#### 场景 C: 新节点加入（Scale Out）

```
新增节点 Node C 加入集群 → NodeID 已分配为 3

初始化：
1. 设置 NodeID = 3
2. LocalSeq 初始值 = 1（或从 Event Store 恢复最大值）
3. 开始生成 GlobalSeq = (3 << 40) | 1, 2, 3, ...
4. 订阅 Kafka 消费事件
5. 向 Event Store 写入事件（带新的 NodeID）
```

**关键**:
- 不同 NodeID 的事件可能乱序写入 Event Store
- 分区内按 global_seq 排序后再处理

---

### 5. 最佳实践

#### 5.1 Event Store 查询

✅ **推荐**:
```go
// 方法 1：聚合根重放（常用）
events, err := eventStore.GetAggregateEvents("OrderBook", "BTC/USDT")

// 方法 2：分区恢复
events, err := eventStore.GetSymbolEvents("BTC/USDT", checkpoint_seq, checkpoint_seq + 1000)

// 方法 3：全局恢复（分布式）
events, err := eventStore.GetGlobalEventStream(global_checkpoint_seq + 1, 1000)
```

❌ **避免**:
```go
// 假设 GlobalSeq 连续
for i := start; i < end; i++ {
  event := eventStore.GetEventBySeq(i)  // ✗ 间隙会导致事件丢失
}
```

#### 5.2 Checkpoint 管理

✅ **推荐**:
```go
// 每个分区独立维护
type Checkpoint struct {
  AggregateType string  // "OrderBook"
  AggregateID   string  // "BTC/USDT"
  GlobalSeq     int64   // 最后已处理
  StateChecksum string  // SHA-256 校验
  UpdatedAt     int64   // 时间戳
}

// 更新策略：每处理 N 条事件或每 T 秒更新一次
for event := range eventStream {
  orderbook.Process(event)
  if count % 1000 == 0 {
    UpdateCheckpoint(event.GlobalSeq)
  }
}
```

#### 5.3 幂等性设计

✅ **推荐**:
```go
// 使用业务幂等键（如 TradeID）
type TradeExecutedEvent struct {
  TradeID string  // 幂等键：f(symbol, buyer_order, seller_order)
  GlobalSeq int64 // 系统唯一性
  ...
}

// 处理时检查：
if eventStore.HasProcessedTradeID(tradeID) {
  return nil  // 幂等处理
}
```

#### 5.4 监控与告警

✅ **推荐**:
```
告警条件：
  1. Checkpoint 落后超过 1 分钟
  2. LocalSeq 接近溢出（> 90%）
  3. Event Store 查询延迟 > 100ms
  4. 同一 GlobalSeq 重复出现
```

---

### 6. 与 Outbox Pattern 的协作

**完整数据流**:

```
业务操作（成交）
  ↓
[Outbox] 在事务内同时写：
  - 业务数据（positions、orders）
  - Outbox 条目
  ↓
Dispatcher 异步发布
  ↓
[Event Store] 持久化：
  - 确保 GlobalSeq 唯一性
  - 支持多维查询
  ↓
Checkpoint 更新：
  - 记录已处理的 GlobalSeq
  - 下次崩溃从此恢复
```

**保障**:
- Outbox 和 Event Store 不重复（GlobalSeq 唯一）
- Checkpoint 是恢复的起点（不会重复处理）
- 分区内顺序一致（GlobalSeq 排序）

---

### 7. 常见误区

| 误区 | 正确理解 |
|------|---------|
| GlobalSeq 是全局总序 | GlobalSeq 只在分区内提供序列保证 |
| 不同节点的事件按 GlobalSeq 有因果关系 | 因果关系需要业务逻辑约束，不是 GlobalSeq 保证 |
| GlobalSeq 必须连续 | 跨分区时有间隙属正常现象 |
| 可用 GlobalSeq 推断事件发生顺序 | 应使用 event_timestamp，不是 GlobalSeq |
| LocalSeq 可以跨节点比较 | LocalSeq 只在单节点内有意义 |

---

### 8. 参考实现

#### Sequencer 结构体

```go
type Sequencer struct {
  nodeID   int
  localSeq int64
  mu       sync.Mutex
}

func (s *Sequencer) NextSeq() int64 {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.localSeq++
  return (int64(s.nodeID) << 40) | s.localSeq
}

// 从 Event Store 恢复
func (s *Sequencer) Restore(eventStore EventStore) error {
  maxSeq, err := eventStore.GetMaxLocalSeqForNode(s.nodeID)
  if err != nil {
    return err
  }
  s.localSeq = maxSeq
  return nil
}
```

#### Event Store 查询示例

```go
// 单分区恢复
func RecoverOrderBook(symbol string, eventStore EventStore, checkpoint int64) error {
  events, err := eventStore.GetSymbolEvents(symbol, checkpoint+1, checkpoint+1000)
  if err != nil {
    return err
  }

  orderbook := NewOrderBook(symbol)
  for _, event := range events {
    if err := orderbook.ProcessEvent(event); err != nil {
      return err
    }
    UpdateCheckpoint(symbol, event.GlobalSeq)
  }
  return nil
}

// 全局恢复
func RecoverAll(eventStore EventStore, globalCheckpoint int64) error {
  // 并发恢复各分区
  events, err := eventStore.GetGlobalEventStream(globalCheckpoint+1, 10000)
  if err != nil {
    return err
  }

  // 按聚合根分组
  eventsByAgg := make(map[string][]*Event)
  for _, event := range events {
    key := event.AggregateType + "/" + event.AggregateID
    eventsByAgg[key] = append(eventsByAgg[key], event)
  }

  // 并发处理各聚合根
  for key, aggEvents := range eventsByAgg {
    go func(k string, events []*Event) {
      // 恢复聚合根
      agg := RestoreAggregate(k, events)
      UpdateAggregateCheckpoint(k, events[len(events)-1].GlobalSeq)
    }(key, aggEvents)
  }

  return nil
}
```

---

### 总结

**GlobalSeq** 是分布式高并发撮合系统的核心设计，提供：

✅ **事件唯一标识** - Checkpoint 和重复检测的基础
✅ **分区内排序** - 确保单个交易对的处理顺序一致
✅ **灵活恢复** - 支持单分区和全局恢复
✅ **可扩展性** - 支持 256 个节点、2^40 个事件

🎯 **关键原则**:
1. GlobalSeq 只在分区内保证顺序（不是全局总序）
2. 跨分区间隙属正常，按 GlobalSeq 排序后处理
3. 幂等性需要业务键（如 TradeID），不能仅依赖 GlobalSeq
4. Checkpoint 记录已处理的 GlobalSeq，作为恢复起点
5. 结合 Outbox Pattern 确保数据一致性
