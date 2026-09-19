# Phase 2: Crash Recovery 集成测试设计文档

**目标**：验证从"crash前的确定状态" → "内存丢失" → "重启恢复" → "状态完全一致"的完整路径。

**关键成功指标**：
- ✅ Pre-crash state checksum == Post-recovery state checksum
- ✅ Pre-crash OrderCount == Post-recovery OrderCount  
- ✅ Pre-crash TradeCount == Post-recovery TradeCount
- ✅ 重复恢复不改变最终状态 (idempotent recovery)

---

## 1. 测试场景设计

### 1.1 主场景：TestCrashRecoveryFullCycle

```
时间线：
  T0: 系统启动
  ├─ EventPipeline启动
  ├─ CheckpointManager初始化
  ├─ Processor注册: InMemoryEventLog, DatabaseProcessor, PositionProcessor
  └─ Sequencer就位

  T1-T10: 事件处理 (10笔订单, 15笔交易)
  ├─ SubmitOrder(user1, BUY, 100.00, 10)  → event_seq=1
  ├─ SubmitOrder(user2, SELL, 100.00, 10) → event_seq=2, MATCH!
  ├─ 产生Trade, 更新Positions
  ├─ EventPipeline dispatch到所有Processor
  └─ CheckpointManager.RecordEventProcessed(...)

  T11: 预crash准备 (Snapshot)
  ├─ GetRecoveryContext(processor="InMemoryEventLog", symbol="BTC/USDT")
  ├─ 记录: lastEventSeq=10, kafkaOffset=100, partitionID=0
  ├─ Compute: StateChecksum = MD5(OrderBook + Positions)
  ├─ Save: {
  │     PreCrashState: {
  │       OrderCount: 10,
  │       TradeCount: 15,
  │       OrderBook: { bids: [...], asks: [...] },
  │       Positions: { user1: { balance: 900 }, user2: { balance: 1100 } },
  │       LastEventSeq: 10,
  │       StateChecksum: "abc123..."
  │     }
  │   }
  └─ 所有checkpoint写入PostgreSQL

  T12: 模拟Crash (Simulate Out-of-Memory)
  ├─ 调用 time.Sleep(100ms) 等待最后一次flush
  ├─ os.Exit(1) 或 panic("simulated crash")
  ├─ 内存丢失: EventLog清空, OrderBook清空, Positions清空
  ├─ 所有内存中的seq计数器清零
  └─ 但PostgreSQL checkpoint安全保存

  T13: 系统重启 (Recovery)
  ├─ main() 启动
  ├─ PostgreSQL连接 → checkpoint表读取
  ├─ GetRecoveryContext() 获取:
  │   {
  │     StartEventSeq: 11 (lastEventSeq + 1),
  │     StartKafkaOffset: 101,
  │     PreCrashChecksum: "abc123...",
  │     ExpectedOrderCount: 10,
  │     ExpectedTradeCount: 15
  │   }
  ├─ ReplayEvents(11, maxSeq) 从EventLog重放
  │   ├─ Event11, Event12, ..., Event20 (recovery的新事件)
  │   └─ 每个事件updateOrderBook, updatePositions
  ├─ Sequencer继续从11开始 (NOT从0!)
  └─ 完成: MatchEngine恢复到pre-crash状态

  T14: 恢复验证 (Validation)
  ├─ Compute: PostRecoveryChecksum = MD5(OrderBook + Positions)
  ├─ Assert: PostRecoveryChecksum == PreCrashChecksum
  ├─ Assert: CurrentOrderCount == 10
  ├─ Assert: CurrentTradeCount == 15
  ├─ Assert: user1.balance == 900, user2.balance == 1100
  └─ ✅ 恢复成功!

  T15: 幂等性验证 (Idempotency)
  ├─ 再次调用 RecoveryExecutor.ExecuteRecovery()
  ├─ 重放events (replay是幂等的)
  ├─ Assert: PostRecoveryChecksum不变
  ├─ Assert: OrderCount, TradeCount不变
  └─ ✅ 幂等恢复确认

  T16: 持续处理验证 (Post-Recovery Processing)
  ├─ 在恢复后的状态上继续处理新事件
  ├─ SubmitOrder(user3, ...) → event_seq=21 (NOT=11!)
  ├─ 验证seq计数正常继续
  └─ ✅ 系统恢复完全正常
```

### 1.2 测试参数矩阵

| 参数 | 值 | 说明 |
|------|-----|-----|
| 初始订单数 | 10, 50, 100 | 小规模, 中规模, 大规模 |
| Crash point | event 10/50/100之后 | 不同阶段crash |
| Symbol数 | 1, 3, 5 | 单个, 多个交易对 |
| Processor类型 | InMemoryEventLog, DatabaseProcessor | 验证每个processor都恢复 |
| Recovery策略 | 从lastSeq重放, 完全重放 | 不同recovery路径 |

### 1.3 高级场景：TestMultipleProcessorsRecovery

```
三个Processor并行恢复：

InMemoryEventLog恢复：
  ├─ EventSeq: 1..10
  ├─ Checkpoints: {InMemoryEventLog:BTC/USDT: seq=10}
  └─ Replay: 1..10

DatabaseProcessor恢复：
  ├─ EventSeq: 1..10 (同步与InMemoryEventLog)
  ├─ Checkpoints: {DatabaseProcessor:BTC/USDT: seq=10}
  └─ Replay: 1..10 (写入order表, trade表)

PositionProcessor恢复：
  ├─ EventSeq: 1..10
  ├─ Checkpoints: {PositionProcessor:BTC/USDT: seq=10}
  └─ Replay: 1..10 (重建user positions)

同时验证：
  ✓ 三个processor的checkpoint seq一致 (都是10)
  ✓ 三个processor的state checksum一致
  ✓ DatabaseProcessor的order/trade表数据正确
  ✓ PositionProcessor的positions数据正确
```

---

## 2. 核心API设计

### 2.1 RecoveryExecutor 接口

```go
// biz/service/recovery_executor.go

type RecoveryExecutor interface {
    // 检查是否需要恢复 (启动时调用)
    ShouldRecover(ctx context.Context) (bool, error)
    
    // 执行恢复流程
    ExecuteRecovery(ctx context.Context, opts RecoveryOptions) (*RecoveryResult, error)
    
    // 获取恢复统计
    GetRecoveryStats() *RecoveryStats
}

type RecoveryOptions struct {
    // 恢复策略
    Strategy RecoveryStrategy // FULL_REPLAY, INCREMENTAL
    
    // 处理器过滤 (为空=所有processor)
    ProcessorNames []string
    
    // 符号过滤 (为空=所有symbol)
    Symbols []string
    
    // 是否验证恢复结果
    ValidateAfterRecovery bool
    
    // 超时时间
    Timeout time.Duration
}

type RecoveryStrategy string
const (
    FULL_REPLAY    = "full_replay"    // 从初始状态重放所有事件
    INCREMENTAL    = "incremental"    // 只重放crash后的事件
)

type RecoveryResult struct {
    // 基本信息
    ProcessorName string
    Symbol        string
    RecoveryStart time.Time
    RecoveryEnd   time.Time
    
    // 恢复统计
    EventsReplayed     int64
    OrdersRestored     int64
    TradesRestored     int64
    
    // 恢复前后状态
    PreCrashChecksum  string
    PostRecoveryChecksum string
    
    // 验证结果
    IsValid           bool
    ValidationError   error
}

type RecoveryStats struct {
    TotalRecoveryTime    time.Duration
    ProcessorsRecovered  int
    EventsProcessed      int64
    StateValidated       bool
    LastRecoveryTime     time.Time
}
```

### 2.2 StateValidator 接口

```go
// biz/service/state_validator.go

type StateValidator interface {
    // 计算状态的checksum
    CalculateChecksum(ob *model.OrderBook, pos map[string]*model.Position) string
    
    // 验证恢复后的状态
    ValidateRecoveryState(
        preState *RecoveryCheckpoint,
        postState *RecoveryCheckpoint,
    ) *ValidationResult
    
    // 获取当前内存状态快照
    CaptureCurrentState() *RecoveryCheckpoint
}

type RecoveryCheckpoint struct {
    Timestamp        time.Time
    LastEventSeq     uint64
    OrderCount       int64
    TradeCount       int64
    OrderBook        *model.OrderBook
    Positions        map[string]*model.Position
    StateChecksum    string
    
    // 额外的验证字段
    BidLevels        int
    AskLevels        int
    TotalBidQty      int64
    TotalAskQty      int64
}

type ValidationResult struct {
    IsValid        bool
    ChecksumMatch  bool
    OrderCountOK   bool
    TradeCountOK   bool
    PositionsOK    bool
    
    // 详细的差异信息
    ChecksumMismatch      string
    OrderCountDifference  int64
    TradeCountDifference  int64
    PositionDiffs         map[string]string
    
    // 完整的pre/post日志
    PreState       string
    PostState      string
}
```

### 2.3 CheckpointManager 增强

```go
// 新增方法到biz/service/checkpoint_manager.go

func (cm *CheckpointManager) GetRecoveryContext(
    processorName string,
    symbol string,
) (*model.RecoveryContext, error) {
    // 获取最后一个checkpoint
    // 计算StartEventSeq = lastSeq + 1
    // 计算StartKafkaOffset = lastOffset + 1
    // 返回recovery所需的全部信息
}

func (cm *CheckpointManager) SaveRecoveryStart(
    processorName string,
    symbol string,
    preChecksum string,
) error {
    // 保存recovery开始时的快照
}

func (cm *CheckpointManager) MarkRecoveryComplete(
    processorName string,
    symbol string,
    success bool,
    postChecksum string,
    recoveredEventCount int64,
) error {
    // 标记recovery完成
}

func (cm *CheckpointManager) ListPendingRecoveries() []RecoveryPending {
    // 返回需要恢复的(processor, symbol)列表
}
```

---

## 3. 恢复流程详细设计

### 3.1 启动时自动恢复检测

```go
// main.go 中的启动逻辑

func main() {
    // ... 初始化
    
    pm := service.NewPartitionAwareMatchEngine(...)
    
    // ✅ NEW: 检查并执行恢复
    recoveryExecutor := service.NewRecoveryExecutor(pm)
    shouldRecover, err := recoveryExecutor.ShouldRecover(context.Background())
    if err != nil {
        log.Fatalf("Failed to check recovery status: %v", err)
    }
    
    if shouldRecover {
        log.Info("System detected incomplete recovery. Starting recovery...")
        result, err := recoveryExecutor.ExecuteRecovery(
            context.Background(),
            &service.RecoveryOptions{
                Strategy: service.INCREMENTAL,
                ValidateAfterRecovery: true,
                Timeout: 5 * time.Minute,
            },
        )
        if err != nil {
            log.Fatalf("Recovery failed: %v", err)
        }
        log.Infof("Recovery completed: %v events replayed", result.EventsReplayed)
    }
    
    // ... 启动HTTP服务
}
```

### 3.2 恢复执行的伪代码

```
执行恢复流程 (ExecuteRecovery):

  Input: RecoveryOptions
  Output: RecoveryResult
  
  Step 1: 获取待恢复列表
  ─────────────────────
    processors = FilterProcessors(opts.ProcessorNames)
    symbols = FilterSymbols(opts.Symbols)
    
    pending = []
    for each processor in processors:
      for each symbol in symbols:
        ctx, err := checkpointMgr.GetRecoveryContext(processor, symbol)
        if ctx.StartEventSeq > 0:
          pending.append((processor, symbol, ctx))
  
  Step 2: 保存recovery起点
  ─────────────────────
    for each (processor, symbol, ctx) in pending:
      preChecksum = calculateStateChecksum()
      saveRecoveryStart(processor, symbol, preChecksum)
  
  Step 3: 获取事件日志 (从PostgreSQL event sourcing log)
  ─────────────────────
    events = []
    for each (processor, symbol, ctx) in pending:
      startSeq = ctx.StartEventSeq
      events = append(events, eventLog.GetEvents(startSeq, maxSeq))
  
  Step 4: 重放事件（恢复状态）
  ─────────────────────
    for each event in events:
      processor = getProcessor(event.ProcessorName)
      processor.ProcessEvent(event)  // 更新OrderBook/Positions
      recordEventProcessed(processor, event)
    
    // 等待所有恢复完成
    wg.Wait()
  
  Step 5: 计算恢复后状态
  ─────────────────────
    postChecksum = calculateStateChecksum()
    recoveredCount = len(events)
    
    result = RecoveryResult{
      EventsReplayed: recoveredCount,
      PreCrashChecksum: preChecksum,
      PostRecoveryChecksum: postChecksum,
    }
  
  Step 6: 验证恢复 (如果启用)
  ─────────────────────
    if opts.ValidateAfterRecovery:
      validation = stateValidator.ValidateRecoveryState(
        preState, postState
      )
      
      if not validation.IsValid:
        result.IsValid = false
        result.ValidationError = validation.GetErrorMessage()
        return result, validation.Error
      
      result.IsValid = true
  
  Step 7: 保存恢复结果
  ─────────────────────
    for each (processor, symbol, ctx) in pending:
      markRecoveryComplete(
        processor, symbol,
        true,  // success
        postChecksum,
        recoveredCount,
      )
  
  return result, nil
```

### 3.3 Processor中的幂等重放

```
Event Replay的关键原则:

每个Processor.ProcessEvent(event) 必须满足幂等性：

  ProcessEvent(Event{seq=5, type=MATCH, ...}) 
    第1次调用 → OrderBook更新, 返回nil
    第2次调用 → 检测seq=5已处理, 跳过, 返回nil
    第N次调用 → 始终返回nil
  
  证明: OrderBook状态相同 ✓

实现方式:
  
  func (p *InMemoryEventLog) ProcessEvent(event *Event) error {
    // 检查是否已处理过
    if lastSeq := p.GetLastProcessedSeq(); event.Seq <= lastSeq {
      log.Debug("Event already processed, skipping: seq=%d", event.Seq)
      return nil  // 幂等跳过
    }
    
    // 首次处理
    p.handleEventLogic(event)
    p.SetLastProcessedSeq(event.Seq)
    return nil
  }
```

---

## 4. 数据库表设计补充

### 4.1 EventOffsetCheckpoint 表扩展

```sql
-- PostgreSQL migration

ALTER TABLE event_offset_checkpoints ADD COLUMN IF NOT EXISTS (
    kafka_offset BIGINT DEFAULT 0,
    partition_id INTEGER DEFAULT 0,
    recovery_status VARCHAR(20) DEFAULT 'none',  -- 'none', 'in_progress', 'complete', 'failed'
    recovery_start_time TIMESTAMP,
    recovery_end_time TIMESTAMP,
    recovery_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_recovery_status 
ON event_offset_checkpoints(processor_name, symbol, recovery_status);
```

### 4.2 EventLog 表 (已存在，用于恢复)

```sql
-- 假设已经存在 (来自Event Sourcing存储)

CREATE TABLE IF NOT EXISTS event_logs (
    id BIGSERIAL PRIMARY KEY,
    sequence_number BIGINT UNIQUE NOT NULL,  -- 全局递增
    event_type VARCHAR(50) NOT NULL,         -- SUBMIT_ORDER, TRADE, etc
    processor_name VARCHAR(100) NOT NULL,    -- 处理器名称
    symbol VARCHAR(20) NOT NULL,
    payload JSONB NOT NULL,                  -- 事件数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_seq (sequence_number),
    INDEX idx_processor_symbol (processor_name, symbol, sequence_number)
);
```

---

## 5. 测试用例实现要点

### 5.1 TestCrashRecoveryFullCycle 结构

```go
// biz/service/crash_recovery_test.go

func TestCrashRecoveryFullCycle(t *testing.T) {
    // Setup
    engine := setupMatchEngine()
    recoveryExec := NewRecoveryExecutor(engine)
    
    // Phase 1: 正常处理事件
    events := processNOrders(engine, 10, 15)
    preState := captureState(engine)
    preChecksum := calculateChecksum(preState)
    
    // Phase 2: 等待checkpoint持久化
    time.Sleep(200 * time.Millisecond)
    
    // Phase 3: 验证checkpoint已写入PostgreSQL
    ctx, err := engine.CheckpointMgr.GetRecoveryContext(...)
    assert.NoError(t, err)
    assert.Equal(t, ctx.LastEventSeq, uint64(10))
    
    // Phase 4: 模拟crash (清空内存)
    engine.Sequencer = nil
    engine.EventLog.Clear()
    engine.OrderBook = nil
    
    // Phase 5: 执行恢复
    result, err := recoveryExec.ExecuteRecovery(ctx)
    assert.NoError(t, err)
    assert.True(t, result.IsValid)
    
    // Phase 6: 验证恢复结果
    postState := captureState(engine)
    assert.Equal(t, preChecksum, result.PostRecoveryChecksum)
    assert.Equal(t, 10, result.OrdersRestored)
    assert.Equal(t, 15, result.TradesRestored)
    
    // Phase 7: 验证幂等性
    result2, _ := recoveryExec.ExecuteRecovery(ctx)
    assert.Equal(t, result.PostRecoveryChecksum, result2.PostRecoveryChecksum)
}
```

### 5.2 断言列表

```
必须验证的项目 (MUST HAVE):

✓ ChecksumMatch: pre == post
✓ OrderCount: 10 == 10
✓ TradeCount: 15 == 15
✓ Sequencer不重置: 继续从seq 11开始
✓ Positions一致: user1.balance == 900
✓ OrderBook结构相同: bids数, asks数
✓ 幂等性: 重放不改变状态
✓ PostgreSQL持久化: checkpoint表有数据

应该验证的项目 (SHOULD HAVE):

◇ Recovery耗时 < 5秒 (性能指标)
◇ EventsReplayed准确度
◇ 所有processor同时恢复
◇ Recovery后正常接收新事件
```

---

## 6. 配置项

### 6.1 conf/conf.go 新增

```go
type RecoveryConfig struct {
    // 是否启用自动恢复 (启动时)
    EnableAutoRecovery bool
    
    // 恢复超时时间
    RecoveryTimeout time.Duration
    
    // 恢复策略 (FULL_REPLAY, INCREMENTAL)
    Strategy string
    
    // 恢复前是否验证状态
    ValidateAfterRecovery bool
    
    // 保留多少天的checkpoint (用于清理)
    CheckpointRetentionDays int
    
    // 恢复失败重试次数
    MaxRetries int
}

type Config struct {
    // ... 现有字段
    Recovery RecoveryConfig `yaml:"recovery"`
}
```

### 6.2 conf/dev/conf.yaml 示例

```yaml
recovery:
  enable_auto_recovery: true
  recovery_timeout: 5m
  strategy: incremental
  validate_after_recovery: true
  checkpoint_retention_days: 7
  max_retries: 3
```

---

## 7. 集成点清单

### 7.1 文件修改清单

| 文件 | 修改内容 | 优先级 |
|------|---------|-------|
| `biz/service/recovery_executor.go` | NEW - 恢复执行器 | 🔴 必须 |
| `biz/service/state_validator.go` | NEW - 状态验证器 | 🔴 必须 |
| `biz/service/checkpoint_manager.go` | 增加恢复相关方法 | 🔴 必须 |
| `biz/service/crash_recovery_test.go` | NEW - 集成测试用例 | 🔴 必须 |
| `main.go` | 添加恢复启动逻辑 | 🟡 重要 |
| `conf/conf.go` | 新增恢复配置 | 🟡 重要 |
| `biz/dal/pg/checkpoint_repo.go` | 增强恢复相关queries | 🟡 重要 |
| `biz/dal/pg/init.go` | 迁移表字段 | 🟡 重要 |

### 7.2 依赖关系

```
recovery_executor.go
    ↓ 依赖
    checkpoint_manager.go
    state_validator.go
    event_pipeline.go
    partition_aware_match_engine.go

main.go
    ↓ 依赖
    recovery_executor.go

crash_recovery_test.go
    ↓ 依赖
    recovery_executor.go
    state_validator.go
```

---

## 8. 预期输出和验收标准

### 8.1 测试通过的输出

```
=== RUN   TestCrashRecoveryFullCycle
    crash_recovery_test.go:45: Pre-crash state checksum: abc123def456
    crash_recovery_test.go:78: Simulating crash at event_seq=10
    crash_recovery_test.go:95: Recovery started, replaying events from seq=11
    crash_recovery_test.go:112: Events replayed: 10
    crash_recovery_test.go:120: Post-recovery state checksum: abc123def456 ✓
    crash_recovery_test.go:125: OrderCount validation: 10 == 10 ✓
    crash_recovery_test.go:130: TradeCount validation: 15 == 15 ✓
    crash_recovery_test.go:135: Positions validation: user1.balance=900 ✓
    crash_recovery_test.go:140: Idempotency check: re-replay passes ✓
    crash_recovery_test.go:145: Post-recovery event processing: seq continues at 21 ✓
--- PASS: TestCrashRecoveryFullCycle (2.34s)

=== RUN   TestMultipleProcessorsRecovery
    crash_recovery_test.go:200: 3 processors detected for recovery
    crash_recovery_test.go:210: InMemoryEventLog recovery: 10 events replayed ✓
    crash_recovery_test.go:215: DatabaseProcessor recovery: 10 events replayed ✓
    crash_recovery_test.go:220: PositionProcessor recovery: 10 events replayed ✓
    crash_recovery_test.go:225: All processors state checksum match ✓
--- PASS: TestMultipleProcessorsRecovery (1.56s)

=== RUN   TestIdempotencyAfterRecovery
    crash_recovery_test.go:270: First recovery: checksum=abc123def456
    crash_recovery_test.go:280: Second recovery: checksum=abc123def456 ✓
    crash_recovery_test.go:285: Third recovery: checksum=abc123def456 ✓
--- PASS: TestIdempotencyAfterRecovery (1.12s)

PASS
ok  cex-hertz-backend/biz/service  5.02s
```

### 8.2 验收标准 (Go Live Criteria)

- [ ] 3个测试用例全部PASS
- [ ] 所有checksum验证通过
- [ ] OrderCount/TradeCount精确匹配
- [ ] 幂等性验证成功
- [ ] 恢复耗时 < 5秒 (对于100笔订单)
- [ ] `go build .` 编译成功 (无warning)
- [ ] `go test ./...` 全部通过 (包括现有单元测试)
- [ ] 代码覆盖率 >= 80% (recovery相关代码)

---

## 9. 风险和缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| PostgreSQL连接失败 | 中 | 恢复阻止 | Retry + 降级日志 |
| Checkpoint表损坏 | 低 | 无法恢复 | 数据库备份 + 手动恢复文档 |
| 事件乱序重放 | 中 | 状态不一致 | Sequencer seq检查 + checksum验证 |
| 内存溢出 (大量events) | 低 | 恢复失败 | 分批重放 + 流式处理 |
| Checksum不匹配 (bug) | 中 | recovery失败 | 详细日志 + 手动调查 |

---

## 10. 后续迭代 (不在Phase 2范围内)

- Phase 3: 完整的端到端durability验证 (Kafka集成)
- 性能优化: 并行恢复多个processor
- 监控: recovery指标暴露给Prometheus
- 文档: 运维手册 + 故障排查指南

---

## 总结

Phase 2是从"设计验证" → "系统可靠性"的关键一步。通过完整的crash recovery测试，我们将证明：

> **任何时刻的crash都不会导致数据丢失。系统可以从任意检查点恢复到crash前的完全一致状态。**

这就是生产级Event Sourcing系统的技术基础。
