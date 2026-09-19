# Partition Ownership State Machine 设计

## 概述

`OwnershipStateMachine` 是一个严格定义的有限状态机，用于管理分区所有权在迁移过程中的状态转移。它确保分区迁移的每一步都经过验证，防止非法状态转移，并提供完整的审计跟踪。

## 核心问题

在分布式系统中，分区所有权的转移涉及多个参与者和异步操作：

```
Node-1 (Owner)  →  Freezes writes  →  Flushes events  →  Transfers
                                                              ↓
                                                          Node-2 (New Owner)
```

关键挑战：
1. **原子性**: 所有权转移必须是原子的 - 不能出现两个节点同时拥有一个分区的情况
2. **顺序性**: Event 必须按全局序列号顺序处理，迁移中不能改变顺序
3. **检查点对齐**: Checkpoint 必须与 Kafka offset 在边界处对齐
4. **失败恢复**: 迁移中断后能够安全地恢复或回滚

## 状态定义

| 状态 | 含义 | 允许的动作 |
|------|------|----------|
| **Owned** | 分区由单个节点拥有，正常处理 | StartMigration → MigrationPending |
| **MigrationPending** | 迁移请求已发出，等待所有者冻结 | Freeze → Frozen 或 Rollback → Failed |
| **Frozen** | 新写入已冻结，待刷盘 | Flush → Flushing 或 Rollback → Failed |
| **Flushing** | 所有待处理事件正在刷盘 | Transfer → Transferring 或 Rollback → Failed |
| **Transferring** | 等待目标节点确认接收 | Commit → Standby 或 Rollback → Failed |
| **Standby** | 转移完成，新所有者接管 | StartMigration → MigrationPending（二次迁移） |
| **Failed** | 迁移失败 | Recover → Owned（恢复原状） |

## 状态转移图

```
                    ┌──────────────────────────────────────┐
                    │                                       │
                    ▼                                       │
   ┌─────────────────────────────────────────────────────────────────┐
   │                                                                   │
   ▼                                                                   │
Owned ─── StartMigration ──→ MigrationPending ─── Freeze ──→ Frozen   │
 △ │                              │                             │     │
 │ │                              │                             │     │
 │ │           Rollback           │                   Rollback  │     │
 │ │              │                │                      │     │     │
 │ │              ▼                ▼                      ▼     │     │
 │ └────────────────────→ Failed ──────────────────────────────┘     │
 │                          │                                         │
 │                 Recover  │                                         │
 │                          ▼                                         │
 └────────────────────────────┘

                Frozen ─── Flush ──→ Flushing ─── Transfer ──→ Transferring
                                                                    │
                                                         Commit     │
                                                            │       │
                                                            ▼       │
                                                          Standby ──┘
```

## 关键操作流程

### 完整的迁移流程

```go
// 1. 启动迁移
coordinator.StartMigration(partitionID, "node-2")
// 状态转移: Owned → MigrationPending
// Guard: TargetOwner 必须设置且不能是当前所有者
// Action: 记录迁移开始时间

// 2. 当前所有者冻结新写入
oldOwner.Freeze(partitionID)
// 状态转移: MigrationPending → Frozen
// Guard: 检查迁移未超时
// Action: 停止新写入

// 3. 当前所有者刷盘所有待处理事件
oldOwner.Flush(partitionID)
// 状态转移: Frozen → Flushing
// Guard: 检查 checkpoint 完整性
// Action: 标记刷盘开始

// 4. 完成刷盘，发送转移请求
oldOwner.Transfer(partitionID)
// 状态转移: Flushing → Transferring
// Guard: 验证所有事件已刷盘
// Action: 发送转移信号到新所有者

// 5. 新所有者确认接收并开始处理
newOwner.Commit(partitionID)
// 状态转移: Transferring → Standby
// Guard: 新所有者必须已启动
// Action: 更新所有权，从 node-1 → node-2
```

### 失败和恢复流程

```go
// 任何阶段失败
currentOwner.Rollback(partitionID)
// 转移至 Failed 状态
// Action: 清除 TargetOwner，保持原所有者

// 解决问题后恢复
currentOwner.Recover(partitionID)
// 状态转移: Failed → Owned
// 现在可以重新尝试迁移
```

## Guard 条件（状态转移的保护条件）

每个状态转移都有对应的 Guard 条件，只有满足这些条件才能执行转移：

### Owned → MigrationPending
```go
// ✓ TargetOwner 必须已设置
// ✓ TargetOwner 不能等于 CurrentOwner
if fsm.metadata.TargetOwner == "" {
    return false, fmt.Errorf("TargetOwner not set")
}
if fsm.metadata.TargetOwner == fsm.metadata.CurrentOwner {
    return false, fmt.Errorf("Cannot migrate to same owner")
}
```

### MigrationPending → Frozen
```go
// ✓ 迁移必须未超时（默认 30s）
elapsed := time.Now().Unix() - fsm.metadata.MigrationStartTime
if elapsed > int64(fsm.migrationTimeout.Seconds()) {
    return false, fmt.Errorf("Migration timeout")
}
```

### Flushing → Transferring
```go
// ✓ 所有事件必须已持久化到 checkpoint
// ✓ Kafka offset 必须已同步
```

### Transferring → Standby
```go
// ✓ 新所有者必须已启动
// ✓ 新所有者已从检查点恢复
```

## 元数据追踪

### OwnershipMetadata 结构

```go
type OwnershipMetadata struct {
    PartitionID          string            // 分区 ID
    CurrentOwner         string            // 当前所有者
    PreviousOwner        string            // 前任所有者（用于 Standby）
    TargetOwner          string            // 迁移目标
    State                OwnershipState    // 当前状态
    Symbols              []string          // 负责的 symbol 列表
    LastProcessedSeq     int64             // 最后处理的全局序列号
    CheckpointSeq        int64             // 最后检查点的序列号
    KafkaOffset          map[string]int64  // 每个 symbol 的 Kafka offset
    Version              int64             // 版本号（并发控制）
    TransitionHistory    []StateTransition // 所有状态转移记录
}
```

### TransitionHistory（完整审计跟踪）

每次状态转移都被记录下来：

```go
type StateTransition struct {
    FromState  OwnershipState  // 源状态
    ToState    OwnershipState  // 目标状态
    Reason     string          // 转移原因
    Timestamp  int64           // 转移时间
    Actor      string          // 执行者（节点 ID）
}
```

示例转移历史：
```
1. Owned → MigrationPending (reason: StartMigration, actor: coordinator, time: 2026-09-19 17:00:00)
2. MigrationPending → Frozen (reason: Freeze, actor: node-1, time: 2026-09-19 17:00:05)
3. Frozen → Flushing (reason: Flush, actor: node-1, time: 2026-09-19 17:00:10)
4. Flushing → Transferring (reason: Transfer, actor: node-1, time: 2026-09-19 17:00:15)
5. Transferring → Standby (reason: Commit, actor: node-2, time: 2026-09-19 17:00:20)
```

## 并发访问安全性

`OwnershipStateMachine` 使用 `sync.RWMutex` 保护内部状态，确保：

1. **读写安全**: 所有元数据修改都在互斥锁保护下进行
2. **原子转移**: 状态转移是原子操作，不存在部分转移
3. **版本控制**: 每次修改都增加版本号，用于检测并发修改

```go
// 线程安全的读取
metadata := fsm.GetMetadata()  // 返回深拷贝

// 线程安全的转移
err := fsm.Transition(toState, reason, actor)  // 原子操作

// 并发更新序列号
fsm.UpdateLastProcessedSeq(seq)
fsm.UpdateCheckpoint(cpSeq)
fsm.UpdateKafkaOffset(symbol, offset)
```

## 监听器模式

应用程序可以订阅所有权变化事件：

```go
type OwnershipListener interface {
    OnOwnershipChange(old, new *OwnershipMetadata)
}

// 注册监听器
fsm.AddListener(myListener)

// 当状态转移时自动调用
listener.OnOwnershipChange(oldMetadata, newMetadata)
```

## 测试覆盖

State Machine 拥有 11 个全面的测试：

- ✅ **BasicTransition**: 基本初始化和状态验证
- ✅ **MigrationFlow**: 完整的迁移流程验证
- ✅ **MigrationFailure**: 失败和恢复流程
- ✅ **GuardConditions**: 转移条件的严格检查
- ✅ **SequenceUpdate**: 序列号单调性保证
- ✅ **KafkaOffsetTracking**: Kafka offset 跟踪
- ✅ **ConcurrentAccess**: 并发安全性测试
- ✅ **TransitionHistory**: 审计历史记录
- ✅ **OwnershipVerification**: 所有权验证
- ✅ **ListenerNotification**: 事件通知系统
- ✅ **StateTransitionDump**: 完整的状态转移报告

## 与其他组件的集成

### Event Ordering Invariant (Phase 4.3)
State Machine 的 `LastProcessedSeq` 确保：
- Event #100 → Node-1
- Event #101 → Node-1  
- Event #102 → Migration
- Event #103 → Node-2

即使 Node-2 先启动，也不会在 #102 之前处理 #103。

### Checkpoint-Kafka Boundary (Phase 4.4)
State Machine 维护：
```
Flushing → Checkpoint seq (N)
         → Kafka offset (N)
         → Transfer
```

两者必须在迁移边界对齐。

### Deterministic Recovery (Phase 4.5)
State Machine 的完整元数据允许：
- 从任何中间状态恢复
- 重放转移历史以验证一致性
- 检测并处理并发迁移冲突

## 性能特征

| 操作 | 时间复杂度 | 说明 |
|------|----------|------|
| Transition | O(1) | 状态转移为常数时间 |
| UpdateLastProcessedSeq | O(1) | 序列号更新为常数时间 |
| GetMetadata | O(n) | n = symbol 数量，需要深拷贝 |
| AddListener | O(1) | 监听器注册为常数时间 |

## 示例使用

```go
// 创建状态机
fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

// 执行迁移流程
if err := fsm.StartMigration("node-2", "coordinator"); err != nil {
    log.Error("Start migration failed:", err)
}

if err := fsm.Freeze("node-1"); err != nil {
    log.Error("Freeze failed:", err)
}

if err := fsm.Flush("node-1"); err != nil {
    log.Error("Flush failed:", err)
}

if err := fsm.Transfer("node-1"); err != nil {
    log.Error("Transfer failed:", err)
}

if err := fsm.Commit("node-2"); err != nil {
    log.Error("Commit failed:", err)
}

// 查询最终状态
metadata := fsm.GetMetadata()
if metadata.State == Standby && metadata.CurrentOwner == "node-2" {
    log.Info("Migration successful")
}

// 查看转移历史
history := fsm.GetTransitionHistory()
for _, transition := range history {
    log.Infof("%s → %s (%s)", transition.FromState, transition.ToState, transition.Reason)
}
```

## 下一步

Phase 4.2 将实现 **Ownership Transfer Protocol**，使用这个 State Machine 来：
1. 协调节点间的通信
2. 确保所有权转移的原子性
3. 处理网络分区和超时
