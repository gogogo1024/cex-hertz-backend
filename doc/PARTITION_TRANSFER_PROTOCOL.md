# Phase 4.2: Partition Ownership Transfer Protocol

## 核心问题

当分区从 Node-1 迁移到 Node-2 时，如何确保：

1. **事件不丢失**: 未处理的事件必须完整转移
2. **顺序不变**: Event #100 → #101 → #102 的顺序必须保持
3. **不重复**: 没有事件被处理两次
4. **确定性**: 即使发生网络分区或崩溃，也能确定恢复

## 关键决策：事件何时算 "Committed"

```
Coordinator → StartTransfer → Source (Node-1) → Freeze → Flush → Checkpoint
                                                                      ↓
                                                              [此时 Committed]
                                                                      ↓
                                                         Transfer → Target (Node-2)
                                                                      ↓
                                                              Resume Processing
```

**Committed 的定义**: 当 Event 在源节点被持久化到 PostgreSQL Checkpoint，且目标节点从该 Checkpoint 启动时。

## 协议消息类型

### 1. TransferRequest (Coordinator → Source)
```protobuf
message TransferRequest {
    string partition_id = 1;
    string source_owner = 2;      // 当前所有者 (Node-1)
    string target_owner = 3;      // 新所有者 (Node-2)
    int64 timeout_ms = 4;         // 转移超时 (30s)
    int32 max_retries = 5;        // 最大重试次数 (3)
}

// Response
message TransferResponse {
    bool accepted = 1;
    string reason = 2;            // 如果拒绝，说明原因
    int64 checkpoint_seq = 3;     // 源节点当前 checkpoint seq
}
```

### 2. FreezeSignal (Source → Partition Event Handler)
```protobuf
message FreezeSignal {
    string partition_id = 1;
    int64 freeze_timestamp = 2;   // 冻结时间点
}

// Action: 停止接受该分区的新写入
// 已有的事件继续处理，但新的 Kafka 消息被缓冲
```

### 3. FlushAck (Source → Coordinator)
```protobuf
message FlushAck {
    string partition_id = 1;
    int64 last_processed_seq = 2;  // 最后处理的全局序列号
    int64 checkpoint_seq = 3;      // Checkpoint 的序列号
    map<string, int64> kafka_offsets = 4;  // 每个 symbol 的 offset
    string source_owner = 5;
}

// Invariant: checkpoint_seq ≤ last_processed_seq
// 含义: 所有已处理的事件都已持久化到检查点
```

### 4. TransferSignal (Coordinator → Target)
```protobuf
message TransferSignal {
    string partition_id = 1;
    string source_owner = 2;
    string target_owner = 3;
    int64 start_from_seq = 4;      // 从该序列号开始处理
    int64 start_from_checkpoint = 5; // 从该 checkpoint 恢复
    map<string, int64> kafka_offsets = 6;
}

// Action: 
// 1. 将 partition 从 "Standby" 转换为 "Owned"
// 2. 从 checkpoint 恢复状态
// 3. 从 Kafka offset 继续消费
```

### 5. CommitSignal (Coordinator → Source)
```protobuf
message CommitSignal {
    string partition_id = 1;
    string target_owner = 2;
    bool success = 3;              // 目标节点是否已启动
}

// Action: 更新 State Machine 状态
```

## 完整的协议流程

```
时间  Coordinator     Source(Node-1)        Target(Node-2)        Kafka
|                         [Owned]            [StandBy/Offline]
|
├─1─→ TransferRequest    [Owned]
│     (start transfer)        ↓
│                      [MigrationPending]
│
├─2─ Freeze Signal        [Frozen]
│    (stop new writes)        ↓
│                      [Frozen]
│     (buffer new msgs)
│
├─3─ Flush Events         [Flushing]
│                             ↓
│                      [Flushing]
│    (drain pending)
│    (write to Checkpoint)
│                             ↓
│
├─4─  FlushAck ←───────   [Flushing Complete]
│     checkpoint_seq=1000
│     kafka_offset={BTC:500, ETH:600}
│
├─5─→ TransferSignal  →──────────────→  [Owned]
│     start_from_seq=1000         (recover from checkpoint)
│                                       ↓
│                                  [Owned]
│
├─6─ CommitSignal ←─────────────────  Ready ✓
│     success=true
│                      [Standby]
│                          ↓
│    Ownership transferred ✓
│
└──→ Kafka Resume  ←─────────────────  Consume from offset
     (BTC:501, ETH:601)
```

## 状态转移时间轴

### Source Node (Node-1) 状态变化

```
时刻    状态              说明
────────────────────────────────────────────────
T0    Owned             正常处理分区 partition-1
      ↓
T1    MigrationPending  收到 TransferRequest
      ↓
T2    Frozen            停止新写入，缓冲 Kafka 消息
      ↓
T3    Flushing          处理所有缓冲事件到 checkpoint
      ↓
T4    Transferring      等待目标节点确认
      ↓
T5    Standby           转移完成，可选:重新启动为备份
```

### Target Node (Node-2) 状态变化

```
时刻    状态              说明
────────────────────────────────────────────────
T0-T4  Offline          等待转移信号
      ↓
T5    Owned             收到 TransferSignal
      ↓    从 checkpoint 恢复状态
      ↓    从 Kafka offset 继续消费
T6    Processing        开始处理下一个事件 #1001
```

## 事件连续性保证

### 关键不变式 (Invariants)

1. **全局序列号单调性**
   ```
   Event #1000 (last from Node-1) → Checkpoint
   Event #1001 (first from Node-2) → from Kafka
   GlobalSeq: 1000 < 1001 ✓
   ```

2. **检查点对齐**
   ```
   Source: checkpoint_seq = 1000
           last_processed_seq = 1000
           
   Target: Recover from checkpoint_seq = 1000
           Next event seq >= 1001
   ```

3. **Kafka Offset 同步**
   ```
   BTC/USDT offset = 500  (last processed in Node-1)
   
   Node-2 consumption starts at offset = 501
   (不会重复处理 offset 500)
   ```

## 失败恢复场景

### 场景 1: 源节点崩溃（在 Flushing 中）
```
状态: Source[Flushing], Target[Offline]

恢复步骤:
1. Coordinator 检测到超时
2. Rollback to Failed 状态
3. 源节点重启后自动恢复到 Owned
4. 重新开始转移
```

### 场景 2: 目标节点启动失败
```
状态: Source[Transferring], Target[启动失败]

恢复步骤:
1. Coordinator 检测到超时
2. 发送 CommitSignal(success=false)
3. 源节点 Rollback to Owned
4. 清除 Kafka 消息缓冲
5. 目标节点修复后，重新启动转移
```

### 场景 3: 网络分区（Coordinator 与 Source/Target 断开）
```
状态: 所有节点认为转移在进行中

恢复步骤:
1. 网络恢复时，Coordinator 检查状态
2. 如果 Source 已转入 Standby，确认转移成功
3. 如果 Source 仍在 Transferring，检查 Target 是否启动
4. 根据实际状态恢复一致性
```

## 实现要点

### Phase 4.2a: 消息定义和序列化
- [ ] 定义所有 5 种消息类型
- [ ] 实现 Protocol Buffers 序列化
- [ ] 实现网络传输层（gRPC 或 HTTP）

### Phase 4.2b: 协调器 (Coordinator)
- [ ] 实现转移请求处理
- [ ] 实现超时检测和重试
- [ ] 实现状态机状态同步

### Phase 4.2c: 源节点消息处理 (Source Handler)
- [ ] 实现冻结逻辑
- [ ] 实现刷盘和检查点生成
- [ ] 实现 FlushAck 响应

### Phase 4.2d: 目标节点消息处理 (Target Handler)
- [ ] 实现 TransferSignal 接收
- [ ] 实现从检查点恢复
- [ ] 实现 Kafka offset 同步

### Phase 4.2e: 完整协议测试
- [ ] 单一成功路径测试
- [ ] 各种失败场景测试
- [ ] 并发迁移测试
- [ ] 网络分区恢复测试

## 与 State Machine 的集成

```go
// Coordinator 使用 State Machine 追踪状态
fsm := ownershipStateMachines[partitionID]

// 1. 启动转移
if err := fsm.StartMigration(targetOwner, "coordinator"); err != nil {
    return err  // TargetOwner 无效
}

// 2. 发送 FreezeSignal 到源节点
source.Freeze(fsm.GetMetadata())

// 3. 等待 FlushAck
flushAck := receiveFlushAck(...)
if err := fsm.Flush("source"); err != nil {
    return err
}

// 4. 发送 TransferSignal 到目标节点
target.Transfer(fsm.GetMetadata(), flushAck)

// 5. 等待目标节点启动
if targetReady := waitForTargetReady(...); targetReady {
    if err := fsm.Commit(targetOwner); err != nil {
        return err
    }
}
```

## 性能目标

- 单次转移时间: < 30s (包括冻结、刷盘、转移)
- 事件处理中断时间: < 1s (Frozen 状态)
- 目标节点恢复时间: < 5s (从 checkpoint)
- 吞吐量损失: < 1% (缓冲期间)

## 下一步 (Phase 4.3+)

Phase 4.2 完成后：
- **Phase 4.3**: 实现事件顺序不变式检查
- **Phase 4.4**: 实现检查点-Kafka 边界同步
- **Phase 4.5**: 实现确定性恢复机制
- **Phase 4.6**: 集成测试验证完整的迁移流程
