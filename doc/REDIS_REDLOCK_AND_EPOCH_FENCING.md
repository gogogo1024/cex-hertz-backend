# Redis 分布式锁与 Epoch Fencing 实装指南

## 概述

本方案使用 **Redis Redlock** + **Epoch Fencing** 实现两个关键的分布式系统保证：

1. **Event Processing Lock**: 防止同一事件被并发处理
2. **Ownership Epoch Fencing**: 防止 stale write（旧所有者继续处理订单）

## 依赖库

```go
// go.mod
require (
    github.com/go-redsync/redsync/v4 v4.x.x
    github.com/redis/go-redis/v9 v9.x.x
)
```

安装：
```bash
go get github.com/go-redsync/redsync/v4
go get github.com/redis/go-redis/v9
```

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Redis Redlock                          │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────┐       ┌──────────────────┐            │
│  │ RedisLockMgr     │       │ OwnershipEpoch   │            │
│  │ ────────────────│       │ Manager          │            │
│  │ WithLock()       │       │ ──────────────── │            │
│  │ TryLock()        │◄──────┤ GetEpoch()       │            │
│  │ IsLocked()       │       │ UpdateOwnership()│            │
│  └──────────────────┘       └──────────────────┘            │
│         △                            △                       │
│         │ wraps                      │ uses                  │
│         │                            │                       │
│  ┌──────────────────────────────────────────┐               │
│  │   RecoveryExecutor                       │               │
│  │   ──────────────────                     │               │
│  │   recoverSingleProcessor() (locked)      │               │
│  └──────────────────────────────────────────┘               │
│                                                               │
│  ┌──────────────────────────────────────────┐               │
│  │   OrderHandler                           │               │
│  │   ──────────────                         │               │
│  │   SubmitOrder() + fencing check          │               │
│  └──────────────────────────────────────────┘               │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. RedisLockManager 

`biz/service/redis_lock_manager.go`

**用途**: 优雅封装 redsync，提供简洁的分布式锁API

**关键方法**:
- `WithLock()` - 阻塞式获取锁，执行函数后自动解锁
- `TryLock()` - 非阻塞尝试获取锁
- `IsLocked()` - 检查锁状态（用于监控）

**便利函数**:
- `WithEventProcessingLock()` - 事件处理锁
- `WithPartitionOwnershipUpdate()` - 分区所有者更新锁
- `WithRecoveryLock()` - 恢复操作锁

### 2. OwnershipEpochManager

`biz/service/ownership_epoch_manager.go`

**用途**: 管理分区所有权的版本号（epoch）

**Redis 存储结构**:
```
Key: partition:{partitionID}:ownership
Hash Fields:
  - epoch: 当前版本号（单调递增）
  - owner: 当前所有者节点ID
  - updated_at: 最后更新时间戳
TTL: 24小时
```

**关键方法**:
- `GetEpoch()` - 快速获取当前epoch
- `GetOwnershipInfo()` - 获取完整所有权信息
- `UpdateOwnershipEpoch()` - 递增epoch并更新所有者
- `VerifyOwnershipFencing()` - 检查是否被fencing

### 3. EventContext 扩展

`biz/model/event_context.go`

添加所有权元数据：
```go
type EventContext struct {
    // ... 原有字段 ...
    
    // NEW: 用于Fencing检查
    OwnershipEpoch int64  // 当前所有者的epoch
    ProcessingNode string // 处理节点ID
}

// 链式API
eventCtx.WithOwnershipEpoch(epoch, nodeID)
```

### 4. RecoveryExecutor 改进

`biz/service/recovery_executor.go`

使用分布式锁保护恢复过程：
```go
type RecoveryExecutor struct {
    // ... 原有字段 ...
    lockMgr *RedisLockManager  // NEW
}

func (re *RecoveryExecutor) recoverSingleProcessor(...) {
    return result, re.lockMgr.WithRecoveryLock(
        ctx, processorName, symbol,
        func(ctx context.Context) error {
            // 恢复逻辑（在锁保护下）
        },
    )
}
```

### 5. OrderHandler Fencing

`biz/handler/order_fencing.go`

新增两个核心函数：
- `VerifyOrderOwnershipFencing()` - 验证当前节点是否还拥有分区
- `OrderOwnershipInterceptor()` - 拦截器形式的fencing检查

## 集成步骤

### Step 1: 初始化组件 (main.go)

```go
package main

import (
    "github.com/gogogo1024/cex-hertz-backend/biz/dal/redis"
    "github.com/gogogo1024/cex-hertz-backend/biz/service"
)

func initServices() {
    // 已有的初始化...
    
    // NEW: 初始化 RedisLockManager
    lockMgr := service.NewRedisLockManager(redis.Client)
    
    // NEW: 初始化 OwnershipEpochManager
    epochMgr := service.NewOwnershipEpochManager(lockMgr)
    
    // 更新 RecoveryExecutor 初始化
    recoveryExecutor := service.NewRecoveryExecutor(
        matchEngine,
        checkpointMgr,
        eventLog,
        lockMgr,  // NEW
    )
    
    // 保存到全局或注入到handler
    g.lockMgr = lockMgr
    g.epochMgr = epochMgr
}
```

### Step 2: 在订单处理中添加 Epoch Fencing

```go
// biz/handler/order.go
func SubmitOrder(ctx context.Context, c *app.RequestContext) {
    var req model.Order
    if err := c.BindAndValidate(&req); err != nil {
        c.JSON(consts.StatusBadRequest, ...)
        return
    }
    
    // NEW: 验证所有权Epoch
    processingCtx, err := handler.VerifyOrderOwnershipFencing(
        ctx,
        g.epochMgr,                // OwnershipEpochManager
        req.Symbol,
        fmt.Sprintf("partition:%s", req.Symbol),  // partitionID
        req.CachedEpoch,           // 从请求中获取
        g.NodeID,                  // 当前节点ID
    )
    
    if err != nil {
        c.JSON(consts.StatusInternalServerError, ...)
        return
    }
    
    // 检查是否被 fencing
    if processingCtx.IsFenced {
        c.JSON(409, map[string]interface{}{  // 409 Conflict
            "error": "PARTITION_MIGRATED",
            "message": processingCtx.ErrorMsg,
            "current_epoch": processingCtx.CurrentEpoch,
        })
        return
    }
    
    // 继续原有的订单处理...
    id, err := util.GenerateOrderID()
    if err != nil {
        c.JSON(consts.StatusInternalServerError, ...)
        return
    }
    // ...
}
```

### Step 3: 在事件处理中填充 Epoch

```go
// EventPipeline 中（通常在 Kafka consumer）
func (ep *EventPipeline) ProcessEventFromKafka(ctx context.Context, event MatchingEngineEvent, metadata KafkaMetadata) error {
    
    // 创建 EventContext
    eventCtx := model.NewEventContext(
        event,
        metadata.Topic,
        metadata.Partition,
        metadata.Offset,
    )
    
    // NEW: 填充所有权Epoch
    partitionID := fmt.Sprintf("partition:%s", event.Symbol())
    err := epochMgr.FillEventContextWithEpoch(ctx, eventCtx, partitionID, g.NodeID)
    if err != nil {
        return fmt.Errorf("failed to fill epoch: %w", err)
    }
    
    // 处理事件
    return ep.ProcessEventWithContext(eventCtx)
}
```

### Step 4: 在分区迁移时更新 Epoch

```go
// PartitionManager.go 中
func (pm *PartitionManager) CompleteOwnershipTransfer(
    partitionID string,
    oldOwner string,
    newOwner string,
) error {
    // NEW: 递增epoch
    newEpoch, err := epochMgr.UpdateOwnershipEpoch(ctx, partitionID, newOwner)
    if err != nil {
        return fmt.Errorf("failed to update epoch: %w", err)
    }
    
    hlog.Infof("Partition %s ownership transferred: %s -> %s (epoch %d)", 
        partitionID, oldOwner, newOwner, newEpoch)
    
    // 继续原有的分区迁移逻辑...
    return pm.updatePartitionTable(...)
}
```

## 客户端集成

### 客户端需要缓存 Epoch

客户端应该在订单请求中携带当前的 epoch：

```json
{
    "order_id": "12345",
    "symbol": "BTC/USDT",
    "side": "buy",
    "price": "43000.50",
    "quantity": "1.0",
    "cached_epoch": 1
}
```

### 处理 409 响应

当收到 409 Conflict 时，客户端应该：

```javascript
// 伪代码
if (response.status === 409) {
    const error = await response.json();
    if (error.error === 'PARTITION_MIGRATED') {
        // 更新本地缓存的epoch
        cachedEpoch = error.current_epoch;
        
        // 重试订单
        return submitOrder(order);
    }
}
```

## 监控和诊断

### 获取诊断信息

```go
// 订单处理器中
diagnostics := handler.GetOwnershipDiagnostics(
    ctx,
    epochMgr,
    []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"},
)

// 返回:
// {
//   "BTC/USDT": {
//     "epoch": 5,
//     "owner": "node-1",
//     "timestamp": 1234567890
//   },
//   ...
// }
```

### 监控锁争用

```go
// 检查是否有锁争用
isLocked, err := lockMgr.IsLocked(ctx, "recovery:lock:processor:BTC/USDT")
if isLocked {
    hlog.Warnf("Recovery lock is contended for BTC/USDT")
}
```

## 性能特性

| 操作 | 延迟 | 说明 |
|------|------|------|
| GetEpoch() | ~1ms | Redis 单次 HGET |
| UpdateOwnershipEpoch() | ~5ms | 需要获取分布式锁 |
| WithLock() | 可配置 | 默认5秒过期，最多3次重试 |
| WithRecoveryLock() | 可配置 | 30分钟过期，不重试 |

## 错误处理

### 常见错误场景

| 场景 | 错误 | 处理 |
|------|------|------|
| Redis 连接失败 | 获取epoch失败 | 返回 500 错误，记录告警 |
| 获取锁超时 | lock failed | 等待后重试（最多N次） |
| Epoch 不匹配 | fencing | 返回 409，提示分区已迁移 |
| 恢复被打断 | context timeout | 释放锁，标记恢复失败 |

### 推荐的错误响应

```go
// 400: 参数错误
c.JSON(400, map[string]interface{}{
    "error": "INVALID_REQUEST",
    "message": err.Error(),
})

// 409: 冲突（fencing）
c.JSON(409, map[string]interface{}{
    "error": "PARTITION_MIGRATED",
    "message": "Partition ownership has changed",
    "current_epoch": 5,
})

// 500: 服务错误
c.JSON(500, map[string]interface{}{
    "error": "INTERNAL_ERROR",
    "message": err.Error(),
})
```

## 测试

### 单元测试示例

```go
func TestRedisLockManager_WithLock(t *testing.T) {
    lockMgr := NewRedisLockManager(redisClient)
    
    executed := false
    err := lockMgr.WithLock(ctx, "test:key", func(ctx context.Context) error {
        executed = true
        return nil
    })
    
    assert.NoError(t, err)
    assert.True(t, executed)
}

func TestOwnershipEpochManager_UpdateEpoch(t *testing.T) {
    epochMgr := NewOwnershipEpochManager(lockMgr)
    
    // 初始 epoch
    epoch1, _ := epochMgr.UpdateOwnershipEpoch(ctx, "p1", "node-1")
    assert.Equal(t, int64(1), epoch1)
    
    // 更新 owner，epoch 递增
    epoch2, _ := epochMgr.UpdateOwnershipEpoch(ctx, "p1", "node-2")
    assert.Equal(t, int64(2), epoch2)
    
    // 相同 owner，epoch 不变
    epoch3, _ := epochMgr.UpdateOwnershipEpoch(ctx, "p1", "node-2")
    assert.Equal(t, int64(2), epoch3)
}
```

### 集成测试

参考已有的 `biz/service/startup_orchestration_test.go`，可以添加 Redlock 和 Epoch 的集成测试。

## 常见问题

**Q: Epoch 如何处理网络分区？**
A: Redlock 本身就是为了处理网络分区。如果发生分区，得不到多数 Redis 实例的锁的节点会被隔离，不会处理事件。

**Q: 旧所有者的缓存如何失效？**
A: 通过 Epoch 检查。当发现 epoch 不匹配时，立即拒绝处理（返回409）。客户端应该感知到迁移，更新本地 epoch。

**Q: Recovery 期间如何处理？**
A: Recovery 操作持有分布式锁，其他节点无法同时恢复同一个 symbol。锁的 TTL 是30分钟。

**Q: Redis 故障时怎么办？**
A: 系统会回退到原有的内存状态机。在生产环境应该部署 Redis Cluster 或 Sentinel。

## 生产部署检查清单

- [ ] Redis 部署完成且有冗余（Cluster/Sentinel）
- [ ] RecoveryExecutor 已集成 RedisLockManager
- [ ] OrderHandler 已添加 Epoch Fencing 检查
- [ ] EventPipeline 会填充 EventContext 的 OwnershipEpoch
- [ ] 分区迁移流程会更新 Redis Epoch
- [ ] 客户端支持处理 409 Conflict 响应
- [ ] 监控告警已配置（锁争用、Epoch 异常等）
- [ ] 压测验证通过（多分区并发迁移场景）
- [ ] 灾难恢复演习完成
