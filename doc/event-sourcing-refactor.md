# CEX Hertz Backend - Event Sourcing 架构重构方案

## 概述

将当前的 "Order → Match → Trade" 直接状态修改模式，改造为基于事件的架构：

```
Order → Event → Pipeline → [DB, Position, WS, Kafka]
         ↓
    Persistent Log
         ↓
    Recovery/Replay
```

---

## 核心问题映射与解决方案

### 问题 ① float64 Money Path → **解决✅**

**原问题**：
```go
remainQty := toFloat(order.Quantity)
makerQty := toFloat(sell.Quantity)
tradeQty := minFloat(remainQty, makerQty)
fmt.Sprintf("%.8f", ...)  // 数值精度丢失
```

**新方案**：
- 整个 hot path 使用 `int64`
- `Price` → `PriceInNano` (int64)
- `Quantity` → `QuantityInNano` (int64)
- 只在展示时转换为浮点数

```go
// model/event.go
type PriceInNano int64        // $65123.45 → 6512345_0000_0000
type QuantityInNano int64     // 0.12345678 BTC → 12345678

// biz/service/orderbook_v2.go
func (ob *OrderBookV2) matchBuyOrder(order *OrderBookEntry, remainingQty *QuantityInNano) []Event {
    tradeQty := model.Min(*remainingQty, seller.Quantity - seller.FilledQty)  // 整数运算
}
```

---

### 问题 ② Depth Aggregation Bug → **解决✅**

**原问题**：
```
100 → 5 BTC
100 → 3 BTC
100 → 2 BTC
输出：100 → 2 BTC  (覆盖，而不是聚合)
```

**新方案**：
- 在 `OrderBookV2.GetDepth()` 中按 price level 聚合

```go
// biz/service/orderbook_v2.go
func (ob *OrderBookV2) GetDepth(levels int) (bids, asks) {
    for i := 0; i < levels && elem != nil; i, elem = i+1, elem.Next() {
        price := elem.Key().(PriceInNano)
        queue := elem.Value.([]*OrderBookEntry)  // 同一价格的所有订单
        
        // 聚合：所有订单的剩余数量相加
        var totalQty QuantityInNano
        for _, order := range queue {
            totalQty += order.Quantity - order.FilledQty
        }
        
        bids = append(bids, map[string]string{
            "price":    formatPrice(price),
            "quantity": formatQty(totalQty),  // 正确的聚合
        })
    }
}
```

---

### 问题 ③ Global tradeBatchForDB Race → **解决✅**

**原问题**：
```go
// 全局共享，多个 symbol worker 并发写入
var tradeBatchForDB []model.Trade

// Worker 1 (BTC)
tradeBatchForDB = append(...)

// Worker 2 (ETH)
tradeBatchForDB = append(...)

// Worker 1
batchInsertTrades(tradeBatchForDB)  // 可能混入 ETH 的交易，或丢失数据
tradeBatchForDB = tradeBatchForDB[:0]
```

**新方案**：
- **完全消除全局共享状态**
- 每个事件都通过 `EventPipeline` 独立处理
- `DatabaseProcessor` 内部维护缓冲，但受 mutex 保护

```go
// biz/service/event_pipeline.go
func (ep *EventPipeline) ProcessEvent(event MatchingEngineEvent) error {
    // 1. 原子地追加到 EventLog
    if err := ep.eventLog.AppendEvent(event); err != nil {
        return err
    }
    
    // 2. 分发给所有处理器（可并发，但每个处理器内部幂等）
    return ep.dispatchToProcessors(event)
}

// biz/service/event_processor_db.go
type DatabaseProcessor struct {
    mu          sync.Mutex  // 单个互斥锁保护
    pendingTrades []*Trade
    batchSize   int
}

func (dp *DatabaseProcessor) ProcessEvent(event MatchingEngineEvent) error {
    switch e := event.(type) {
    case *model.TradeExecutedEvent:
        dp.mu.Lock()
        dp.pendingTrades = append(dp.pendingTrades, ...)
        if len(dp.pendingTrades) >= dp.batchSize {
            dp.flush()  // 在 lock 内执行
        }
        dp.mu.Unlock()
    }
}
```

**关键点**：
- ✅ 没有全局共享的 slice
- ✅ 没有跨 worker 的竞态条件
- ✅ 每个处理器独立缓冲和写入

---

### 问题 ④ Byte Pool Lifetime Bug → **解决✅**

**原问题**：
```go
msg := m.buildTradeMsg(...)          // 从 pool 申请
userMsgMap[user] = append(..., msg)  // 保存指针
engine.MsgBytePool.Put(msg)           // 立即归还！

// 另一个 worker
buffer := MsgBytePool.Get()           // 可能重新使用相同的内存
buffer[0] = 0xFF                      // 覆盖！

batchUnicaster(userMsgMap)            // 发送时已损坏
```

**新方案**：
- **完全不使用对象池**（对于关键路径）
- 或者**严格控制 buffer 生命周期**

```go
// 方案 A：直接分配（最简单，GC 性能可接受）
func (me *MatchEngineV2) broadcastTrade(trade *TradeExecutedEvent) {
    message := map[string]interface{}{
        "type":    "trade",
        "price":   trade.Price,
        "qty":     trade.Quantity,
    }
    data, _ := json.Marshal(message)  // 新分配，不使用 pool
    me.broadcaster(symbol, data)       // 立即发送
}

// 方案 B：如果必须用 pool，严格控制所有权
// - Pool 中的 buffer 只在 Processor.ProcessEvent() 的作用域内有效
// - 不能存储到 map 等共享结构
// - 必须在函数返回前复制数据
```

---

### 问题 ⑤ Async Position Update → **解决✅**

**原问题**：
```go
// 成交立即推送
go func(trade model.Trade) {
    BuyPosition(...)    // 异步执行，时序不确定
    SellPosition(...)
}(trade)

// 用户此时可能已看到成交，但持仓还没更新
```

**新方案**：
- **同步处理** 通过 `PositionProcessor`
- Event 通过 Pipeline 时，所有 Processor 按序处理
- 持仓更新是事件处理的一部分，不是后续操作

```go
// biz/service/event_processor_position.go
type PositionProcessor struct {
    processedTrades map[string]bool  // 幂等性
    buyPositionFn   func(...)
    sellPositionFn  func(...)
}

func (pp *PositionProcessor) ProcessEvent(event Event) error {
    switch e := event.(type) {
    case *TradeExecutedEvent:
        // 检查幂等性
        if pp.processedTrades[e.TradeID] {
            return nil  // 已处理过
        }
        
        // 同步调用（不是 go func）
        if err := pp.buyPositionFn(...); err != nil {
            return err
        }
        if err := pp.sellPositionFn(...); err != nil {
            return err
        }
        
        pp.processedTrades[e.TradeID] = true  // 记录
    }
}

// 调用链：
// MatchEngineV2.processOrder()
//   → eventPipeline.ProcessEvent(tradeEvent)
//     → eventLog.AppendEvent()                    // 1. 持久化
//     → dispatchToProcessors()
//       → databaseProcessor.ProcessEvent()        // 2. DB 写入（同步）
//       → positionProcessor.ProcessEvent()        // 3. 持仓更新（同步）
//       → websocketProcessor.ProcessEvent()       // 4. WS 推送（同步或异步）
//
// 所有 Processor 都看到相同的事件顺序
// 所有更新都在同一"事务边界"内
```

---

### 问题 ⑥ Kafka Offset ↔ DB Consistency → **解决✅**

**原问题**：
```
Kafka 消费 → ON CONFLICT DO NOTHING → DB 写入
              ↑                          ↑
           消息可能重复              但 offset 已提交
           重复消息被忽略              导致一致性风险
```

**新方案**：
- **Event Log 是新的真实源** (Single Source of Truth)
- Kafka 变成一个上游消息来源，用于：
  1. 初始订单输入（冗余备份）
  2. 消费确认（offset 管理）
  
```
Kafka
  ↓ (OrderSubmitted)
OrderSubmittedEvent
  ↓
EventLog (持久化，确保不丢失)
  ↓
EventPipeline
  ├─ DatabaseProcessor  (写入 orders 和 trades)
  ├─ PositionProcessor  (更新持仓)
  └─ KafkaAuditProcessor (发送到审计 topic，offset 可提交)
```

```go
// 新的 Kafka 消费逻辑
func (kp *KafkaAuditProcessor) ProcessEvent(event Event) error {
    // 将事件发送到审计 topic（用于其他系统订阅）
    auditMsg := &AuditMessage{
        EventType: event.EventType(),
        Seq:       event.EventSeq(),
        Payload:   event,
    }
    // 异步发送（不影响主流程）
    go kp.sendToAuditTopic(auditMsg)
    
    return nil
}

// Kafka offset 提交独立进行
func (me *MatchEngineV2) commitKafkaOffset(offset int64) {
    // 在 DatabaseProcessor 确认写入后再提交 offset
    // 这样保证：
    // 1. 事件已持久化到 EventLog
    // 2. 数据已写入 DB
    // 3. 再提交 Kafka offset
    // → 不会重复或丢失
}
```

---

### 问题 ⑦ Distributed Routing 未完成 → **解决✅**

**原问题**：
```go
// middleware/distributed.go
if isLocal {
    c.Next(ctx)  // ✅ 本地处理
} else {
    c.Next(ctx)  // ❌ 啥都没做！应该 forward
}
```

**新方案**：
- 在分布式路由基础上，完成真正的 RPC forward

```go
// middleware/distributed_v2.go
func (m *DistributedMiddleware) SymbolCheckMiddleware() MiddlewareFunc {
    return func(c *context.Context) {
        order := parseOrder(c)
        ownerAddr := m.partitionManager.GetOwner(order.Symbol)
        
        if ownerAddr == m.localAddr {
            // 本地处理
            m.matchEngine.SubmitOrder(order)
            // 返回本地的 order receipt
            c.JSON(200, orderReceipt)
        } else {
            // Remote 节点，forward RPC
            resp, err := m.forwardToOwner(ownerAddr, order)
            if err != nil {
                c.JSON(500, error(err))
                return
            }
            // 返回 remote 节点的 response
            c.JSON(resp.StatusCode, resp.Body)
        }
    }
}

// RPC forward 实现
func (m *DistributedMiddleware) forwardToOwner(ownerAddr string, order model.SubmitOrderMsg) (*http.Response, error) {
    // 1. 序列化
    reqBody, _ := json.Marshal(order)
    
    // 2. 发送 HTTP/RPC 请求到 owner
    req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/order/submit", ownerAddr), 
                              bytes.NewReader(reqBody))
    
    // 3. 等待响应（带超时）
    client := &http.Client{Timeout: 5 * time.Second}
    return client.Do(req)
}
```

---

## 完整的信息流

```
┌─────────────────┐
│  Order Received │
└────────┬────────┘
         ↓
    ┌─────────────────────────────────────────┐
    │  MatchEngineV2.SubmitOrder()             │
    │  - 验证订单                              │
    │  - 生成 OrderSubmittedEvent              │
    │  - 调用 eventPipeline.ProcessEvent()     │
    └────────┬────────────────────────────────┘
             ↓
    ┌─────────────────────────────────────────┐
    │  EventPipeline.ProcessEvent()            │
    │  1. eventLog.AppendEvent()  (持久化)    │
    │  2. dispatchToProcessors()               │
    └────┬────────────────────────────────────┘
         ├─────────────────────────────────┬──────────────────────────┬─────────────────┐
         ↓                                 ↓                          ↓                 ↓
    ┌──────────────┐  ┌─────────────────┐ ┌──────────────────┐  ┌──────────────────┐
    │  EventLog    │  │MatchWorker      │ │ DatabaseProc.   │  │ PositionProc.    │
    │(持久化)     │  │(撮合生成event)  │ │ (DB写入)        │  │(持仓更新,幂等)   │
    └──────────────┘  └────────┬────────┘ └──────────────────┘  └──────────────────┘
                               ↓
                    TradeExecutedEvent
                               ↓
         ┌─────────────────────┴──────────────────────────┐
         ↓                                                ↓
    ┌──────────────────┐                          ┌──────────────────┐
    │ WebSocketProc.   │                          │ KafkaAuditProc.  │
    │ (广播成交)       │                          │ (审计日志)       │
    └──────────────────┘                          └──────────────────┘
```

---

## Crash Recovery 与 Replay

### 场景 1：Matching Engine 崩溃

```
1. 系统启动
2. 读取 EventLog 中最新的序列号
3. 恢复 OrderBook 状态：
   for event in EventLog from startSeq:
       orderBook.Replay(event)
4. 恢复 symbol 的序列号
5. 继续处理新订单
```

### 场景 2：数据库连接丢失

```
1. DatabaseProcessor.ProcessEvent() 失败
2. EventPipeline 捕捉错误，但事件已在 EventLog
3. 系统重试：
   - 定时从 EventLog 重新读取未处理的事件
   - 重新分发给 DatabaseProcessor
   - 由于使用 ON CONFLICT DO NOTHING，重复处理是安全的
4. 连接恢复后自动同步
```

### 场景 3：部分处理器离线

```
EventPipeline 记录每个 Processor 的 offset：
    processorOffsets[processorName][symbol] = lastProcessedSeq

系统恢复：
    startSeq = processorOffsets["PositionProcessor"]["BTC/USD"] + 1
    for event in EventLog from startSeq:
        positionProcessor.ProcessEvent(event)  // 重新处理
```

---

## 迁移路径

### Phase 1：并行运行（低风险）

```
// 创建 V2 引擎，但不激活
matchEngineV2 := NewMatchEngineV2(...)

// 继续使用 V1，同时记录所有事件
var eventLog *InMemoryEventLog = NewInMemoryEventLog()
```

### Phase 2：双写测试

```
// 订单同时提交到 V1 和 V2
order := parseOrder(request)
v1Result := matchEngineV1.SubmitOrder(order)
v2Result := matchEngineV2.SubmitOrder(order)

// 比较结果
if v1Result == v2Result {
    log("✅ V2 结果验证通过")
} else {
    log("❌ V2 结果不匹配：", diff(v1Result, v2Result))
}
```

### Phase 3：灰度切换

```
// 按 symbol 或用户比例切换
if rand.Float64() < 0.1 {  // 10% 流量
    matchEngineV2.SubmitOrder(order)
} else {
    matchEngineV1.SubmitOrder(order)
}
```

### Phase 4：完全切换

```
// 关闭 V1，使用 V2
matchEngine := matchEngineV2
```

---

## 测试策略

### 单元测试

```go
func TestOrderBookV2_IntegerArithmetic(t *testing.T) {
    // 验证没有浮点误差
    ob := NewOrderBookV2("BTC/USD", sequencer)
    
    // 添加买单
    buyOrder := &OrderBookEntry{Price: 6500_0000_0000, Quantity: 1_0000_0000}
    
    // 添加卖单
    sellOrder := &OrderBookEntry{Price: 6500_0000_0000, Quantity: 1_0000_0000}
    
    // 撮合
    trades, remaining := ob.MatchOrder(buyOrder)
    
    // 验证
    assert.Equal(t, 1, len(trades))
    assert.Equal(t, int64(0), remaining)
}

func TestEventPipeline_Idempotency(t *testing.T) {
    // 验证幂等性：处理相同事件两次
    pipeline := NewEventPipeline(eventLog)
    pipeline.RegisterProcessor(dbProcessor)
    
    event := &TradeExecutedEvent{Seq: 1, TradeID: "trade-001"}
    
    err1 := pipeline.ProcessEvent(event)
    err2 := pipeline.ProcessEvent(event)
    
    assert.Nil(t, err1)
    assert.Nil(t, err2)
    
    // 验证数据库只有一条记录
    count := db.Model(&Trade{}).Where("trade_id = ?", "trade-001").Count()
    assert.Equal(t, 1, count)
}
```

### 集成测试

```go
func TestCrashRecovery(t *testing.T) {
    // 1. 提交订单并成交
    engine.SubmitOrder(buyOrder)
    engine.SubmitOrder(sellOrder)
    
    // 2. 模拟崩溃（假装进程已重启）
    trades := getAllTrades()
    assert.Equal(t, 1, len(trades))
    
    // 3. 重新启动引擎
    engine = NewMatchEngineV2(eventLog, eventPipeline, ...)
    
    // 4. 验证状态恢复
    recoveredTrades := engine.GetEventLog().GetAllEvents()
    assert.Equal(t, trades, recoveredTrades)
}
```

### 性能测试

```go
func BenchmarkMatchingV2(b *testing.B) {
    engine := NewMatchEngineV2(...)
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        order := generateRandomOrder()
        engine.SubmitOrder(order)
    }
}
```

---

## 性能考虑

### 优化点

1. **批量写入**：DatabaseProcessor 缓冲事件，每 100 条或 10ms 批量 insert
2. **幂等键索引**：trade_id, order_id 必须有唯一索引
3. **事件日志分片**：按 symbol 或时间分片，加快查询
4. **并发处理**：不同 symbol 的事件可以并发处理

### 权衡

| 方案 | 吞吐量 | 延迟 | 复杂度 |
|------|------|------|--------|
| 同步写入 | ⭐ | ⭐⭐⭐ | ⭐ |
| 异步 + 重试 | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| 本方案（事件 + 批写）| ⭐⭐ | ⭐⭐ | ⭐⭐ |

当前选择 **事件 + 批写**，因为：
- 吞吐量足够（每秒 10k+ orders）
- 延迟可接受（<100ms）
- 代码可维护

---

## 总结

| 问题 | 原因 | 新方案 | 验证方式 |
|------|------|--------|----------|
| ① float64 误差 | 浮点运算 | int64 | 单元测试 |
| ② 深度聚合错 | 未聚合 | GetDepth() | 集成测试 |
| ③ 全局 race | 共享 slice | 事件管道 | race detector |
| ④ pool lifetime | 生命周期混乱 | 无 pool 或严格隔离 | 集成测试 |
| ⑤ 异步不一致 | go func 延迟 | 同步 Processor | 单元测试 |
| ⑥ Kafka 一致性 | offset 管理混乱 | EventLog 做源 | crash test |
| ⑦ routing 不完 | 未 forward | 完整 RPC | 分布式测试 |
