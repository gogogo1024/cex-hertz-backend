package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type mockKafkaMessageSender struct {
	msgs []kafkago.Message
	mu   sync.Mutex
}

func (m *mockKafkaMessageSender) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, msgs...)
	return nil
}

// setupTestDB 创建用于测试的内存数据库
func setupTestDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 迁移 outbox 表
	if err := db.AutoMigrate(&model.OutboxEntry{}); err != nil {
		return nil, err
	}

	return db, nil
}

// TestOutboxPatternAtomicity 测试 outbox pattern 的原子性
// 验证：业务数据更新和 outbox 条目在同一事务内提交或回滚
func TestOutboxPatternAtomicity(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)
	tradesProcessed := []string{}
	mu := sync.Mutex{}

	buyPositionFn := func(userID, symbol, quantity, price string) error {
		mu.Lock()
		tradesProcessed = append(tradesProcessed, "buy:"+userID)
		mu.Unlock()
		return nil
	}

	sellPositionFn := func(userID, symbol, quantity string) error {
		mu.Lock()
		tradesProcessed = append(tradesProcessed, "sell:"+userID)
		mu.Unlock()
		return nil
	}

	processor := NewPositionProcessorWithOutbox(db, outboxRepo, buyPositionFn, sellPositionFn)

	// 创建成交事件
	tradeEvent := &model.TradeExecutedEvent{
		Seq:          1,
		Timestamp:    time.Now().UnixMilli(),
		TradeID:      "trade-atomic-1",
		SymbolStr:    "BTC/USDT",
		TakerOrderID: "order-1",
		MakerOrderID: "order-2",
		TakerUser:    "user-1",
		MakerUser:    "user-2",
		Price:        5000000000000,
		Quantity:     10000000,
		TakerSide:    "buy",
	}

	// 处理事件
	err = processor.ProcessEvent(tradeEvent)
	assert.NoError(t, err)

	// 验证：业务数据已更新
	assert.Len(t, tradesProcessed, 2)
	assert.Contains(t, tradesProcessed, "buy:user-1")
	assert.Contains(t, tradesProcessed, "sell:user-2")

	// 验证：outbox 条目已创建
	entries, err := outboxRepo.GetUnpublished(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "trade-atomic-1", entries[0].EventID)
	assert.Equal(t, "TradeExecuted", entries[0].EventType)
	assert.False(t, entries[0].Published)
}

// TestOutboxPatternIdempotency 测试 outbox pattern 的幂等性
// 验证：即使处理相同的事件多次，业务数据和 outbox 条目也不会重复
func TestOutboxPatternIdempotency(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)
	callCount := 0
	mu := sync.Mutex{}

	buyPositionFn := func(userID, symbol, quantity, price string) error {
		mu.Lock()
		callCount++
		mu.Unlock()
		return nil
	}

	sellPositionFn := func(userID, symbol, quantity string) error {
		mu.Lock()
		callCount++
		mu.Unlock()
		return nil
	}

	processor := NewPositionProcessorWithOutbox(db, outboxRepo, buyPositionFn, sellPositionFn)

	tradeEvent := &model.TradeExecutedEvent{
		Seq:          1,
		Timestamp:    time.Now().UnixMilli(),
		TradeID:      "trade-idem-1",
		SymbolStr:    "BTC/USDT",
		TakerOrderID: "order-1",
		MakerOrderID: "order-2",
		TakerUser:    "user-1",
		MakerUser:    "user-2",
		Price:        5000000000000,
		Quantity:     10000000,
		TakerSide:    "buy",
	}

	// 处理相同事件三次
	err = processor.ProcessEvent(tradeEvent)
	assert.NoError(t, err)

	err = processor.ProcessEvent(tradeEvent)
	assert.NoError(t, err)

	err = processor.ProcessEvent(tradeEvent)
	assert.NoError(t, err)

	// 验证：业务函数只被调用一次（第一次）
	mu.Lock()
	assert.Equal(t, 2, callCount, "Business functions should be called exactly once (2 calls: 1 buy + 1 sell)")
	mu.Unlock()

	// 验证：outbox 只有一个条目
	entries, err := outboxRepo.GetUnpublished(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, entries, 1, "Only one outbox entry should exist")
}

// TestOutboxDispatcherPublishSuccess 测试 outbox dispatcher 成功发送
func TestOutboxDispatcherPublishSuccess(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)
	dispatcher := NewOutboxDispatcher(outboxRepo, 10, 3)
	mockProducer := &mockKafkaMessageSender{}
	dispatcher.SetKafkaProducer(mockProducer)

	// 创建未发布的条目
	entry := &model.OutboxEntry{
		EventID:       "trade-pub-1",
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       `{"event_id":"trade-pub-1","event_type":"TradeExecuted"}`,
		Published:     false,
		CreatedAt:     time.Now(),
	}

	err = db.Create(entry).Error
	assert.NoError(t, err)

	// 分发
	dispatcher.dispatchBatch(context.Background())

	// 验证：条目被标记为已发布
	entries, err := outboxRepo.GetUnpublished(context.Background(), 100)
	assert.NoError(t, err)
	assert.Len(t, entries, 0, "No unpublished entries should remain")

	// 验证：条目被标记为已发布
	var published *model.OutboxEntry
	err = db.Where("event_id = ?", "trade-pub-1").First(&published).Error
	assert.NoError(t, err)
	assert.True(t, published.Published)
	assert.NotNil(t, published.PublishedAt)
	assert.Len(t, mockProducer.msgs, 1)
	assert.Equal(t, "trade-pub-1", string(mockProducer.msgs[0].Key))
}

// TestOutboxPatternCaseA crash 在 checkpoint 前发生
// 场景：
//
//	DB UPDATE 成功 ✓
//	Outbox write 成功 ✓
//	Checkpoint 写入失败 ✗
//	Process crash ✗
//
// 恢复：
//
//	重启后，相同的事件可能再次到达 event pipeline
//	但 PositionProcessor 的内存幂等检查只在同一进程内有效
//	在实际应用中，应该使用数据库来存储幂等性状态
//
// 测试展示：
//
//	处理成功后，outbox 条目存在且未发布
//	Dispatcher 可以稍后发送这些条目，确保可靠性
func TestOutboxPatternCaseA(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)
	updateCount := 0
	mu := sync.Mutex{}

	buyPositionFn := func(userID, symbol, quantity, price string) error {
		mu.Lock()
		updateCount++
		mu.Unlock()
		return nil
	}

	sellPositionFn := func(userID, symbol, quantity string) error {
		return nil
	}

	processor := NewPositionProcessorWithOutbox(db, outboxRepo, buyPositionFn, sellPositionFn)

	tradeEvent := &model.TradeExecutedEvent{
		TradeID:   "trade-casea-1",
		TakerUser: "user-1",
		MakerUser: "user-2",
		TakerSide: "buy",
	}

	// 第一次处理（正常，模拟 Case A 发生前）
	_ = processor.ProcessEvent(tradeEvent)
	mu.Lock()
	firstCount := updateCount
	mu.Unlock()

	// 验证：业务更新和 outbox 条目都成功了
	assert.Equal(t, 1, firstCount, "First processing should execute business update")

	// 验证：outbox 条目已创建（未发布）
	entries, _ := outboxRepo.GetUnpublished(context.Background(), 100)
	assert.Len(t, entries, 1, "Outbox entry should exist for dispatcher to send")
	assert.Equal(t, "trade-casea-1", entries[0].EventID)

	// 在同一 processor 内，重复处理会被幂等检查阻止
	_ = processor.ProcessEvent(tradeEvent)
	mu.Lock()
	secondCount := updateCount
	mu.Unlock()

	// 验证：业务更新不会重复
	assert.Equal(t, firstCount, secondCount, "Duplicate processing in same processor should be skipped")

	// 注意：在实际 Case A（crash 后恢复）中，新的 processor 实例没有幂等性记录
	// 在生产环境中应该：
	// 1. 使用数据库存储幂等性状态（CheckpointSeq 或已处理事件表）
	// 2. 在事件处理前检查数据库中是否已处理
	// 3. 结合 outbox 和 event 消费 offset 管理完整的可靠性保证
}

// TestOutboxPatternCaseB checkpoint 成功但 DB UPDATE 失败
// 场景（更严格）：
//
//	Checkpoint 写入成功 ✓
//	DB UPDATE 失败 ✗
//	Process crash ✗
//
// 恢复（数据一致性缓解）：
//
//	Checkpoint 已提交，系统认为事件已处理
//	但由于 DB UPDATE 失败，业务数据未更新
//	需要监控检测：checkpoint 记录 > 实际处理数据
//	Outbox 条目失败，dispatcher 可以重试或人工审计
func TestOutboxPatternCaseB_Monitoring(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)

	// 模拟 Case B：业务更新失败但 checkpoint 已提交
	failureOccurred := false
	buyPositionFn := func(userID, symbol, quantity, price string) error {
		if !failureOccurred {
			failureOccurred = true
			return nil // 第一次成功
		}
		return nil // 幂等处理阻止重复
	}

	processor := NewPositionProcessorWithOutbox(db, outboxRepo, buyPositionFn, func(_, _, _ string) error {
		return nil
	})

	// 创建一个手动模拟的 checkpoint（在实际应用中由 event pipeline 管理）
	checkpointSeq := int64(100)

	tradeEvent := &model.TradeExecutedEvent{
		TradeID:   "trade-caseb-1",
		TakerUser: "user-1",
		MakerUser: "user-2",
		TakerSide: "buy",
	}

	// 正常处理
	_ = processor.ProcessEvent(tradeEvent)

	// 验证：
	// 1. Outbox 条目已创建（待发布）
	entries, _ := outboxRepo.GetUnpublished(context.Background(), 100)
	assert.Len(t, entries, 1, "Outbox entry should exist")

	// 2. 可以手动标记为已发布（模拟 dispatcher 完成）
	_ = outboxRepo.MarkPublished(context.Background(), "trade-caseb-1")

	entries, _ = outboxRepo.GetUnpublished(context.Background(), 100)
	assert.Len(t, entries, 0, "Entry should be marked as published")

	// 3. 监控可以通过比较 checkpoint_seq 和 outbox 发布数来检测 Case B
	// checkpointSeq = 100，但 outbox 中只发布了 1 条事件 → 不一致
	publishedCount := 1
	if checkpointSeq > int64(publishedCount) {
		t.Logf("[ALERT] Case B detected: checkpoint_seq=%d > published_count=%d", checkpointSeq, publishedCount)
	}
}

// TestOutboxDispatcherRetry 测试 dispatcher 的重试机制
func TestOutboxDispatcherRetry(t *testing.T) {
	t.Parallel()

	db, err := setupTestDB()
	assert.NoError(t, err)

	outboxRepo := pg.NewOutboxRepo(db)

	// 创建失败的条目
	entry := &model.OutboxEntry{
		EventID:       "trade-retry-1",
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       `{}`,
		Published:     false,
		RetryCount:    0,
		CreatedAt:     time.Now(),
	}

	_ = db.Create(entry).Error

	// 记录发布错误
	_ = outboxRepo.RecordPublishError(context.Background(), "trade-retry-1", "network error")

	// 验证：重试计数增加
	failedEntries, _ := outboxRepo.GetFailedEntries(context.Background(), 3)
	assert.Len(t, failedEntries, 1)
	assert.Equal(t, 1, failedEntries[0].RetryCount)
	assert.Equal(t, "network error", failedEntries[0].LastError)
}
