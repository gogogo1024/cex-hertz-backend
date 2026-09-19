package pg

import (
	"testing"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupEventStoreDB 创建用于测试的事件存储数据库
func setupEventStoreDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 迁移事件表
	if err := db.AutoMigrate(&model.PersistentEvent{}); err != nil {
		return nil, err
	}

	return db, nil
}

// TestWriteEvent 测试写入单个事件
func TestWriteEvent(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	event := &model.PersistentEvent{
		GlobalSeq:      1,
		EventType:      "TradeExecuted",
		AggregateID:    "BTC/USDT",
		AggregateType:  "OrderBook",
		Symbol:         "BTC/USDT",
		Payload:        `{"price": 50000, "quantity": 1.0}`,
		EventTimestamp: 1609459200000,
		Version:        1,
	}

	err = store.WriteEvent(event)
	assert.NoError(t, err)

	// 验证事件已保存
	var saved *model.PersistentEvent
	_ = db.Where("global_seq = ?", 1).First(&saved)
	assert.NotNil(t, saved)
	assert.Equal(t, "TradeExecuted", saved.EventType)
}

// TestWriteEventsBatch 测试批量写入事件
func TestWriteEventsBatch(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	events := []*model.PersistentEvent{
		{
			GlobalSeq:      1,
			EventType:      "TradeExecuted",
			AggregateID:    "BTC/USDT",
			AggregateType:  "OrderBook",
			Symbol:         "BTC/USDT",
			Payload:        `{"price": 50000}`,
			EventTimestamp: 1609459200000,
		},
		{
			GlobalSeq:      2,
			EventType:      "TradeExecuted",
			AggregateID:    "ETH/USDT",
			AggregateType:  "OrderBook",
			Symbol:         "ETH/USDT",
			Payload:        `{"price": 3000}`,
			EventTimestamp: 1609459201000,
		},
	}

	err = store.WriteEventsBatch(events)
	assert.NoError(t, err)

	// 验证事件已保存
	var count int64
	_ = db.Model(&model.PersistentEvent{}).Count(&count)
	assert.Equal(t, int64(2), count)
}

// TestGetAggregateEvents 测试查询特定聚合根的事件
func TestGetAggregateEvents(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入不同聚合根的事件
	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq:     1,
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
	})

	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq:     2,
		EventType:     "TradeExecuted",
		AggregateID:   "ETH/USDT",
		AggregateType: "OrderBook",
	})

	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq:     3,
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
	})

	// 查询 BTC/USDT 的事件
	events, err := store.GetAggregateEvents("OrderBook", "BTC/USDT")
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, int64(1), events[0].GlobalSeq)
	assert.Equal(t, int64(3), events[1].GlobalSeq)
}

// TestGetSymbolEvents 测试按符号查询事件
func TestGetSymbolEvents(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入事件
	for i := int64(1); i <= 5; i++ {
		_ = store.WriteEvent(&model.PersistentEvent{
			GlobalSeq:      i,
			EventType:      "TradeExecuted",
			AggregateID:    "BTC/USDT",
			AggregateType:  "OrderBook",
			Symbol:         "BTC/USDT",
			EventTimestamp: 1609459200000 + i*1000,
		})
	}

	// 查询序列号 2-4 的事件
	events, err := store.GetSymbolEvents("BTC/USDT", 2, 4)
	assert.NoError(t, err)
	assert.Len(t, events, 3)
	assert.Equal(t, int64(2), events[0].GlobalSeq)
	assert.Equal(t, int64(4), events[2].GlobalSeq)
}

// TestGetGlobalEventStream 测试全局事件流查询
func TestGetGlobalEventStream(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入 10 个事件
	for i := int64(1); i <= 10; i++ {
		_ = store.WriteEvent(&model.PersistentEvent{
			GlobalSeq:      i,
			EventType:      "TradeExecuted",
			AggregateID:    "test",
			AggregateType:  "OrderBook",
			EventTimestamp: 1609459200000 + i*1000,
		})
	}

	// 从序列号 5 开始，获取 3 条事件
	events, err := store.GetGlobalEventStream(5, 3)
	assert.NoError(t, err)
	assert.Len(t, events, 3)
	assert.Equal(t, int64(5), events[0].GlobalSeq)
	assert.Equal(t, int64(7), events[2].GlobalSeq)
}

// TestGetLatestGlobalSeq 测试获取最新序列号
func TestGetLatestGlobalSeq(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 初始状态，应该是 0
	seq, _ := store.GetLatestGlobalSeq()
	assert.Equal(t, int64(0), seq)

	// 写入事件
	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq: 100,
	})

	seq, _ = store.GetLatestGlobalSeq()
	assert.Equal(t, int64(100), seq)
}

// TestCountEvents 测试事件计数
func TestCountEvents(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入 5 条事件
	for i := 1; i <= 5; i++ {
		_ = store.WriteEvent(&model.PersistentEvent{
			GlobalSeq:     int64(i),
			AggregateID:   "BTC/USDT",
			AggregateType: "OrderBook",
		})
	}

	count, err := store.CountEvents("OrderBook", "BTC/USDT")
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

// TestGetEventsByType 测试按事件类型查询
func TestGetEventsByType(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入不同类型的事件
	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq: 1,
		EventType: "TradeExecuted",
	})

	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq: 2,
		EventType: "OrderCancelled",
	})

	_ = store.WriteEvent(&model.PersistentEvent{
		GlobalSeq: 3,
		EventType: "TradeExecuted",
	})

	// 查询 TradeExecuted 事件
	events, err := store.GetEventsByType("TradeExecuted", 10)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
}

// TestGetUnprocessedEvents 测试获取未处理的事件
func TestGetUnprocessedEvents(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 插入 5 条事件
	for i := int64(1); i <= 5; i++ {
		_ = store.WriteEvent(&model.PersistentEvent{
			GlobalSeq: i,
		})
	}

	// 获取序列号 > 2 的事件
	events, err := store.GetUnprocessedEvents(2, 10)
	assert.NoError(t, err)
	assert.Len(t, events, 3)
	assert.Equal(t, int64(3), events[0].GlobalSeq)
	assert.Equal(t, int64(5), events[2].GlobalSeq)
}

// TestEventStoreReplay 测试事件重放（模拟恢复）
func TestEventStoreReplay(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 模拟一系列交易事件
	trades := []struct {
		seq      int64
		symbol   string
		price    int64
		quantity float64
	}{
		{1, "BTC/USDT", 50000, 1.0},
		{2, "BTC/USDT", 50100, 0.5},
		{3, "BTC/USDT", 49900, 2.0},
	}

	for _, trade := range trades {
		_ = store.WriteEvent(&model.PersistentEvent{
			GlobalSeq:     trade.seq,
			EventType:     "TradeExecuted",
			AggregateID:   trade.symbol,
			AggregateType: "OrderBook",
			Symbol:        trade.symbol,
		})
	}

	// 模拟恢复：获取所有事件并重放
	events, err := store.GetGlobalEventStream(1, 100)
	assert.NoError(t, err)
	assert.Len(t, events, 3)

	// 验证事件顺序
	for i, event := range events {
		assert.Equal(t, int64(i+1), event.GlobalSeq)
	}
}

// TestEventStoreAtomicity 测试事件存储的原子性
// 如果批量写入中途失败，应该全部回滚
func TestEventStoreAtomicity(t *testing.T) {
	t.Parallel()

	db, err := setupEventStoreDB()
	assert.NoError(t, err)

	store := NewPostgresPersistentEventStore(db)

	// 创建会导致违反约束的事件（同一 global_seq）
	events := []*model.PersistentEvent{
		{GlobalSeq: 1, EventType: "Type1"},
		{GlobalSeq: 1, EventType: "Type2"}, // 违反 UNIQUE 约束
	}

	// 写入应该失败（并回滚）
	err = store.WriteEventsBatch(events)
	assert.Error(t, err, "Should fail due to unique constraint violation")

	// 验证没有事件被写入
	count, _ := store.CountEvents("", "")
	assert.Equal(t, int64(0), count, "All events should be rolled back")
}
