package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// setupPostgresEventLogTestDB 为测试创建内存 SQLite 数据库
func setupPostgresEventLogTestDB() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic("Failed to setup test database: " + err.Error())
	}

	// 创建 events 表
	if err := db.AutoMigrate(&model.PersistentEvent{}); err != nil {
		panic("Failed to migrate events table: " + err.Error())
	}

	return db
}

// TestPostgresEventLog_AppendEvent 验证事件追加
func TestPostgresEventLog_AppendEvent(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	// 创建一个交易事件
	event := &model.TradeExecutedEvent{
		Seq:          1,
		Timestamp:    1000,
		TradeID:      "TRADE-001",
		SymbolStr:    "BTC/USDT",
		TakerOrderID: "ORDER-1",
		MakerOrderID: "ORDER-2",
		TakerUser:    "user1",
		MakerUser:    "user2",
		Price:        50000 * 1e8, // 50000 in nano
		Quantity:     1 * 1e8,      // 1 in nano
		TakerSide:    "buy",
	}

	// 追加事件
	err := eventLog.AppendEvent(event)
	assert.NoError(t, err, "AppendEvent should succeed")

	// 验证事件被保存
	events, err := eventLog.GetEventsBySymbol("BTC/USDT", 0, 10)
	assert.NoError(t, err, "GetEventsBySymbol should succeed")
	assert.Equal(t, 1, len(events), "Should have 1 event")
	assert.Equal(t, "trade_executed", events[0].EventType, "Event type should be trade_executed")
}

// TestPostgresEventLog_GetEventsBySymbol 验证按 symbol 查询
func TestPostgresEventLog_GetEventsBySymbol(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	// 添加多个事件
	symbols := []string{"BTC/USDT", "BTC/USDT", "ETH/USDT", "BTC/USDT"}
	for i, symbol := range symbols {
		event := &model.TradeExecutedEvent{
			Seq:          uint64(i + 1),
			Timestamp:    int64(1000 + i),
			TradeID:      "TRADE-" + string(rune(48+i)),
			SymbolStr:    symbol,
			TakerOrderID: "ORDER-" + string(rune(48+i)),
			MakerOrderID: "ORDER-" + string(rune(48+i*10)),
			TakerUser:    "user1",
			MakerUser:    "user2",
			Price:        50000 * 1e8,
			Quantity:     1 * 1e8,
			TakerSide:    "buy",
		}
		err := eventLog.AppendEvent(event)
		assert.NoError(t, err)
	}

	// 查询 BTC/USDT 的事件
	btcEvents, err := eventLog.GetEventsBySymbol("BTC/USDT", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(btcEvents), "Should have 3 BTC/USDT events")

	// 查询 ETH/USDT 的事件
	ethEvents, err := eventLog.GetEventsBySymbol("ETH/USDT", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ethEvents), "Should have 1 ETH/USDT event")
}

// TestPostgresEventLog_GetLatestSeq 验证获取最新序列号
func TestPostgresEventLog_GetLatestSeq(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	// 初始应该是 0
	seq, err := eventLog.GetLatestSeq("BTC/USDT")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "Initial seq should be 0")

	// 添加一个事件
	event := &model.TradeExecutedEvent{
		Seq:          1,
		Timestamp:    1000,
		TradeID:      "TRADE-001",
		SymbolStr:    "BTC/USDT",
		TakerOrderID: "ORDER-1",
		MakerOrderID: "ORDER-2",
		TakerUser:    "user1",
		MakerUser:    "user2",
		Price:        50000 * 1e8,
		Quantity:     1 * 1e8,
		TakerSide:    "buy",
	}
	err = eventLog.AppendEvent(event)
	assert.NoError(t, err)

	// 再次查询，应该返回非零值
	seq, err = eventLog.GetLatestSeq("BTC/USDT")
	assert.NoError(t, err)
	assert.Greater(t, seq, uint64(0), "Seq should be greater than 0 after appending event")
}

// TestPostgresEventLog_GetEventsSinceTime 验证按时间查询
func TestPostgresEventLog_GetEventsSinceTime(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	// 添加多个事件，时间戳不同
	for i := 0; i < 3; i++ {
		event := &model.TradeExecutedEvent{
			Seq:          uint64(i + 1),
			Timestamp:    int64(1000 + i*100),
			TradeID:      "TRADE-" + string(rune(48+i)),
			SymbolStr:    "BTC/USDT",
			TakerOrderID: "ORDER-1",
			MakerOrderID: "ORDER-2",
			TakerUser:    "user1",
			MakerUser:    "user2",
			Price:        50000 * 1e8,
			Quantity:     1 * 1e8,
			TakerSide:    "buy",
		}
		err := eventLog.AppendEvent(event)
		assert.NoError(t, err)
	}

	// 查询时间戳 >= 1100 的事件
	events, err := eventLog.GetEventsSinceTime("BTC/USDT", 1100)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events), "Should have 2 events with timestamp >= 1100")
}

// TestPostgresEventLog_Persistence 验证数据持久化
// 模拟重启场景：创建新的 EventLog 实例，应该能读到之前的数据
func TestPostgresEventLog_Persistence(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	// 创建第一个 EventLog 实例并添加事件
	eventLog1 := NewPostgresEventLog(eventStore, sequencer)
	event := &model.TradeExecutedEvent{
		Seq:          1,
		Timestamp:    1000,
		TradeID:      "TRADE-001",
		SymbolStr:    "BTC/USDT",
		TakerOrderID: "ORDER-1",
		MakerOrderID: "ORDER-2",
		TakerUser:    "user1",
		MakerUser:    "user2",
		Price:        50000 * 1e8,
		Quantity:     1 * 1e8,
		TakerSide:    "buy",
	}
	err := eventLog1.AppendEvent(event)
	assert.NoError(t, err)

	// 创建第二个 EventLog 实例（模拟重启）
	// 使用相同的 sequencer 确保 LocalSeq 连续
	eventStore2 := pg.NewPostgresPersistentEventStore(db)
	eventLog2 := NewPostgresEventLog(eventStore2, sequencer)

	// 应该能读到之前的事件
	events, err := eventLog2.GetEventsBySymbol("BTC/USDT", 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events), "Should retrieve persisted event")
	assert.Equal(t, "trade_executed", events[0].EventType)
}

// TestPostgresEventLog_MultipleSymbols 验证多交易对支持
func TestPostgresEventLog_MultipleSymbols(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}

	// 为每个交易对添加事件
	for _, symbol := range symbols {
		for i := 0; i < 2; i++ {
			event := &model.TradeExecutedEvent{
				Seq:          uint64(i + 1),
				Timestamp:    1000,
				TradeID:      symbol + "-TRADE-" + string(rune(48+i)),
				SymbolStr:    symbol,
				TakerOrderID: "ORDER-1",
				MakerOrderID: "ORDER-2",
				TakerUser:    "user1",
				MakerUser:    "user2",
				Price:        50000 * 1e8,
				Quantity:     1 * 1e8,
				TakerSide:    "buy",
			}
			err := eventLog.AppendEvent(event)
			assert.NoError(t, err)
		}
	}

	// 验证每个交易对的事件数
	for _, symbol := range symbols {
		events, err := eventLog.GetEventsBySymbol(symbol, 0, 100)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(events), "Each symbol should have 2 events")
	}
}

// TestPostgresEventLog_OrderPreservation 验证事件顺序
func TestPostgresEventLog_OrderPreservation(t *testing.T) {
	db := setupPostgresEventLogTestDB()
	eventStore := pg.NewPostgresPersistentEventStore(db)
	sequencer := NewSequencer()

	eventLog := NewPostgresEventLog(eventStore, sequencer)

	// 添加多个事件
	const eventCount = 5
	for i := 0; i < eventCount; i++ {
		event := &model.TradeExecutedEvent{
			Seq:          uint64(i + 1),
			Timestamp:    int64(1000 + i),
			TradeID:      "TRADE-" + string(rune(48+i)),
			SymbolStr:    "BTC/USDT",
			TakerOrderID: "ORDER-1",
			MakerOrderID: "ORDER-2",
			TakerUser:    "user1",
			MakerUser:    "user2",
			Price:        50000 * 1e8,
			Quantity:     1 * 1e8,
			TakerSide:    "buy",
		}
		err := eventLog.AppendEvent(event)
		assert.NoError(t, err)
	}

	// 查询所有事件
	events, err := eventLog.GetEventsBySymbol("BTC/USDT", 0, eventCount*2)
	assert.NoError(t, err)
	assert.Equal(t, eventCount, len(events), "Should have all events")

	// 验证顺序是否按 GlobalSeq 递增
	for i := 1; i < len(events); i++ {
		assert.LessOrEqual(t, events[i-1].Seq, events[i].Seq,
			"Events should be ordered by Seq in ascending order")
	}
}
