package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegerArithmetic 验证整数算术的正确性
func TestIntegerArithmetic(t *testing.T) {
	// 测试场景：没有浮点误差
	// 0.1 + 0.2 = 0.3（而不是 0.30000000000000004）

	// 10^8 个最小单位 = 1.0
	qty1 := model.QuantityInNano(1_0000_0000) // 1.0
	qty2 := model.QuantityInNano(2_0000_0000) // 2.0

	sum := qty1 + qty2
	assert.Equal(t, model.QuantityInNano(3_0000_0000), sum)

	// 1000 次循环相加
	var total model.QuantityInNano
	for i := 0; i < 1000; i++ {
		total += model.QuantityInNano(1_0000_0000)
	}
	assert.Equal(t, model.QuantityInNano(1000_0000_0000), total)
}

// TestOrderBookMatching 验证 OrderBook 的撮合逻辑
func TestOrderBookMatching(t *testing.T) {
	sequencer := NewSequencer()
	ob := NewOrderBook("BTC/USD", sequencer)

	// 测试场景：
	// 1. 添加一个卖单：@100, qty=1.0
	// 2. 提交一个买单：@100, qty=1.0
	// 3. 验证完全成交，没有浮点误差

	// 卖单
	sellOrder := &OrderBookEntry{
		OrderID:     "sell-001",
		UserID:      "maker",
		Symbol:      "BTC/USD",
		Side:        "sell",
		Price:       100_0000_0000, // $100
		Quantity:    1_0000_0000,   // 1.0 BTC
		SubmitTime:  time.Now().UnixMilli(),
		SequenceNum: sequencer.NextGlobalSeq(),
	}

	_, remainSell := ob.MatchOrder(sellOrder)
	assert.Equal(t, model.QuantityInNano(1_0000_0000), remainSell)

	// 买单
	buyOrder := &OrderBookEntry{
		OrderID:     "buy-001",
		UserID:      "taker",
		Symbol:      "BTC/USD",
		Side:        "buy",
		Price:       100_0000_0000, // $100
		Quantity:    1_0000_0000,   // 1.0 BTC
		SubmitTime:  time.Now().UnixMilli(),
		SequenceNum: sequencer.NextGlobalSeq(),
	}

	trades, remainBuy := ob.MatchOrder(buyOrder)

	// 验证
	assert.Equal(t, 1, len(trades))
	assert.Equal(t, model.QuantityInNano(0), remainBuy)

	trade := trades[0].(*model.TradeExecutedEvent)
	assert.Equal(t, "BTC/USD", trade.Symbol())
	assert.Equal(t, model.PriceInNano(100_0000_0000), trade.Price)
	assert.Equal(t, model.QuantityInNano(1_0000_0000), trade.Quantity)
	assert.Equal(t, "taker", trade.TakerUser)
	assert.Equal(t, "maker", trade.MakerUser)
}

// TestDepthAggregation 验证深度聚合的正确性
func TestDepthAggregation(t *testing.T) {
	sequencer := NewSequencer()
	ob := NewOrderBook("BTC/USD", sequencer)

	// 测试场景：
	// 同一价位有多个订单，深度应该聚合
	// 100 -> 0.5 BTC (order 1)
	// 100 -> 0.3 BTC (order 2)
	// 100 -> 0.2 BTC (order 3)
	// 聚合应该是：100 -> 1.0 BTC

	for i := 1; i <= 3; i++ {
		var qty model.QuantityInNano
		switch i {
		case 1:
			qty = 5000_0000 // 0.5
		case 2:
			qty = 3000_0000 // 0.3
		case 3:
			qty = 2000_0000 // 0.2
		}

		order := &OrderBookEntry{
			OrderID:     fmt.Sprintf("buy-%d", i),
			UserID:      fmt.Sprintf("user-%d", i),
			Symbol:      "BTC/USD",
			Side:        "buy",
			Price:       100_0000_0000,
			Quantity:    qty,
			SubmitTime:  time.Now().UnixMilli(),
			SequenceNum: sequencer.NextGlobalSeq(),
		}

		_, _ = ob.MatchOrder(order)
	}

	// 获取深度
	bids, _ := ob.GetDepth(10)

	// 验证
	assert.Equal(t, 1, len(bids))
	assert.Equal(t, "100.00000000", bids[0]["price"])
	assert.Equal(t, "1.00000000", bids[0]["quantity"]) // 聚合后的总量
}

// TestEventPipeline_Idempotency 验证事件管道的幂等性
func TestEventPipeline_Idempotency(t *testing.T) {
	eventLog := NewInMemoryEventLog()
	pipeline := NewEventPipeline(eventLog)

	// 创建一个简单的测试处理器
	var processedCount int
	testProcessor := &testProcessor{
		processCount: &processedCount,
	}

	pipeline.RegisterProcessor(testProcessor)

	// 创建事件
	event := &model.TradeExecutedEvent{
		Seq:       1,
		Timestamp: time.Now().UnixMilli(),
		TradeID:   "trade-001",
		SymbolStr: "BTC/USD",
		Price:     100_0000_0000,
		Quantity:  1_0000_0000,
	}

	// 处理相同事件两次
	err1 := pipeline.ProcessEvent(event)
	err2 := pipeline.ProcessEvent(event)

	// 第二次应该被 skip（幂等性）
	require.Nil(t, err1)
	require.Nil(t, err2)
	assert.Equal(t, 1, processedCount) // 只被处理了一次
}

// TestEventLog_Recovery 验证从 EventLog 恢复的能力
func TestEventLog_Recovery(t *testing.T) {
	eventLog := NewInMemoryEventLog()

	// 提交几个事件
	events := []*model.OrderSubmittedEvent{
		{
			Seq:       1,
			OrderID:   "ord-001",
			SymbolStr: "BTC/USD",
			UserID:    "user-1",
			Status:    "submitted",
		},
		{
			Seq:       2,
			OrderID:   "ord-002",
			SymbolStr: "ETH/USD",
			UserID:    "user-2",
			Status:    "submitted",
		},
	}

	for _, e := range events {
		err := eventLog.AppendEvent(e)
		require.Nil(t, err)
	}

	// 从 EventLog 恢复
	recovered, err := eventLog.GetEventsBySymbol("BTC/USD", 0, 10)
	require.Nil(t, err)
	assert.Equal(t, 1, len(recovered))

	// 验证事件内容
	recoveredEvent, _ := model.UnmarshalEvent(recovered[0])
	ordEvent := recoveredEvent.(*model.OrderSubmittedEvent)
	assert.Equal(t, "ord-001", ordEvent.OrderID)
}

// TestPositionProcessor_Idempotency 验证持仓处理的幂等性
func TestPositionProcessor_Idempotency(t *testing.T) {
	var updateLog []string

	buyFn := func(userID, symbol, qty, price string) error {
		updateLog = append(updateLog, fmt.Sprintf("BUY:%s:%s:%s", userID, symbol, qty))
		return nil
	}

	sellFn := func(userID, symbol, qty string) error {
		updateLog = append(updateLog, fmt.Sprintf("SELL:%s:%s:%s", userID, symbol, qty))
		return nil
	}

	processor := NewPositionProcessor(buyFn, sellFn)

	// 创建成交事件
	trade := &model.TradeExecutedEvent{
		Seq:          1,
		TradeID:      "trade-001",
		SymbolStr:    "BTC/USD",
		TakerOrderID: "ord-1",
		MakerOrderID: "ord-2",
		TakerUser:    "user-1",
		MakerUser:    "user-2",
		Price:        100_0000_0000,
		Quantity:     1_0000_0000,
		TakerSide:    "buy",
	}

	// 处理相同事件两次
	err1 := processor.ProcessEvent(trade)
	err2 := processor.ProcessEvent(trade)

	require.Nil(t, err1)
	require.Nil(t, err2)

	// 验证：只进行了一次更新
	assert.Equal(t, 2, len(updateLog)) // 一个 BUY，一个 SELL
	assert.Equal(t, "BUY:user-1:BTC/USD:1.00000000", updateLog[0])
	assert.Equal(t, "SELL:user-2:BTC/USD:1.00000000", updateLog[1])

	// 重置并再处理一次
	updateLog = make([]string, 0)
	processor.Reset()

	err3 := processor.ProcessEvent(trade)
	require.Nil(t, err3)
	assert.Equal(t, 2, len(updateLog)) // 又进行了一次更新（因为重置了）
}

// testProcessor 用于测试的处理器
type testProcessor struct {
	processCount *int
}

func (tp *testProcessor) ProcessEvent(event model.MatchingEngineEvent) error {
	*tp.processCount++
	return nil
}

func (tp *testProcessor) ProcessorName() string {
	return "TestProcessor"
}

// BenchmarkOrderBookMatching 性能基准测试
func BenchmarkOrderBookMatching(b *testing.B) {
	sequencer := NewSequencer()
	ob := NewOrderBook("BTC/USD", sequencer)

	// 预先添加一些卖单
	for i := 0; i < 100; i++ {
		order := &OrderBookEntry{
			OrderID:     fmt.Sprintf("sell-%d", i),
			UserID:      "maker",
			Symbol:      "BTC/USD",
			Side:        "sell",
			Price:       (100_0000_0000 + model.PriceInNano(i)),
			Quantity:    1_0000_0000,
			SubmitTime:  time.Now().UnixMilli(),
			SequenceNum: sequencer.NextGlobalSeq(),
		}
		ob.MatchOrder(order)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buyOrder := &OrderBookEntry{
			OrderID:     fmt.Sprintf("buy-%d", i),
			UserID:      "taker",
			Symbol:      "BTC/USD",
			Side:        "buy",
			Price:       (100_0000_0000 + model.PriceInNano(i%100)),
			Quantity:    1_0000_0000,
			SubmitTime:  time.Now().UnixMilli(),
			SequenceNum: sequencer.NextGlobalSeq(),
		}
		ob.MatchOrder(buyOrder)
	}
}
