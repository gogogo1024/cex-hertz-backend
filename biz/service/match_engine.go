package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// MatchEngine - 改进的撮合引擎，基于 Event Sourcing
// 核心特点：
// 1. 所有状态变化都通过事件表达
// 2. 支持幂等性和 replay
// 3. 完整的 crash recovery 能力
// 4. 使用 int64 精确计算
type MatchEngine struct {
	// 每个 symbol 一个队列和一个 worker
	orderQueues sync.Map // symbol -> chan OrderQueueItem

	// 每个 symbol 一个 OrderBook
	orderBooks sync.Map // symbol -> *OrderBookV2

	// 事件管道
	eventPipeline *EventPipeline

	// 事件日志
	eventLog model.EventStore

	// 序列号生成器
	sequencer *Sequencer

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// WebSocket 广播函数
	broadcaster func(symbol string, data []byte)
	unicaster   func(userID string, data []byte)
}

// OrderQueueItem 订单队列中的项
type OrderQueueItem struct {
	orderMsg model.SubmitOrderMsg
	// 其他元数据可以添加在这里
}

// NewMatchEngine 创建一个新的撮合引擎
func NewMatchEngine(
	eventPipeline *EventPipeline,
	eventLog model.EventStore,
	sequencer *Sequencer,
	broadcaster func(symbol string, data []byte),
	unicaster func(userID string, data []byte),
) *MatchEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &MatchEngine{
		eventPipeline: eventPipeline,
		eventLog:      eventLog,
		sequencer:     sequencer,
		broadcaster:   broadcaster,
		unicaster:     unicaster,
		ctx:           ctx,
		cancel:        cancel,
	}

	return engine
}

// SubmitOrder 提交订单
// 这是外部接口，负责：
// 1. 验证订单
// 2. 生成 OrderSubmittedEvent
// 3. 将订单入队处理
func (me *MatchEngine) SubmitOrder(order model.SubmitOrderMsg) error {
	// 基本验证
	if order.Symbol == "" || order.Side == "" || order.UserID == "" {
		return fmt.Errorf("invalid order: missing required fields")
	}

	// 生成订单 ID（如果还没有的话）
	if order.OrderID == "" {
		order.OrderID = fmt.Sprintf("ord-%d-%d", time.Now().UnixNano(), me.sequencer.NextGlobalSeq())
	}

	// 创建 OrderSubmittedEvent
	// 这里我们假设 order.Price 和 order.Quantity 是字符串格式
	// 需要转换为 int64
	priceInNano := me.parsePrice(order.Price)
	quantityInNano := me.parseQuantity(order.Quantity)

	event := &model.OrderSubmittedEvent{
		Seq:       me.sequencer.NextGlobalSeq(),
		Timestamp: time.Now().UnixMilli(),
		OrderID:   order.OrderID,
		UserID:    order.UserID,
		SymbolStr: order.Symbol,
		Side:      order.Side,
		Price:     priceInNano,
		Quantity:  quantityInNano,
		Status:    "submitted",
	}

	// 将事件发送到管道（会持久化并分发给所有处理器）
	if err := me.eventPipeline.ProcessEvent(event); err != nil {
		hlog.Errorf("[MatchEngine] Failed to process OrderSubmittedEvent: %v", err)
		return err
	}

	// 将订单入队，等待撮合处理
	me.enqueueOrder(order, priceInNano, quantityInNano)

	return nil
}

// enqueueOrder 将订单加入处理队列
func (me *MatchEngine) enqueueOrder(order model.SubmitOrderMsg, priceInNano model.PriceInNano, quantityInNano model.QuantityInNano) {
	symbol := order.Symbol

	// 获取或创建该 symbol 的队列
	queueAny, _ := me.orderQueues.LoadOrStore(symbol, make(chan OrderQueueItem, 10000))
	queue := queueAny.(chan OrderQueueItem)

	// 获取或创建该 symbol 的 OrderBook
	_, loaded := me.orderBooks.LoadOrStore(symbol, NewOrderBook(symbol, me.sequencer))
	if !loaded {
		// 第一次创建该 symbol 的 OrderBook，启动 worker
		go me.matchWorker(symbol, queue)
	}

	// 将订单入队
	item := OrderQueueItem{
		orderMsg: order,
	}
	select {
	case queue <- item:
		// 成功入队
	case <-me.ctx.Done():
		hlog.Warnf("[MatchEngine] Engine is shutting down, dropping order: %s", order.OrderID)
	}
}

// matchWorker 每个 symbol 一个 worker，串行处理该 symbol 的所有订单
// 这保证了同一 symbol 的订单处理顺序，避免了竞态条件
func (me *MatchEngine) matchWorker(symbol string, queue chan OrderQueueItem) {
	obAny, _ := me.orderBooks.Load(symbol)
	ob := obAny.(*OrderBookV2)

	hlog.Infof("[MatchEngine] Started worker for symbol: %s", symbol)

	for {
		select {
		case item := <-queue:
			me.processOrder(symbol, ob, item.orderMsg)

		case <-me.ctx.Done():
			hlog.Infof("[MatchEngine] Worker shutting down for symbol: %s", symbol)
			return
		}
	}
}

// processOrder 处理单个订单
// 这是实际的撮合逻辑
func (me *MatchEngine) processOrder(symbol string, ob *OrderBookV2, orderMsg model.SubmitOrderMsg) {
	priceInNano := me.parsePrice(orderMsg.Price)
	quantityInNano := me.parseQuantity(orderMsg.Quantity)

	// 构建 OrderBookEntry
	entry := &OrderBookEntry{
		OrderID:     orderMsg.OrderID,
		UserID:      orderMsg.UserID,
		Symbol:      symbol,
		Side:        orderMsg.Side,
		Price:       priceInNano,
		Quantity:    quantityInNano,
		FilledQty:   0,
		SubmitTime:  time.Now().UnixMilli(),
		SequenceNum: me.sequencer.NextGlobalSeq(),
	}

	// 调用 OrderBook 进行撮合
	// 返回生成的成交事件和剩余数量
	tradeEvents, remainingQty := ob.MatchOrder(entry)

	// 处理所有成交事件
	for _, event := range tradeEvents {
		if err := me.eventPipeline.ProcessEvent(event); err != nil {
			hlog.Errorf("[MatchEngine] Failed to process TradeExecutedEvent: %v", err)
		}

		// 广播成交事件给 WebSocket 订阅者
		me.broadcastTrade(event.(*model.TradeExecutedEvent))
	}

	// 如果订单完全成交或未能成交的部分已加入 OrderBook
	if remainingQty == 0 && len(tradeEvents) > 0 {
		// 订单完全成交，发送 order filled 通知
		me.notifyOrderFilled(orderMsg)
	} else if remainingQty > 0 {
		// 订单部分或完全未成交，已加入 OrderBook，发送 order pending 通知
		me.notifyOrderPending(orderMsg, remainingQty)
	}

	hlog.Infof("[MatchEngine] Processed order: %s, symbol: %s, side: %s, trades: %d",
		orderMsg.OrderID, symbol, orderMsg.Side, len(tradeEvents))
}

// broadcastTrade 广播成交信息
func (me *MatchEngine) broadcastTrade(trade *model.TradeExecutedEvent) {
	if me.broadcaster == nil {
		return
	}

	message := map[string]interface{}{
		"type":      "trade",
		"trade_id":  trade.TradeID,
		"symbol":    trade.Symbol(),
		"price":     fmt.Sprintf("%.8f", float64(trade.Price)/1e8),
		"quantity":  fmt.Sprintf("%.8f", float64(trade.Quantity)/1e8),
		"timestamp": trade.Timestamp,
		"taker":     trade.TakerUser,
		"maker":     trade.MakerUser,
	}

	data, _ := json.Marshal(message)
	me.broadcaster(trade.Symbol(), data)

	// 也发送给 taker 和 maker
	if trade.TakerUser != "" {
		me.unicaster(trade.TakerUser, data)
	}
	if trade.MakerUser != "" {
		me.unicaster(trade.MakerUser, data)
	}
}

// notifyOrderFilled 通知订单已完全成交
func (me *MatchEngine) notifyOrderFilled(order model.SubmitOrderMsg) {
	if me.unicaster == nil {
		return
	}

	message := map[string]interface{}{
		"type":      "order_status",
		"order_id":  order.OrderID,
		"status":    "filled",
		"timestamp": time.Now().UnixMilli(),
	}

	data, _ := json.Marshal(message)
	me.unicaster(order.UserID, data)
}

// notifyOrderPending 通知订单挂单待成
func (me *MatchEngine) notifyOrderPending(order model.SubmitOrderMsg, remainingQty model.QuantityInNano) {
	if me.unicaster == nil {
		return
	}

	message := map[string]interface{}{
		"type":          "order_status",
		"order_id":      order.OrderID,
		"status":        "pending",
		"remaining_qty": fmt.Sprintf("%.8f", float64(remainingQty)/1e8),
		"timestamp":     time.Now().UnixMilli(),
	}

	data, _ := json.Marshal(message)
	me.unicaster(order.UserID, data)
}

// parsePrice 将字符串价格解析为 int64（nanoUSD）
func (me *MatchEngine) parsePrice(priceStr string) model.PriceInNano {
	// 简单实现：假设输入是浮点数字符串
	// 实际应该使用 decimal 库避免浮点精度问题
	var price float64
	fmt.Sscanf(priceStr, "%f", &price)
	return model.PriceInNano(int64(price * 1e8))
}

// parseQuantity 将字符串数量解析为 int64
func (me *MatchEngine) parseQuantity(qtyStr string) model.QuantityInNano {
	var qty float64
	fmt.Sscanf(qtyStr, "%f", &qty)
	return model.QuantityInNano(int64(qty * 1e8))
}

// Shutdown 优雅关闭引擎
func (me *MatchEngine) Shutdown() {
	me.cancel()
	hlog.Infof("[MatchEngine] Shutdown initiated")
	// 可以添加超时和清理逻辑
}

// GetOrderBook 获取 symbol 的 OrderBook（用于查询深度）
func (me *MatchEngine) GetOrderBook(symbol string) *OrderBookV2 {
	obAny, ok := me.orderBooks.Load(symbol)
	if !ok {
		return nil
	}
	return obAny.(*OrderBookV2)
}

// GetDepth 获取市场深度
func (me *MatchEngine) GetDepth(symbol string, levels int) (bids []map[string]string, asks []map[string]string) {
	ob := me.GetOrderBook(symbol)
	if ob == nil {
		return
	}
	return ob.GetDepth(levels)
}

// === Crash Recovery Support Methods ===

// GetEventPipeline 获取事件管道（用于recovery）
func (me *MatchEngine) GetEventPipeline() *EventPipeline {
	return me.eventPipeline
}

// GetEventLog 获取事件日志（用于recovery的event replay）
func (me *MatchEngine) GetEventLog() model.EventStore {
	return me.eventLog
}

// GetSequencer 获取序列号生成器
func (me *MatchEngine) GetSequencer() *Sequencer {
	return me.sequencer
}

// GetPositions 获取所有持仓（用于crash recovery验证）
// 返回一个空的位置映射（实际实现应该从状态管理器获取）
func (me *MatchEngine) GetPositions() map[string]*model.Position {
	// TODO: 从持仓管理器中获取真实的持仓数据
	// 现在返回空映射作为占位符
	return make(map[string]*model.Position)
}
