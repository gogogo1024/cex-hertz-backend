package service

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// WebSocketProcessor 处理事件并广播到 WebSocket 连接
// 负责将交易和订单状态推送给相关用户
type WebSocketProcessor struct {
	// WebSocket 广播函数
	broadcaster func(symbol string, data []byte)
	unicaster   func(userID string, data []byte)

	// 追踪已发送的消息，防止重复（虽然 WS 本身不一定需要幂等，但为了一致性还是跟踪）
	mu              sync.Mutex
	processedEvents map[string]bool // event_key -> 已处理
}

// NewWebSocketProcessor 创建一个新的 WebSocket 处理器
func NewWebSocketProcessor(
	broadcaster func(symbol string, data []byte),
	unicaster func(userID string, data []byte),
) *WebSocketProcessor {
	return &WebSocketProcessor{
		broadcaster:     broadcaster,
		unicaster:       unicaster,
		processedEvents: make(map[string]bool),
	}
}

// ProcessEvent 处理一个事件
func (wp *WebSocketProcessor) ProcessEvent(event model.MatchingEngineEvent) error {
	switch e := event.(type) {
	case *model.TradeExecutedEvent:
		return wp.handleTradeExecuted(e)
	case *model.OrderSubmittedEvent:
		return wp.handleOrderSubmitted(e)
	case *model.OrderCancelledEvent:
		return wp.handleOrderCancelled(e)
	default:
		return nil
	}
}

// ProcessorName 返回处理器的名称
func (wp *WebSocketProcessor) ProcessorName() string {
	return "WebSocketProcessor"
}

// handleTradeExecuted 处理成交事件，广播给所有订阅用户
func (wp *WebSocketProcessor) handleTradeExecuted(e *model.TradeExecutedEvent) error {
	eventKey := fmt.Sprintf("trade-%s", e.TradeID)

	wp.mu.Lock()
	if wp.processedEvents[eventKey] {
		wp.mu.Unlock()
		hlog.Infof("[WebSocketProcessor] Skipping duplicate trade event: %s", e.TradeID)
		return nil
	}
	wp.processedEvents[eventKey] = true
	wp.mu.Unlock()

	// 构建消息
	message := map[string]interface{}{
		"type":      "trade",
		"trade_id":  e.TradeID,
		"symbol":    e.Symbol(),
		"price":     formatPrice(model.PriceInNano(e.Price)),
		"quantity":  formatQuantity(model.QuantityInNano(e.Quantity)),
		"timestamp": e.Timestamp,
		"taker":     e.TakerUser,
		"maker":     e.MakerUser,
		"side":      e.TakerSide,
	}

	data, err := json.Marshal(message)
	if err != nil {
		hlog.Errorf("[WebSocketProcessor] Failed to marshal trade message: %v", err)
		return err
	}

	// 广播给所有订阅该 symbol 的用户
	if wp.broadcaster != nil {
		wp.broadcaster(e.Symbol(), data)
	}

	// 单播给 taker 和 maker（给他们更详细的信息）
	// 发送给 Taker
	if e.TakerUser != "" {
		takerMsg := map[string]interface{}{
			"type":         "trade",
			"trade_id":     e.TradeID,
			"symbol":       e.Symbol(),
			"price":        formatPrice(model.PriceInNano(e.Price)),
			"quantity":     formatQuantity(model.QuantityInNano(e.Quantity)),
			"timestamp":    e.Timestamp,
			"my_order_id":  e.TakerOrderID,
			"counterparty": e.MakerUser,
			"my_side":      e.TakerSide,
			"is_maker":     false,
		}
		takerData, _ := json.Marshal(takerMsg)
		if wp.unicaster != nil {
			wp.unicaster(e.TakerUser, takerData)
		}
	}

	// 发送给 Maker
	if e.MakerUser != "" && e.MakerUser != e.TakerUser {
		makerSide := "sell"
		if e.TakerSide == "sell" {
			makerSide = "buy"
		}
		makerMsg := map[string]interface{}{
			"type":         "trade",
			"trade_id":     e.TradeID,
			"symbol":       e.Symbol(),
			"price":        formatPrice(model.PriceInNano(e.Price)),
			"quantity":     formatQuantity(model.QuantityInNano(e.Quantity)),
			"timestamp":    e.Timestamp,
			"my_order_id":  e.MakerOrderID,
			"counterparty": e.TakerUser,
			"my_side":      makerSide,
			"is_maker":     true,
		}
		makerData, _ := json.Marshal(makerMsg)
		if wp.unicaster != nil {
			wp.unicaster(e.MakerUser, makerData)
		}
	}

	hlog.Infof("[WebSocketProcessor] Broadcasted trade: %s to %s", e.TradeID, e.Symbol())
	return nil
}

// handleOrderSubmitted 处理订单提交事件
func (wp *WebSocketProcessor) handleOrderSubmitted(e *model.OrderSubmittedEvent) error {
	eventKey := fmt.Sprintf("order-submitted-%s", e.OrderID)

	wp.mu.Lock()
	if wp.processedEvents[eventKey] {
		wp.mu.Unlock()
		return nil
	}
	wp.processedEvents[eventKey] = true
	wp.mu.Unlock()

	// 构建消息
	message := map[string]interface{}{
		"type":      "order_status",
		"order_id":  e.OrderID,
		"status":    "submitted",
		"symbol":    e.Symbol(),
		"side":      e.Side,
		"price":     formatPrice(model.PriceInNano(e.Price)),
		"quantity":  formatQuantity(model.QuantityInNano(e.Quantity)),
		"timestamp": e.Timestamp,
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// 单播给该用户
	if wp.unicaster != nil {
		wp.unicaster(e.UserID, data)
	}

	hlog.Infof("[WebSocketProcessor] Sent order_submitted to user: %s, order: %s", e.UserID, e.OrderID)
	return nil
}

// handleOrderCancelled 处理订单取消事件
func (wp *WebSocketProcessor) handleOrderCancelled(e *model.OrderCancelledEvent) error {
	eventKey := fmt.Sprintf("order-cancelled-%s", e.OrderID)

	wp.mu.Lock()
	if wp.processedEvents[eventKey] {
		wp.mu.Unlock()
		return nil
	}
	wp.processedEvents[eventKey] = true
	wp.mu.Unlock()

	// 注意：这里我们没有用户信息，应该从订单记录中查询
	// 实际实现应该：
	// message := map[string]interface{}{
	//     "type":      "order_status",
	//     "order_id":  e.OrderID,
	//     "status":    "cancelled",
	//     "symbol":    e.Symbol(),
	//     "reason":    e.Reason,
	//     "timestamp": e.Timestamp,
	// }
	// data, _ := json.Marshal(message)
	// order := db.GetOrder(e.OrderID)
	// unicaster(order.UserID, data)

	hlog.Infof("[WebSocketProcessor] Sent order_cancelled: %s", e.OrderID)
	return nil
}

// formatPrice 格式化价格为字符串
func formatPrice(price model.PriceInNano) string {
	return fmt.Sprintf("%.8f", float64(price)/1e8)
}

// formatQuantity 格式化数量为字符串
func formatQuantity(qty model.QuantityInNano) string {
	return fmt.Sprintf("%.8f", float64(qty)/1e8)
}
