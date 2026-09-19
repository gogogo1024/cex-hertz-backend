package service

import (
	"fmt"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// PositionProcessor 处理事件并更新用户持仓
// 保证幂等性：使用 trade_id 作为幂等键
// 确保即使重复处理同一事件，也不会重复更新持仓
type PositionProcessor struct {
	// 用于记录已处理的 trade_id，防止重复处理
	mu              sync.Mutex
	processedTrades map[string]bool // trade_id -> 已处理

	// 实际的持仓更新函数（由外部提供）
	buyPositionFn  func(userID, symbol, quantity, price string) error
	sellPositionFn func(userID, symbol, quantity string) error
}

// NewPositionProcessor 创建一个新的持仓处理器
func NewPositionProcessor(
	buyPositionFn func(userID, symbol, quantity, price string) error,
	sellPositionFn func(userID, symbol, quantity string) error,
) *PositionProcessor {
	return &PositionProcessor{
		processedTrades: make(map[string]bool),
		buyPositionFn:   buyPositionFn,
		sellPositionFn:  sellPositionFn,
	}
}

// ProcessEvent 处理一个事件
func (pp *PositionProcessor) ProcessEvent(event model.MatchingEngineEvent) error {
	switch e := event.(type) {
	case *model.TradeExecutedEvent:
		return pp.handleTradeExecuted(e)
	case *model.OrderCancelledEvent:
		// 订单取消不需要更新持仓（因为取消的是未成交的订单）
		return nil
	default:
		// 其他事件类型不处理
		return nil
	}
}

// ProcessorName 返回处理器的名称
func (pp *PositionProcessor) ProcessorName() string {
	return "PositionProcessor"
}

// handleTradeExecuted 处理成交事件
// 注意：这是同步的，所以持仓更新会立即进行（与 matching 在同一事务边界内）
func (pp *PositionProcessor) handleTradeExecuted(e *model.TradeExecutedEvent) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	// 检查幂等性：是否已经处理过这个 trade
	if pp.processedTrades[e.TradeID] {
		hlog.Infof("[PositionProcessor] Skipping duplicate trade: %s", e.TradeID)
		return nil
	}

	// 转换为浮点数用于显示
	quantityStr := fmt.Sprintf("%.8f", float64(e.Quantity)/1e8)
	priceStr := fmt.Sprintf("%.8f", float64(e.Price)/1e8)

	// 根据 taker 的方向决定操作
	// 如果 taker 是买方，则 taker 买入，maker 卖出
	if e.TakerSide == "buy" {
		// Taker 买入
		if err := pp.buyPositionFn(e.TakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update taker buy position: %v", err)
			return err
		}

		// Maker 卖出
		if err := pp.sellPositionFn(e.MakerUser, e.Symbol(), quantityStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update maker sell position: %v", err)
			return err
		}
	} else {
		// Taker 卖出
		if err := pp.sellPositionFn(e.TakerUser, e.Symbol(), quantityStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update taker sell position: %v", err)
			return err
		}

		// Maker 买入
		if err := pp.buyPositionFn(e.MakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update maker buy position: %v", err)
			return err
		}
	}

	// 标记为已处理（幂等性保证）
	pp.processedTrades[e.TradeID] = true

	hlog.Infof("[PositionProcessor] Updated position for trade: %s (taker: %s, maker: %s)",
		e.TradeID, e.TakerUser, e.MakerUser)

	return nil
}

// Reset 重置幂等性记录（仅用于测试）
func (pp *PositionProcessor) Reset() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.processedTrades = make(map[string]bool)
}
