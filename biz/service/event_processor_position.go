package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"gorm.io/gorm"
)

// PositionProcessor 处理事件并更新用户持仓
// 保证幂等性：使用 trade_id 作为幂等键
// 确保即使重复处理同一事件，也不会重复更新持仓
//
// 事务化保证（Transactional Outbox Pattern）：
//   1. 业务数据更新和 outbox 条目在同一数据库事务内提交
//   2. 事务成功：业务数据 + outbox 条目都持久化
//   3. 事务失败：两者都回滚（Case A 的源头）
//   4. Checkpoint 后的 crash 被 outbox dispatcher 恢复（Case B 的缓解）
type PositionProcessor struct {
	// 用于记录已处理的 trade_id，防止重复处理
	mu              sync.Mutex
	processedTrades map[string]bool // trade_id -> 已处理

	// 实际的持仓更新函数（由外部提供）
	buyPositionFn  func(userID, symbol, quantity, price string) error
	sellPositionFn func(userID, symbol, quantity string) error

	// 数据库与 outbox 仓库（用于事务化写入）
	db        *gorm.DB
	outboxRepo *pg.OutboxRepo
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

// NewPositionProcessorWithOutbox 创建带 outbox 支持的持仓处理器
// 用于事务化处理和可靠事件发布
func NewPositionProcessorWithOutbox(
	db *gorm.DB,
	outboxRepo *pg.OutboxRepo,
	buyPositionFn func(userID, symbol, quantity, price string) error,
	sellPositionFn func(userID, symbol, quantity string) error,
) *PositionProcessor {
	return &PositionProcessor{
		processedTrades: make(map[string]bool),
		buyPositionFn:   buyPositionFn,
		sellPositionFn:  sellPositionFn,
		db:              db,
		outboxRepo:      outboxRepo,
	}
}

// ProcessEvent 处理一个事件
// 如果配置了 db 和 outboxRepo，则使用事务化处理；否则使用原有逻辑
func (pp *PositionProcessor) ProcessEvent(event model.MatchingEngineEvent) error {
	switch e := event.(type) {
	case *model.TradeExecutedEvent:
		if pp.db != nil && pp.outboxRepo != nil {
			// 使用事务化处理 + outbox
			return pp.handleTradeExecutedTransactional(e)
		}
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

// handleTradeExecutedTransactional 事务化处理成交事件（带 outbox 支持）
// 保证业务数据更新和 outbox 条目的原子性
//
// 流程：
//   1. 开始数据库事务
//   2. 在事务内更新业务数据（持仓）
//   3. 在同一事务内写入 outbox 条目
//   4. 事务提交或回滚
//   5. Outbox dispatcher 异步读取并发布事件
func (pp *PositionProcessor) handleTradeExecutedTransactional(e *model.TradeExecutedEvent) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	// 检查幂等性：是否已经处理过这个 trade
	if pp.processedTrades[e.TradeID] {
		hlog.Infof("[PositionProcessor] Skipping duplicate trade: %s", e.TradeID)
		return nil
	}

	// 在事务内执行业务逻辑和 outbox 写入
	err := pp.db.Transaction(func(tx *gorm.DB) error {
		// Step 1: 执行业务数据更新
		quantityStr := fmt.Sprintf("%.8f", float64(e.Quantity)/1e8)
		priceStr := fmt.Sprintf("%.8f", float64(e.Price)/1e8)

		if e.TakerSide == "buy" {
			if err := pp.buyPositionFn(e.TakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker buy position: %v", err)
				return err
			}
			if err := pp.sellPositionFn(e.MakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker sell position: %v", err)
				return err
			}
		} else {
			if err := pp.sellPositionFn(e.TakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker sell position: %v", err)
				return err
			}
			if err := pp.buyPositionFn(e.MakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker buy position: %v", err)
				return err
			}
		}

		// Step 2: 写入 outbox 条目（在同一事务内）
		payloadJSON, _ := json.Marshal(e)
		outboxEntry := &model.OutboxEntry{
			EventID:       e.TradeID,
			EventType:     "TradeExecuted",
			AggregateID:   e.Symbol(),
			AggregateType: "OrderBook",
			Payload:       string(payloadJSON),
			Published:     false,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
		}

		if err := pp.outboxRepo.WriteOutboxEntry(tx, outboxEntry); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to write outbox entry: %v", err)
			return err
		}

		return nil
	})

	if err != nil {
		hlog.Errorf("[PositionProcessor] Transaction failed for trade %s: %v", e.TradeID, err)
		return err
	}

	// 标记为已处理（幂等性保证）
	pp.processedTrades[e.TradeID] = true

	hlog.Infof("[PositionProcessor] Updated position for trade: %s (taker: %s, maker: %s) with outbox",
		e.TradeID, e.TakerUser, e.MakerUser)

	return nil
}

// Reset 重置幂等性记录（仅用于测试）
func (pp *PositionProcessor) Reset() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.processedTrades = make(map[string]bool)
}
