package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// DatabaseProcessor 处理事件并将其持久化到数据库
// 保证幂等性：
// - 对于 TradeExecutedEvent，使用 trade_id 作为唯一键
// - 对于 OrderSubmittedEvent，使用 order_id 作为唯一键
// - 对于 OrderCancelledEvent，更新现有订单状态
type DatabaseProcessor struct {
	db *gorm.DB

	// 用于批量写入的缓冲
	mu            sync.Mutex
	pendingTrades []*model.Trade
	pendingOrders []*model.Order
	batchSize     int
	flushTicker   *time.Ticker
}

// NewDatabaseProcessor 创建一个新的数据库处理器
func NewDatabaseProcessor(db *gorm.DB, batchSize int) *DatabaseProcessor {
	processor := &DatabaseProcessor{
		db:            db,
		pendingTrades: make([]*model.Trade, 0, batchSize),
		pendingOrders: make([]*model.Order, 0, batchSize),
		batchSize:     batchSize,
	}

	// 定期刷新未提交的事件
	processor.flushTicker = time.NewTicker(100 * time.Millisecond)
	go processor.flushWorker()

	return processor
}

// ProcessEvent 处理一个事件
func (dp *DatabaseProcessor) ProcessEvent(event model.MatchingEngineEvent) error {
	switch e := event.(type) {
	case *model.TradeExecutedEvent:
		return dp.handleTradeExecuted(e)
	case *model.OrderSubmittedEvent:
		return dp.handleOrderSubmitted(e)
	case *model.OrderCancelledEvent:
		return dp.handleOrderCancelled(e)
	default:
		return fmt.Errorf("unknown event type: %T", event)
	}
}

// ProcessorName 返回处理器的名称
func (dp *DatabaseProcessor) ProcessorName() string {
	return "DatabaseProcessor"
}

// handleTradeExecuted 处理成交事件
// 使用幂等性：先查询是否存在，再决定是否插入
func (dp *DatabaseProcessor) handleTradeExecuted(e *model.TradeExecutedEvent) error {
	trade := &model.Trade{
		TradeID:      e.TradeID,
		Symbol:       e.Symbol(),
		Price:        fmt.Sprintf("%.8f", float64(e.Price)/1e8), // 转换回浮点数用于显示
		Quantity:     fmt.Sprintf("%.8f", float64(e.Quantity)/1e8),
		Timestamp:    e.Timestamp,
		TakerOrderID: e.TakerOrderID,
		MakerOrderID: e.MakerOrderID,
		Side:         e.TakerSide,
		EngineID:     "", // 可以从上下文中获取
		TakerUser:    e.TakerUser,
		MakerUser:    e.MakerUser,
	}

	// 幂等性：检查成交是否已存在
	existing := &model.Trade{}
	if err := dp.db.Where("trade_id = ?", e.TradeID).First(existing).Error; err == nil {
		// 成交已存在，跳过
		return nil
	}

	// 插入新成交
	if err := dp.db.Create(trade).Error; err != nil {
		hlog.Errorf("[DatabaseProcessor] Failed to insert trade: %v", err)
		return err
	}

	hlog.Debugf("[DatabaseProcessor] Inserted trade: %s", e.TradeID)
	return nil
}

// handleOrderSubmitted 处理订单提交事件
func (dp *DatabaseProcessor) handleOrderSubmitted(e *model.OrderSubmittedEvent) error {
	order := &model.Order{
		OrderID:   e.OrderID,
		UserID:    e.UserID,
		Symbol:    e.Symbol(),
		Side:      e.Side,
		Price:     int64(e.Price),
		Quantity:  int64(e.Quantity),
		Status:    e.Status,
		CreatedAt: e.Timestamp,
		UpdatedAt: e.Timestamp,
	}

	// 幂等性：检查订单是否已存在
	existing := &model.Order{}
	if err := dp.db.Where("order_id = ?", e.OrderID).First(existing).Error; err == nil {
		// 订单已存在，跳过
		return nil
	}

	// 插入新订单
	if err := dp.db.Create(order).Error; err != nil {
		hlog.Errorf("[DatabaseProcessor] Failed to insert order: %v", err)
		return err
	}

	hlog.Debugf("[DatabaseProcessor] Inserted order: %s", e.OrderID)
	return nil
}

// handleOrderCancelled 处理订单取消事件
func (dp *DatabaseProcessor) handleOrderCancelled(e *model.OrderCancelledEvent) error {
	// 幂等性：只有"active"状态的订单才能取消
	if err := dp.db.Model(&model.Order{}).
		Where("order_id = ? AND status = ?", e.OrderID, "active").
		Update("status", "cancelled").Error; err != nil {
		hlog.Errorf("[DatabaseProcessor] Failed to cancel order: %v", err)
		return err
	}

	hlog.Debugf("[DatabaseProcessor] Cancelled order: %s", e.OrderID)
	return nil
}

// flushWorker 定期刷新未提交的事件
func (dp *DatabaseProcessor) flushWorker() {
	for range dp.flushTicker.C {
		dp.flush()
	}
}

// flush 执行待处理事件的批量写入
func (dp *DatabaseProcessor) flush() {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if len(dp.pendingTrades) > 0 {
		if err := dp.db.CreateInBatches(dp.pendingTrades, 100).Error; err != nil {
			hlog.Errorf("[DatabaseProcessor] Batch insert trades failed: %v", err)
		}
		dp.pendingTrades = dp.pendingTrades[:0]
	}

	if len(dp.pendingOrders) > 0 {
		if err := dp.db.CreateInBatches(dp.pendingOrders, 100).Error; err != nil {
			hlog.Errorf("[DatabaseProcessor] Batch insert orders failed: %v", err)
		}
		dp.pendingOrders = dp.pendingOrders[:0]
	}
}

// Shutdown 优雅关闭处理器
func (dp *DatabaseProcessor) Shutdown() {
	dp.flushTicker.Stop()
	dp.flush()
}
