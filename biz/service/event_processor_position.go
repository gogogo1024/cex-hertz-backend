package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// PositionProcessor 处理事件并更新用户持仓
// 保证幂等性：使用 trade_id 作为幂等键
// 确保即使重复处理同一事件，也不会重复更新持仓
//
// 事务化保证（Transactional Outbox Pattern）：
//  1. 业务数据更新和 outbox 条目在同一数据库事务内提交
//  2. 事务成功：业务数据 + outbox 条目都持久化
//  3. 事务失败：两者都回滚（Case A 的源头）
//  4. Checkpoint 后的 crash 被 outbox dispatcher 恢复（Case B 的缓解）
type PositionProcessor struct {
	// processedTrades 存储已处理的 trade_id，防止重复处理
	// 使用独立的 mutex 保护 map 访问；并使用按 symbol 的锁以提高并发性
	processedMu     sync.Mutex
	processedTrades map[string]bool // trade_id -> 已处理 (fallback when DB dedup unavailable)
	// optional DB-based dedup repo (when constructed with outbox/db)
	processedRepo *pg.ProcessedRepo

	// 按 symbol 的锁集合：key=symbol -> *sync.Mutex
	locks sync.Map // map[string]*sync.Mutex

	// 实际的持仓更新函数（由外部提供）
	// 现在支持事务化版本：tx-aware 函数签名
	buyPositionFn  func(tx *gorm.DB, userID, symbol, quantity, price string) error
	sellPositionFn func(tx *gorm.DB, userID, symbol, quantity string) error

	// 数据库与 outbox 仓库（用于事务化写入）
	db         *gorm.DB
	outboxRepo *pg.OutboxRepo
}

// NewPositionProcessor 创建一个新的持仓处理器
func NewPositionProcessor(
	buyPositionFn func(tx *gorm.DB, userID, symbol, quantity, price string) error,
	sellPositionFn func(tx *gorm.DB, userID, symbol, quantity string) error,
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
	buyPositionFn func(tx *gorm.DB, userID, symbol, quantity, price string) error,
	sellPositionFn func(tx *gorm.DB, userID, symbol, quantity string) error,
) *PositionProcessor {
	proc := &PositionProcessor{
		processedTrades: make(map[string]bool),
		buyPositionFn:   buyPositionFn,
		sellPositionFn:  sellPositionFn,
		db:              db,
		outboxRepo:      outboxRepo,
	}
	// 初始化 DB 层的去重仓库，用于替代进程内缓存的幂等判断
	if db != nil {
		proc.processedRepo = pg.NewProcessedRepo(db)
	}
	return proc
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
	// 获取对应 symbol 的锁以提高并发性
	symbol := e.Symbol()
	l := pp.getLockForSymbol(symbol)
	l.Lock()
	defer l.Unlock()

	// 优先使用 DB 去重（如果可用），否则使用内存缓存作为回退
	if pp.processedRepo != nil {
		inserted, perr := pp.processedRepo.WriteIfNotExists(e.TradeID)
		if perr != nil {
			hlog.Errorf("[PositionProcessor] ProcessedRepo error: %v", perr)
			return perr
		}
		if !inserted {
			hlog.Infof("[PositionProcessor] Skipping duplicate trade (DB): %s", e.TradeID)
			return nil
		}
		// 如果后续更新失败，需要删除已插入的去重标记以便重试
		insertedFlag := true
		// 执行更新逻辑，若失败则 cleanup
		quantityStr := fmt.Sprintf("%.8f", float64(e.Quantity)/1e8)
		priceStr := fmt.Sprintf("%.8f", float64(e.Price)/1e8)

		var opErr error
		if e.TakerSide == "buy" {
			if err := pp.buyPositionFn(nil, e.TakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker buy position: %v", err)
				opErr = err
			} else if err := pp.sellPositionFn(nil, e.MakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker sell position: %v", err)
				opErr = err
			}
		} else {
			if err := pp.sellPositionFn(nil, e.TakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker sell position: %v", err)
				opErr = err
			} else if err := pp.buyPositionFn(nil, e.MakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker buy position: %v", err)
				opErr = err
			}
		}
		if opErr != nil {
			if insertedFlag {
				_ = pp.processedRepo.Delete(e.TradeID)
			}
			return opErr
		}

		hlog.Infof("[PositionProcessor] Updated position for trade: %s (taker: %s, maker: %s)",
			e.TradeID, e.TakerUser, e.MakerUser)
		return nil
	}

	// 回退：使用内存缓存判断幂等性（仅在没有 DB 去重时使用）
	pp.processedMu.Lock()
	if pp.processedTrades[e.TradeID] {
		pp.processedMu.Unlock()
		hlog.Infof("[PositionProcessor] Skipping duplicate trade: %s", e.TradeID)
		return nil
	}
	pp.processedMu.Unlock()

	// 转换为浮点数用于显示
	quantityStr := fmt.Sprintf("%.8f", float64(e.Quantity)/1e8)
	priceStr := fmt.Sprintf("%.8f", float64(e.Price)/1e8)

	// 根据 taker 的方向决定操作
	// 如果 taker 是买方，则 taker 买入，maker 卖出
	if e.TakerSide == "buy" {
		// Taker 买入
		if err := pp.buyPositionFn(nil, e.TakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update taker buy position: %v", err)
			return err
		}

		// Maker 卖出
		if err := pp.sellPositionFn(nil, e.MakerUser, e.Symbol(), quantityStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update maker sell position: %v", err)
			return err
		}
	} else {
		// Taker 卖出
		if err := pp.sellPositionFn(nil, e.TakerUser, e.Symbol(), quantityStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update taker sell position: %v", err)
			return err
		}

		// Maker 买入
		if err := pp.buyPositionFn(nil, e.MakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
			hlog.Errorf("[PositionProcessor] Failed to update maker buy position: %v", err)
			return err
		}
	}

	// 标记为已处理（幂等性保证）
	pp.processedMu.Lock()
	pp.processedTrades[e.TradeID] = true
	pp.processedMu.Unlock()

	hlog.Infof("[PositionProcessor] Updated position for trade: %s (taker: %s, maker: %s)",
		e.TradeID, e.TakerUser, e.MakerUser)

	return nil
}

// handleTradeExecutedTransactional 事务化处理成交事件（带 outbox 支持）
// 保证业务数据更新和 outbox 条目的原子性
//
// 流程：
//  1. 开始数据库事务
//  2. 在事务内更新业务数据（持仓）
//  3. 在同一事务内写入 outbox 条目
//  4. 事务提交或回滚
//  5. Outbox dispatcher 异步读取并发布事件
func (pp *PositionProcessor) handleTradeExecutedTransactional(e *model.TradeExecutedEvent) error {
	// 获取 symbol 的锁以允许跨 symbol 并发处理
	symbol := e.Symbol()
	l := pp.getLockForSymbol(symbol)
	l.Lock()
	defer l.Unlock()

	// 对于事务化路径，由 outboxRepo 在事务内做原子幂等检查（ON CONFLICT DO NOTHING），无需进程内缓存

	// 在事务内执行业务逻辑和 outbox 写入
	alreadyProcessed := false
	err := pp.db.Transaction(func(tx *gorm.DB) error {
		// 先尝试写入 outbox 条目作为幂等性检查点：
		// 如果已经存在（unique constraint），说明该事件已被其它实例处理，直接跳过。
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

		inserted, werr := pp.outboxRepo.WriteOutboxEntryIfNotExists(tx, outboxEntry)
		if werr != nil {
			hlog.Errorf("[PositionProcessor] Failed to write outbox entry: %v", werr)
			return werr
		}
		if !inserted {
			// 已存在，说明其它实例已处理该事件
			alreadyProcessed = true
			return nil
		}

		// 写入 outbox 成功后再执行业务数据更新（写入同一事务）
		quantityStr := fmt.Sprintf("%.8f", float64(e.Quantity)/1e8)
		priceStr := fmt.Sprintf("%.8f", float64(e.Price)/1e8)

		if e.TakerSide == "buy" {
			if err := pp.buyPositionFn(tx, e.TakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker buy position: %v", err)
				return err
			}
			if err := pp.sellPositionFn(tx, e.MakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker sell position: %v", err)
				return err
			}
		} else {
			if err := pp.sellPositionFn(tx, e.TakerUser, e.Symbol(), quantityStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update taker sell position: %v", err)
				return err
			}
			if err := pp.buyPositionFn(tx, e.MakerUser, e.Symbol(), quantityStr, priceStr); err != nil {
				hlog.Errorf("[PositionProcessor] Failed to update maker buy position: %v", err)
				return err
			}
		}

		return nil
	})

	if err != nil {
		hlog.Errorf("[PositionProcessor] Transaction failed for trade %s: %v", e.TradeID, err)
		return err
	}

	if err != nil {
		hlog.Errorf("[PositionProcessor] Transaction failed for trade %s: %v", e.TradeID, err)
		return err
	}

	if alreadyProcessed {
		hlog.Infof("[PositionProcessor] Skipping duplicate trade (detected by DB): %s", e.TradeID)
		return nil
	}

	// 对于带 outbox 的事务路径，不再依赖内存缓存；outbox 的存在已作为去重标记

	hlog.Infof("[PositionProcessor] Updated position for trade: %s (taker: %s, maker: %s) with outbox",
		e.TradeID, e.TakerUser, e.MakerUser)

	return nil
}

// Reset 重置幂等性记录（仅用于测试）
func (pp *PositionProcessor) Reset() {
	// Reset processed trades map
	pp.processedMu.Lock()
	defer pp.processedMu.Unlock()
	pp.processedTrades = make(map[string]bool)
}

// getLockForSymbol 返回某个 symbol 对应的 mutex（缓存于 sync.Map）
func (pp *PositionProcessor) getLockForSymbol(symbol string) *sync.Mutex {
	if symbol == "" {
		// fallback: use a global anonymous lock if symbol empty
		m := &sync.Mutex{}
		return m
	}
	actual, _ := pp.locks.LoadOrStore(symbol, &sync.Mutex{})
	return actual.(*sync.Mutex)
}
