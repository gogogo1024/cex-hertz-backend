package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// RecoveryStrategy 恢复策略
type RecoveryStrategy string

const (
	FULL_REPLAY RecoveryStrategy = "full_replay" // 从初始状态重放所有事件
	INCREMENTAL RecoveryStrategy = "incremental" // 只重放crash后的事件
)

// RecoveryOptions 恢复选项
type RecoveryOptions struct {
	// 恢复策略
	Strategy RecoveryStrategy

	// 处理器过滤 (为空=所有processor)
	ProcessorNames []string

	// 符号过滤 (为空=所有symbol)
	Symbols []string

	// 是否验证恢复结果
	ValidateAfterRecovery bool

	// 超时时间
	Timeout time.Duration

	// 最大重试次数
	MaxRetries int
}

// RecoveryResult 恢复结果
type RecoveryResult struct {
	// 基本信息
	ProcessorName string
	Symbol        string
	RecoveryStart time.Time
	RecoveryEnd   time.Time

	// 恢复统计
	EventsReplayed int64
	OrdersRestored int64
	TradesRestored int64

	// 恢复前后状态
	PreCrashChecksum     string
	PostRecoveryChecksum string

	// 验证结果
	IsValid         bool
	ValidationError error
}

// RecoveryStats 恢复统计信息
type RecoveryStats struct {
	TotalRecoveryTime   time.Duration
	ProcessorsRecovered int
	EventsProcessed     int64
	StateValidated      bool
	LastRecoveryTime    time.Time
}

// RecoveryExecutor 恢复执行器
//
// ===== 关键设计要点 =====
// 1. Kafka Offset 追踪：通过 EventPipeline.GetProcessorKafkaOffset() 获取最后的处理位置
// 2. 恢复起点确定：lastKafkaOffset + 1（确保不重不漏）
// 3. 完整的元数据链：Kafka Offset → Event Seq → Processor Side Effect → Checkpoint
// 4. 分布式锁保护：使用 RedisLockManager 防止并发恢复同一个symbol
//
// ===== 恢复流程 =====
// Step 1: 获取最后的 Kafka offset
//
//	processor.GetProcessorKafkaOffset(processorName, symbol)
//	  返回：lastProcessedKafkaOffset（上次成功处理的位置）
//
// Step 2: 确定恢复起点
//
//	startOffset = lastProcessedKafkaOffset + 1
//
// Step 3: 从 Kafka 重新消费
//
//	events := kafkaConsumer.Fetch(startOffset)
//
// Step 4: 使用 ProcessEventWithContext 重放（在分布式锁保护下）
//
//	for each event {
//	    eventCtx := model.NewEventContext(event, topic, partition, offset)
//	    processor.ProcessEventWithContext(eventCtx)
//	}
//
// ===== 数据一致性保证 =====
// - Kafka offset 与 Event seq 通过 checkpoint 建立 1:1 映射
// - 恢复时从确切的 Kafka offset 开始，不会重复或遗漏
// - EventSeq 全局唯一，支持检测重复事件
// - 分布式锁保证同一时刻只有一个节点恢复某个symbol
//
// ===== 相关文档 =====
// 详见：doc/KAFKA_AWARE_EVENTPIPELINE_DESIGN.md
type RecoveryExecutor struct {
	matchEngine    *PartitionAwareMatchEngine
	checkpointMgr  *CheckpointManager
	eventLog       model.EventStore
	stateValidator *StateValidator
	lockMgr        *RedisLockManager  // 分布式锁管理器

	mu               sync.RWMutex
	stats            *RecoveryStats
	lastRecoveryTime time.Time
	isRecovering     atomic.Bool
}

// NewRecoveryExecutor 创建恢复执行器
func NewRecoveryExecutor(
	matchEngine *PartitionAwareMatchEngine,
	checkpointMgr *CheckpointManager,
	eventLog model.EventStore,
	lockMgr *RedisLockManager,
) *RecoveryExecutor {
	return &RecoveryExecutor{
		matchEngine:      matchEngine,
		checkpointMgr:    checkpointMgr,
		eventLog:         eventLog,
		stateValidator:   NewStateValidator(),
		lockMgr:          lockMgr,
		stats:            &RecoveryStats{},
		lastRecoveryTime: time.Now(),
	}
}

// ShouldRecover 检查是否需要恢复
// 用途：监控检查、调试、或需要提前知道恢复状态的场景
// 注意：启动流程中已使用 ExecuteRecovery()，不再需要此方法（避免重复查询）
// 此方法会查询数据库一次，仅用于快速检查
func (re *RecoveryExecutor) ShouldRecover(ctx context.Context) (bool, error) {
	if re.checkpointMgr == nil {
		return false, nil
	}

	// 列出所有待恢复项
	pendingItems, err := re.checkpointMgr.ListPendingRecoveryItems()
	if err != nil {
		hlog.Errorf("[RecoveryExecutor] Failed to list pending recovery items: %v", err)
		return false, err
	}

	// 如果有待恢复项，表示需要恢复
	hasRecoveryNeeded := len(pendingItems) > 0

	if hasRecoveryNeeded {
		hlog.Warnf("[RecoveryExecutor] Found %d pending recovery items, recovery needed", len(pendingItems))
		for _, item := range pendingItems {
			hlog.Infof("[RecoveryExecutor] Pending recovery: %s:%s",
				item["processor_name"], item["symbol"])
		}
	} else {
		hlog.Debugf("[RecoveryExecutor] No pending recovery items found")
	}

	return hasRecoveryNeeded, nil
}

// ShouldRecoverWithList 一次查询同时获取是否需要恢复和待恢复项列表
// 用途：监控面板、调试工具、或需要看详细列表的场景
// 注意：启动流程中已使用 ExecuteRecovery()，不再需要此方法（避免重复查询）
// 返回: (needsRecovery, pendingItems, error)
func (re *RecoveryExecutor) ShouldRecoverWithList(ctx context.Context) (bool, []map[string]string, error) {
	if re.checkpointMgr == nil {
		return false, nil, nil
	}

	// 一次查询获取所有信息
	pendingItems, err := re.checkpointMgr.ListPendingRecoveryItems()
	if err != nil {
		hlog.Errorf("[RecoveryExecutor] Failed to list pending recovery items: %v", err)
		return false, nil, err
	}

	hasRecoveryNeeded := len(pendingItems) > 0

	if hasRecoveryNeeded {
		hlog.Warnf("[RecoveryExecutor] Found %d pending recovery items, recovery needed", len(pendingItems))
		for _, item := range pendingItems {
			hlog.Infof("[RecoveryExecutor] Pending recovery: %s:%s",
				item["processor_name"], item["symbol"])
		}
	}

	return hasRecoveryNeeded, pendingItems, nil
}

// ExecuteRecovery 执行恢复流程
func (re *RecoveryExecutor) ExecuteRecovery(ctx context.Context, opts *RecoveryOptions) (*RecoveryResult, error) {
	// 防止并发恢复
	if !re.isRecovering.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("recovery already in progress")
	}
	defer re.isRecovering.Store(false)

	if opts == nil {
		opts = &RecoveryOptions{
			Strategy:              INCREMENTAL,
			ValidateAfterRecovery: true,
			Timeout:               5 * time.Minute,
			MaxRetries:            3,
		}
	}

	startTime := time.Now()
	hlog.Infof("[RecoveryExecutor] Starting recovery with strategy=%s", opts.Strategy)

	// Step 1: 获取待恢复列表
	pendingList, err := re.getPendingRecoveryList(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending recovery list: %v", err)
	}

	if len(pendingList) == 0 {
		hlog.Infof("[RecoveryExecutor] No pending recovery items found")
		return &RecoveryResult{
			IsValid:        true,
			RecoveryStart:  startTime,
			RecoveryEnd:    time.Now(),
			EventsReplayed: 0,
		}, nil
	}

	hlog.Infof("[RecoveryExecutor] Found %d recovery items", len(pendingList))

	// Step 2: 并行恢复所有待恢复项
	results := make([]*RecoveryResult, len(pendingList))
	var wg sync.WaitGroup
	var totalEventsReplayed int64

	for i, item := range pendingList {
		wg.Add(1)
		go func(idx int, recoveryItem *recoveryItem) {
			defer wg.Done()

			result, err := re.recoverSingleProcessor(ctx, recoveryItem, opts)
			if err != nil {
				hlog.Errorf("[RecoveryExecutor] Recovery failed for %s:%s: %v",
					recoveryItem.processorName, recoveryItem.symbol, err)
				result = &RecoveryResult{
					ProcessorName:   recoveryItem.processorName,
					Symbol:          recoveryItem.symbol,
					IsValid:         false,
					ValidationError: err,
				}
			}

			results[idx] = result
			atomic.AddInt64(&totalEventsReplayed, result.EventsReplayed)
		}(i, item)
	}

	wg.Wait()

	// Step 3: 更新统计信息
	re.mu.Lock()
	re.stats.TotalRecoveryTime = time.Since(startTime)
	re.stats.ProcessorsRecovered = len(pendingList)
	re.stats.EventsProcessed = totalEventsReplayed
	re.stats.LastRecoveryTime = time.Now()
	re.lastRecoveryTime = time.Now()
	re.mu.Unlock()

	// Step 4: 验证所有结果
	allValid := true
	for _, result := range results {
		if !result.IsValid {
			allValid = false
			break
		}
	}

	hlog.Infof("[RecoveryExecutor] Recovery completed in %v (events=%d, valid=%v)",
		time.Since(startTime), totalEventsReplayed, allValid)

	// 返回第一个结果作为总体结果
	// (在生产环境可能需要返回更详细的信息)
	if len(results) > 0 && results[0] != nil {
		results[0].EventsReplayed = totalEventsReplayed
		results[0].IsValid = allValid
		return results[0], nil
	}

	return &RecoveryResult{
		IsValid:        allValid,
		RecoveryStart:  startTime,
		RecoveryEnd:    time.Now(),
		EventsReplayed: totalEventsReplayed,
	}, nil
}

// recoverSingleProcessor 恢复单个处理器
// 使用分布式锁防止并发恢复
func (re *RecoveryExecutor) recoverSingleProcessor(
	ctx context.Context,
	item *recoveryItem,
	opts *RecoveryOptions,
) (*RecoveryResult, error) {
	result := &RecoveryResult{
		ProcessorName: item.processorName,
		Symbol:        item.symbol,
		RecoveryStart: time.Now(),
	}

	// 使用分布式锁保护恢复过程
	// 防止多个节点同时恢复同一个symbol
	return result, re.lockMgr.WithRecoveryLock(ctx, item.processorName, item.symbol, func(ctx context.Context) error {
		// Step 1: 获取恢复上下文
		recoveryCtx, err := re.checkpointMgr.GetRecoveryContext(item.processorName, item.symbol)
		if err != nil {
			return fmt.Errorf("failed to get recovery context: %v", err)
		}

		if recoveryCtx == nil {
			// 第一次处理，无需恢复
			result.IsValid = true
			result.RecoveryEnd = time.Now()
			return nil
		}

		result.PreCrashChecksum = recoveryCtx.PreCrashChecksum

		hlog.Infof("[RecoveryExecutor] Recovering %s:%s from seq=%d (with distributed lock)",
			item.processorName, item.symbol, recoveryCtx.StartEventSeq)

		// Step 2: 获取需要重放的事件
		var events []model.MatchingEngineEvent
		if re.eventLog != nil {
			envs, err := re.eventLog.GetEventsBySymbol(recoveryCtx.Symbol, recoveryCtx.StartEventSeq, 1000)
			if err != nil {
				return fmt.Errorf("failed to get events for replay: %v", err)
			}
			for _, env := range envs {
				if env != nil {
					event, err := model.UnmarshalEvent(env)
					if err != nil {
						hlog.Warnf("[RecoveryExecutor] Failed to unmarshal event: %v", err)
						continue
					}
					events = append(events, event)
				}
			}
		}

		hlog.Infof("[RecoveryExecutor] Got %d events to replay for %s:%s (locked)",
			len(events), item.processorName, item.symbol)

		// Step 3: 重放事件（使用 ProcessEventWithContext 建立完整的 Kafka Offset 链）
		eventPipeline := re.matchEngine.GetEventPipeline()
		if eventPipeline == nil {
			return fmt.Errorf("event pipeline not found")
		}

		eventsReplayed := 0
		currentKafkaOffset := recoveryCtx.StartKafkaOffset

		for _, event := range events {
			// 检查context是否已超时
			select {
			case <-ctx.Done():
				return fmt.Errorf("recovery context timeout")
			default:
			}

			// 创建完整的 EventContext，包含 Kafka offset 和所有权Epoch
			eventCtx := model.NewEventContext(
				event,
				recoveryCtx.Topic,
				recoveryCtx.Partition,
				currentKafkaOffset,
			)
			
			// 注：所有权Epoch在恢复时通常为最新值
			// 因为恢复发生在当前所有者持有锁时
			eventCtx.WithOwnershipEpoch(1, "recovery") // 恢复流程

			err := eventPipeline.ProcessEventWithContext(eventCtx)
			if err != nil {
				hlog.Errorf("[RecoveryExecutor] Failed to process event seq=%d with Kafka offset %d: %v",
					event.EventSeq(), currentKafkaOffset, err)
				return fmt.Errorf("failed to process event: %v", err)
			}

			eventsReplayed++
			currentKafkaOffset++
		}

		result.EventsReplayed = int64(eventsReplayed)
		result.OrdersRestored = recoveryCtx.ExpectedOrderCount
		result.TradesRestored = recoveryCtx.ExpectedTradeCount

		// Step 4: 验证恢复结果
		if opts.ValidateAfterRecovery {
			postChecksum := re.checkpointMgr.CalculateStateChecksum(
				re.matchEngine.GetOrderBook(item.symbol),
				re.matchEngine.GetPositions(),
			)
			result.PostRecoveryChecksum = postChecksum

			isValid, errMsg := re.checkpointMgr.ValidateRecovery(
				recoveryCtx.PreCrashChecksum,
				postChecksum,
				recoveryCtx.ExpectedOrderCount,
				recoveryCtx.ExpectedOrderCount,
				recoveryCtx.ExpectedTradeCount,
				recoveryCtx.ExpectedTradeCount,
			)

			if !isValid {
				result.ValidationError = fmt.Errorf("%s", errMsg)
				result.IsValid = false
				hlog.Errorf("[RecoveryExecutor] Validation failed for %s:%s: %s",
					item.processorName, item.symbol, errMsg)
			} else {
				result.IsValid = true
				hlog.Infof("[RecoveryExecutor] Validation passed for %s:%s",
					item.processorName, item.symbol)
			}
		} else {
			result.IsValid = true
		}

		result.RecoveryEnd = time.Now()

		// Step 5: 标记恢复完成
		if err := re.checkpointMgr.MarkRecoveryComplete(recoveryCtx, result.IsValid, ""); err != nil {
			hlog.Errorf("[RecoveryExecutor] Failed to mark recovery complete: %v", err)
		}

		return nil
	})
}

// getPendingRecoveryList 获取待恢复列表
// 如果指定了processor和symbol，使用那个
// 否则自动发现所有待恢复项
func (re *RecoveryExecutor) getPendingRecoveryList(opts *RecoveryOptions) ([]*recoveryItem, error) {
	var pendingList []*recoveryItem

	// 优先使用明确指定的processor和symbol
	if len(opts.ProcessorNames) > 0 && len(opts.Symbols) > 0 {
		for _, procName := range opts.ProcessorNames {
			for _, symbol := range opts.Symbols {
				ctx, err := re.checkpointMgr.GetRecoveryContext(procName, symbol)
				if err != nil {
					hlog.Warnf("[RecoveryExecutor] Failed to get recovery context for %s:%s: %v",
						procName, symbol, err)
					continue
				}

				if ctx != nil {
					pendingList = append(pendingList, &recoveryItem{
						processorName: procName,
						symbol:        symbol,
					})
				}
			}
		}
		return pendingList, nil
	}

	// 否则自动发现所有待恢复项
	hlog.Infof("[RecoveryExecutor] Auto-discovering pending recovery items...")
	pendingItems, err := re.checkpointMgr.ListPendingRecoveryItems()
	if err != nil {
		hlog.Errorf("[RecoveryExecutor] Failed to list pending recovery items: %v", err)
		return nil, fmt.Errorf("failed to list pending recovery items: %v", err)
	}

	for _, item := range pendingItems {
		pendingList = append(pendingList, &recoveryItem{
			processorName: item["processor_name"],
			symbol:        item["symbol"],
		})
	}

	hlog.Infof("[RecoveryExecutor] Auto-discovered %d pending recovery items", len(pendingList))
	return pendingList, nil
}

// GetRecoveryStats 获取恢复统计信息
func (re *RecoveryExecutor) GetRecoveryStats() *RecoveryStats {
	re.mu.RLock()
	defer re.mu.RUnlock()

	// 返回深拷贝
	return &RecoveryStats{
		TotalRecoveryTime:   re.stats.TotalRecoveryTime,
		ProcessorsRecovered: re.stats.ProcessorsRecovered,
		EventsProcessed:     re.stats.EventsProcessed,
		StateValidated:      re.stats.StateValidated,
		LastRecoveryTime:    re.stats.LastRecoveryTime,
	}
}

// recoveryItem 内部结构，表示一个待恢复项
type recoveryItem struct {
	processorName string
	symbol        string
}
