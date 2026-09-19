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
	FULL_REPLAY    RecoveryStrategy = "full_replay"    // 从初始状态重放所有事件
	INCREMENTAL    RecoveryStrategy = "incremental"    // 只重放crash后的事件
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
	EventsReplayed     int64
	OrdersRestored     int64
	TradesRestored     int64

	// 恢复前后状态
	PreCrashChecksum   string
	PostRecoveryChecksum string

	// 验证结果
	IsValid           bool
	ValidationError   error
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
type RecoveryExecutor struct {
	matchEngine      *PartitionAwareMatchEngine
	checkpointMgr    *CheckpointManager
	eventLog         model.EventStore
	stateValidator   *StateValidator

	mu                sync.RWMutex
	stats             *RecoveryStats
	lastRecoveryTime  time.Time
	isRecovering      atomic.Bool
}

// NewRecoveryExecutor 创建恢复执行器
func NewRecoveryExecutor(
	matchEngine *PartitionAwareMatchEngine,
	checkpointMgr *CheckpointManager,
	eventLog model.EventStore,
) *RecoveryExecutor {
	return &RecoveryExecutor{
		matchEngine:     matchEngine,
		checkpointMgr:   checkpointMgr,
		eventLog:        eventLog,
		stateValidator:  NewStateValidator(),
		stats:           &RecoveryStats{},
		lastRecoveryTime: time.Now(),
	}
}

// ShouldRecover 检查是否需要恢复 (启动时调用)
func (re *RecoveryExecutor) ShouldRecover(ctx context.Context) (bool, error) {
	// 检查是否有未完成的恢复
	// 或者检查checkpoint中是否有pending的recovery记录

	// 对于现在的简单实现，我们检查是否有最近的checkpoint
	// 如果有checkpoint但EventLog是空的，说明需要恢复

	if re.eventLog == nil {
		return false, nil
	}

	// TODO: 实现具体的检查逻辑
	// 现在简单返回false，表示不需要恢复
	return false, nil
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
			IsValid:           true,
			RecoveryStart:     startTime,
			RecoveryEnd:       time.Now(),
			EventsReplayed:    0,
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
					ProcessorName: recoveryItem.processorName,
					Symbol:        recoveryItem.symbol,
					IsValid:       false,
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
func (re *RecoveryExecutor) recoverSingleProcessor(
	ctx context.Context,
	item *recoveryItem,
	opts *RecoveryOptions,
) (*RecoveryResult, error) {
	result := &RecoveryResult{
		ProcessorName:     item.processorName,
		Symbol:            item.symbol,
		RecoveryStart:     time.Now(),
	}

	// Step 1: 获取恢复上下文
	recoveryCtx, err := re.checkpointMgr.GetRecoveryContext(item.processorName, item.symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get recovery context: %v", err)
	}

	if recoveryCtx == nil {
		// 第一次处理，无需恢复
		result.IsValid = true
		result.RecoveryEnd = time.Now()
		return result, nil
	}

	result.PreCrashChecksum = recoveryCtx.PreCrashChecksum

	hlog.Infof("[RecoveryExecutor] Recovering %s:%s from seq=%d", 
		item.processorName, item.symbol, recoveryCtx.StartEventSeq)

	// Step 2: 获取需要重放的事件
	var events []model.MatchingEngineEvent
	if re.eventLog != nil {
		envs, err := re.eventLog.GetEventsBySymbol(recoveryCtx.Symbol, recoveryCtx.StartEventSeq, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to get events for replay: %v", err)
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

	hlog.Infof("[RecoveryExecutor] Got %d events to replay for %s:%s", 
		len(events), item.processorName, item.symbol)

	// Step 3: 重放事件
	eventsReplayed := 0
	for _, event := range events {
		// 检查context是否已超时
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("recovery context timeout")
		default:
		}

		// 找到对应的处理器
		processor := re.matchEngine.GetProcessor(item.processorName)
		if processor == nil {
			return nil, fmt.Errorf("processor %s not found", item.processorName)
		}

		// 处理事件 (幂等的)
		if err := processor.ProcessEvent(event); err != nil {
			hlog.Errorf("[RecoveryExecutor] Failed to process event seq=%d: %v", event.EventSeq(), err)
			return nil, fmt.Errorf("failed to process event: %v", err)
		}

		eventsReplayed++
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

		// 验证checksum是否匹配
		isValid, errMsg := re.checkpointMgr.ValidateRecovery(
			recoveryCtx.PreCrashChecksum,
			postChecksum,
			recoveryCtx.ExpectedOrderCount,
			recoveryCtx.ExpectedOrderCount,
			recoveryCtx.ExpectedTradeCount,
			recoveryCtx.ExpectedTradeCount,
		)

		if !isValid {
			result.ValidationError = fmt.Errorf(errMsg)
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
		// 不返回错误，只记录日志
	}

	return result, nil
}

// getPendingRecoveryList 获取待恢复列表
func (re *RecoveryExecutor) getPendingRecoveryList(opts *RecoveryOptions) ([]*recoveryItem, error) {
	// TODO: 从checkpointMgr获取待恢复列表
	// 现在简单实现：返回空列表

	var pendingList []*recoveryItem

	// 如果指定了processor和symbol，直接用那个
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
	}

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
