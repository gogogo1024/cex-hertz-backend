package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// EventPipeline 事件处理管道（Event Sourcing核心）
// 负责将事件从 Event Log 分发给多个 Processor
// 保证：
// 1. 事件按序处理
// 2. 支持幂等性（同一事件重复处理结果相同）
// 3. 支持并发处理（不同 symbol 的事件可以并发，但同一 symbol 的事件保证顺序）
// 4. 支持crash recovery（通过checkpoint机制）
type EventPipeline struct {
	eventLog model.EventStore

	// 处理器列表
	processors []model.EventProcessor

	// 用于控制管道的生命周期
	ctx    context.Context
	cancel context.CancelFunc

	// 用于追踪每个处理器对每个 symbol 的处理进度
	mu               sync.RWMutex
	processorOffsets map[string]map[string]uint64 // processorName -> symbol -> lastProcessedSeq

	// Checkpoint管理器（用于crash recovery）
	checkpointMgr *CheckpointManager
}

// NewEventPipeline 创建一个新的事件处理管道
func NewEventPipeline(eventLog model.EventStore) *EventPipeline {
	ctx, cancel := context.WithCancel(context.Background())
	return &EventPipeline{
		eventLog:         eventLog,
		processors:       make([]model.EventProcessor, 0),
		ctx:              ctx,
		cancel:           cancel,
		processorOffsets: make(map[string]map[string]uint64),
		checkpointMgr:    nil, // 稍后由SetCheckpointManager设置
	}
}

// SetCheckpointManager 设置checkpoint管理器（用于crash recovery）
// 必须在注册处理器后、处理事件前调用
func (ep *EventPipeline) SetCheckpointManager(cm *CheckpointManager) {
	ep.checkpointMgr = cm
}

// RegisterProcessor 注册一个事件处理器
func (ep *EventPipeline) RegisterProcessor(processor model.EventProcessor) error {
	if processor == nil {
		return fmt.Errorf("processor cannot be nil")
	}

	ep.mu.Lock()
	defer ep.mu.Unlock()

	ep.processors = append(ep.processors, processor)
	name := processor.ProcessorName()
	ep.processorOffsets[name] = make(map[string]uint64)

	hlog.Infof("[EventPipeline] Registered processor: %s", name)
	return nil
}

// ProcessEvent 处理一个事件（同步）
// 该方法会将事件分发给所有已注册的处理器
// 如果任何处理器失败，将返回错误
func (ep *EventPipeline) ProcessEvent(event model.MatchingEngineEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	// 首先将事件追加到日志
	if err := ep.eventLog.AppendEvent(event); err != nil {
		hlog.Errorf("[EventPipeline] Failed to append event to log: %v", err)
		return err
	}

	// 然后分发给所有处理器
	return ep.dispatchToProcessors(event)
}

// dispatchToProcessors 内部方法，将事件分发给所有处理器
func (ep *EventPipeline) dispatchToProcessors(event model.MatchingEngineEvent) error {
	ep.mu.Lock()
	defer ep.mu.Unlock()

	symbol := event.Symbol()
	seq := event.EventSeq()

	// 并发分发给所有处理器
	// 虽然这里用 goroutines，但每个 symbol 的处理顺序在发送端已经保证了
	var wg sync.WaitGroup
	errChan := make(chan error, len(ep.processors))

	for _, processor := range ep.processors {
		wg.Add(1)
		go func(p model.EventProcessor) {
			defer wg.Done()

			procName := p.ProcessorName()
			lastSeq := ep.processorOffsets[procName][symbol]

			// 检查是否已处理过（幂等性检查）
			if seq <= lastSeq {
				hlog.Infof("[EventPipeline] Skipping duplicate event for processor %s, symbol %s, seq %d (lastSeq: %d)",
					procName, symbol, seq, lastSeq)
				return
			}

			// 处理事件
			if err := p.ProcessEvent(event); err != nil {
				hlog.Errorf("[EventPipeline] Processor %s failed to process event: %v", procName, err)
				errChan <- fmt.Errorf("processor %s: %w", procName, err)
				return
			}

			// 更新处理进度
			ep.processorOffsets[procName][symbol] = seq

			// 记录到checkpoint（用于crash recovery）
			if ep.checkpointMgr != nil {
				// 简单版本：每个事件都记录一次
				// 生产环境可以改成批量记录以提高性能
				ep.checkpointMgr.RecordEventProcessed(
					procName,
					symbol,
					seq,
					0, // kafkaOffset (稍后集成Kafka时填充)
					0, // partitionID (稍后集成Kafka时填充)
					"", // stateChecksum (稍后集成时计算)
					0, // orderCount (稍后集成时计算)
					0, // tradeCount (稍后集成时计算)
				)
			}

			hlog.Debugf("[EventPipeline] Processor %s processed event, symbol %s, seq %d", procName, symbol, seq)
		}(processor)
	}

	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// 事件处理完成后，flush checkpoint到数据库
	if ep.checkpointMgr != nil {
		if err := ep.checkpointMgr.FlushCheckpoints(); err != nil {
			hlog.Warnf("[EventPipeline] Failed to flush checkpoints: %v", err)
			// 不return error，因为事件已经处理成功
			// checkpoint失败只会影响恢复效率，不影响当前运行
		}
	}

	return nil
}

// ProcessEventsFromLog 从事件日志中批量处理事件
// 用于恢复或重放场景
func (ep *EventPipeline) ProcessEventsFromLog(symbol string, startSeq uint64, limit int) error {
	events, err := ep.eventLog.GetEventsBySymbol(symbol, startSeq, limit)
	if err != nil {
		return err
	}

	for _, envelope := range events {
		event, err := model.UnmarshalEvent(envelope)
		if err != nil {
			hlog.Errorf("[EventPipeline] Failed to unmarshal event: %v", err)
			continue
		}

		if err := ep.dispatchToProcessors(event); err != nil {
			hlog.Errorf("[EventPipeline] Failed to process event from log: %v", err)
			// 根据策略决定是否继续或中止
			// 这里选择继续，但记录错误
		}
	}

	return nil
}

// GetProcessorOffset 获取某个处理器对某个 symbol 的处理进度
func (ep *EventPipeline) GetProcessorOffset(processorName, symbol string) uint64 {
	ep.mu.RLock()
	defer ep.mu.RUnlock()

	if offsets, ok := ep.processorOffsets[processorName]; ok {
		return offsets[symbol]
	}
	return 0
}

// Shutdown 优雅关闭事件管道
func (ep *EventPipeline) Shutdown() {
	ep.cancel()
	hlog.Infof("[EventPipeline] Shutdown complete")
}
