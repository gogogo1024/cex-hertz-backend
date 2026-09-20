package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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
// 5. Kafka-aware：完整的 transport metadata 追踪
type EventPipeline struct {
	eventLog model.EventStore

	// 处理器列表
	processors []model.EventProcessor

	// 用于控制管道的生命周期
	ctx    context.Context
	cancel context.CancelFunc

	// ===== 改进的并发模型 =====
	// 1. 只在需要读 processors 列表时持 mu
	// 2. offset 更新使用 per-symbol sync.Mutex 避免全局串行化
	regMu sync.RWMutex // 保护 processors 列表的读

	processorOffsets map[string]map[string]uint64 // processorName -> symbol -> lastProcessedSeq
	offsetsMu        sync.RWMutex                 // 保护 processorOffsets 的读

	// ===== Kafka Offset 追踪 =====
	// 用于建立 Kafka Offset ↔ Event Seq 的映射
	kafkaOffsets  map[string]map[string]int64 // processorName -> symbol -> lastKafkaOffset
	kafkaOffsetMu sync.RWMutex                // 保护 kafkaOffsets 的读写

	// per-processor+symbol locks，保证同一 processor 对同一 symbol 的处理为单飞
	// key 格式： processorName + 0x00 + symbol
	procSymLocks sync.Map // map[string]*sync.Mutex

	// Checkpoint管理器（用于crash recovery）
	checkpointMgr *CheckpointManager

	// 处理统计
	processedEventCount int64 // 原子操作
	failedEventCount    int64 // 原子操作
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
		kafkaOffsets:     make(map[string]map[string]int64),
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

	ep.regMu.Lock()
	defer ep.regMu.Unlock()

	ep.processors = append(ep.processors, processor)
	name := processor.ProcessorName()

	ep.offsetsMu.Lock()
	ep.processorOffsets[name] = make(map[string]uint64)
	ep.offsetsMu.Unlock()

	ep.kafkaOffsetMu.Lock()
	ep.kafkaOffsets[name] = make(map[string]int64)
	ep.kafkaOffsetMu.Unlock()

	hlog.Infof("[EventPipeline] Registered processor: %s", name)
	return nil
}

// ProcessEvent 处理一个事件（同步）
// 该方法会将事件分发给所有已注册的处理器
// 如果任何处理器失败，将返回错误
//
// 注意：这个方法的 Kafka offset 信息会被硬编码为 0，用于向后兼容
// 推荐使用 ProcessEventWithContext() 来获得完整的 Kafka 元数据支持
func (ep *EventPipeline) ProcessEvent(event model.MatchingEngineEvent) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	// 首先将事件追加到日志
	if err := ep.eventLog.AppendEvent(event); err != nil {
		hlog.Errorf("[EventPipeline] Failed to append event to log: %v", err)
		atomic.AddInt64(&ep.failedEventCount, 1)
		return err
	}

	// 然后分发给所有处理器（不含 Kafka 元数据）
	return ep.dispatchToProcessorsInternal(event, nil)
}

// ProcessEventWithContext 处理一个事件，携带完整的 Kafka 上下文
// 这是推荐的方式，能够建立完整的 Kafka Offset ↔ Event Seq 映射链
//
// 完整的链路：
//
//	Kafka Record {topic, partition, offset}
//	  ↓
//	EventContext {Event + Transport Metadata}
//	  ↓
//	EventStore.AppendEvent() ← 保存 event
//	  ↓
//	ProcessEventWithContext() ← 处理 + 记录完整信息
//	  ↓
//	RecordEventProcessed(..., kafkaOffset, ...) ← 记录真实的 offset
//	  ↓
//	Checkpoint {EventSeq, KafkaOffset, StateChecksum}
func (ep *EventPipeline) ProcessEventWithContext(eventCtx *model.EventContext) error {
	if eventCtx == nil {
		return fmt.Errorf("event context cannot be nil")
	}

	if !eventCtx.IsValid() {
		return fmt.Errorf("event context is invalid")
	}

	event := eventCtx.Event

	// 首先将事件追加到日志
	if err := ep.eventLog.AppendEvent(event); err != nil {
		hlog.Errorf("[EventPipeline] Failed to append event to log: %v", err)
		atomic.AddInt64(&ep.failedEventCount, 1)
		return err
	}

	// 然后分发给所有处理器（携带 Kafka 元数据）
	return ep.dispatchToProcessorsInternal(event, eventCtx)
}

// dispatchToProcessorsInternal 内部方法，将事件分发给所有处理器
// 这个版本改进了并发模型：
// - 只在获取 processors 列表时持锁
// - 立即释放锁，让 Kafka consumer 不被阻塞
// - 使用原子操作来更新 offset
// - 支持 Kafka metadata（通过 eventCtx）
func (ep *EventPipeline) dispatchToProcessorsInternal(
	event model.MatchingEngineEvent,
	eventCtx *model.EventContext, // nil-safe，如果为 nil 则使用零值
) error {
	symbol := event.Symbol()
	seq := event.EventSeq()

	// ===== PHASE 1: 快速读取处理器列表并释放锁 =====
	ep.regMu.RLock()
	// 复制 processors 列表以避免在处理事件时被列表修改影响
	processorsCopy := make([]model.EventProcessor, len(ep.processors))
	copy(processorsCopy, ep.processors)
	ep.regMu.RUnlock()

	// ===== PHASE 2: 并发分发给所有处理器（不持锁） =====
	var wg sync.WaitGroup
	errChan := make(chan error, len(processorsCopy))

	for _, processor := range processorsCopy {
		wg.Add(1)
		go func(p model.EventProcessor) {
			defer wg.Done()

			procName := p.ProcessorName()
			// 使用 per-processor+symbol 的单飞锁，保证相同 processor 对相同 symbol 的事件串行执行
			lockKey := procName + "\x00" + symbol
			val, _ := ep.procSymLocks.LoadOrStore(lockKey, &sync.Mutex{})
			mu := val.(*sync.Mutex)
			mu.Lock()
			defer mu.Unlock()

			// 读取当前的 lastSeq（精细锁）
			ep.offsetsMu.RLock()
			lastSeq := ep.processorOffsets[procName][symbol]
			ep.offsetsMu.RUnlock()

			// 检查是否已处理过（幂等性检查）
			if seq <= lastSeq {
				hlog.Infof("[EventPipeline] Skipping duplicate event for processor %s, symbol %s, seq %d (lastSeq: %d)",
					procName, symbol, seq, lastSeq)
				return
			}

			// ===== 处理事件（在单飞锁下执行，保证同一 processor+symbol 串行） =====
			if err := p.ProcessEvent(event); err != nil {
				hlog.Errorf("[EventPipeline] Processor %s failed to process event: %v", procName, err)
				errChan <- fmt.Errorf("processor %s: %w", procName, err)
				atomic.AddInt64(&ep.failedEventCount, 1)
				return
			}

			// ===== PHASE 3: 更新处理进度（精细锁） =====
			ep.offsetsMu.Lock()
			ep.processorOffsets[procName][symbol] = seq
			ep.offsetsMu.Unlock()

			// 记录 Kafka offset（如果有的话）
			if eventCtx != nil && eventCtx.Offset >= 0 {
				ep.kafkaOffsetMu.Lock()
				ep.kafkaOffsets[procName][symbol] = eventCtx.Offset
				ep.kafkaOffsetMu.Unlock()
			}

			// 记录到checkpoint（用于crash recovery）
			if ep.checkpointMgr != nil {
				kafkaOffset := int64(0)
				partitionID := int32(0)

				// 从 eventCtx 中提取真实的 Kafka metadata
				if eventCtx != nil {
					kafkaOffset = eventCtx.Offset
					partitionID = eventCtx.Partition
				}

				// 简单版本：每个事件都记录一次
				// 生产环境可以改成批量记录以提高性能
				ep.checkpointMgr.RecordEventProcessed(
					procName,
					symbol,
					seq,
					kafkaOffset,            // ✅ 真实的 Kafka offset（不再是零值）
					partitionID,            // ✅ 真实的 partition（不再是零值）
					eventCtx.StateChecksum, // 后续可由状态验证器填充
					eventCtx.OrderCount,    // 后续可由状态验证器填充
					eventCtx.TradeCount,    // 后续可由状态验证器填充
				)
			}

			atomic.AddInt64(&ep.processedEventCount, 1)
			hlog.Debugf("[EventPipeline] Processor %s processed event, symbol %s, seq %d, kafkaOffset %d",
				procName, symbol, seq, func() int64 {
					if eventCtx != nil {
						return eventCtx.Offset
					}
					return 0
				}())
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

// dispatchToProcessors 已弃用：使用 dispatchToProcessorsInternal
// 保留以支持向后兼容
func (ep *EventPipeline) dispatchToProcessors(event model.MatchingEngineEvent) error {
	return ep.dispatchToProcessorsInternal(event, nil)
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

		if err := ep.dispatchToProcessorsInternal(event, nil); err != nil {
			hlog.Errorf("[EventPipeline] Failed to process event from log: %v", err)
			// 根据策略决定是否继续或中止
			// 这里选择继续，但记录错误
		}
	}

	return nil
}

// GetProcessorOffset 获取某个处理器对某个 symbol 的处理进度
func (ep *EventPipeline) GetProcessorOffset(processorName, symbol string) uint64 {
	ep.offsetsMu.RLock()
	defer ep.offsetsMu.RUnlock()

	if offsets, ok := ep.processorOffsets[processorName]; ok {
		return offsets[symbol]
	}
	return 0
}

// GetProcessorKafkaOffset 获取某个处理器对某个 symbol 的最后一次处理的 Kafka offset
// 用于确定恢复时从 Kafka 的哪个位置重新消费
func (ep *EventPipeline) GetProcessorKafkaOffset(processorName, symbol string) int64 {
	ep.kafkaOffsetMu.RLock()
	defer ep.kafkaOffsetMu.RUnlock()

	if offsets, ok := ep.kafkaOffsets[processorName]; ok {
		return offsets[symbol]
	}
	return -1 // 表示未处理过
}

// GetProcessor 获取指定名称的处理器（用于crash recovery）
func (ep *EventPipeline) GetProcessor(processorName string) model.EventProcessor {
	ep.regMu.RLock()
	defer ep.regMu.RUnlock()

	for _, processor := range ep.processors {
		if processor.ProcessorName() == processorName {
			return processor
		}
	}
	return nil
}

// GetProcessors 获取所有处理器的副本（用于监控/统计）
func (ep *EventPipeline) GetProcessors() []model.EventProcessor {
	ep.regMu.RLock()
	defer ep.regMu.RUnlock()

	processorsCopy := make([]model.EventProcessor, len(ep.processors))
	copy(processorsCopy, ep.processors)
	return processorsCopy
}

// GetCheckpointManager 获取checkpoint管理器（用于crash recovery）
func (ep *EventPipeline) GetCheckpointManager() *CheckpointManager {
	return ep.checkpointMgr
}

// GetPipelineStats 获取管道统计信息
type PipelineStats struct {
	ProcessedEvents int64
	FailedEvents    int64
}

func (ep *EventPipeline) GetPipelineStats() PipelineStats {
	return PipelineStats{
		ProcessedEvents: atomic.LoadInt64(&ep.processedEventCount),
		FailedEvents:    atomic.LoadInt64(&ep.failedEventCount),
	}
}

// Shutdown 优雅关闭事件管道
func (ep *EventPipeline) Shutdown() {
	ep.cancel()
	hlog.Infof("[EventPipeline] Shutdown complete")
}
