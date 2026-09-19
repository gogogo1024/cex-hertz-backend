package service

import (
	"encoding/json"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// PostgresEventLog 是基于 PostgreSQL 的事件日志实现
// 替代 InMemoryEventLog，提供持久化存储和高可用性
type PostgresEventLog struct {
	mu          sync.RWMutex
	eventStore  model.PersistentEventStore
	sequencer   *Sequencer
	localSeqMap map[string]int64 // 追踪各 symbol 的最新 LocalSeq
}

// NewPostgresEventLog 创建一个基于 PostgreSQL 的事件日志
// eventStore: 持久化事件存储实现（通常是 PostgresPersistentEventStore）
// sequencer: 全局序列号生成器
func NewPostgresEventLog(eventStore model.PersistentEventStore, sequencer *Sequencer) *PostgresEventLog {
	return &PostgresEventLog{
		eventStore:  eventStore,
		sequencer:   sequencer,
		localSeqMap: make(map[string]int64),
	}
}

// AppendEvent 追加事件到 PostgreSQL
// 将 MatchingEngineEvent 序列化后存入数据库
func (pl *PostgresEventLog) AppendEvent(event model.MatchingEngineEvent) error {
	if event == nil {
		return ErrEventNil
	}

	// 生成全局序列号
	globalSeq := pl.sequencer.NextGlobalSeq()

	// 序列化事件
	envelope, err := model.MarshalEvent(event)
	if err != nil {
		hlog.Error("[EventLog] Failed to marshal event:", err)
		return err
	}

	// 转换为 PersistentEvent
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		hlog.Error("[EventLog] Failed to marshal payload:", err)
		return err
	}

	persistEvent := &model.PersistentEvent{
		GlobalSeq:      int64(globalSeq),
		EventType:      envelope.EventType,
		AggregateID:    "", // 为空（向后兼容）
		AggregateType:  "MatchingEngine",
		Symbol:         event.Symbol(),
		Payload:        string(payloadBytes),
		EventTimestamp: event.EventTimestamp(),
		CreatedAt:      envelope.Timestamp, // 使用事件时间戳
		Version:        1,
	}

	// 写入数据库
	if err := pl.eventStore.WriteEvent(persistEvent); err != nil {
		hlog.Error("[EventLog] Failed to write event to database:", err)
		return err
	}

	// 更新本地序列号追踪
	pl.mu.Lock()
	defer pl.mu.Unlock()
	symbol := event.Symbol()
	pl.localSeqMap[symbol] = int64(globalSeq)

	hlog.Debugf("[EventLog] Event appended (seq=%d, type=%s, symbol=%s)", globalSeq, envelope.EventType, symbol)
	return nil
}

// GetEventsBySymbol 从 PostgreSQL 获取指定 symbol 的事件
// 按 GlobalSeq 排序，确保顺序一致性
func (pl *PostgresEventLog) GetEventsBySymbol(symbol string, startSeq uint64, limit int) ([]*model.EventEnvelope, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// 查询数据库
	persistEvents, err := pl.eventStore.GetSymbolEvents(symbol, int64(startSeq), int64(startSeq)+int64(limit))
	if err != nil {
		hlog.Error("[EventLog] Failed to get events by symbol:", err)
		return nil, err
	}

	// 转换回 EventEnvelope
	result := make([]*model.EventEnvelope, 0, len(persistEvents))
	for _, pe := range persistEvents {
		envelope := &model.EventEnvelope{
			Seq:       uint64(pe.GlobalSeq),
			Timestamp: pe.CreatedAt,
			EventType: pe.EventType,
			Symbol:    pe.Symbol,
			Payload:   json.RawMessage(pe.Payload),
		}
		result = append(result, envelope)
	}

	return result, nil
}

// GetEventsSinceTime 从 PostgreSQL 获取指定时间戳之后的事件
// 按 GlobalSeq 排序
func (pl *PostgresEventLog) GetEventsSinceTime(symbol string, timestamp int64) ([]*model.EventEnvelope, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// 获取最新序列号
	latestSeq, err := pl.eventStore.GetLatestGlobalSeq()
	if err != nil {
		hlog.Error("[EventLog] Failed to get latest seq:", err)
		return nil, err
	}

	// 查询该 symbol 从时间戳开始的事件
	persistEvents, err := pl.eventStore.GetSymbolEvents(symbol, 0, latestSeq+1)
	if err != nil {
		hlog.Error("[EventLog] Failed to get events by symbol:", err)
		return nil, err
	}

	// 过滤时间戳 >= timestamp 的事件
	result := make([]*model.EventEnvelope, 0)
	for _, pe := range persistEvents {
		if pe.EventTimestamp >= timestamp {
			envelope := &model.EventEnvelope{
				Seq:       uint64(pe.GlobalSeq),
				Timestamp: pe.CreatedAt,
				EventType: pe.EventType,
				Symbol:    pe.Symbol,
				Payload:   json.RawMessage(pe.Payload),
			}
			result = append(result, envelope)
		}
	}

	return result, nil
}

// GetLatestSeq 从 PostgreSQL 获取最新的序列号
func (pl *PostgresEventLog) GetLatestSeq(symbol string) (uint64, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// 从本地追踪获取（快速路径）
	if seq, ok := pl.localSeqMap[symbol]; ok {
		return uint64(seq), nil
	}

	// 从数据库查询最新序列号
	latestSeq, err := pl.eventStore.GetLatestGlobalSeq()
	if err != nil {
		hlog.Error("[EventLog] Failed to get latest seq from database:", err)
		return 0, err
	}

	return uint64(latestSeq), nil
}

// GetAllEvents 获取所有事件（用于测试和调试）
func (pl *PostgresEventLog) GetAllEvents() ([]*model.EventEnvelope, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// 获取全局事件流（不限制序列号范围）
	persistEvents, err := pl.eventStore.GetGlobalEventStream(0, 100000)
	if err != nil {
		hlog.Error("[EventLog] Failed to get all events:", err)
		return nil, err
	}

	// 转换回 EventEnvelope
	result := make([]*model.EventEnvelope, 0, len(persistEvents))
	for _, pe := range persistEvents {
		envelope := &model.EventEnvelope{
			Seq:       uint64(pe.GlobalSeq),
			Timestamp: pe.CreatedAt,
			EventType: pe.EventType,
			Symbol:    pe.Symbol,
			Payload:   json.RawMessage(pe.Payload),
		}
		result = append(result, envelope)
	}

	return result, nil
}

// GetEventsBySymbolRange 按 symbol 和序列号范围查询事件
func (pl *PostgresEventLog) GetEventsBySymbolRange(symbol string, startSeq, endSeq int64, limit int) ([]*model.EventEnvelope, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// 查询数据库
	persistEvents, err := pl.eventStore.GetSymbolEvents(symbol, startSeq, endSeq)
	if err != nil {
		hlog.Error("[EventLog] Failed to get events by symbol range:", err)
		return nil, err
	}

	// 应用 limit
	if limit > 0 && len(persistEvents) > limit {
		persistEvents = persistEvents[:limit]
	}

	// 转换回 EventEnvelope
	result := make([]*model.EventEnvelope, 0, len(persistEvents))
	for _, pe := range persistEvents {
		envelope := &model.EventEnvelope{
			Seq:       uint64(pe.GlobalSeq),
			Timestamp: pe.CreatedAt,
			EventType: pe.EventType,
			Symbol:    pe.Symbol,
			Payload:   json.RawMessage(pe.Payload),
		}
		result = append(result, envelope)
	}

	return result, nil
}

// ClearSymbolEvents 清空指定 symbol 的事件（仅用于测试）
func (pl *PostgresEventLog) ClearSymbolEvents(symbol string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// 清理本地追踪
	delete(pl.localSeqMap, symbol)

	// Note: 数据库中的事件不删除（append-only）
	// 只在测试环境中清理，生产环境不应调用此方法
	hlog.Warnf("[EventLog] Symbol events cleared from local cache (symbol=%s)", symbol)
	return nil
}
