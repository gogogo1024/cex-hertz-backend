package service

import (
	"errors"
	"sync"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

var (
	// ErrEventNil 事件为 nil
	ErrEventNil = errors.New("event cannot be nil")
)

// InMemoryEventLog 是一个简单的内存实现的事件日志
// 用于测试和开发，生产环境应使用持久化版本（如 PostgreSQL）
type InMemoryEventLog struct {
	mu sync.RWMutex

	// 全局事件序列
	events []*model.EventEnvelope

	// symbol -> 该 symbol 的事件索引列表
	symbolIndexes map[string][]int

	// symbol -> 最新序列号
	symbolSeq map[string]uint64
}

// NewInMemoryEventLog 创建一个新的内存事件日志
func NewInMemoryEventLog() *InMemoryEventLog {
	return &InMemoryEventLog{
		events:        make([]*model.EventEnvelope, 0, 10000),
		symbolIndexes: make(map[string][]int),
		symbolSeq:     make(map[string]uint64),
	}
}

// AppendEvent 追加事件到日志
func (el *InMemoryEventLog) AppendEvent(event model.MatchingEngineEvent) error {
	if event == nil {
		return ErrEventNil
	}

	envelope, err := model.MarshalEvent(event)
	if err != nil {
		return err
	}

	el.mu.Lock()
	defer el.mu.Unlock()

	// 追加到全局事件序列
	index := len(el.events)
	el.events = append(el.events, envelope)

	// 记录到 symbol 索引
	symbol := event.Symbol()
	el.symbolIndexes[symbol] = append(el.symbolIndexes[symbol], index)
	el.symbolSeq[symbol] = event.EventSeq()

	return nil
}

// GetEventsBySymbol 获取某个 symbol 的所有事件
func (el *InMemoryEventLog) GetEventsBySymbol(symbol string, startSeq uint64, limit int) ([]*model.EventEnvelope, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	indexes, ok := el.symbolIndexes[symbol]
	if !ok {
		return []*model.EventEnvelope{}, nil
	}

	var result []*model.EventEnvelope
	for _, idx := range indexes {
		if idx >= len(el.events) {
			break
		}
		envelope := el.events[idx]
		if envelope.Seq >= startSeq {
			result = append(result, envelope)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, nil
}

// GetEventsSinceTime 获取从某个时间戳之后的事件
func (el *InMemoryEventLog) GetEventsSinceTime(symbol string, timestamp int64) ([]*model.EventEnvelope, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	indexes, ok := el.symbolIndexes[symbol]
	if !ok {
		return []*model.EventEnvelope{}, nil
	}

	var result []*model.EventEnvelope
	for _, idx := range indexes {
		if idx >= len(el.events) {
			break
		}
		envelope := el.events[idx]
		if envelope.Timestamp >= timestamp {
			result = append(result, envelope)
		}
	}

	return result, nil
}

// GetLatestSeq 获取最新的事件序列号
func (el *InMemoryEventLog) GetLatestSeq(symbol string) (uint64, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	seq, ok := el.symbolSeq[symbol]
	if !ok {
		return 0, nil
	}
	return seq, nil
}

// GetAllEvents 获取所有事件（用于调试和回放）
func (el *InMemoryEventLog) GetAllEvents() []*model.EventEnvelope {
	el.mu.RLock()
	defer el.mu.RUnlock()

	result := make([]*model.EventEnvelope, len(el.events))
	copy(result, el.events)
	return result
}

// GetEventsBySymbolAndSeq 获取某个 symbol 从指定序列号开始的所有事件
func (el *InMemoryEventLog) GetEventsBySymbolAndSeq(symbol string, startSeq uint64) ([]*model.EventEnvelope, error) {
	return el.GetEventsBySymbol(symbol, startSeq, -1) // -1 表示无限制
}
