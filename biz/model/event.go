package model

import (
	"encoding/json"
	"fmt"
)

// PriceInNano 和 QuantityInNano 是类型别名，用于表示以纳单位表示的价格和数量
type PriceInNano int64

// String 将 PriceInNano 转换为字符串（浮点数格式）
func (p PriceInNano) String() string {
	return fmt.Sprintf("%.8f", float64(p)/1e8)
}

type QuantityInNano int64

// String 将 QuantityInNano 转换为字符串（浮点数格式）
func (q QuantityInNano) String() string {
	return fmt.Sprintf("%.8f", float64(q)/1e8)
}

// MatchingEngineEvent 是所有撮合引擎事件的基类
// 这是 Event Sourcing 的核心
type MatchingEngineEvent interface {
	EventType() string
	EventSeq() uint64
	EventTimestamp() int64
	Symbol() string
}

// OrderSubmittedEvent 订单提交事件
type OrderSubmittedEvent struct {
	Seq       uint64 // 全局序列号（确保事件顺序）
	Timestamp int64  // 事件时间戳
	OrderID   string
	UserID    string
	SymbolStr string         // 交易对符号
	Side      string         // "buy" or "sell"
	Price     PriceInNano    // 整数表示的价格（精确到最小单位）
	Quantity  QuantityInNano // 整数表示的数量
	Status    string         // "submitted"
}

func (e *OrderSubmittedEvent) EventType() string     { return "order_submitted" }
func (e *OrderSubmittedEvent) EventSeq() uint64      { return e.Seq }
func (e *OrderSubmittedEvent) EventTimestamp() int64 { return e.Timestamp }
func (e *OrderSubmittedEvent) Symbol() string        { return e.SymbolStr }

// TradeExecutedEvent 成交事件
// 这个事件是幂等的：相同的 TradeID 重复产生相同的结果
type TradeExecutedEvent struct {
	Seq          uint64 // 全局序列号
	Timestamp    int64
	TradeID      string
	SymbolStr    string // 交易对符号
	TakerOrderID string
	MakerOrderID string
	TakerUser    string
	MakerUser    string
	Price        PriceInNano    // 成交价（整数表示）
	Quantity     QuantityInNano // 成交量（整数表示）
	TakerSide    string         // "buy" or "sell"（Taker 的方向）
}

func (e *TradeExecutedEvent) EventType() string     { return "trade_executed" }
func (e *TradeExecutedEvent) EventSeq() uint64      { return e.Seq }
func (e *TradeExecutedEvent) EventTimestamp() int64 { return e.Timestamp }
func (e *TradeExecutedEvent) Symbol() string        { return e.SymbolStr }

// OrderCancelledEvent 订单取消事件
type OrderCancelledEvent struct {
	Seq       uint64
	Timestamp int64
	OrderID   string
	SymbolStr string // 交易对符号
	Reason    string
}

func (e *OrderCancelledEvent) EventType() string     { return "order_cancelled" }
func (e *OrderCancelledEvent) EventSeq() uint64      { return e.Seq }
func (e *OrderCancelledEvent) EventTimestamp() int64 { return e.Timestamp }
func (e *OrderCancelledEvent) Symbol() string        { return e.SymbolStr }

// EventEnvelope 事件包装，用于持久化和传输
// 注意：EventEnvelope 不实现 MatchingEngineEvent 接口
// 它只是一个容器，用于序列化和反序列化
type EventEnvelope struct {
	Seq       uint64          `json:"seq"`
	Timestamp int64           `json:"timestamp"`
	EventType string          `json:"event_type"`
	Symbol    string          `json:"symbol"`
	Payload   json.RawMessage `json:"payload"`
}

// MarshalEvent 将任何事件序列化为 EventEnvelope
func MarshalEvent(event MatchingEngineEvent) (*EventEnvelope, error) {
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	return &EventEnvelope{
		Seq:       event.EventSeq(),
		Timestamp: event.EventTimestamp(),
		EventType: event.EventType(),
		Symbol:    event.Symbol(),
		Payload:   payloadBytes,
	}, nil
}

// UnmarshalEvent 将 EventEnvelope 反序列化为具体事件
func UnmarshalEvent(envelope *EventEnvelope) (MatchingEngineEvent, error) {
	switch envelope.EventType {
	case "order_submitted":
		var event OrderSubmittedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			return nil, err
		}
		return &event, nil

	case "trade_executed":
		var event TradeExecutedEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			return nil, err
		}
		return &event, nil

	case "order_cancelled":
		var event OrderCancelledEvent
		if err := json.Unmarshal(envelope.Payload, &event); err != nil {
			return nil, err
		}
		return &event, nil

	default:
		// 未知事件类型返回错误
		return nil, nil
	}
}

// Min 返回两个数中的最小值
func Min(a, b QuantityInNano) QuantityInNano {
	if a < b {
		return a
	}
	return b
}

// EventStore 事件存储接口
type EventStore interface {
	// AppendEvent 追加事件到事件日志
	AppendEvent(event MatchingEngineEvent) error

	// GetEventsBySym 获取某个 symbol 的所有事件（从 startSeq 开始）
	GetEventsBySymbol(symbol string, startSeq uint64, limit int) ([]*EventEnvelope, error)

	// GetEventsSinceTime 获取从某个时间戳之后的事件
	GetEventsSinceTime(symbol string, timestamp int64) ([]*EventEnvelope, error)

	// GetLatestSeq 获取最新的事件序列号
	GetLatestSeq(symbol string) (uint64, error)
}

// MatchingEngineState 撮合引擎的状态快照
// 用于恢复状态
type MatchingEngineState struct {
	Symbol        string
	LastSeq       uint64
	LastTimestamp int64
	// 可以扩展为保存 OrderBook 快照等
}

// EventProcessor 事件处理器接口
// 实现者需要处理不同类型的事件，并更新下游系统（DB、Position、WS 等）
type EventProcessor interface {
	ProcessEvent(event MatchingEngineEvent) error
	ProcessorName() string
}

// ===== Persistent Event Store (Phase 2) =====

// PersistentEvent 持久化事件存储模型
// Append-only：只允许插入，不允许修改或删除
// 支持完整的事件历史回放和崩溃恢复
type PersistentEvent struct {
	// 数据库主键（自增）
	ID int64 `gorm:"primaryKey;column:id"`

	// 分布式全局序列号
	// 计算方式：(NodeID << 40) | LocalSeq
	// 用途：事件唯一标识 + 分区内排序
	GlobalSeq int64 `gorm:"column:global_seq;uniqueIndex"`

	// 事件类型（TradeExecuted、OrderCancelled等）
	EventType string `gorm:"column:event_type;index"`

	// 聚合根 ID（如 symbol、user-id）
	AggregateID string `gorm:"column:aggregate_id"`

	// 聚合根类型（OrderBook、Position等）
	AggregateType string `gorm:"column:aggregate_type"`

	// 交易对（如 BTC/USDT），可为空
	Symbol string `gorm:"column:symbol;index"`

	// 事件负载（JSON）
	Payload string `gorm:"column:payload;type:text"`

	// 事件时间戳（毫秒级，业务时间）
	EventTimestamp int64 `gorm:"column:event_timestamp"`

	// 创建时间（入库时间）
	CreatedAt int64 `gorm:"column:created_at;autoCreateTime:milli;index"`

	// 版本控制（用于演进事件 schema）
	Version int `gorm:"column:version;default:1"`
}

// TableName 指定表名
func (PersistentEvent) TableName() string {
	return "events"
}

// PersistentEventStore 持久化事件存储接口
// 用于替代 InMemoryEventLog
type PersistentEventStore interface {
	// 写入单个事件
	WriteEvent(event *PersistentEvent) error

	// 批量写入事件
	WriteEventsBatch(events []*PersistentEvent) error

	// 查询特定聚合根的所有事件
	GetAggregateEvents(aggregateType, aggregateID string) ([]*PersistentEvent, error)

	// 查询指定符号的事件（订单簿重建）
	GetSymbolEvents(symbol string, startSeq, endSeq int64) ([]*PersistentEvent, error)

	// 获取全局事件流（从特定序列号开始）
	GetGlobalEventStream(startSeq int64, limit int) ([]*PersistentEvent, error)

	// 获取最新的序列号
	GetLatestGlobalSeq() (int64, error)

	// 统计事件数量
	CountEvents(aggregateType, aggregateID string) (int64, error)

	// 清理旧事件（可选归档）
	ArchiveEventsBefore(timestamp int64) (int64, error)
}
