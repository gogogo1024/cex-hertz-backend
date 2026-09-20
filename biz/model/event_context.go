package model

import (
	"fmt"
	"time"
)

// EventContext 事件完整上下文
// 携带：业务数据 + Transport Metadata + 处理链信息
// 用于建立以下映射链：
//
//	Kafka Offset → EventSeq → Checkpoint → OwnershipEpoch (Fencing)
type EventContext struct {
	// 业务数据
	Event MatchingEngineEvent

	// ===== Transport Metadata (从 Kafka 来) =====
	// 这些信息在 EventPipeline 中被追踪，用于恢复时定位重新消费的位置
	Topic     string // Kafka topic
	Partition int32  // Kafka partition
	Offset    int64  // Kafka offset

	// ===== Processing Chain Metadata =====
	Timestamp   int64     // 事件时间戳（可能来自 Kafka header 或事件本身）
	ProcessedAt time.Time // 本地处理时间戳

	// ===== Ownership & Fencing Metadata =====
	// 用于在分布式环境中防止 stale write
	// 当分区所有者发生迁移时，旧所有者的缓存epoch必须与当前epoch匹配
	// 否则拒绝处理（防止两个节点同时处理同一事件）
	OwnershipEpoch int64  // 当前所有者的epoch版本号
	ProcessingNode string // 处理节点ID（用于诊断）

	// ===== Optional: State Snapshot (处理完后填充) =====
	// 这些在处理器处理事件后被填充，用于 checkpoint
	StateChecksum string // 处理后的状态校验和
	OrderCount    int64  // 此时的订单总数
	TradeCount    int64  // 此时的成交总数
}

// NewEventContext 创建一个新的事件上下文
// 通常由 Kafka Consumer 调用
func NewEventContext(
	event MatchingEngineEvent,
	topic string,
	partition int32,
	offset int64,
) *EventContext {
	// 如果 event 为 nil，避免直接调用方法导致 panic
	var ts int64
	if event != nil {
		ts = event.EventTimestamp()
	} else {
		ts = time.Now().UnixNano() / int64(time.Millisecond)
	}

	return &EventContext{
		Event:       event,
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		Timestamp:   ts,
		ProcessedAt: time.Now(),
		// OwnershipEpoch 和 ProcessingNode 由调用者填充
		OwnershipEpoch: 0, // 默认值，调用者应该更新
		ProcessingNode: "",
	}
}

// WithOwnershipEpoch 设置所有权epoch（用于fencing检查）
// 返回self以支持链式调用
func (ec *EventContext) WithOwnershipEpoch(epoch int64, nodeID string) *EventContext {
	ec.OwnershipEpoch = epoch
	ec.ProcessingNode = nodeID
	return ec
}

// Key 返回事件的唯一标识（用于追踪和去重）
func (ec *EventContext) Key() string {
	if ec == nil {
		return ""
	}
	if ec.Event == nil {
		return fmt.Sprintf("%s:%d:%d", ec.Topic, ec.Partition, ec.Offset)
	}
	return fmt.Sprintf("%s:%s:%d", ec.Topic, ec.Event.Symbol(), ec.Event.EventSeq())
}

// IsValid 检查上下文是否有效
func (ec *EventContext) IsValid() bool {
	return ec.Event != nil &&
		ec.Topic != "" &&
		ec.Partition >= 0 &&
		ec.Offset >= 0
}
