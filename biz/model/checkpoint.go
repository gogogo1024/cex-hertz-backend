package model

import (
	"time"
)

// EventOffsetCheckpoint 事件处理进度的持久化检查点
// 用于crash recovery时从正确的offset重放事件
type EventOffsetCheckpoint struct {
	ID            int64     `gorm:"primaryKey" json:"id"`
	ProcessorName string    `gorm:"index:idx_processor;type:varchar(100)" json:"processor_name"`
	Symbol        string    `gorm:"index:idx_symbol;type:varchar(50)" json:"symbol"`
	EventSeq      uint64    `gorm:"index:idx_event_seq" json:"event_seq"`       // 最后成功处理的event seq
	KafkaOffset   int64     `gorm:"index:idx_kafka_offset" json:"kafka_offset"` // Kafka中的offset
	PartitionID   int32     `gorm:"index:idx_partition" json:"partition_id"`    // Kafka分区ID
	Timestamp     int64     `json:"timestamp"`                                  // 检查点创建时间
	StateChecksum string    `gorm:"type:varchar(64)" json:"state_checksum"`     // 状态校验和 (用于恢复验证)
	OrderCount    int64     `json:"order_count"`                                // 当前orderbook中的订单数
	TradeCount    int64     `json:"trade_count"`                                // 累计成交笔数
	// Phase 2.7: 恢复状态跟踪字段
	RecoveryStatus    string    `gorm:"index:idx_recovery_status;type:varchar(20);default:'none'" json:"recovery_status"`  // 'none', 'in_progress', 'complete', 'failed'
	RecoveryStartTime *time.Time `gorm:"index" json:"recovery_start_time"`  // 恢复开始时间
	RecoveryEndTime   *time.Time `gorm:"index" json:"recovery_end_time"`    // 恢复结束时间
	RecoveryError     string    `gorm:"type:text" json:"recovery_error"`     // 恢复错误信息
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName 指定表名
func (EventOffsetCheckpoint) TableName() string {
	return "event_offset_checkpoints"
}

// CheckpointSummary 检查点汇总（用于crash recovery的起点决策）
type CheckpointSummary struct {
	Symbol             string
	ProcessorName      string
	LastEventSeq       uint64
	LastKafkaOffset    int64
	LastCheckpointTime time.Time
	StateChecksum      string
	SecondsSinceLastCP int64 // 距离最后一个检查点的秒数（用于判断是否需要full recovery）
}

// RecoveryContext 恢复上下文（crash recovery时使用）
type RecoveryContext struct {
	Symbol              string
	ProcessorName       string
	StartEventSeq       uint64 // 从哪个seq开始重放
	StartKafkaOffset    int64  // 从Kafka的哪个offset开始消费
	PreCrashChecksum    string // crash前的校验和（用于验证恢复后的一致性）
	ExpectedOrderCount  int64
	ExpectedTradeCount  int64
	SecondsSinceLastCP  int64 // 距离最后一个检查点的秒数（用于判断是否需要full recovery）
	RecoveryStartTime   time.Time
	RecoveryEndTime     time.Time
	RecoveredEventCount int64
	RecoverySuccess     bool
	RecoveryError       string
}
