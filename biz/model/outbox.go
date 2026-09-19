package model

import "time"

// OutboxEntry 出站队列条目
// 用于实现 transactional outbox pattern
// 保证业务变更与事件发布的原子性：
//   1. 业务逻辑在 DB 事务内同时写业务表和 outbox 表
//   2. 事务提交后，outbox dispatcher 异步读取并发布
//   3. 发布成功后标记为 published，定期清理
type OutboxEntry struct {
	ID            int64         `gorm:"primaryKey;column:id"`
	EventID       string        `gorm:"column:event_id;uniqueIndex"`              // TradeID / OrderID
	EventType     string        `gorm:"column:event_type"`                        // "TradeExecuted" / "PositionUpdated"
	AggregateID   string        `gorm:"column:aggregate_id;index"`                // 聚合根 ID (symbol / user-id)
	AggregateType string        `gorm:"column:aggregate_type"`                    // "OrderBook" / "Position"
	Payload       string        `gorm:"column:payload;type:text"`                 // JSON 序列化的事件体
	Published     bool          `gorm:"column:published;index;default:false"`     // 是否已发布
	PublishedAt   *time.Time    `gorm:"column:published_at"`                      // 发布时间
	CreatedAt     time.Time     `gorm:"column:created_at;autoCreateTime"`         // 创建时间
	UpdatedAt     time.Time     `gorm:"column:updated_at;autoUpdateTime"`         // 更新时间
	RetryCount    int           `gorm:"column:retry_count;default:0"`             // 重试次数
	LastError     string        `gorm:"column:last_error;type:text"`              // 最后一次错误信息
}

// TableName 指定表名
func (OutboxEntry) TableName() string {
	return "outbox"
}

// OutboxEvent 出站事件数据结构
// 用于序列化和反序列化 outbox payload
type OutboxEvent struct {
	EventID       string      `json:"event_id"`
	EventType     string      `json:"event_type"`
	AggregateID   string      `json:"aggregate_id"`
	AggregateType string      `json:"aggregate_type"`
	Payload       interface{} `json:"payload"`
	Timestamp     int64       `json:"timestamp"`
}

// PublishResult 发布结果
type PublishResult struct {
	EventID   string
	Success   bool
	Error     string
	Timestamp time.Time
}
