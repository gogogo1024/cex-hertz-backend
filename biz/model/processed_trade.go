package model

import "time"

// ProcessedTrade 用于记录已处理的事件（幂等键）
type ProcessedTrade struct {
	EventID   string    `gorm:"primaryKey;column:event_id;type:varchar(128)"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (ProcessedTrade) TableName() string {
	return "processed_trades"
}
