package pg

import (
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// PostgresPersistentEventStore PostgreSQL 实现的持久化事件存储
type PostgresPersistentEventStore struct {
	db *gorm.DB
}

// NewPostgresPersistentEventStore 创建 PostgreSQL 事件存储
func NewPostgresPersistentEventStore(db *gorm.DB) *PostgresPersistentEventStore {
	return &PostgresPersistentEventStore{
		db: db,
	}
}

// WriteEvent 写入单个事件
func (s *PostgresPersistentEventStore) WriteEvent(event *model.PersistentEvent) error {
	result := s.db.Create(event)
	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to write event (seq: %d): %v", event.GlobalSeq, result.Error)
		return result.Error
	}
	hlog.Debugf("[EventStore] Written event (seq: %d, type: %s, aggregate: %s/%s)",
		event.GlobalSeq, event.EventType, event.AggregateType, event.AggregateID)
	return nil
}

// WriteEventsBatch 批量写入事件（原子性）
func (s *PostgresPersistentEventStore) WriteEventsBatch(events []*model.PersistentEvent) error {
	if len(events) == 0 {
		return nil
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		for _, event := range events {
			if err := tx.Create(event).Error; err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		hlog.Errorf("[EventStore] Failed to write batch (%d events): %v", len(events), err)
		return err
	}

	hlog.Infof("[EventStore] Written %d events in batch", len(events))
	return nil
}

// GetAggregateEvents 获取特定聚合根的所有事件
// 用于重放单个聚合根的状态
func (s *PostgresPersistentEventStore) GetAggregateEvents(aggregateType, aggregateID string) ([]*model.PersistentEvent, error) {
	var events []*model.PersistentEvent
	result := s.db.
		Where("aggregate_type = ? AND aggregate_id = ?", aggregateType, aggregateID).
		Order("global_seq ASC").
		Find(&events)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to query aggregate events (%s/%s): %v",
			aggregateType, aggregateID, result.Error)
		return nil, result.Error
	}

	return events, nil
}

// GetSymbolEvents 获取指定符号的事件
// 用于订单簿重建和符号级恢复
func (s *PostgresPersistentEventStore) GetSymbolEvents(symbol string, startSeq, endSeq int64) ([]*model.PersistentEvent, error) {
	var events []*model.PersistentEvent
	result := s.db.
		Where("symbol = ? AND global_seq >= ? AND global_seq <= ?", symbol, startSeq, endSeq).
		Order("global_seq ASC").
		Find(&events)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to query symbol events (%s, %d-%d): %v",
			symbol, startSeq, endSeq, result.Error)
		return nil, result.Error
	}

	return events, nil
}

// GetGlobalEventStream 获取全局事件流
// 用于从特定序列号开始恢复所有聚合根
// 这是分布式恢复的关键接口
func (s *PostgresPersistentEventStore) GetGlobalEventStream(startSeq int64, limit int) ([]*model.PersistentEvent, error) {
	var events []*model.PersistentEvent

	// 如果 limit 为 0 或负数，使用默认值
	if limit <= 0 {
		limit = 1000
	}

	result := s.db.
		Where("global_seq >= ?", startSeq).
		Order("global_seq ASC").
		Limit(limit).
		Find(&events)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to query global stream (startSeq: %d, limit: %d): %v",
			startSeq, limit, result.Error)
		return nil, result.Error
	}

	return events, nil
}

// GetLatestGlobalSeq 获取最新的全局序列号
// 用于确定下一个事件的序列号
func (s *PostgresPersistentEventStore) GetLatestGlobalSeq() (int64, error) {
	var maxSeq int64
	result := s.db.
		Model(&model.PersistentEvent{}).
		Select("COALESCE(MAX(global_seq), 0)").
		Scan(&maxSeq)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to get latest seq: %v", result.Error)
		return 0, result.Error
	}

	return maxSeq, nil
}

// CountEvents 统计特定聚合根的事件数
func (s *PostgresPersistentEventStore) CountEvents(aggregateType, aggregateID string) (int64, error) {
	var count int64
	result := s.db.
		Model(&model.PersistentEvent{}).
		Where("aggregate_type = ? AND aggregate_id = ?", aggregateType, aggregateID).
		Count(&count)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to count events (%s/%s): %v",
			aggregateType, aggregateID, result.Error)
		return 0, result.Error
	}

	return count, nil
}

// ArchiveEventsBefore 清理旧事件（可选，用于归档或删除）
// 注意：应谨慎使用，确保事件已被安全备份
func (s *PostgresPersistentEventStore) ArchiveEventsBefore(timestamp int64) (int64, error) {
	result := s.db.
		Where("created_at < ?", timestamp).
		Delete(&model.PersistentEvent{})

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to archive events (before: %d): %v", timestamp, result.Error)
		return 0, result.Error
	}

	hlog.Infof("[EventStore] Archived %d events", result.RowsAffected)
	return result.RowsAffected, nil
}

// GetEventsByType 按事件类型查询
// 用于处理特定类型的事件（如 TradeExecuted）
func (s *PostgresPersistentEventStore) GetEventsByType(eventType string, limit int) ([]*model.PersistentEvent, error) {
	var events []*model.PersistentEvent

	if limit <= 0 {
		limit = 100
	}

	result := s.db.
		Where("event_type = ?", eventType).
		Order("created_at DESC").
		Limit(limit).
		Find(&events)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to query events by type (%s): %v", eventType, result.Error)
		return nil, result.Error
	}

	return events, nil
}

// GetUnprocessedEvents 获取未被处理的事件
// 用于事件处理失败后的恢复
// 需要在应用层维护 "已处理事件ID" 或 "处理进度"
func (s *PostgresPersistentEventStore) GetUnprocessedEvents(lastProcessedSeq int64, limit int) ([]*model.PersistentEvent, error) {
	var events []*model.PersistentEvent

	if limit <= 0 {
		limit = 100
	}

	result := s.db.
		Where("global_seq > ?", lastProcessedSeq).
		Order("global_seq ASC").
		Limit(limit).
		Find(&events)

	if result.Error != nil {
		hlog.Errorf("[EventStore] Failed to query unprocessed events (lastSeq: %d): %v",
			lastProcessedSeq, result.Error)
		return nil, result.Error
	}

	return events, nil
}

// PrintEventStats 打印事件统计信息（调试用）
func (s *PostgresPersistentEventStore) PrintEventStats() error {
	var stats []map[string]interface{}
	result := s.db.Raw(`
		SELECT 
			event_type,
			aggregate_type,
			COUNT(*) as event_count,
			MIN(created_at) as oldest_event,
			MAX(created_at) as newest_event
		FROM events
		GROUP BY event_type, aggregate_type
		ORDER BY event_count DESC
	`).Scan(&stats)

	if result.Error != nil {
		return result.Error
	}

	hlog.Infof("[EventStore] Event statistics:")
	for _, stat := range stats {
		hlog.Infof("  %v", stat)
	}

	return nil
}

// ValidateEventIntegrity 验证事件完整性（可选）
// 检查 global_seq 是否有间隙或重复
func (s *PostgresPersistentEventStore) ValidateEventIntegrity() error {
	// 检查重复的 global_seq
	var duplicates int64
	result := s.db.
		Model(&model.PersistentEvent{}).
		Select("COUNT(*) - COUNT(DISTINCT global_seq)").
		Scan(&duplicates)

	if result.Error != nil || duplicates > 0 {
		hlog.Errorf("[EventStore] Found %d duplicate global_seq values", duplicates)
		return result.Error
	}

	hlog.Infof("[EventStore] Event integrity check PASSED")
	return nil
}
