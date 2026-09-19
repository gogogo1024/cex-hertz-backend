package pg

import (
	"context"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// OutboxRepo 出站队列存储库
type OutboxRepo struct {
	db *gorm.DB
}

// NewOutboxRepo 创建出站队列仓库
func NewOutboxRepo(db *gorm.DB) *OutboxRepo {
	return &OutboxRepo{db: db}
}

// WriteOutboxEntry 在同一事务中写入业务数据和 outbox 条目
// 用法：在 DB 事务内调用
func (r *OutboxRepo) WriteOutboxEntry(tx *gorm.DB, entry *model.OutboxEntry) error {
	return tx.Create(entry).Error
}

// GetUnpublished 获取未发布的 outbox 条目（分批）
func (r *OutboxRepo) GetUnpublished(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	var entries []*model.OutboxEntry
	err := r.db.WithContext(ctx).
		Where("published = ?", false).
		Order("created_at ASC").
		Limit(limit).
		Find(&entries).Error
	return entries, err
}

// MarkPublished 标记条目为已发布（可在 outbox dispatcher 中调用）
func (r *OutboxRepo) MarkPublished(ctx context.Context, eventID string) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEntry{}).
		Where("event_id = ?", eventID).
		Updates(map[string]interface{}{
			"published":    true,
			"published_at": time.Now(),
		}).Error
}

// MarkPublishedBatch 批量标记为已发布
func (r *OutboxRepo) MarkPublishedBatch(ctx context.Context, eventIDs []string) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEntry{}).
		Where("event_id IN ?", eventIDs).
		Updates(map[string]interface{}{
			"published":    true,
			"published_at": time.Now(),
		}).Error
}

// RecordPublishError 记录发布错误并增加重试计数
func (r *OutboxRepo) RecordPublishError(ctx context.Context, eventID string, errMsg string) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEntry{}).
		Where("event_id = ?", eventID).
		Updates(map[string]interface{}{
			"retry_count": gorm.Expr("retry_count + 1"),
			"last_error":  errMsg,
		}).Error
}

// CleanPublished 清理已发布的旧条目（保留配置天数）
func (r *OutboxRepo) CleanPublished(ctx context.Context, retentionDays int) (int64, error) {
	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)
	result := r.db.WithContext(ctx).
		Where("published = ? AND published_at < ?", true, cutoffTime).
		Delete(&model.OutboxEntry{})
	return result.RowsAffected, result.Error
}

// GetFailedEntries 获取发布失败的条目（用于重试策略）
func (r *OutboxRepo) GetFailedEntries(ctx context.Context, maxRetries int) ([]*model.OutboxEntry, error) {
	var entries []*model.OutboxEntry
	err := r.db.WithContext(ctx).
		Where("published = ? AND retry_count < ?", false, maxRetries).
		Order("retry_count ASC, created_at ASC").
		Limit(100).
		Find(&entries).Error
	return entries, err
}
