package pg

import (
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ProcessedRepo 管理已处理事件的去重表
type ProcessedRepo struct {
	db *gorm.DB
}

func NewProcessedRepo(db *gorm.DB) *ProcessedRepo {
	// 自动迁移 processed_trades 表，方便测试与本地运行
	_ = db.AutoMigrate(&model.ProcessedTrade{})
	return &ProcessedRepo{db: db}
}

// WriteIfNotExists 尝试插入已处理事件记录，若已存在返回 inserted=false
func (r *ProcessedRepo) WriteIfNotExists(eventID string) (bool, error) {
	entry := &model.ProcessedTrade{EventID: eventID, CreatedAt: time.Now()}
	res := r.db.Clauses(clause.OnConflict{DoNothing: true}).Create(entry)
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}

// Delete 删除已处理标记（用于在本次处理失败时回滚标记，允许后续重试）
func (r *ProcessedRepo) Delete(eventID string) error {
	return r.db.Where("event_id = ?", eventID).Delete(&model.ProcessedTrade{}).Error
}
