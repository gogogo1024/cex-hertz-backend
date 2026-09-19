package pg

import (
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CheckpointRepo 检查点持久化仓库
type CheckpointRepo struct {
	db *gorm.DB
}

// NewCheckpointRepo 创建检查点仓库
func NewCheckpointRepo(db *gorm.DB) *CheckpointRepo {
	return &CheckpointRepo{db: db}
}

// SaveCheckpoint 保存或更新检查点（幂等性：同一processor+symbol每次覆盖）
func (r *CheckpointRepo) SaveCheckpoint(cp *model.EventOffsetCheckpoint) error {
	if cp.ProcessorName == "" || cp.Symbol == "" {
		return fmt.Errorf("processor_name and symbol must not be empty")
	}

	cp.Timestamp = time.Now().UnixMilli()

	// Upsert: 基于(processor_name, symbol)唯一索引
	result := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "processor_name"}, {Name: "symbol"}},
		DoUpdates: clause.AssignmentColumns([]string{"event_seq", "kafka_offset", "partition_id", "state_checksum", "order_count", "trade_count", "timestamp", "updated_at"}),
	}).Create(cp)

	if result.Error != nil {
		hlog.Errorf("[CheckpointRepo] Failed to save checkpoint for %s:%s: %v", cp.ProcessorName, cp.Symbol, result.Error)
		return result.Error
	}

	hlog.Debugf("[CheckpointRepo] Checkpoint saved: processor=%s, symbol=%s, seq=%d, offset=%d", cp.ProcessorName, cp.Symbol, cp.EventSeq, cp.KafkaOffset)
	return nil
}

// GetLatestCheckpoint 获取最新检查点
func (r *CheckpointRepo) GetLatestCheckpoint(processorName, symbol string) (*model.EventOffsetCheckpoint, error) {
	var cp model.EventOffsetCheckpoint

	result := r.db.
		Where("processor_name = ? AND symbol = ?", processorName, symbol).
		Order("event_seq DESC").
		First(&cp)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil // 第一次，没有检查点
		}
		hlog.Errorf("[CheckpointRepo] Failed to get checkpoint for %s:%s: %v", processorName, symbol, result.Error)
		return nil, result.Error
	}

	return &cp, nil
}

// GetCheckpointSummary 获取检查点汇总（用于crash recovery的起点决策）
func (r *CheckpointRepo) GetCheckpointSummary(processorName, symbol string) (*model.CheckpointSummary, error) {
	cp, err := r.GetLatestCheckpoint(processorName, symbol)
	if err != nil {
		return nil, err
	}

	if cp == nil {
		return nil, nil // 第一次处理
	}

	secondsSinceCP := (time.Now().UnixMilli() - cp.Timestamp) / 1000

	return &model.CheckpointSummary{
		Symbol:             symbol,
		ProcessorName:      processorName,
		LastEventSeq:       cp.EventSeq,
		LastKafkaOffset:    cp.KafkaOffset,
		LastCheckpointTime: time.UnixMilli(cp.Timestamp),
		StateChecksum:      cp.StateChecksum,
		SecondsSinceLastCP: secondsSinceCP,
	}, nil
}

// GetRecoveryContext 构建recovery上下文（crash recovery时调用）
func (r *CheckpointRepo) GetRecoveryContext(processorName, symbol string) (*model.RecoveryContext, error) {
	summary, err := r.GetCheckpointSummary(processorName, symbol)
	if err != nil {
		return nil, err
	}

	ctx := &model.RecoveryContext{
		Symbol:            symbol,
		ProcessorName:     processorName,
		RecoveryStartTime: time.Now(),
		RecoverySuccess:   false,
	}

	if summary != nil {
		// 从上一个检查点+1开始恢复
		ctx.StartEventSeq = summary.LastEventSeq + 1
		ctx.StartKafkaOffset = summary.LastKafkaOffset + 1
		ctx.PreCrashChecksum = summary.StateChecksum
		ctx.SecondsSinceLastCP = summary.SecondsSinceLastCP

		hlog.Infof("[CheckpointRepo] Recovery context: processor=%s, symbol=%s, startSeq=%d, startOffset=%d, lastCP=%s ago",
			processorName, symbol, ctx.StartEventSeq, ctx.StartKafkaOffset, formatDuration(time.Duration(summary.SecondsSinceLastCP)*time.Second))
	} else {
		// 第一次恢复，从头开始
		ctx.StartEventSeq = 1
		ctx.StartKafkaOffset = 0
		hlog.Warnf("[CheckpointRepo] No checkpoint found for %s:%s, will do full recovery from start", processorName, symbol)
	}

	return ctx, nil
}

// SaveRecoveryResult 保存恢复结果
func (r *CheckpointRepo) SaveRecoveryResult(ctx *model.RecoveryContext) error {
	ctx.RecoveryEndTime = time.Now()

	// 在audit表中记录恢复事件（可选的detailed logging）
	hlog.Infof("[CheckpointRepo] Recovery result: processor=%s, symbol=%s, success=%v, events_recovered=%d, duration=%s",
		ctx.ProcessorName, ctx.Symbol, ctx.RecoverySuccess, ctx.RecoveredEventCount, ctx.RecoveryEndTime.Sub(ctx.RecoveryStartTime).String())

	if !ctx.RecoverySuccess && ctx.RecoveryError != "" {
		hlog.Errorf("[CheckpointRepo] Recovery failed: %s", ctx.RecoveryError)
	}

	return nil
}

// ListAllCheckpoints 列出所有检查点（用于监控和调试）
func (r *CheckpointRepo) ListAllCheckpoints(limit int) ([]model.EventOffsetCheckpoint, error) {
	var checkpoints []model.EventOffsetCheckpoint

	result := r.db.
		Order("updated_at DESC").
		Limit(limit).
		Find(&checkpoints)

	if result.Error != nil {
		return nil, result.Error
	}

	return checkpoints, nil
}

// DeleteOldCheckpoints 删除过期检查点（可选的清理策略）
// keepDays: 保留多少天的检查点历史
func (r *CheckpointRepo) DeleteOldCheckpoints(keepDays int) error {
	cutoff := time.Now().AddDate(0, 0, -keepDays)

	result := r.db.
		Where("created_at < ?", cutoff).
		Delete(&model.EventOffsetCheckpoint{})

	if result.Error != nil {
		hlog.Errorf("[CheckpointRepo] Failed to delete old checkpoints: %v", result.Error)
		return result.Error
	}

	hlog.Infof("[CheckpointRepo] Deleted %d old checkpoints (older than %d days)", result.RowsAffected, keepDays)
	return nil
}

// formatDuration 格式化持续时间
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}
