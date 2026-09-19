package service

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// CheckpointManager 管理事件处理进度的持久化检查点
// 支持crash recovery和幂等性验证
type CheckpointManager struct {
	repo          *pg.CheckpointRepo
	checkpoints   map[string]*model.EventOffsetCheckpoint // key: "processor:symbol"
	mu            sync.RWMutex
	flushInterval time.Duration // 多久flush一次checkpoint到数据库
	lastFlushTime map[string]time.Time
}

// NewCheckpointManager 创建检查点管理器
func NewCheckpointManager(repo *pg.CheckpointRepo) *CheckpointManager {
	return &CheckpointManager{
		repo:          repo,
		checkpoints:   make(map[string]*model.EventOffsetCheckpoint),
		flushInterval: 5 * time.Second, // 默认5秒flush一次
		lastFlushTime: make(map[string]time.Time),
	}
}

// RecordEventProcessed 记录一个事件被成功处理
// 这不会立即写入数据库，而是在内存中accumulate，定期flush
func (cm *CheckpointManager) RecordEventProcessed(
	processorName string,
	symbol string,
	eventSeq uint64,
	kafkaOffset int64,
	partitionID int32,
	stateChecksum string,
	orderCount int64,
	tradeCount int64,
) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", processorName, symbol)

	cp := &model.EventOffsetCheckpoint{
		ProcessorName: processorName,
		Symbol:        symbol,
		EventSeq:      eventSeq,
		KafkaOffset:   kafkaOffset,
		PartitionID:   partitionID,
		Timestamp:     time.Now().UnixMilli(),
		StateChecksum: stateChecksum,
		OrderCount:    orderCount,
		TradeCount:    tradeCount,
	}

	cm.checkpoints[key] = cp
}

// GetCurrentCheckpoint 获取当前内存中的检查点
func (cm *CheckpointManager) GetCurrentCheckpoint(processorName, symbol string) *model.EventOffsetCheckpoint {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", processorName, symbol)
	return cm.checkpoints[key]
}

// FlushCheckpoints 将内存中的检查点写入数据库
// 在EventPipeline处理完一个batch后调用
func (cm *CheckpointManager) FlushCheckpoints() error {
	cm.mu.Lock()
	checkpointsToFlush := make([]*model.EventOffsetCheckpoint, 0, len(cm.checkpoints))
	for _, cp := range cm.checkpoints {
		checkpointsToFlush = append(checkpointsToFlush, cp)
	}
	cm.mu.Unlock()

	if len(checkpointsToFlush) == 0 {
		return nil
	}

	for _, cp := range checkpointsToFlush {
		if err := cm.repo.SaveCheckpoint(cp); err != nil {
			hlog.Errorf("[CheckpointManager] Failed to flush checkpoint for %s:%s: %v", cp.ProcessorName, cp.Symbol, err)
			return err
		}
	}

	hlog.Debugf("[CheckpointManager] Flushed %d checkpoints to database", len(checkpointsToFlush))
	return nil
}

// GetRecoveryContext 获取crash recovery所需的上下文
func (cm *CheckpointManager) GetRecoveryContext(processorName, symbol string) (*model.RecoveryContext, error) {
	return cm.repo.GetRecoveryContext(processorName, symbol)
}

// MarkRecoveryComplete 标记恢复完成并保存结果
func (cm *CheckpointManager) MarkRecoveryComplete(ctx *model.RecoveryContext, success bool, err string) error {
	ctx.RecoverySuccess = success
	ctx.RecoveryError = err

	return cm.repo.SaveRecoveryResult(ctx)
}

// CalculateStateChecksum 计算当前状态的校验和（用于recovery验证）
// orderBook: 当前的订单簿状态
// positions: 当前的持仓状态
func (cm *CheckpointManager) CalculateStateChecksum(orderBook interface{}, positions interface{}) string {
	// 简单实现：使用MD5哈希
	// 生产环境可以改成更精细的状态序列化

	data := fmt.Sprintf("orderbook:%v|positions:%v|timestamp:%d",
		orderBook, positions, time.Now().UnixMilli())

	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

// ValidateRecovery 验证recovery后的状态是否与pre-crash状态一致
func (cm *CheckpointManager) ValidateRecovery(
	preChecksum string,
	postChecksum string,
	expectedOrderCount int64,
	actualOrderCount int64,
	expectedTradeCount int64,
	actualTradeCount int64,
) (bool, string) {
	if preChecksum != postChecksum {
		return false, fmt.Sprintf("state checksum mismatch: pre=%s, post=%s", preChecksum, postChecksum)
	}

	if expectedOrderCount != actualOrderCount {
		return false, fmt.Sprintf("order count mismatch: expected=%d, actual=%d", expectedOrderCount, actualOrderCount)
	}

	if expectedTradeCount != actualTradeCount {
		return false, fmt.Sprintf("trade count mismatch: expected=%d, actual=%d", expectedTradeCount, actualTradeCount)
	}

	return true, ""
}

// ListCheckpoints 列出最近的检查点（监控用）
func (cm *CheckpointManager) ListCheckpoints(limit int) ([]model.EventOffsetCheckpoint, error) {
	return cm.repo.ListAllCheckpoints(limit)
}

// CleanupOldCheckpoints 清理过期的检查点
func (cm *CheckpointManager) CleanupOldCheckpoints(keepDays int) error {
	return cm.repo.DeleteOldCheckpoints(keepDays)
}
