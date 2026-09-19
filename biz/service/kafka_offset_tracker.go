package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// OffsetTracker 跟踪Kafka offset的三个关键状态
// 约束: CommittedOffset <= ConsumedOffset <= ProducedOffset
type OffsetTracker struct {
	symbol        string
	processorName string

	// 内存跟踪 (real-time)
	mu              sync.RWMutex
	consumedOffset  int64 // 已消费的最高offset
	committedOffset int64 // 已提交到PostgreSQL的offset
	lastCommitTime  time.Time

	// Kafka状态跟踪
	kafkaProducerOffset int64 // Kafka中消息的最高offset
	kafkaConsumerGroup  string

	// 持久化
	checkpointRepo *pg.CheckpointRepo

	// 统计
	commitCount        int64
	commitFailureCount int64
	lagHighWaterMark   int64
}

// NewOffsetTracker 创建offset跟踪器
func NewOffsetTracker(
	symbol string,
	processorName string,
	checkpointRepo *pg.CheckpointRepo,
) *OffsetTracker {
	return &OffsetTracker{
		symbol:             symbol,
		processorName:      processorName,
		checkpointRepo:     checkpointRepo,
		kafkaConsumerGroup: fmt.Sprintf("cex-hertz-%s", processorName),
		lastCommitTime:     time.Now(),
	}
}

// TrackConsumedOffset 记录已消费的offset
// 这个offset是从Kafka consumer获得的，表示消息已被拉取到内存
func (t *OffsetTracker) TrackConsumedOffset(offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset > t.consumedOffset {
		t.consumedOffset = offset
		hlog.Debugf("[OffsetTracker] ConsumedOffset updated: %d for %s:%s",
			offset, t.processorName, t.symbol)
	}
}

// CommitOffset 提交offset到PostgreSQL
// 关键: 这是将offset写入persistent storage的操作
// 恢复时将从CommittedOffset+1开始重放
func (t *OffsetTracker) CommitOffset(ctx context.Context, offset int64, eventSeq uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 验证约束: CommittedOffset <= ConsumedOffset
	if offset > t.consumedOffset {
		return fmt.Errorf("cannot commit offset %d > consumedOffset %d",
			offset, t.consumedOffset)
	}

	checkpoint := &model.EventOffsetCheckpoint{
		ProcessorName:  t.processorName,
		Symbol:         t.symbol,
		EventSeq:       eventSeq,
		KafkaOffset:    offset,
		RecoveryStatus: "none",
		Timestamp:      time.Now().UnixMilli(),
	}

	// 保存到PostgreSQL
	if err := t.checkpointRepo.SaveCheckpoint(checkpoint); err != nil {
		t.commitFailureCount++
		hlog.Errorf("[OffsetTracker] Failed to commit offset: %v", err)
		return fmt.Errorf("save checkpoint failed: %w", err)
	}

	t.committedOffset = offset
	t.lastCommitTime = time.Now()
	t.commitCount++

	hlog.Infof("[OffsetTracker] Offset committed: %d for %s:%s (eventSeq=%d)",
		offset, t.processorName, t.symbol, eventSeq)

	return nil
}

// GetConsumerLag 获取消费延迟
// ConsumerLag = ProducedOffset - CommittedOffset
// 表示有多少条消息在Kafka中但还未被提交处理
func (t *OffsetTracker) GetConsumerLag() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	lag := t.kafkaProducerOffset - t.committedOffset

	// 更新lag的高水位
	if lag > t.lagHighWaterMark {
		t.lagHighWaterMark = lag
	}

	return lag
}

// GetConsumedButNotCommitted 获取已消费但未提交的消息数量
func (t *OffsetTracker) GetConsumedButNotCommitted() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.consumedOffset - t.committedOffset
}

// GetRecoveryStartOffset 获取恢复起点
// 恢复应该从CommittedOffset+1开始
func (t *OffsetTracker) GetRecoveryStartOffset() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.committedOffset + 1
}

// VerifyOffsetConsistency 验证offset状态的一致性
func (t *OffsetTracker) VerifyOffsetConsistency() error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// 约束1: CommittedOffset <= ConsumedOffset
	if t.committedOffset > t.consumedOffset {
		return fmt.Errorf("offset constraint violated: committedOffset=%d > consumedOffset=%d",
			t.committedOffset, t.consumedOffset)
	}

	// 约束2: ConsumedOffset <= ProducedOffset
	if t.consumedOffset > t.kafkaProducerOffset {
		return fmt.Errorf("offset constraint violated: consumedOffset=%d > producedOffset=%d",
			t.consumedOffset, t.kafkaProducerOffset)
	}

	// 约束3: ConsumerLag >= 0
	lag := t.kafkaProducerOffset - t.committedOffset
	if lag < 0 {
		return fmt.Errorf("negative consumer lag: %d", lag)
	}

	// 警告: 如果lag过高
	if lag > 10000 {
		hlog.Warnf("[OffsetTracker] High consumer lag detected: %d for %s:%s",
			lag, t.processorName, t.symbol)
	}

	return nil
}

// UpdateKafkaProducerOffset 更新Kafka生产者offset
// 这应该从Kafka metadata中获取
func (t *OffsetTracker) UpdateKafkaProducerOffset(offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset > t.kafkaProducerOffset {
		t.kafkaProducerOffset = offset
	}
}

// GetStatus 获取当前的offset状态快照
func (t *OffsetTracker) GetStatus() OffsetStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return OffsetStatus{
		Symbol:                  t.symbol,
		ProcessorName:           t.processorName,
		ConsumedOffset:          t.consumedOffset,
		CommittedOffset:         t.committedOffset,
		ProducedOffset:          t.kafkaProducerOffset,
		ConsumerLag:             t.kafkaProducerOffset - t.committedOffset,
		ConsumedButNotCommitted: t.consumedOffset - t.committedOffset,
		LastCommitTime:          t.lastCommitTime,
		CommitCount:             t.commitCount,
		CommitFailureCount:      t.commitFailureCount,
		LagHighWaterMark:        t.lagHighWaterMark,
	}
}

// OffsetStatus 表示offset的完整状态
type OffsetStatus struct {
	Symbol                  string
	ProcessorName           string
	ConsumedOffset          int64
	CommittedOffset         int64
	ProducedOffset          int64
	ConsumerLag             int64
	ConsumedButNotCommitted int64
	LastCommitTime          time.Time
	CommitCount             int64
	CommitFailureCount      int64
	LagHighWaterMark        int64
}

// String 返回可读的状态字符串
func (s OffsetStatus) String() string {
	return fmt.Sprintf(
		"[%s:%s] Consumed=%d Committed=%d Produced=%d Lag=%d (consumed_not_committed=%d) Commits=%d(failures=%d)",
		s.ProcessorName, s.Symbol,
		s.ConsumedOffset, s.CommittedOffset, s.ProducedOffset,
		s.ConsumerLag, s.ConsumedButNotCommitted,
		s.CommitCount, s.CommitFailureCount,
	)
}

// OffsetTrackerManager 管理多个processor的offset跟踪
type OffsetTrackerManager struct {
	mu             sync.RWMutex
	trackers       map[string]*OffsetTracker // key: "processorName:symbol"
	checkpointRepo *pg.CheckpointRepo
}

// NewOffsetTrackerManager 创建manager
func NewOffsetTrackerManager(checkpointRepo *pg.CheckpointRepo) *OffsetTrackerManager {
	return &OffsetTrackerManager{
		trackers:       make(map[string]*OffsetTracker),
		checkpointRepo: checkpointRepo,
	}
}

// GetOrCreateTracker 获取或创建tracker
func (m *OffsetTrackerManager) GetOrCreateTracker(processorName, symbol string) *OffsetTracker {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", processorName, symbol)

	if tracker, exists := m.trackers[key]; exists {
		return tracker
	}

	tracker := NewOffsetTracker(symbol, processorName, m.checkpointRepo)
	m.trackers[key] = tracker

	return tracker
}

// GetAllTrackers 获取所有tracker
func (m *OffsetTrackerManager) GetAllTrackers() []*OffsetTracker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	trackers := make([]*OffsetTracker, 0, len(m.trackers))
	for _, tracker := range m.trackers {
		trackers = append(trackers, tracker)
	}

	return trackers
}

// VerifyAllConsistency 验证所有tracker的一致性
func (m *OffsetTrackerManager) VerifyAllConsistency() error {
	trackers := m.GetAllTrackers()

	for _, tracker := range trackers {
		if err := tracker.VerifyOffsetConsistency(); err != nil {
			return fmt.Errorf("consistency check failed for %s:%s: %w",
				tracker.processorName, tracker.symbol, err)
		}
	}

	return nil
}

// PrintStatus 打印所有tracker的状态
func (m *OffsetTrackerManager) PrintStatus() {
	trackers := m.GetAllTrackers()

	hlog.Infof("[OffsetTrackerManager] Total trackers: %d", len(trackers))
	for _, tracker := range trackers {
		status := tracker.GetStatus()
		hlog.Infof("  - %s", status.String())
	}
}
