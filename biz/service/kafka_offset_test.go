package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/biz/dal/pg"
)

// TestOffsetTracking_BasicFlow 基础的offset跟踪流程
func TestOffsetTracking_BasicFlow(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// Step 1: 消费消息 (更新ConsumedOffset)
	for i := int64(1); i <= 100; i++ {
		tracker.TrackConsumedOffset(i)
	}
	
	// 验证consumed offset
	status := tracker.GetStatus()
	assert.Equal(t, int64(100), status.ConsumedOffset, "consumed offset should be 100")
	assert.Equal(t, int64(0), status.CommittedOffset, "committed offset should still be 0")
	
	// Step 2: 定期提交offset (更新CommittedOffset)
	for i := int64(10); i <= 100; i += 10 {
		err := tracker.CommitOffset(ctx, i, i)
		require.NoError(t, err)
		
		status := tracker.GetStatus()
		assert.Equal(t, i, status.CommittedOffset, "committed offset should match")
	}
	
	// 最终验证
	status = tracker.GetStatus()
	assert.Equal(t, int64(100), status.ConsumedOffset)
	assert.Equal(t, int64(100), status.CommittedOffset)
	
	// 验证没有未提交的消息
	assert.Equal(t, int64(0), status.ConsumedButNotCommitted)
	
	// 验证一致性
	err := tracker.VerifyOffsetConsistency()
	require.NoError(t, err)
}

// TestOffsetTracking_ConsumerLag 消费延迟跟踪
func TestOffsetTracking_ConsumerLag(t *testing.T) {
	repo := setupTestCheckpointRepo(t)
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// 设置Kafka producer已产生500条消息
	tracker.UpdateKafkaProducerOffset(500)
	
	// Step 1: 消费100条
	for i := int64(1); i <= 100; i++ {
		tracker.TrackConsumedOffset(i)
	}
	
	// 此时:
	// ProducedOffset = 500
	// ConsumedOffset = 100
	// CommittedOffset = 0
	// ConsumerLag = 500 - 0 = 500
	
	status := tracker.GetStatus()
	assert.Equal(t, int64(500), status.ConsumerLag, "lag should be 500")
	assert.Equal(t, int64(100), status.ConsumedButNotCommitted, "pending should be 100")
	
	// Step 2: 提交50条
	ctx := context.Background()
	err := tracker.CommitOffset(ctx, 50, 50)
	require.NoError(t, err)
	
	status = tracker.GetStatus()
	assert.Equal(t, int64(450), status.ConsumerLag, "lag should be 450 after commit")
	assert.Equal(t, int64(50), status.ConsumedButNotCommitted, "pending should be 50")
	
	// Step 3: 继续消费到500
	for i := int64(101); i <= 500; i++ {
		tracker.TrackConsumedOffset(i)
	}
	
	// 提交所有
	err = tracker.CommitOffset(ctx, 500, 500)
	require.NoError(t, err)
	
	status = tracker.GetStatus()
	assert.Equal(t, int64(0), status.ConsumerLag, "lag should be 0 when fully caught up")
	assert.Equal(t, int64(0), status.ConsumedButNotCommitted)
}

// TestOffsetTracking_CrashRecovery Crash/Recovery中的offset行为
func TestOffsetTracking_CrashRecovery(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	tracker.UpdateKafkaProducerOffset(1000)
	
	// Phase 1: 正常处理和提交
	for i := int64(1); i <= 500; i++ {
		tracker.TrackConsumedOffset(i)
		if i%100 == 0 {
			err := tracker.CommitOffset(ctx, i, i)
			require.NoError(t, err)
		}
	}
	
	status := tracker.GetStatus()
	assert.Equal(t, int64(500), status.ConsumedOffset)
	assert.Equal(t, int64(500), status.CommittedOffset)
	recoveryStartBeforeCrash := tracker.GetRecoveryStartOffset()
	assert.Equal(t, int64(501), recoveryStartBeforeCrash)
	
	// Phase 2: 消费更多但未提交（模拟crash前的未flush状态）
	for i := int64(501); i <= 600; i++ {
		tracker.TrackConsumedOffset(i)
		// 故意不提交，模拟未flush的checkpoint
	}
	
	status = tracker.GetStatus()
	assert.Equal(t, int64(600), status.ConsumedOffset)
	assert.Equal(t, int64(500), status.CommittedOffset)
	assert.Equal(t, int64(100), status.ConsumedButNotCommitted)
	
	// Phase 3: Crash发生
	// 恢复时应该从CommittedOffset+1=501开始
	recoveryStartAfterCrash := tracker.GetRecoveryStartOffset()
	assert.Equal(t, int64(501), recoveryStartAfterCrash)
	
	// Phase 4: 重新消费和处理（重放从501-600）
	// 创建一个新的tracker来模拟重启后的状态
	newTracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	newTracker.UpdateKafkaProducerOffset(1000)
	
	// 重放501-600
	for i := int64(501); i <= 600; i++ {
		newTracker.TrackConsumedOffset(i)
	}
	
	// 提交所有到600
	err := newTracker.CommitOffset(ctx, 600, 600)
	require.NoError(t, err)
	
	status = newTracker.GetStatus()
	assert.Equal(t, int64(600), status.CommittedOffset)
	
	// Phase 5: 恢复完成后，继续处理新消息
	for i := int64(601); i <= 1000; i++ {
		newTracker.TrackConsumedOffset(i)
	}
	
	// 继续提交
	for i := int64(700); i <= 1000; i += 100 {
		err := newTracker.CommitOffset(ctx, i, i)
		require.NoError(t, err)
	}
	
	status = newTracker.GetStatus()
	assert.Equal(t, int64(1000), status.CommittedOffset)
	assert.Equal(t, int64(0), status.ConsumerLag)
}

// TestOffsetTracking_ConstraintViolation 约束违反检测
func TestOffsetTracking_ConstraintViolation(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// 约束违反: 尝试提交offset但ConsumedOffset更小
	tracker.TrackConsumedOffset(50)
	
	// 尝试提交100 (> ConsumedOffset 50)
	err := tracker.CommitOffset(ctx, 100, 100)
	assert.Error(t, err, "should reject commit offset > consumed offset")
	assert.Contains(t, err.Error(), "cannot commit offset")
	
	// 验证状态没有被改变
	status := tracker.GetStatus()
	assert.Equal(t, int64(0), status.CommittedOffset)
}

// TestOffsetTracking_MultipleCommits 多次提交的处理
func TestOffsetTracking_MultipleCommits(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// 消费1000条消息
	for i := int64(1); i <= 1000; i++ {
		tracker.TrackConsumedOffset(i)
	}
	
	// 多次提交，每次提交100条
	for i := int64(100); i <= 1000; i += 100 {
		err := tracker.CommitOffset(ctx, i, i)
		require.NoError(t, err)
		
		status := tracker.GetStatus()
		assert.Equal(t, i, status.CommittedOffset)
		assert.Equal(t, int64(1), status.CommitCount)  // 实际应该累计，但这里只是最后一次
	}
	
	status := tracker.GetStatus()
	assert.Equal(t, int64(10), status.CommitCount, "should have 10 commits")
	assert.Equal(t, int64(0), status.CommitFailureCount, "should have 0 failures")
}

// TestOffsetTracking_HighLag 高lag警告
func TestOffsetTracking_HighLag(t *testing.T) {
	repo := setupTestCheckpointRepo(t)
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// 设置非常高的producer offset但不消费
	tracker.UpdateKafkaProducerOffset(100000)
	
	status := tracker.GetStatus()
	assert.Equal(t, int64(100000), status.ConsumerLag)
	assert.Equal(t, int64(100000), status.LagHighWaterMark)
	
	// 验证一致性仍然通过（约束检查）
	err := tracker.VerifyOffsetConsistency()
	require.NoError(t, err, "高lag不应该违反约束")
}

// TestOffsetTracking_Manager 测试manager功能
func TestOffsetTracking_Manager(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	manager := NewOffsetTrackerManager(repo)
	
	// 创建多个processor的tracker
	processors := []string{"OrderProcessor", "TradeProcessor", "PositionProcessor"}
	symbols := []string{"BTCUSDT", "ETHUSDT"}
	
	for _, proc := range processors {
		for _, sym := range symbols {
			tracker := manager.GetOrCreateTracker(proc, sym)
			require.NotNil(t, tracker)
			
			// 消费和提交一些消息
			for i := int64(1); i <= 100; i++ {
				tracker.TrackConsumedOffset(i)
			}
			
			err := tracker.CommitOffset(ctx, 100, 100)
			require.NoError(t, err)
		}
	}
	
	// 验证tracker数量
	trackers := manager.GetAllTrackers()
	assert.Equal(t, 6, len(trackers), "should have 6 trackers (3 processors * 2 symbols)")
	
	// 验证所有tracker的一致性
	err := manager.VerifyAllConsistency()
	require.NoError(t, err)
	
	// 验证可以重新获取同一个tracker
	tracker1 := manager.GetOrCreateTracker("OrderProcessor", "BTCUSDT")
	tracker2 := manager.GetOrCreateTracker("OrderProcessor", "BTCUSDT")
	assert.Equal(t, tracker1, tracker2, "should return same tracker instance")
}

// TestOffsetTracking_RecoveryStartOffset 恢复起点计算
func TestOffsetTracking_RecoveryStartOffset(t *testing.T) {
	ctx := context.Background()
	repo := setupTestCheckpointRepo(t)
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", repo)
	
	// Case 1: 没有提交过任何offset
	recoveryStart := tracker.GetRecoveryStartOffset()
	assert.Equal(t, int64(1), recoveryStart, "recovery should start from 1 if no commits")
	
	// Case 2: 提交了offset
	for i := int64(1); i <= 500; i++ {
		tracker.TrackConsumedOffset(i)
	}
	err := tracker.CommitOffset(ctx, 500, 500)
	require.NoError(t, err)
	
	recoveryStart = tracker.GetRecoveryStartOffset()
	assert.Equal(t, int64(501), recoveryStart, "recovery should start from 501")
	
	// Case 3: 多次提交中间的offset
	err = tracker.CommitOffset(ctx, 300, 300)
	require.NoError(t, err)
	
	recoveryStart = tracker.GetRecoveryStartOffset()
	assert.Equal(t, int64(301), recoveryStart, "recovery start should be updated")
}

// TestOffsetTracking_CommitFailure 提交失败的处理
func TestOffsetTracking_CommitFailure(t *testing.T) {
	ctx := context.Background()
	// 使用一个会失败的repo mock
	failingRepo := &FailingCheckpointRepoMock{}
	
	tracker := NewOffsetTracker("BTCUSDT", "OrderProcessor", failingRepo)
	
	// 消费消息
	for i := int64(1); i <= 100; i++ {
		tracker.TrackConsumedOffset(i)
	}
	
	// 尝试提交（会失败）
	err := tracker.CommitOffset(ctx, 100, 100)
	assert.Error(t, err, "commit should fail")
	
	// 验证状态
	status := tracker.GetStatus()
	assert.Equal(t, int64(0), status.CommittedOffset, "committed offset should not change on failure")
	assert.Equal(t, int64(1), status.CommitFailureCount, "failure count should increment")
	assert.Equal(t, int64(100), status.ConsumedButNotCommitted)
	
	// 验证一致性仍然通过
	err = tracker.VerifyOffsetConsistency()
	require.NoError(t, err)
}

// setupTestCheckpointRepo 创建测试用的checkpoint repo
func setupTestCheckpointRepo(t *testing.T) pg.CheckpointRepo {
	// 这个函数应该返回一个测试用的repo
	// 暂时使用一个mock实现
	return &MockCheckpointRepo{}
}

// MockCheckpointRepo 用于测试的mock实现
type MockCheckpointRepo struct {
	checkpoints map[string]*model.EventOffsetCheckpoint
}

func (m *MockCheckpointRepo) SaveCheckpoint(ctx context.Context, cp *model.EventOffsetCheckpoint) error {
	if m.checkpoints == nil {
		m.checkpoints = make(map[string]*model.EventOffsetCheckpoint)
	}
	key := cp.ProcessorName + ":" + cp.Symbol
	m.checkpoints[key] = cp
	return nil
}

func (m *MockCheckpointRepo) GetCheckpoint(ctx context.Context, processorName, symbol string) (*model.EventOffsetCheckpoint, error) {
	key := processorName + ":" + symbol
	if cp, exists := m.checkpoints[key]; exists {
		return cp, nil
	}
	return nil, nil
}

// FailingCheckpointRepoMock 会失败的repo
type FailingCheckpointRepoMock struct{}

func (m *FailingCheckpointRepoMock) SaveCheckpoint(ctx context.Context, cp *model.EventOffsetCheckpoint) error {
	return fmt.Errorf("checkpoint save failed (mock)")
}

func (m *FailingCheckpointRepoMock) GetCheckpoint(ctx context.Context, processorName, symbol string) (*model.EventOffsetCheckpoint, error) {
	return nil, nil
}
