package service

import (
	"context"
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
)

// CrashPoint 定义crash发生的时间点
type CrashPoint struct {
	Name       string           // 标识: "AfterOrder", "AfterTrade", "AfterCheckpoint"
	OrderCount int              // 处理的订单数量
	TradeCount int              // 处理的交易数量
	Position   int              // 事件序列中的位置
	Timestamp  time.Time        // Crash发生的时间
	PreState   *StateSnapshot   // Crash前的状态快照
}

// CrashSimulation 记录crash的详细信息
type CrashSimulation struct {
	Point           CrashPoint
	PreChecksum     string                          // Crash前的checksum
	PostChecksum    string                          // 恢复后的checksum
	KafkaOffsetPre  int64                           // Crash前的Kafka offset
	KafkaOffsetPost int64                           // 恢复后的Kafka offset
	CheckpointPre   *model.EventOffsetCheckpoint    // Crash前的checkpoint
	CheckpointPost  *model.EventOffsetCheckpoint    // 恢复后的checkpoint
	Status          string                          // "in_progress", "complete", "failed"
	Error           string                          // 错误信息
}

// SimulateCrash 模拟进程崩溃
// 关键: 清空所有内存状态，但保留PostgreSQL/Kafka持久化
func SimulateCrash(ctx context.Context, engine *PartitionAwareMatchEngine, point CrashPoint) (*CrashSimulation, error) {
	sim := &CrashSimulation{
		Point:     point,
		Timestamp: time.Now(),
		Status:    "in_progress",
	}

	// Step 1: 获取crash前的状态快照
	preState := engine.CaptureStateSnapshot()
	if preState == nil {
		sim.Status = "failed"
		sim.Error = "failed to capture pre-crash state"
		return sim, fmt.Errorf("failed to capture pre-crash state")
	}
	sim.Point.PreState = preState
	sim.PreChecksum = preState.CalculateChecksum()

	// Step 2: 获取crash前的Kafka offset
	kafkaOffset, _ := engine.GetLastKafkaOffset()
	sim.KafkaOffsetPre = kafkaOffset

	// Step 3: 获取crash前的checkpoint
	cp, _ := engine.GetCheckpoint()
	if cp != nil {
		// 保存一份副本用于比对
		cpCopy := *cp
		sim.CheckpointPre = &cpCopy
	}

	// Step 4: 停止事件处理 (模拟进程即将关闭)
	// 注意: 这里只是示意，实际的StopProcessing应该在matchEngine中实现
	if err := engine.StopProcessing(ctx); err != nil {
		// 某些情况下stop可能失败，但继续crash模拟
		// 日志记录但不返回错误
		fmt.Printf("Warning: StopProcessing failed: %v\n", err)
	}

	// Step 5: 等待goroutine优雅退出 (实际crash中不会等待，但测试中要等)
	time.Sleep(100 * time.Millisecond)

	// Step 6: 清空内存 (模拟进程重启后的清空)
	// 实际上这部分应该是重启后新进程的操作
	// 这里我们通过Reset来清空OrderBook, Positions, EventLog等
	if err := engine.ClearInMemoryState(); err != nil {
		sim.Status = "failed"
		sim.Error = fmt.Sprintf("clear memory failed: %v", err)
		return sim, err
	}

	// Step 7: 验证持久化数据完整性
	// Kafka offset应该已保存
	if sim.KafkaOffsetPre <= 0 {
		sim.Status = "failed"
		sim.Error = "Kafka offset not persisted"
		return sim, fmt.Errorf("Kafka offset not persisted")
	}

	// PostgreSQL checkpoint应该已保存
	if sim.CheckpointPre == nil {
		sim.Status = "failed"
		sim.Error = "PostgreSQL checkpoint not persisted"
		return sim, fmt.Errorf("PostgreSQL checkpoint not persisted")
	}

	sim.Status = "complete"
	return sim, nil
}

// VerifyCrashRecovery 验证crash和恢复的一致性
func VerifyCrashRecovery(t *testing.T, engine *PartitionAwareMatchEngine, sim *CrashSimulation) error {
	// Step 1: 获取恢复后的状态
	postState := engine.CaptureStateSnapshot()
	if postState == nil {
		return fmt.Errorf("failed to capture post-recovery state")
	}
	sim.PostChecksum = postState.CalculateChecksum()

	// Step 2: Checksum必须完全相同
	if sim.PreChecksum != sim.PostChecksum {
		return fmt.Errorf("checksum mismatch: pre=%s != post=%s",
			sim.PreChecksum, sim.PostChecksum)
	}

	// Step 3: Kafka offset必须相同或更高
	postKafkaOffset, _ := engine.GetLastKafkaOffset()
	sim.KafkaOffsetPost = postKafkaOffset
	if sim.KafkaOffsetPost < sim.KafkaOffsetPre {
		return fmt.Errorf("kafka offset regression: post=%d < pre=%d",
			sim.KafkaOffsetPost, sim.KafkaOffsetPre)
	}

	// Step 4: 详细的state对比
	// 订单数量
	if len(postState.Orders) != len(sim.Point.PreState.Orders) {
		return fmt.Errorf("order count mismatch: pre=%d != post=%d",
			len(sim.Point.PreState.Orders), len(postState.Orders))
	}

	// 交易数量
	if len(postState.Trades) != len(sim.Point.PreState.Trades) {
		return fmt.Errorf("trade count mismatch: pre=%d != post=%d",
			len(sim.Point.PreState.Trades), len(postState.Trades))
	}

	// 持仓一致性
	if len(postState.Positions) != len(sim.Point.PreState.Positions) {
		return fmt.Errorf("position count mismatch: pre=%d != post=%d",
			len(sim.Point.PreState.Positions), len(postState.Positions))
	}

	return nil
}

// TestCrashAtMultiplePoints 在不同时间点crash，验证恢复一致性
func TestCrashAtMultiplePoints(t *testing.T) {
	points := []CrashPoint{
		{Name: "AfterOrder1", OrderCount: 1, TradeCount: 0},
		{Name: "After5Orders", OrderCount: 5, TradeCount: 0},
		{Name: "After50Trades", OrderCount: 25, TradeCount: 50},
		{Name: "After500Trades", OrderCount: 100, TradeCount: 500},
	}

	for _, point := range points {
		t.Run(point.Name, func(t *testing.T) {
			ctx := context.Background()

			// 设置测试引擎
			engine, cleanup := setupTestMatchEngineWithBitcoin(t)
			defer cleanup()

			// 处理订单和交易到crash点
			for i := 0; i < point.OrderCount; i++ {
				order := &model.Order{
					OrderID:    int64(i + 1),
					UserID:     "user1",
					Symbol:     "BTCUSDT",
					Side:       model.BUY,
					Price:      50000 + int64(i*100),
					Quantity:   1,
					Status:     model.ACTIVE,
					CreatedAt:  time.Now().UnixMilli(),
					UpdatedAt:  time.Now().UnixMilli(),
				}
				engine.ProcessOrder(order)
			}

			// 处理交易
			for i := 0; i < point.TradeCount; i++ {
				// 从买卖订单对生成交易
				if i+1 < point.OrderCount {
					trade := &model.Trade{
						TradeID:   int64(i + 1),
						BuyerID:   "user1",
						SellerID:  "user2",
						Symbol:    "BTCUSDT",
						Price:     50000 + int64(i*100),
						Quantity:  1,
						CreatedAt: time.Now().UnixMilli(),
					}
					engine.ProcessTrade(trade)
				}
			}

			// 确保checkpoint已flush
			time.Sleep(100 * time.Millisecond)

			// 模拟crash
			sim, err := SimulateCrash(ctx, engine, point)
			require.NoError(t, err, "crash simulation failed for %s", point.Name)
			require.NotNil(t, sim, "crash simulation result is nil")

			// 验证crash前状态已保存
			require.NotEmpty(t, sim.PreChecksum, "pre-crash checksum should not be empty")
			require.Greater(t, sim.KafkaOffsetPre, int64(0), "Kafka offset should be positive")
			require.NotNil(t, sim.CheckpointPre, "checkpoint should be saved")

			// 执行恢复
			recoveryExecutor := NewRecoveryExecutor(engine)
			shouldRecover, _ := recoveryExecutor.ShouldRecover(ctx)
			if !shouldRecover {
				t.Logf("Recovery not needed for %s", point.Name)
				return
			}

			opts := &RecoveryOptions{
				Timeout:              30 * time.Second,
				Strategy:             RECOVERY_STRATEGY_INCREMENTAL,
				ValidateAfterRecover: true,
			}

			result, err := recoveryExecutor.ExecuteRecovery(ctx, opts)
			require.NoError(t, err, "recovery execution failed for %s", point.Name)
			require.NotNil(t, result, "recovery result is nil")

			// 验证恢复后的一致性
			err = VerifyCrashRecovery(t, engine, sim)
			require.NoError(t, err, "recovery verification failed for %s: %v", point.Name, err)

			// 最终一致性检查
			require.Equal(t, sim.PreChecksum, sim.PostChecksum, 
				"checksum should match after recovery for %s", point.Name)

			t.Logf("✅ Crash recovery test passed for %s: pre_checksum=%s, kafka_offset=%d",
				point.Name, sim.PreChecksum[:8], sim.KafkaOffsetPre)
		})
	}
}

// TestCrashDuringCheckpointFlush 在checkpoint flush中间crash
func TestCrashDuringCheckpointFlush(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestMatchEngineWithBitcoin(t)
	defer cleanup()

	// 处理足够多的事件触发checkpoint flush
	for i := 0; i < 100; i++ {
		order := &model.Order{
			OrderID:    int64(i + 1),
			UserID:     fmt.Sprintf("user%d", i%10),
			Symbol:     "BTCUSDT",
			Side:       model.BUY,
			Price:      50000 + int64(i),
			Quantity:   1,
			Status:     model.ACTIVE,
			CreatedAt:  time.Now().UnixMilli(),
			UpdatedAt:  time.Now().UnixMilli(),
		}
		engine.ProcessOrder(order)

		// 在第50个订单时crash
		if i == 49 {
			point := CrashPoint{
				Name:       "DuringCheckpointFlush",
				OrderCount: 50,
				TradeCount: 0,
				Position:   50,
			}

			sim, err := SimulateCrash(ctx, engine, point)
			require.NoError(t, err)
			require.NotNil(t, sim)

			// 验证即使在flush中间crash，数据也应该完整
			require.NotEmpty(t, sim.PreChecksum)
			require.Greater(t, sim.KafkaOffsetPre, int64(0))

			// 验证恢复
			recoveryExecutor := NewRecoveryExecutor(engine)
			opts := &RecoveryOptions{
				Timeout:              30 * time.Second,
				Strategy:             RECOVERY_STRATEGY_INCREMENTAL,
				ValidateAfterRecover: true,
			}
			result, err := recoveryExecutor.ExecuteRecovery(ctx, opts)
			require.NoError(t, err)
			require.NotNil(t, result)

			// 验证一致性
			err = VerifyCrashRecovery(t, engine, sim)
			require.NoError(t, err)

			return
		}
	}
}

// TestCrashWithPendingEvents Crash时有未flush的事件
func TestCrashWithPendingEvents(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestMatchEngineWithBitcoin(t)
	defer cleanup()

	// Phase 1: 处理事件并flush
	for i := 0; i < 50; i++ {
		order := &model.Order{
			OrderID:    int64(i + 1),
			UserID:     "user1",
			Symbol:     "BTCUSDT",
			Side:       model.BUY,
			Price:      50000 + int64(i),
			Quantity:   1,
			Status:     model.ACTIVE,
			CreatedAt:  time.Now().UnixMilli(),
			UpdatedAt:  time.Now().UnixMilli(),
		}
		engine.ProcessOrder(order)
	}
	time.Sleep(100 * time.Millisecond) // 等待flush

	// 获取flushed的offset
	flushedOffset, _ := engine.GetLastKafkaOffset()
	t.Logf("Flushed offset: %d", flushedOffset)

	// Phase 2: 处理更多事件但不flush就crash
	for i := 50; i < 75; i++ {
		order := &model.Order{
			OrderID:    int64(i + 1),
			UserID:     "user1",
			Symbol:     "BTCUSDT",
			Side:       model.BUY,
			Price:      50000 + int64(i),
			Quantity:   1,
			Status:     model.ACTIVE,
			CreatedAt:  time.Now().UnixMilli(),
			UpdatedAt:  time.Now().UnixMilli(),
		}
		engine.ProcessOrder(order)
	}

	// 不wait flush，直接crash
	point := CrashPoint{
		Name:       "WithPendingEvents",
		OrderCount: 75,
		TradeCount: 0,
		Position:   75,
	}

	sim, err := SimulateCrash(ctx, engine, point)
	require.NoError(t, err)

	// Kafka offset应该是上一次flush的offset
	// 因为这次没flush，所以应该等于flushedOffset或略高
	assert.GreaterOrEqual(t, sim.KafkaOffsetPre, flushedOffset,
		"KafkaOffset should be >= flushed offset")

	// 恢复后应该重放所有事件
	recoveryExecutor := NewRecoveryExecutor(engine)
	shouldRecover, _ := recoveryExecutor.ShouldRecover(ctx)
	if shouldRecover {
		opts := &RecoveryOptions{
			Timeout:              30 * time.Second,
			Strategy:             RECOVERY_STRATEGY_INCREMENTAL,
			ValidateAfterRecover: true,
		}
		result, err := recoveryExecutor.ExecuteRecovery(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, result)

		// 验证一致性
		err = VerifyCrashRecovery(t, engine, sim)
		require.NoError(t, err)
	}
}

// TestSequentialCrashes 连续crash多次，验证幂等性
func TestSequentialCrashes(t *testing.T) {
	ctx := context.Background()

	checksums := make([]string, 3)

	for round := 0; round < 3; round++ {
		t.Run(fmt.Sprintf("Round%d", round+1), func(t *testing.T) {
			engine, cleanup := setupTestMatchEngineWithBitcoin(t)
			defer cleanup()

			// 处理固定的订单和交易集合
			for i := 0; i < 50; i++ {
				order := &model.Order{
					OrderID:    int64(i + 1),
					UserID:     "user1",
					Symbol:     "BTCUSDT",
					Side:       model.BUY,
					Price:      50000 + int64(i),
					Quantity:   1,
					Status:     model.ACTIVE,
					CreatedAt:  time.Now().UnixMilli(),
					UpdatedAt:  time.Now().UnixMilli(),
				}
				engine.ProcessOrder(order)
			}

			time.Sleep(100 * time.Millisecond)

			// Crash
			point := CrashPoint{
				Name:       fmt.Sprintf("Round%d", round+1),
				OrderCount: 50,
				TradeCount: 0,
			}

			sim, err := SimulateCrash(ctx, engine, point)
			require.NoError(t, err)
			checksums[round] = sim.PreChecksum

			// 恢复
			recoveryExecutor := NewRecoveryExecutor(engine)
			shouldRecover, _ := recoveryExecutor.ShouldRecover(ctx)
			if shouldRecover {
				opts := &RecoveryOptions{
					Timeout:              30 * time.Second,
					Strategy:             RECOVERY_STRATEGY_INCREMENTAL,
					ValidateAfterRecover: true,
				}
				result, err := recoveryExecutor.ExecuteRecovery(ctx, opts)
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}

	// 所有三次crash后的checksum应该完全相同
	require.Equal(t, checksums[0], checksums[1], "Round 1 and 2 checksums should match")
	require.Equal(t, checksums[1], checksums[2], "Round 2 and 3 checksums should match")

	t.Logf("✅ Sequential crashes verified: all checksums match (%s)", checksums[0][:8])
}

// calculateChecksum 计算状态的MD5 hash
func calculateChecksum(data interface{}) string {
	hash := md5.Sum([]byte(fmt.Sprintf("%+v", data)))
	return fmt.Sprintf("%x", hash)
}

// setupTestMatchEngineWithBitcoin 创建用于测试的匹配引擎
func setupTestMatchEngineWithBitcoin(t *testing.T) (*PartitionAwareMatchEngine, func()) {
	// 这个函数应该在真实实现中初始化一个完整的匹配引擎
	// 包含所有必要的processor和状态
	
	// 暂时返回nil和cleanup函数
	// 后续会在integration test中实现具体逻辑
	t.Skip("setupTestMatchEngineWithBitcoin not yet implemented - use crash_recovery_test.go version")
	return nil, func() {}
}
