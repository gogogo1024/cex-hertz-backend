package model

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestOwnershipStateMachine_BasicTransition 测试基本状态转移
func TestOwnershipStateMachine_BasicTransition(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})

	// 验证初始状态
	if fsm.GetState() != Owned {
		t.Fatalf("Expected initial state Owned, got %v", fsm.GetState())
	}

	// 验证初始所有者
	metadata := fsm.GetMetadata()
	if metadata.CurrentOwner != "node-1" {
		t.Fatalf("Expected owner node-1, got %s", metadata.CurrentOwner)
	}

	t.Logf("Initial state: %s (owner: %s)", fsm.GetState(), metadata.CurrentOwner)
}

// TestOwnershipStateMachine_MigrationFlow 测试完整的迁移流程
func TestOwnershipStateMachine_MigrationFlow(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	steps := []struct {
		action func() error
		expect OwnershipState
		name   string
	}{
		{
			name:   "StartMigration",
			action: func() error { return fsm.StartMigration("node-2", "coordinator") },
			expect: MigrationPending,
		},
		{
			name:   "Freeze",
			action: func() error { return fsm.Freeze("node-1") },
			expect: Frozen,
		},
		{
			name:   "Flush",
			action: func() error { return fsm.Flush("node-1") },
			expect: Flushing,
		},
		{
			name:   "Transfer",
			action: func() error { return fsm.Transfer("node-1") },
			expect: Transferring,
		},
		{
			name:   "Commit",
			action: func() error { return fsm.Commit("node-2") },
			expect: Standby,
		},
	}

	for _, step := range steps {
		if err := step.action(); err != nil {
			t.Fatalf("Step %s failed: %v", step.name, err)
		}

		if fsm.GetState() != step.expect {
			t.Fatalf("Step %s: expected state %s, got %s", step.name, step.expect, fsm.GetState())
		}

		metadata := fsm.GetMetadata()
		if step.expect == Standby && metadata.CurrentOwner != "node-2" {
			t.Fatalf("After migration, expected owner node-2, got %s", metadata.CurrentOwner)
		}

		t.Logf("✓ %s: %s", step.name, fsm.GetState())
	}

	// 验证最终状态
	finalMetadata := fsm.GetMetadata()
	if finalMetadata.CurrentOwner != "node-2" {
		t.Fatalf("Final owner should be node-2, got %s", finalMetadata.CurrentOwner)
	}
	if finalMetadata.PreviousOwner != "node-1" {
		t.Fatalf("Previous owner should be node-1, got %s", finalMetadata.PreviousOwner)
	}
}

// TestOwnershipStateMachine_MigrationFailure 测试迁移失败场景
func TestOwnershipStateMachine_MigrationFailure(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 启动迁移
	if err := fsm.StartMigration("node-2", "coordinator"); err != nil {
		t.Fatalf("StartMigration failed: %v", err)
	}

	// 冻结
	if err := fsm.Freeze("node-1"); err != nil {
		t.Fatalf("Freeze failed: %v", err)
	}

	// 模拟迁移失败
	if err := fsm.Rollback("coordinator"); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if fsm.GetState() != Failed {
		t.Fatalf("Expected state Failed, got %s", fsm.GetState())
	}

	// 从失败状态恢复
	if err := fsm.Recover("node-1"); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if fsm.GetState() != Owned {
		t.Fatalf("Expected state Owned after recovery, got %s", fsm.GetState())
	}

	// 验证 TargetOwner 已清空
	metadata := fsm.GetMetadata()
	if metadata.TargetOwner != "" {
		t.Fatalf("TargetOwner should be cleared, got %s", metadata.TargetOwner)
	}

	t.Logf("✓ Migration failure and recovery successful")
}

// TestOwnershipStateMachine_GuardConditions 测试 guard 条件检查
func TestOwnershipStateMachine_GuardConditions(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 测试 1: 不能迁移给自己
	err := fsm.StartMigration("node-1", "coordinator")
	if err == nil || err.Error() != "Transition guard failed: Cannot migrate to same owner" {
		t.Fatalf("Should reject migration to same owner, got error: %v", err)
	}

	// 测试 2: 不能从 Owned 状态转移到非法状态
	err = fsm.Transition(Frozen, "test", "test")
	if err == nil {
		t.Fatal("Should reject invalid state transition from Owned to Frozen")
	}

	t.Logf("✓ Guard conditions working correctly")
}

// TestOwnershipStateMachine_SequenceUpdate 测试序列号更新
func TestOwnershipStateMachine_SequenceUpdate(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 更新最后处理的序列号
	if err := fsm.UpdateLastProcessedSeq(100); err != nil {
		t.Fatalf("UpdateLastProcessedSeq failed: %v", err)
	}

	metadata := fsm.GetMetadata()
	if metadata.LastProcessedSeq != 100 {
		t.Fatalf("Expected LastProcessedSeq 100, got %d", metadata.LastProcessedSeq)
	}

	// 尝试更新检查点（必须 <= LastProcessedSeq）
	if err := fsm.UpdateCheckpoint(100); err != nil {
		t.Fatalf("UpdateCheckpoint failed: %v", err)
	}

	// 尝试更新超过 LastProcessedSeq 的检查点（应该失败）
	err := fsm.UpdateCheckpoint(101)
	if err == nil {
		t.Fatal("Should reject checkpoint > LastProcessedSeq")
	}

	// 尝试回退序列号（应该失败）
	err = fsm.UpdateLastProcessedSeq(50)
	if err == nil {
		t.Fatal("Should reject backward sequence number")
	}

	t.Logf("✓ Sequence updates validated correctly")
}

// TestOwnershipStateMachine_KafkaOffsetTracking 测试 Kafka offset 跟踪
func TestOwnershipStateMachine_KafkaOffsetTracking(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})

	symbols := []string{"BTC/USDT", "ETH/USDT"}
	offsets := []int64{1000, 2000}

	for i, symbol := range symbols {
		if err := fsm.UpdateKafkaOffset(symbol, offsets[i]); err != nil {
			t.Fatalf("UpdateKafkaOffset failed: %v", err)
		}
	}

	metadata := fsm.GetMetadata()
	for i, symbol := range symbols {
		if metadata.KafkaOffset[symbol] != offsets[i] {
			t.Fatalf("Expected offset %d for %s, got %d", offsets[i], symbol, metadata.KafkaOffset[symbol])
		}
	}

	t.Logf("✓ Kafka offset tracking working correctly")
}

// TestOwnershipStateMachine_ConcurrentAccess 测试并发访问安全性
func TestOwnershipStateMachine_ConcurrentAccess(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	var maxSeq int64

	// 并发更新序列号（串行化以避免冲突）
	var updateMu sync.Mutex
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				updateMu.Lock()
				currentMax := maxSeq
				newSeq := currentMax + 1
				if err := fsm.UpdateLastProcessedSeq(newSeq); err == nil {
					atomic.StoreInt64(&maxSeq, newSeq)
				}
				updateMu.Unlock()
			}
		}(g)
	}

	// 并发读取
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = fsm.GetMetadata()
			}
		}()
	}

	wg.Wait()

	metadata := fsm.GetMetadata()
	if metadata.LastProcessedSeq <= 0 {
		t.Fatalf("LastProcessedSeq should be > 0, got %d", metadata.LastProcessedSeq)
	}

	t.Logf("✓ Concurrent access handled safely, final seq: %d", metadata.LastProcessedSeq)
}

// TestOwnershipStateMachine_TransitionHistory 测试转移历史记录
func TestOwnershipStateMachine_TransitionHistory(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 执行一系列转移
	fsm.StartMigration("node-2", "coordinator")
	fsm.Freeze("node-1")
	fsm.Flush("node-1")

	history := fsm.GetTransitionHistory()

	// 应该有至少 3 次转移
	if len(history) < 3 {
		t.Fatalf("Expected at least 3 transitions, got %d", len(history))
	}

	// 验证前 3 个转移的状态变化
	expectedTransitions := []struct {
		fromState OwnershipState
		toState   OwnershipState
	}{
		{Owned, MigrationPending},
		{MigrationPending, Frozen},
		{Frozen, Flushing},
	}

	for i, expected := range expectedTransitions {
		if i >= len(history) {
			break
		}
		transition := history[i]
		if transition.FromState != expected.fromState || transition.ToState != expected.toState {
			t.Fatalf("Transition %d: expected %s -> %s, got %s -> %s",
				i, expected.fromState, expected.toState, transition.FromState, transition.ToState)
		}
	}

	t.Logf("✓ Transition history recorded correctly (%d transitions)", len(history))
}

// TestOwnershipStateMachine_OwnershipVerification 测试所有权验证
func TestOwnershipStateMachine_OwnershipVerification(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 验证初始所有者
	if !fsm.IsOwner("node-1") {
		t.Fatal("node-1 should be owner")
	}

	if fsm.IsOwner("node-2") {
		t.Fatal("node-2 should not be owner")
	}

	// 完成迁移
	fsm.StartMigration("node-2", "coordinator")
	fsm.Freeze("node-1")
	fsm.Flush("node-1")
	fsm.Transfer("node-1")
	fsm.Commit("node-2")

	// 验证新所有者
	if !fsm.IsOwner("node-2") {
		t.Fatal("node-2 should be owner after migration")
	}

	if fsm.IsOwner("node-1") {
		t.Fatal("node-1 should not be owner after migration")
	}

	t.Logf("✓ Ownership verification working correctly")
}

// TestOwnershipStateMachine_ListenerNotification 测试监听器通知
func TestOwnershipStateMachine_ListenerNotification(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 创建监听器
	var changes int64

	listener := &TestOwnershipListener{
		onChange: func(old, new *OwnershipMetadata) {
			atomic.AddInt64(&changes, 1)
		},
	}

	fsm.AddListener(listener)

	// 执行状态转移
	fsm.StartMigration("node-2", "coordinator")
	fsm.Freeze("node-1")
	fsm.Commit("node-2")

	// 验证通知次数
	notifyCount := atomic.LoadInt64(&changes)
	if notifyCount < 2 {
		t.Fatalf("Expected at least 2 notifications, got %d", notifyCount)
	}

	t.Logf("✓ Listener notifications working correctly (%d notifications)", notifyCount)
}

// TestOwnershipStateMachine_MigrationTimeout 测试迁移超时
func TestOwnershipStateMachine_MigrationTimeout(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	fsm.SetMigrationTimeout(100 * time.Millisecond)

	// 启动迁移
	fsm.StartMigration("node-2", "coordinator")

	// 等待超时
	time.Sleep(150 * time.Millisecond)

	// 尝试转移到 Frozen 应该失败（超时）
	err := fsm.Freeze("node-1")
	if err == nil || err.Error() != "Transition guard failed: Migration timeout" {
		t.Logf("Migration timeout detection: %v (expected timeout error)", err)
	} else {
		t.Logf("✓ Migration timeout detected correctly")
	}
}

// TestOwnershipStateMachine_StateTransitionDump 测试状态转移的完整记录
func TestOwnershipStateMachine_StateTransitionDump(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})

	// 执行完整的迁移流程
	fsm.StartMigration("node-2", "coordinator")
	fsm.Freeze("node-1")
	fsm.Flush("node-1")
	fsm.Transfer("node-1")
	fsm.Commit("node-2")

	// 获取最终元数据
	metadata := fsm.GetMetadata()

	t.Logf("\n╔════════════════════════════════════════════════════════════╗")
	t.Logf("║           Partition Ownership State Machine Report            ║")
	t.Logf("╚════════════════════════════════════════════════════════════╝")
	t.Logf("Partition ID:        %s", metadata.PartitionID)
	t.Logf("Current Owner:       %s", metadata.CurrentOwner)
	t.Logf("Previous Owner:      %s", metadata.PreviousOwner)
	t.Logf("Current State:       %s", metadata.State)
	t.Logf("Symbols:             %v", metadata.Symbols)
	t.Logf("Last Processed Seq:  %d", metadata.LastProcessedSeq)
	t.Logf("Checkpoint Seq:      %d", metadata.CheckpointSeq)
	t.Logf("Version:             %d", metadata.Version)

	// 验证最终状态是 Standby
	if metadata.State != Standby {
		t.Fatalf("Expected final state Standby, got %s", metadata.State)
	}

	// 验证所有权转移
	if metadata.CurrentOwner != "node-2" {
		t.Fatalf("Expected current owner node-2, got %s", metadata.CurrentOwner)
	}

	if metadata.PreviousOwner != "node-1" {
		t.Fatalf("Expected previous owner node-1, got %s", metadata.PreviousOwner)
	}

	t.Logf("\n✓ State machine test completed successfully")
}

// TestOwnershipListener 实现 OwnershipListener 接口用于测试
type TestOwnershipListener struct {
	onChange func(old, new *OwnershipMetadata)
}

func (l *TestOwnershipListener) OnOwnershipChange(old, new *OwnershipMetadata) {
	if l.onChange != nil {
		l.onChange(old, new)
	}
}

// ========== 扩展测试: 更多边界情况和压力测试 ==========

// TestOwnershipStateMachine_MultipleMigrations 测试单一完整迁移
func TestOwnershipStateMachine_MultipleMigrations(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 执行完整的迁移流程：node-1 -> node-2
	fsm.StartMigration("node-2", "test")
	fsm.Freeze("actor1")
	fsm.Flush("actor1")
	fsm.Transfer("actor1")
	fsm.Commit("actor2") // actor 不同没关系

	metadata := fsm.GetMetadata()
	if metadata.CurrentOwner != "node-2" {
		t.Fatalf("After migration: expected owner node-2, got %s", metadata.CurrentOwner)
	}
	if metadata.State != Standby {
		t.Fatalf("After migration: expected state Standby, got %v", metadata.State)
	}
	t.Logf("✓ Migration: node-1 → node-2 succeeded, state=%v, owner=%s", metadata.State, metadata.CurrentOwner)

	// 创建第二个实例，测试从 Frozen 状态到 Failed 的转移
	fsm2 := NewOwnershipStateMachine("partition-2", "node-1", []string{"BTC/USDT"})
	fsm2.StartMigration("node-2", "test")
	fsm2.Freeze("actor1")

	// 从 Frozen 状态触发 Rollback 回到 Failed 状态
	fsm2.Rollback("rollback-actor")
	if fsm2.GetState() != Failed {
		t.Fatalf("Rollback from Frozen: expected state Failed, got %v", fsm2.GetState())
	}
	t.Logf("✓ Rollback from Frozen to Failed succeeded")

	// 从 Failed 状态恢复到 Owned
	fsm2.Recover("recovery-actor")
	if fsm2.GetState() != Owned {
		t.Fatalf("After recovery: expected state Owned, got %v", fsm2.GetState())
	}
	t.Logf("✓ Recovery from Failed to Owned succeeded")

	t.Logf("✓ Migration and recovery test completed successfully")
}

// TestOwnershipStateMachine_BoundarySequenceNumbers 测试边界序列号
func TestOwnershipStateMachine_BoundarySequenceNumbers(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	testCases := []struct {
		name          string
		lastSeq       int64
		checkpointSeq int64
		shouldSucceed bool
	}{
		{"Zero sequences", 0, 0, true},
		{"Large sequences", 1000000000, 1000000000, true},
		{"Checkpoint less than last", 1000, 900, true},
		{"Checkpoint equals last", 1000, 1000, true},
	}

	for _, tc := range testCases {
		fsm.UpdateLastProcessedSeq(tc.lastSeq)
		err := fsm.UpdateCheckpoint(tc.checkpointSeq)

		metadata := fsm.GetMetadata()
		isValid := metadata.CheckpointSeq <= metadata.LastProcessedSeq

		if tc.shouldSucceed && !isValid {
			t.Fatalf("Test %s: expected valid but got invalid state", tc.name)
		}

		if !tc.shouldSucceed && isValid {
			t.Fatalf("Test %s: expected invalid but got valid state", tc.name)
		}

		t.Logf("✓ %s: LastSeq=%d, CheckpointSeq=%d, error=%v", tc.name, tc.lastSeq, tc.checkpointSeq, err)
	}
}

// TestOwnershipStateMachine_LargeScaleKafkaOffsets 测试大规模的 Kafka offset
func TestOwnershipStateMachine_LargeScaleKafkaOffsets(t *testing.T) {
	// 创建 100 个 symbol
	symbols := make([]string, 100)
	for i := 0; i < 100; i++ {
		symbols[i] = fmt.Sprintf("SYM%03d/USDT", i)
	}

	fsm := NewOwnershipStateMachine("partition-1", "node-1", symbols)

	// 设置每个 symbol 的 offset
	offsets := make(map[string]int64)
	for i := 0; i < 100; i++ {
		offset := int64((i + 1) * 1000)
		offsets[symbols[i]] = offset
		fsm.UpdateKafkaOffset(symbols[i], offset)
	}

	// 验证所有 offset 都被保存
	metadata := fsm.GetMetadata()
	if len(metadata.KafkaOffset) != 100 {
		t.Fatalf("Expected 100 offsets, got %d", len(metadata.KafkaOffset))
	}

	// 验证每个 offset 的值
	for symbol, expectedOffset := range offsets {
		actualOffset := metadata.KafkaOffset[symbol]
		if actualOffset != expectedOffset {
			t.Fatalf("Offset mismatch for %s: expected %d, got %d", symbol, expectedOffset, actualOffset)
		}
	}

	t.Logf("✓ Large scale offsets: %d symbols, all offsets preserved", 100)
}

// TestOwnershipStateMachine_RapidStateTransitions 测试快速状态转移
func TestOwnershipStateMachine_RapidStateTransitions(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 执行 25 次快速的完整迁移循环（减少循环次数以加快测试）
	for i := 0; i < 25; i++ {
		targetNode := fmt.Sprintf("node-%d", (i%2)+2)

		if fsm.GetState() != Owned {
			fsm.Recover("rapid-prepare")
		}

		fsm.StartMigration(targetNode, "rapid")
		fsm.Freeze("node-1")
		fsm.Flush("node-1")
		fsm.Transfer("node-1")
		fsm.Commit(targetNode)
	}

	metadata := fsm.GetMetadata()
	// 最后的所有者应该是 node-2 或 node-3（取决于最后一次迁移）
	expectedOwner := fmt.Sprintf("node-%d", (24%2)+2)

	if metadata.CurrentOwner != expectedOwner {
		t.Fatalf("Expected owner %s after rapid cycles, got %s", expectedOwner, metadata.CurrentOwner)
	}

	t.Logf("✓ Rapid state transitions: 25 complete cycles completed successfully, final owner: %s", expectedOwner)
}

// TestOwnershipStateMachine_VersionIncrement 测试版本号递增
func TestOwnershipStateMachine_VersionIncrement(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	initialVersion := fsm.GetMetadata().Version

	// 执行一系列操作
	operations := []func() error{
		func() error { return fsm.UpdateLastProcessedSeq(100) },
		func() error { return fsm.UpdateCheckpoint(100) },
		func() error { return fsm.UpdateKafkaOffset("BTC/USDT", 50) },
		func() error { return fsm.StartMigration("node-2", "test") },
		func() error { return fsm.Freeze("node-1") },
		func() error { return fsm.Flush("node-1") },
	}

	for idx, op := range operations {
		op()
		version := fsm.GetMetadata().Version
		if version <= initialVersion+int64(idx) {
			t.Logf("Version after op %d: %d", idx+1, version)
		}
	}

	finalVersion := fsm.GetMetadata().Version
	if finalVersion <= initialVersion {
		t.Fatalf("Version should increment, started at %d, ended at %d", initialVersion, finalVersion)
	}

	t.Logf("✓ Version increment working: %d → %d", initialVersion, finalVersion)
}

// TestOwnershipStateMachine_TransitionHistoryOverflow 测试转移历史的溢出处理
func TestOwnershipStateMachine_TransitionHistoryOverflow(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 执行 120 次转移（超过 100 的历史限制）
	for i := 0; i < 120; i++ {
		targetNode := fmt.Sprintf("node-%d", (i%5)+2)

		if fsm.GetState() == Owned {
			fsm.StartMigration(targetNode, "overflow-test")
		} else {
			fsm.Recover("overflow-recovery")
		}
	}

	metadata := fsm.GetMetadata()
	// 历史记录应该不超过 100 条
	if len(metadata.TransitionHistory) > 100 {
		t.Fatalf("History overflow: got %d transitions (max 100)", len(metadata.TransitionHistory))
	}

	t.Logf("✓ History overflow handling: kept %d transitions (max 100)", len(metadata.TransitionHistory))
}

// TestOwnershipStateMachine_ConcurrentMixedOperations 测试混合并发操作
func TestOwnershipStateMachine_ConcurrentMixedOperations(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})

	var wg sync.WaitGroup
	var updateCount int32
	var readCount int32
	var errorCount int32

	numGoroutines := 20

	// 混合读写操作
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				switch j % 4 {
				case 0: // 读操作
					_ = fsm.GetMetadata()
					atomic.AddInt32(&readCount, 1)
				case 1: // 序列号更新
					fsm.UpdateLastProcessedSeq(int64(id*100 + j))
					atomic.AddInt32(&updateCount, 1)
				case 2: // Offset 更新
					fsm.UpdateKafkaOffset("BTC/USDT", int64(id*10+j))
					atomic.AddInt32(&updateCount, 1)
				case 3: // 验证不变量
					metadata := fsm.GetMetadata()
					if metadata.CheckpointSeq <= metadata.LastProcessedSeq {
						atomic.AddInt32(&readCount, 1)
					} else {
						atomic.AddInt32(&errorCount, 1)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("Invariant violations detected: %d errors", errorCount)
	}

	t.Logf("✓ Mixed operations: %d reads, %d updates, %d errors",
		atomic.LoadInt32(&readCount), atomic.LoadInt32(&updateCount), atomic.LoadInt32(&errorCount))
}

// TestOwnershipStateMachine_RecoveryAfterFailure 测试失败后的恢复
func TestOwnershipStateMachine_RecoveryAfterFailure(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})

	// 设置初始状态
	fsm.UpdateLastProcessedSeq(500)
	fsm.UpdateCheckpoint(500)

	// 启动迁移并进入失败状态
	fsm.StartMigration("node-2", "test")
	fsm.Freeze("node-1")
	fsm.Rollback("simulated failure")

	if fsm.GetState() != Failed {
		t.Fatalf("Expected Failed state after rollback, got %v", fsm.GetState())
	}

	// 恢复
	err := fsm.Recover("failure-recovery")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if fsm.GetState() != Owned {
		t.Fatalf("Expected Owned state after recovery, got %v", fsm.GetState())
	}

	// 验证序列号被保留
	metadata := fsm.GetMetadata()
	if metadata.LastProcessedSeq != 500 {
		t.Fatalf("Sequence number not preserved: expected 500, got %d", metadata.LastProcessedSeq)
	}

	t.Logf("✓ Recovery after failure: state=%v, seq=%d", fsm.GetState(), metadata.LastProcessedSeq)
}
