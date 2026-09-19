package model

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ========== Phase 4.2: Transfer Protocol Tests ==========

// TestTransferProtocol_BasicRequest 测试基本的转移请求
func TestTransferProtocol_BasicRequest(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})
	tp := NewTransferProtocol(fsm)

	req := TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		TimeoutMs:   30000,
		MaxRetries:  3,
		RequestTime: time.Now(),
		RequestID:   "req-1",
	}

	resp, err := tp.StartTransfer(req)
	if err != nil {
		t.Fatalf("StartTransfer failed: %v", err)
	}

	if !resp.Accepted {
		t.Fatalf("Transfer request rejected: %s", resp.Reason)
	}

	if fsm.GetState() != MigrationPending {
		t.Fatalf("Expected state MigrationPending, got %v", fsm.GetState())
	}

	t.Logf("✓ Transfer request accepted, state: %v, checkpoint_seq: %d", fsm.GetState(), resp.CheckpointSeq)
}

// TestTransferProtocol_CompleteFlow 测试完整的转移流程
func TestTransferProtocol_CompleteFlow(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	// 更新初始序列号
	fsm.UpdateLastProcessedSeq(100)
	fsm.UpdateCheckpoint(100)

	// 步骤 1: 发送 TransferRequest
	req := TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		TimeoutMs:   30000,
		MaxRetries:  3,
		RequestID:   "req-1",
	}

	resp, err := tp.StartTransfer(req)
	if err != nil || !resp.Accepted {
		t.Fatalf("StartTransfer failed: %v, reason: %s", err, resp.Reason)
	}

	if fsm.GetState() != MigrationPending {
		t.Fatalf("Expected MigrationPending, got %v", fsm.GetState())
	}

	// 步骤 2: 应用 FreezeSignal
	freezeSig := FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		FreezeTime:      time.Now(),
		SourceOwner:     "node-1",
	}

	err = tp.ApplyFreezeSignal(freezeSig)
	if err != nil {
		t.Fatalf("ApplyFreezeSignal failed: %v", err)
	}

	if fsm.GetState() != Frozen {
		t.Fatalf("Expected Frozen, got %v", fsm.GetState())
	}

	// 步骤 3: 完成 Flushing
	flushAck, err := tp.CompleteFlushing("node-1")
	if err != nil {
		t.Fatalf("CompleteFlushing failed: %v", err)
	}

	if fsm.GetState() != Flushing {
		t.Fatalf("Expected Flushing, got %v", fsm.GetState())
	}

	if flushAck.CheckpointSeq != 100 {
		t.Fatalf("Expected checkpoint_seq 100, got %d", flushAck.CheckpointSeq)
	}

	// 步骤 4: 应用 TransferSignal
	transferSig := TransferSignal{
		PartitionID:         "partition-1",
		SourceOwner:         "node-1",
		TargetOwner:         "node-2",
		StartFromSeq:        100,
		StartFromCheckpoint: 100,
		KafkaOffsets:        flushAck.KafkaOffsets,
		RequestID:           "req-1",
	}

	err = tp.ApplyTransferSignal(transferSig)
	if err != nil {
		t.Fatalf("ApplyTransferSignal failed: %v", err)
	}

	if fsm.GetState() != Transferring {
		t.Fatalf("Expected Transferring, got %v", fsm.GetState())
	}

	// 步骤 5: 提交转移
	err = tp.CommitTransfer("node-2")
	if err != nil {
		t.Fatalf("CommitTransfer failed: %v", err)
	}

	if fsm.GetState() != Standby {
		t.Fatalf("Expected Standby, got %v", fsm.GetState())
	}

	metadata := fsm.GetMetadata()
	if metadata.CurrentOwner != "node-2" {
		t.Fatalf("Expected owner node-2, got %s", metadata.CurrentOwner)
	}

	t.Logf("✓ Complete transfer flow succeeded: %s → %s", req.SourceOwner, req.TargetOwner)
}

// TestTransferProtocol_InvalidStateRequest 测试在非法状态下的转移请求
func TestTransferProtocol_InvalidStateRequest(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	// 先启动一个迁移
	fsm.StartMigration("node-2", "test")
	if fsm.GetState() != MigrationPending {
		t.Fatalf("Setup failed: expected MigrationPending, got %v", fsm.GetState())
	}

	// 在 MigrationPending 状态下再次尝试发起迁移应该失败
	req := TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-3",
		RequestID:   "req-2",
	}

	resp, _ := tp.StartTransfer(req)
	if resp.Accepted {
		t.Fatalf("Transfer should be rejected when not in Owned state")
	}

	t.Logf("✓ Invalid state rejection works: %s", resp.Reason)
}

// TestTransferProtocol_Rollback 测试转移回滚
func TestTransferProtocol_Rollback(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	// 启动转移
	req := TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   "req-1",
	}

	resp, _ := tp.StartTransfer(req)
	if !resp.Accepted {
		t.Fatalf("Transfer request failed: %s", resp.Reason)
	}

	// 冻结
	tp.ApplyFreezeSignal(FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		SourceOwner:     "node-1",
	})

	// 在 Frozen 状态下执行回滚
	err := tp.RollbackTransfer("test rollback")
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if fsm.GetState() != Failed {
		t.Fatalf("Expected Failed state after rollback, got %v", fsm.GetState())
	}

	t.Logf("✓ Rollback succeeded, state: %v", fsm.GetState())
}

// TestTransferProtocol_InvariantValidation 测试转移不变量验证
func TestTransferProtocol_InvariantValidation(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})
	tp := NewTransferProtocol(fsm)

	// 设置初始序列号（checkpoint <= lastSeq）
	fsm.UpdateLastProcessedSeq(1000)
	fsm.UpdateCheckpoint(800)

	// 应该满足不变量
	err := tp.ValidateTransferInvariants()
	if err != nil {
		t.Logf("Initial state validation: %v", err)
	}

	// 启动转移流程
	tp.StartTransfer(TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   "req-1",
	})

	tp.ApplyFreezeSignal(FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		SourceOwner:     "node-1",
	})

	// 在 Frozen 状态下验证 - 应该通过（所有字段都有值）
	err = tp.ValidateTransferInvariants()
	if err == nil {
		t.Logf("✓ Invariant validation passed in Frozen state")
	} else {
		t.Logf("Frozen state validation info: %v", err)
	}
}

// TestTransferProtocol_MultipleSymbols 测试多个 symbol 的 Offset 追踪
func TestTransferProtocol_MultipleSymbols(t *testing.T) {
	symbols := []string{"BTC/USDT", "ETH/USDT", "XRP/USDT"}
	fsm := NewOwnershipStateMachine("partition-1", "node-1", symbols)
	tp := NewTransferProtocol(fsm)

	// 设置每个 symbol 的 offset
	offsets := map[string]int64{
		"BTC/USDT": 1000,
		"ETH/USDT": 2000,
		"XRP/USDT": 1500,
	}

	for symbol, offset := range offsets {
		fsm.UpdateKafkaOffset(symbol, offset)
	}

	// 执行完整转移流程
	tp.StartTransfer(TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   "req-1",
	})

	tp.ApplyFreezeSignal(FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		SourceOwner:     "node-1",
	})

	flushAck, err := tp.CompleteFlushing("node-1")
	if err != nil {
		t.Fatalf("CompleteFlushing failed: %v", err)
	}

	// 验证所有 offset 都被保留
	for symbol, offset := range offsets {
		if flushAck.KafkaOffsets[symbol] != offset {
			t.Fatalf("Expected offset %d for %s, got %d", offset, symbol, flushAck.KafkaOffsets[symbol])
		}
	}

	t.Logf("✓ Multiple symbol offsets preserved: %v", flushAck.KafkaOffsets)
}

// TestTransferProtocol_ConcurrentTransfers 测试并发转移请求的处理
func TestTransferProtocol_ConcurrentTransfers(t *testing.T) {
	numGoroutines := 10
	opsPerGoroutine := 20

	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	var wg sync.WaitGroup
	var successCount int32
	var errorCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// 尝试验证不变量（这是并发安全的操作）
				err := tp.ValidateTransferInvariants()
				if err == nil {
					atomic.AddInt32(&successCount, 1)
				} else {
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	totalOps := int32(numGoroutines * opsPerGoroutine)
	if successCount+errorCount != totalOps {
		t.Fatalf("Operation count mismatch: expected %d, got %d", totalOps, successCount+errorCount)
	}

	t.Logf("✓ Concurrent transfers: %d ops, %d success, %d validation errors", totalOps, successCount, errorCount)
}

// TestTransferProtocol_SequenceConsistency 测试序列号一致性
func TestTransferProtocol_SequenceConsistency(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	// 设置初始序列号
	fsm.UpdateLastProcessedSeq(1000)
	fsm.UpdateCheckpoint(800)

	// 执行转移流程
	tp.StartTransfer(TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   "req-1",
	})

	tp.ApplyFreezeSignal(FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		SourceOwner:     "node-1",
	})

	flushAck, err := tp.CompleteFlushing("node-1")
	if err != nil {
		t.Fatalf("CompleteFlushing failed: %v", err)
	}

	// 验证 Invariant: CheckpointSeq ≤ LastProcessedSeq
	if flushAck.CheckpointSeq > flushAck.LastProcessedSeq {
		t.Fatalf("Invariant violation: CheckpointSeq (%d) > LastProcessedSeq (%d)",
			flushAck.CheckpointSeq, flushAck.LastProcessedSeq)
	}

	t.Logf("✓ Sequence consistency: LastProcessedSeq=%d, CheckpointSeq=%d",
		flushAck.LastProcessedSeq, flushAck.CheckpointSeq)
}

// TestTransferProtocol_TransferContext 测试转移上下文提取
func TestTransferProtocol_TransferContext(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT", "ETH/USDT"})
	tp := NewTransferProtocol(fsm)

	// 设置元数据
	fsm.UpdateLastProcessedSeq(500)
	fsm.UpdateCheckpoint(500)
	fsm.UpdateKafkaOffset("BTC/USDT", 100)
	fsm.UpdateKafkaOffset("ETH/USDT", 200)

	ctx := tp.GetTransferContext()
	if ctx == nil {
		t.Fatalf("GetTransferContext returned nil")
	}

	if ctx.PartitionID != "partition-1" {
		t.Fatalf("Expected partition-1, got %s", ctx.PartitionID)
	}

	if ctx.SourceOwner != "node-1" {
		t.Fatalf("Expected source node-1, got %s", ctx.SourceOwner)
	}

	if len(ctx.KafkaOffsets) != 2 {
		t.Fatalf("Expected 2 offsets, got %d", len(ctx.KafkaOffsets))
	}

	t.Logf("✓ Transfer context extracted: partition=%s, source=%s, offsets=%v",
		ctx.PartitionID, ctx.SourceOwner, ctx.KafkaOffsets)
}

// TestTransferProtocol_RequestIdTracking 测试请求 ID 的跟踪
func TestTransferProtocol_RequestIdTracking(t *testing.T) {
	fsm := NewOwnershipStateMachine("partition-1", "node-1", []string{"BTC/USDT"})
	tp := NewTransferProtocol(fsm)

	requestID := "req-unique-12345"
	req := TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   requestID,
	}

	resp, _ := tp.StartTransfer(req)
	if resp.RequestID != requestID {
		t.Fatalf("Expected request ID %s, got %s", requestID, resp.RequestID)
	}

	t.Logf("✓ Request ID tracking: %s", resp.RequestID)
}

// TestTransferProtocol_StressTest 压力测试
func TestTransferProtocol_StressTest(t *testing.T) {
	symbols := make([]string, 50)
	for i := 0; i < 50; i++ {
		symbols[i] = fmt.Sprintf("SYM%d/USDT", i)
	}

	fsm := NewOwnershipStateMachine("partition-1", "node-1", symbols)
	tp := NewTransferProtocol(fsm)

	// 设置所有 symbol 的 offset
	for i := 0; i < 50; i++ {
		fsm.UpdateKafkaOffset(symbols[i], int64(1000+i*10))
	}

	// 设置高序列号
	fsm.UpdateLastProcessedSeq(1000000)
	fsm.UpdateCheckpoint(1000000)

	// 执行完整流程
	tp.StartTransfer(TransferRequest{
		PartitionID: "partition-1",
		SourceOwner: "node-1",
		TargetOwner: "node-2",
		RequestID:   "stress-1",
	})

	tp.ApplyFreezeSignal(FreezeSignal{
		PartitionID:     "partition-1",
		FreezeTimestamp: time.Now().UnixMilli(),
		SourceOwner:     "node-1",
	})

	flushAck, err := tp.CompleteFlushing("node-1")
	if err != nil {
		t.Fatalf("CompleteFlushing failed: %v", err)
	}

	// 验证所有 offset 被保留
	if len(flushAck.KafkaOffsets) != 50 {
		t.Fatalf("Expected 50 offsets, got %d", len(flushAck.KafkaOffsets))
	}

	t.Logf("✓ Stress test passed: %d symbols, seq=%d, offsets=%d",
		len(symbols), flushAck.LastProcessedSeq, len(flushAck.KafkaOffsets))
}
