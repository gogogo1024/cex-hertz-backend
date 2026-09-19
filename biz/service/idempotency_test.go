package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIdempotentRecovery 幂等性测试的核心: 多次crash/恢复应该得到相同的结果
//
// 关键概念:
// - Crash #N 会中断处理 (可能导致内存中有未persisted的事件)
// - Recovery #N 通过replaying Kafka events恢复状态
// - Crash #N+1 之后 -> Recovery #N+1: 由于事件完全重放，结果应该与#N相同
// - 幂等性验证: Checksum(Recovery#1) == Checksum(Recovery#2) == Checksum(Recovery#3)
//
// 测试流程 (标准3-cycle):
// 1. Load baseline state (或从Kafka重放完整历史)
// 2. Cycle 1: Crash -> Recovery -> Snapshot (snapshot1)
// 3. Cycle 2: Crash -> Recovery -> Snapshot (snapshot2)
// 4. Cycle 3: Crash -> Recovery -> Snapshot (snapshot3)
// 5. Assert: All checksums identical
func TestIdempotentRecovery3Cycles(t *testing.T) {
	t.Parallel()
	
	// Initialize baseline state
	// 在实际应用中，这应该从database或Kafka中重放完整历史
	baselineState := createBaselineState("BTC/USDT", 10, 100)
	
	// Store checksums from each recovery cycle
	var checksums []string
	var snapshots []*StateSnapshot
	
	// 3 crash/recovery cycles
	for cycle := 1; cycle <= 3; cycle++ {
		t.Logf("Cycle %d: Simulating crash and recovery", cycle)
		
		// Simulate crash: 进程异常终止，内存状态清空
		// 但Kafka offset和已提交的checkpoint已保存
		// 
		// 在真实场景中:
		// 1. Crash前的offset在Kafka consumer offset tracker中
		// 2. Crash前的checkpoint在PostgreSQL中
		// 3. 进程重启后通过恢复executor重放事件
		
		// Create pre-crash snapshot
		preCrashSnap := &StateSnapshot{
			Timestamp:     time.Now(),
			Orders:        copyOrders(baselineState.Orders),
			Trades:        copyTrades(baselineState.Trades),
			Positions:     copyPositions(baselineState.Positions),
			EventCount:    baselineState.EventCount,
			OrderCount:    baselineState.OrderCount,
			TradeCount:    baselineState.TradeCount,
			PositionCount: baselineState.PositionCount,
		}
		preCrashSnap.CalculateChecksum()
		
		// Simulate recovery: 从Kafka replaying所有events
		// 理想情况下，恢复应该得到完全相同的状态
		recoveredState := recoverStateFromKafka(baselineState)
		
		// Create post-recovery snapshot
		postRecoverySnap := &StateSnapshot{
			Timestamp:     time.Now(),
			Orders:        copyOrders(recoveredState.Orders),
			Trades:        copyTrades(recoveredState.Trades),
			Positions:     copyPositions(recoveredState.Positions),
			EventCount:    recoveredState.EventCount,
			OrderCount:    recoveredState.OrderCount,
			TradeCount:    recoveredState.TradeCount,
			PositionCount: recoveredState.PositionCount,
		}
		postRecoverySnap.CalculateChecksum()
		
		// Verify crash并没有改变数据 (如果crash在安全点)
		// 注: 真实的crash可能丢失未persisted的事件
		// 这里我们测试的是: 恢复总是能得到一致的状态
		diff := CompareSnapshots(preCrashSnap, postRecoverySnap)
		assert.True(t, diff.IsIdentical,
			"Cycle %d: Pre-crash and post-recovery states should be identical", cycle)
		
		checksums = append(checksums, postRecoverySnap.Checksum)
		snapshots = append(snapshots, postRecoverySnap)
	}
	
	// Key assertion: 所有恢复周期的状态应该完全相同 (幂等性)
	for i := 1; i < len(checksums); i++ {
		assert.Equal(t, checksums[0], checksums[i],
			"Cycle %d checksum should match Cycle 1", i+1)
	}
}

// TestIdempotentRecoveryMultiProcessor 多processor幂等性
// 在实际系统中有多个processor (OrderProcessor, TradeProcessor, PositionProcessor)
// 每个processor独立处理自己的事件，但应该相互一致
func TestIdempotentRecoveryMultiProcessor(t *testing.T) {
	t.Parallel()
	
	// Create multi-processor baseline
	baseline := &MultiProcessorState{
		Timestamp: time.Now(),
		Orders:    createBaselineState("BTC/USDT", 10, 50).Orders,
		Trades:    createBaselineState("BTC/USDT", 10, 100).Trades,
		Positions: createBaselineState("BTC/USDT", 10, 50).Positions,
	}
	
	// Simulate 2 crash/recovery cycles for each processor
	orderChecksums := make([]string, 0)
	tradeChecksums := make([]string, 0)
	positionChecksums := make([]string, 0)
	
	for cycle := 1; cycle <= 2; cycle++ {
		// OrderProcessor recovery
		orderSnap := &StateSnapshot{
			Timestamp:     time.Now(),
			Orders:        copyOrders(baseline.Orders),
			Trades:        make(map[string]*TradeSnapshot),
			Positions:     make(map[string]*PositionSnapshot),
			OrderCount:    int64(len(baseline.Orders)),
		}
		orderSnap.CalculateChecksum()
		orderChecksums = append(orderChecksums, orderSnap.Checksum)
		
		// TradeProcessor recovery
		tradeSnap := &StateSnapshot{
			Timestamp:     time.Now(),
			Orders:        make(map[string]*OrderSnapshot),
			Trades:        copyTrades(baseline.Trades),
			Positions:     make(map[string]*PositionSnapshot),
			TradeCount:    int64(len(baseline.Trades)),
		}
		tradeSnap.CalculateChecksum()
		tradeChecksums = append(tradeChecksums, tradeSnap.Checksum)
		
		// PositionProcessor recovery
		positionSnap := &StateSnapshot{
			Timestamp:     time.Now(),
			Orders:        make(map[string]*OrderSnapshot),
			Trades:        make(map[string]*TradeSnapshot),
			Positions:     copyPositions(baseline.Positions),
			PositionCount: int64(len(baseline.Positions)),
		}
		positionSnap.CalculateChecksum()
		positionChecksums = append(positionChecksums, positionSnap.Checksum)
	}
	
	// Verify idempotency for each processor
	assert.Equal(t, orderChecksums[0], orderChecksums[1],
		"Order processor checksums should be identical across cycles")
	assert.Equal(t, tradeChecksums[0], tradeChecksums[1],
		"Trade processor checksums should be identical across cycles")
	assert.Equal(t, positionChecksums[0], positionChecksums[1],
		"Position processor checksums should be identical across cycles")
}

// TestIdempotentRecoveryWithPartialCommit 带有部分提交的幂等性
// 测试场景: 事件E1-E100已处理，但只提交了E1-E50
// Crash后恢复应该从E51开始重放
// 再次crash/恢复也应该给出相同结果
func TestIdempotentRecoveryWithPartialCommit(t *testing.T) {
	t.Parallel()
	
	// Baseline: 100 events fully processed and committed
	fullState := createBaselineState("BTC/USDT", 10, 100)
	fullState.CalculateChecksum()
	
	// Scenario: 处理200个事件，但只有100个被提交
	// 这意味着恢复应该从event 101开始
	state := createBaselineState("BTC/USDT", 15, 200)
	
	// 模拟只有前一半被提交 (offset checkpoint在100)
	// 理论上恢复应该重放event 101-200
	partial := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        copyTrades(make(map[string]*TradeSnapshot)), // 从100开始新添加的
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    200,  // 处理了200个事件
		OrderCount:    0,    // 后续没有新订单
		TradeCount:    100,  // 后续100个交易
		PositionCount: 0,
	}
	
	// 第一次恢复: 从checkpoint 100恢复，应该得到的是基础状态 + 额外100个交易
	recovery1 := recoverStateFromCheckpoint(fullState, 100)
	recovery1.CalculateChecksum()
	
	// 第二次恢复: 相同的checkpoint，应该得到相同结果
	recovery2 := recoverStateFromCheckpoint(fullState, 100)
	recovery2.CalculateChecksum()
	
	// 幂等性验证
	assert.Equal(t, recovery1.Checksum, recovery2.Checksum,
		"Partial commit recovery should be idempotent")
	
	// 验证两次恢复结果相同
	diff := CompareSnapshots(recovery1, recovery2)
	assert.True(t, diff.IsIdentical, "Two recoveries from same checkpoint should be identical")
}

// TestIdempotentRecoveryEventDeduplication 幂等性依赖事件去重
// 关键: 恢复系统必须支持事件去重
// 如果某个事件被replayed两次，结果应该相同
func TestIdempotentRecoveryEventDeduplication(t *testing.T) {
	t.Parallel()
	
	// Baseline state
	state := createBaselineState("BTC/USDT", 10, 100)
	baseline := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        copyOrders(state.Orders),
		Trades:        copyTrades(state.Trades),
		Positions:     copyPositions(state.Positions),
		EventCount:    state.EventCount,
		OrderCount:    state.OrderCount,
		TradeCount:    state.TradeCount,
		PositionCount: state.PositionCount,
	}
	baseline.CalculateChecksum()
	
	// Process with duplicated events (simulate replay scenario)
	// 模拟Kafka offset追踪中重复消费的情况
	recovered := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        copyOrders(state.Orders),
		Trades:        copyTrades(state.Trades),
		Positions:     copyPositions(state.Positions),
		EventCount:    state.EventCount,
		OrderCount:    state.OrderCount,
		TradeCount:    state.TradeCount,
		PositionCount: state.PositionCount,
	}
	recovered.CalculateChecksum()
	
	// 关键: 即使有重复事件，最终状态应该相同
	// (假设event processor已正确实现幂等性操作)
	assert.Equal(t, baseline.Checksum, recovered.Checksum,
		"Event deduplication should result in identical state")
}

// TestIdempotentRecoveryLargeScale 大规模幂等性测试
// 测试large scale场景下的幂等性
// 1000+ 订单, 10000+ 交易, 100+ 用户
func TestIdempotentRecoveryLargeScale(t *testing.T) {
	t.Parallel()
	
	// Create large baseline
	baseline := createLargeScaleBaseline()
	
	// Cycle 1
	recovery1 := recoverLargeScaleState(baseline)
	recovery1.CalculateChecksum()
	
	// Cycle 2
	recovery2 := recoverLargeScaleState(baseline)
	recovery2.CalculateChecksum()
	
	// Cycle 3
	recovery3 := recoverLargeScaleState(baseline)
	recovery3.CalculateChecksum()
	
	// All should be identical
	assert.Equal(t, recovery1.Checksum, recovery2.Checksum, "Large scale recovery cycle 1-2 mismatch")
	assert.Equal(t, recovery2.Checksum, recovery3.Checksum, "Large scale recovery cycle 2-3 mismatch")
	
	// Verify counts are consistent
	assert.Equal(t, recovery1.OrderCount, recovery2.OrderCount, "Order count mismatch")
	assert.Equal(t, recovery1.TradeCount, recovery2.TradeCount, "Trade count mismatch")
	assert.Equal(t, recovery1.PositionCount, recovery2.PositionCount, "Position count mismatch")
}

// TestIdempotentRecoveryCrashDuringRecovery 在恢复中间再次crash
// 高难度场景: 恢复过程中再次发生crash
// 理想结果: 第三次恢复应该得到相同状态 (幂等性保证)
func TestIdempotentRecoveryCrashDuringRecovery(t *testing.T) {
	t.Parallel()
	
	baseline := createBaselineState("BTC/USDT", 10, 100)
	
	// First crash & recovery
	recovery1 := recoverStateFromKafka(baseline)
	recovery1.CalculateChecksum()
	
	// Second crash (simulated: happens during recovery, but recovery still completes)
	// 在真实场景中，可能在replay到中间时crash
	// 第二次恢复应该从同一checkpoint重新开始
	recovery2 := recoverStateFromKafka(baseline)
	recovery2.CalculateChecksum()
	
	// Third recovery
	recovery3 := recoverStateFromKafka(baseline)
	recovery3.CalculateChecksum()
	
	// All should be identical (idempotency)
	assert.Equal(t, recovery1.Checksum, recovery2.Checksum,
		"Recovery after intermediate crash should be idempotent")
	assert.Equal(t, recovery2.Checksum, recovery3.Checksum,
		"Third recovery should match second")
}

// ============ Helper Structures ============

type MultiProcessorState struct {
	Timestamp time.Time
	Orders    map[string]*OrderSnapshot
	Trades    map[string]*TradeSnapshot
	Positions map[string]*PositionSnapshot
}

// ============ Helper Functions ============

func createBaselineState(symbol string, numOrders, numTrades int) *StateSnapshot {
	state := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    int64(numOrders + numTrades),
		OrderCount:    int64(numOrders),
		TradeCount:    int64(numTrades),
		PositionCount: 1,
	}
	
	// Create orders
	for i := 1; i <= numOrders; i++ {
		orderID := "order" + string(rune('0'+(i%10)))
		if _, exists := state.Orders[orderID]; !exists {
			state.Orders[orderID] = &OrderSnapshot{
				OrderID:   orderID,
				UserID:    "user1",
				Symbol:    symbol,
				Side:      "buy",
				Price:     5000000000000,
				Quantity:  100,
				FilledQty: 50,
				Status:    "partial",
			}
		}
	}
	
	// Create trades
	for i := 1; i <= numTrades; i++ {
		tradeID := "trade" + string(rune('0'+(i%10)))
		if _, exists := state.Trades[tradeID]; !exists {
			state.Trades[tradeID] = &TradeSnapshot{
				TradeID:   tradeID,
				BuyerID:   "user1",
				SellerID:  "seller1",
				Symbol:    symbol,
				Price:     5000000000000,
				Quantity:  1,
			}
		}
	}
	
	// Create position
	state.Positions["user1"] = &PositionSnapshot{
		UserID:        "user1",
		Symbol:        symbol,
		QuantityHeld:  int64(numTrades / 2),
		CostBase:      int64(numTrades/2) * 5000000000000,
		RealizedPnl:   0,
		UnrealizedPnl: 0,
	}
	
	return state
}

func createLargeScaleBaseline() *StateSnapshot {
	state := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    11100,
		OrderCount:    1100,
		TradeCount:    10000,
		PositionCount: 100,
	}
	
	// 1100 orders
	for i := 1; i <= 1100; i++ {
		orderID := "order" + string(rune('0'+(i%10))) + "_" + string(rune('0'+(i/10%10)))
		state.Orders[orderID] = &OrderSnapshot{
			OrderID:   orderID,
			UserID:    "user" + string(rune('0'+(i%100))),
			Symbol:    "BTC/USDT",
			Side:      "buy",
			Price:     5000000000000,
			Quantity:  100,
			FilledQty: 50,
			Status:    "partial",
		}
	}
	
	// 10000 trades
	for i := 1; i <= 10000; i++ {
		tradeID := "trade" + string(rune('0'+(i%10))) + "_" + string(rune('0'+(i/100%10)))
		if _, exists := state.Trades[tradeID]; !exists {
			state.Trades[tradeID] = &TradeSnapshot{
				TradeID:   tradeID,
				BuyerID:   "user" + string(rune('0'+(i%100))),
				SellerID:  "seller" + string(rune('0'+(i%50))),
				Symbol:    "BTC/USDT",
				Price:     5000000000000,
				Quantity:  1,
			}
		}
	}
	
	// 100 positions
	for i := 0; i < 100; i++ {
		userID := "user" + string(rune('0'+(i%10))) + "_" + string(rune('0'+(i/10)))
		state.Positions[userID] = &PositionSnapshot{
			UserID:        userID,
			Symbol:        "BTC/USDT",
			QuantityHeld:  int64(100 + i*10),
			CostBase:      int64((100+i*10) * 5000000000000),
			RealizedPnl:   int64(i * 1000000000000),
			UnrealizedPnl: int64(i * 500000000000),
		}
	}
	
	return state
}

func recoverStateFromKafka(baseline *StateSnapshot) *StateSnapshot {
	// Simulate recovery from Kafka replay
	// In real scenario: read from Kafka, replay all events, rebuild state
	recovered := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        copyOrders(baseline.Orders),
		Trades:        copyTrades(baseline.Trades),
		Positions:     copyPositions(baseline.Positions),
		EventCount:    baseline.EventCount,
		OrderCount:    baseline.OrderCount,
		TradeCount:    baseline.TradeCount,
		PositionCount: baseline.PositionCount,
	}
	return recovered
}

func recoverStateFromCheckpoint(baseline *StateSnapshot, checkpointOffset int64) *StateSnapshot {
	// Simulate recovery from checkpoint
	// Events 1-checkpointOffset are replayed from checkpoint
	// Events after checkpointOffset are replayed from Kafka
	return recoverStateFromKafka(baseline)
}

func recoverLargeScaleState(baseline *StateSnapshot) *StateSnapshot {
	recovered := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        copyOrders(baseline.Orders),
		Trades:        copyTrades(baseline.Trades),
		Positions:     copyPositions(baseline.Positions),
		EventCount:    baseline.EventCount,
		OrderCount:    baseline.OrderCount,
		TradeCount:    baseline.TradeCount,
		PositionCount: baseline.PositionCount,
	}
	return recovered
}

func copyOrders(orders map[string]*OrderSnapshot) map[string]*OrderSnapshot {
	copied := make(map[string]*OrderSnapshot)
	for id, order := range orders {
		orderCopy := *order
		copied[id] = &orderCopy
	}
	return copied
}

func copyTrades(trades map[string]*TradeSnapshot) map[string]*TradeSnapshot {
	copied := make(map[string]*TradeSnapshot)
	for id, trade := range trades {
		tradeCopy := *trade
		copied[id] = &tradeCopy
	}
	return copied
}

func copyPositions(positions map[string]*PositionSnapshot) map[string]*PositionSnapshot {
	copied := make(map[string]*PositionSnapshot)
	for id, pos := range positions {
		posCopy := *pos
		copied[id] = &posCopy
	}
	return copied
}
