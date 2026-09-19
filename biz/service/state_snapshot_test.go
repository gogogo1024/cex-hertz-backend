package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStateSnapshotChecksum 验证快照的checksum计算正确性
func TestStateSnapshotChecksum(t *testing.T) {
	t.Parallel()
	
	// Create snapshot 1
	snap1 := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    100,
		OrderCount:    10,
		TradeCount:    50,
		PositionCount: 5,
	}
	
	snap1.Orders["order1"] = &OrderSnapshot{
		OrderID:   "order1",
		UserID:    "user1",
		Symbol:    "BTC/USDT",
		Side:      "buy",
		Price:     5000000000000,  // 50000 * 10^8
		Quantity:  100,
		FilledQty: 50,
		Status:    "partial",
	}
	
	snap1.Trades["trade1"] = &TradeSnapshot{
		TradeID:   "trade1",
		BuyerID:   "user1",
		SellerID:  "user2",
		Symbol:    "BTC/USDT",
		Price:     5000000000000,
		Quantity:  50,
	}
	
	snap1.Positions["user1"] = &PositionSnapshot{
		UserID:        "user1",
		Symbol:        "BTC/USDT",
		QuantityHeld:  50,
		CostBase:      250000000000000,
		RealizedPnl:   0,
		UnrealizedPnl: 0,
	}
	
	checksum1a := snap1.CalculateChecksum()
	checksum1b := snap1.CalculateChecksum()
	
	assert.Equal(t, checksum1a, checksum1b, "Checksum should be deterministic")
}

// TestCompareSnapshotsIdentical 测试完全相同的快照
func TestCompareSnapshotsIdentical(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	diff := CompareSnapshots(pre, post)
	
	assert.True(t, diff.IsIdentical, "Identical snapshots should be marked as identical")
	assert.False(t, diff.HasDifferences(), "Should have no differences")
}

// TestCompareSnapshotsOrderChange 测试订单变化检测
func TestCompareSnapshotsOrderChange(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Modify an order in post-snapshot
	post.Orders["order1"].FilledQty = 100
	post.Orders["order1"].Status = "filled"
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Modified snapshots should not be identical")
	assert.NotEmpty(t, diff.ChangedOrders, "Should detect changed orders")
}

// TestCompareSnapshotsMissingOrder 测试缺失订单检测
func TestCompareSnapshotsMissingOrder(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Remove an order from post-snapshot
	delete(post.Orders, "order1")
	post.OrderCount--
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Snapshots should not be identical")
	assert.NotEmpty(t, diff.MissingOrders, "Should detect missing orders")
	assert.Contains(t, diff.MissingOrders, "order1", "Should report missing order1")
}

// TestCompareSnapshotsExtraOrder 测试多余订单检测
func TestCompareSnapshotsExtraOrder(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Add an extra order in post-snapshot
	post.Orders["order2"] = &OrderSnapshot{
		OrderID:   "order2",
		UserID:    "user2",
		Symbol:    "ETH/USDT",
		Side:      "sell",
		Price:     300000000000,
		Quantity:  200,
		FilledQty: 0,
		Status:    "pending",
	}
	post.OrderCount++
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Snapshots should not be identical")
	assert.NotEmpty(t, diff.ExtraOrders, "Should detect extra orders")
	assert.Contains(t, diff.ExtraOrders, "order2", "Should report extra order2")
}

// TestCompareSnapshotsMissingTrade 测试缺失交易检测
func TestCompareSnapshotsMissingTrade(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Remove a trade from post-snapshot
	delete(post.Trades, "trade1")
	post.TradeCount--
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Snapshots should not be identical")
	assert.NotEmpty(t, diff.MissingTrades, "Should detect missing trades")
}

// TestCompareSnapshotsPositionChange 测试持仓变化检测
func TestCompareSnapshotsPositionChange(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Modify position in post-snapshot
	post.Positions["user1"].QuantityHeld = 75
	post.Positions["user1"].RealizedPnl = 5000000000000
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Snapshots should not be identical")
	assert.NotEmpty(t, diff.ChangedPositions, "Should detect changed positions")
}

// TestCompareSnapshotsOrderBookChange 测试订单簿变化检测
func TestCompareSnapshotsOrderBookChange(t *testing.T) {
	t.Parallel()
	
	pre := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		OrderBook: &OrderBookSnapshot{
			Symbol:         "BTC/USDT",
			LastTradePrice: 5000000000000,
			BestBid:        4999000000000,
			BestAsk:        5001000000000,
			BidDepth:       1000,
			AskDepth:       1000,
			Spread:         2000000000,
		},
	}
	
	post := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		OrderBook: &OrderBookSnapshot{
			Symbol:         "BTC/USDT",
			LastTradePrice: 5000000000000,
			BestBid:        4998000000000,  // Changed
			BestAsk:        5002000000000,  // Changed
			BidDepth:       900,             // Changed
			AskDepth:       1100,            // Changed
			Spread:         4000000000,      // Changed
		},
	}
	
	diff := CompareSnapshots(pre, post)
	
	assert.False(t, diff.IsIdentical, "Snapshots should not be identical")
	assert.NotNil(t, diff.OrderBookDiff, "Should detect OrderBook differences")
	assert.True(t, diff.OrderBookDiff.BestBidMismatch, "Should detect BestBid mismatch")
	assert.True(t, diff.OrderBookDiff.BestAskMismatch, "Should detect BestAsk mismatch")
}

// TestGenerateReport 测试报告生成
func TestGenerateReport(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	post := createTestSnapshot("user1", 50, 10)
	
	// Introduce some differences
	post.Orders["order1"].FilledQty = 75
	delete(post.Trades, "trade1")
	post.TradeCount--
	
	diff := CompareSnapshots(pre, post)
	report := diff.GenerateReport()
	
	assert.NotEmpty(t, report, "Report should not be empty")
	assert.Contains(t, report, "State Snapshot Comparison Report", "Report should have header")
	assert.Contains(t, report, "Changed Orders", "Report should mention changed orders")
	assert.Contains(t, report, "Missing Trades", "Report should mention missing trades")
	
	t.Logf("Generated Report:\n%s", report)
}

// TestCrashRecoveryScenario1 crash前后快照对比
func TestCrashRecoveryScenario1(t *testing.T) {
	t.Parallel()
	
	// Simulate state before crash
	preCrash := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    1500,
		OrderCount:    5,
		TradeCount:    50,
		PositionCount: 2,
	}
	
	// 添加5个订单
	for i := 1; i <= 5; i++ {
		orderID := "order" + string(rune('0'+i))
		preCrash.Orders[orderID] = &OrderSnapshot{
			OrderID:   orderID,
			UserID:    "user1",
			Symbol:    "BTC/USDT",
			Side:      "buy",
			Price:     5000000000000,
			Quantity:  100,
			FilledQty: 50,
			Status:    "partial",
		}
	}
	
	// 添加50个交易
	for i := 1; i <= 50; i++ {
		tradeID := "trade" + string(rune('0'+(i%10)))
		if _, exists := preCrash.Trades[tradeID]; !exists {
			preCrash.Trades[tradeID] = &TradeSnapshot{
				TradeID:   tradeID,
				BuyerID:   "user1",
				SellerID:  "seller1",
				Symbol:    "BTC/USDT",
				Price:     5000000000000,
				Quantity:  10,
			}
		}
	}
	
	preCrash.Positions["user1"] = &PositionSnapshot{
		UserID:        "user1",
		Symbol:        "BTC/USDT",
		QuantityHeld:  250,
		CostBase:      1250000000000000,
		RealizedPnl:   0,
		UnrealizedPnl: 0,
	}
	
	preCrash.CalculateChecksum()
	
	// Simulate recovery - 状态应该完全恢复
	postRecovery := &StateSnapshot{
		Timestamp:     time.Now().Add(10 * time.Second),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    1500,
		OrderCount:    5,
		TradeCount:    50,
		PositionCount: 2,
	}
	
	// Copy orders
	for id, order := range preCrash.Orders {
		postRecovery.Orders[id] = &OrderSnapshot{
			OrderID:   order.OrderID,
			UserID:    order.UserID,
			Symbol:    order.Symbol,
			Side:      order.Side,
			Price:     order.Price,
			Quantity:  order.Quantity,
			FilledQty: order.FilledQty,
			Status:    order.Status,
		}
	}
	
	// Copy trades
	for id, trade := range preCrash.Trades {
		postRecovery.Trades[id] = &TradeSnapshot{
			TradeID:   trade.TradeID,
			BuyerID:   trade.BuyerID,
			SellerID:  trade.SellerID,
			Symbol:    trade.Symbol,
			Price:     trade.Price,
			Quantity:  trade.Quantity,
		}
	}
	
	// Copy positions
	for userID, pos := range preCrash.Positions {
		postRecovery.Positions[userID] = &PositionSnapshot{
			UserID:        pos.UserID,
			Symbol:        pos.Symbol,
			QuantityHeld:  pos.QuantityHeld,
			CostBase:      pos.CostBase,
			RealizedPnl:   pos.RealizedPnl,
			UnrealizedPnl: pos.UnrealizedPnl,
		}
	}
	
	postRecovery.CalculateChecksum()
	
	// Compare
	diff := CompareSnapshots(preCrash, postRecovery)
	
	// Expected: Completely identical after recovery
	assert.True(t, diff.IsIdentical, "Post-recovery state should be identical to pre-crash")
	assert.Equal(t, preCrash.Checksum, postRecovery.Checksum,
		"Checksums must match exactly after recovery")
	assert.False(t, diff.HasDifferences(), "Should have no differences after recovery")
}

// TestCrashRecoveryScenario2 不完整恢复 - 部分数据丢失
func TestCrashRecoveryScenario2(t *testing.T) {
	t.Parallel()
	
	pre := createTestSnapshot("user1", 50, 10)
	pre.CalculateChecksum()
	
	post := createTestSnapshot("user1", 50, 10)
	
	// Simulate partial data loss: some trades lost during recovery
	delete(post.Trades, "trade1")
	delete(post.Trades, "trade2")
	delete(post.Trades, "trade3")
	post.TradeCount = 7  // Originally 10
	
	post.CalculateChecksum()
	
	diff := CompareSnapshots(pre, post)
	
	// Expected: Differences detected
	assert.False(t, diff.IsIdentical, "Partial recovery should not match")
	assert.NotNil(t, diff.TradeCountMismatch, "Should detect trade count mismatch")
	assert.Equal(t, int64(10), diff.TradeCountMismatch.Expected)
	assert.Equal(t, int64(7), diff.TradeCountMismatch.Actual)
	assert.Len(t, diff.MissingTrades, 3, "Should report 3 missing trades")
}

// Helper function to create a test snapshot
func createTestSnapshot(userID string, qty, tradeCount int64) *StateSnapshot {
	snap := &StateSnapshot{
		Timestamp:     time.Now(),
		Orders:        make(map[string]*OrderSnapshot),
		Trades:        make(map[string]*TradeSnapshot),
		Positions:     make(map[string]*PositionSnapshot),
		EventCount:    tradeCount * 2,
		OrderCount:    1,
		TradeCount:    tradeCount,
		PositionCount: 1,
	}
	
	snap.Orders["order1"] = &OrderSnapshot{
		OrderID:   "order1",
		UserID:    userID,
		Symbol:    "BTC/USDT",
		Side:      "buy",
		Price:     5000000000000,
		Quantity:  100,
		FilledQty: qty,
		Status:    "partial",
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
	}
	
	for i := int64(1); i <= tradeCount; i++ {
		tradeID := "trade" + string(rune('0'+(i%10)))
		if _, exists := snap.Trades[tradeID]; !exists {
			snap.Trades[tradeID] = &TradeSnapshot{
				TradeID:   tradeID,
				BuyerID:   userID,
				SellerID:  "seller1",
				Symbol:    "BTC/USDT",
				Price:     5000000000000,
				Quantity:  1,
				CreatedAt: time.Now().UnixMilli(),
			}
		}
	}
	
	snap.Positions[userID] = &PositionSnapshot{
		UserID:        userID,
		Symbol:        "BTC/USDT",
		QuantityHeld:  qty,
		CostBase:      qty * 5000000000000,
		RealizedPnl:   0,
		UnrealizedPnl: 0,
		UpdatedAt:     time.Now().UnixMilli(),
	}
	
	return snap
}
