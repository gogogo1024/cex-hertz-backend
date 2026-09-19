package service

import (
	"crypto/md5"
	"fmt"
	"sort"
	"strings"
	"time"
)

// StateSnapshot 表示系统在某个时刻的完整状态快照
// 用于crash前后的一致性验证
type StateSnapshot struct {
	Timestamp     time.Time
	Orders        map[string]*OrderSnapshot    // 订单ID -> 订单信息
	Trades        map[string]*TradeSnapshot    // 交易ID -> 交易信息
	Positions     map[string]*PositionSnapshot // 用户ID -> 持仓信息
	OrderBook     *OrderBookSnapshot           // 整个订单簿
	Checksum      string                       // MD5 hash of this snapshot
	EventCount    int64                        // 总事件数
	OrderCount    int64                        // 订单数
	TradeCount    int64                        // 交易数
	PositionCount int64                        // 持仓用户数
}

// OrderSnapshot 订单簿中的单个订单
type OrderSnapshot struct {
	OrderID   string
	UserID    string
	Symbol    string
	Side      string // "buy" or "sell"
	Price     int64  // 价格 (单位: nanoUST = 10^-8)
	Quantity  int64  // 数量
	FilledQty int64  // 已成交数量
	Status    string // "pending", "partial", "filled", "cancelled"
	CreatedAt int64
	UpdatedAt int64
}

// TradeSnapshot 成交记录
type TradeSnapshot struct {
	TradeID   string
	BuyerID   string
	SellerID  string
	Symbol    string
	Price     int64 // 成交价 (nanoUST)
	Quantity  int64
	CreatedAt int64
}

// PositionSnapshot 用户持仓信息
type PositionSnapshot struct {
	UserID        string
	Symbol        string
	QuantityHeld  int64 // 持有数量
	CostBase      int64 // 成本基数 (总成本)
	RealizedPnl   int64 // 已实现盈亏
	UnrealizedPnl int64 // 未实现盈亏 (当前价格 - 成本)
	UpdatedAt     int64
}

// OrderBookSnapshot 订单簿完整状态
type OrderBookSnapshot struct {
	Symbol         string
	LastTradePrice int64           // 最近成交价
	BidBook        map[int64]int64 // 价格 -> 数量 (买单)
	AskBook        map[int64]int64 // 价格 -> 数量 (卖单)
	BestBid        int64           // 最高买价
	BestAsk        int64           // 最低卖价
	BidDepth       int64           // 买单总数量
	AskDepth       int64           // 卖单总数量
	Spread         int64           // 价差 = BestAsk - BestBid
}

// CalculateChecksum 计算快照的MD5校验和
// 用于快速验证两个快照是否相同
func (s *StateSnapshot) CalculateChecksum() string {
	h := md5.New()

	// 订单部分
	orderKeys := make([]string, 0, len(s.Orders))
	for k := range s.Orders {
		orderKeys = append(orderKeys, k)
	}
	sort.Strings(orderKeys)

	for _, id := range orderKeys {
		entry := s.Orders[id]
		fmt.Fprintf(h, "O:%s:%s:%s:%d:%d:%d:%s|",
			entry.OrderID, entry.UserID, entry.Symbol,
			entry.Price, entry.Quantity, entry.FilledQty, entry.Status)
	}

	// 交易部分
	tradeKeys := make([]string, 0, len(s.Trades))
	for k := range s.Trades {
		tradeKeys = append(tradeKeys, k)
	}
	sort.Strings(tradeKeys)

	for _, id := range tradeKeys {
		entry := s.Trades[id]
		fmt.Fprintf(h, "T:%s:%s:%s:%d:%d|",
			entry.TradeID, entry.BuyerID, entry.SellerID,
			entry.Price, entry.Quantity)
	}

	// 持仓部分
	posKeys := make([]string, 0, len(s.Positions))
	for k := range s.Positions {
		posKeys = append(posKeys, k)
	}
	sort.Strings(posKeys)

	for _, userID := range posKeys {
		entry := s.Positions[userID]
		fmt.Fprintf(h, "P:%s:%s:%d:%d:%d:%d|",
			entry.UserID, entry.Symbol, entry.QuantityHeld,
			entry.CostBase, entry.RealizedPnl, entry.UnrealizedPnl)
	}

	// 订单簿部分
	if s.OrderBook != nil {
		fmt.Fprintf(h, "B:%s:%d:%d:%d|",
			s.OrderBook.Symbol, s.OrderBook.LastTradePrice,
			s.OrderBook.BestBid, s.OrderBook.BestAsk)
	}

	// 计数部分
	fmt.Fprintf(h, "C:%d:%d:%d:%d|",
		s.EventCount, s.OrderCount, s.TradeCount, s.PositionCount)

	s.Checksum = fmt.Sprintf("%x", h.Sum(nil))
	return s.Checksum
}

// CompareSnapshots 详细比较两个快照的差异
// 返回所有不匹配的项目
func CompareSnapshots(pre, post *StateSnapshot) *SnapshotDiff {
	diff := &SnapshotDiff{
		PreChecksum:  pre.CalculateChecksum(),
		PostChecksum: post.CalculateChecksum(),
		Timestamp:    time.Now(),
	}

	// Step 1: Checksum快速比对
	if diff.PreChecksum == diff.PostChecksum {
		diff.IsIdentical = true
		return diff
	}

	// Step 2: 计数级别对比
	if pre.EventCount != post.EventCount {
		diff.EventCountMismatch = &CountMismatch{
			Field:    "EventCount",
			Expected: pre.EventCount,
			Actual:   post.EventCount,
		}
	}

	if pre.OrderCount != post.OrderCount {
		diff.OrderCountMismatch = &CountMismatch{
			Field:    "OrderCount",
			Expected: pre.OrderCount,
			Actual:   post.OrderCount,
		}
	}

	if pre.TradeCount != post.TradeCount {
		diff.TradeCountMismatch = &CountMismatch{
			Field:    "TradeCount",
			Expected: pre.TradeCount,
			Actual:   post.TradeCount,
		}
	}

	if pre.PositionCount != post.PositionCount {
		diff.PositionCountMismatch = &CountMismatch{
			Field:    "PositionCount",
			Expected: pre.PositionCount,
			Actual:   post.PositionCount,
		}
	}

	// Step 3: 订单级别对比
	diff.MissingOrders = findMissingOrders(pre.Orders, post.Orders)
	diff.ExtraOrders = findExtraOrders(pre.Orders, post.Orders)
	diff.ChangedOrders = findChangedOrders(pre.Orders, post.Orders)

	// Step 4: 交易级别对比
	diff.MissingTrades = findMissingTrades(pre.Trades, post.Trades)
	diff.ExtraTrades = findExtraTrades(pre.Trades, post.Trades)

	// Step 5: 持仓级别对比
	diff.MissingPositions = findMissingPositions(pre.Positions, post.Positions)
	diff.ExtraPositions = findExtraPositions(pre.Positions, post.Positions)
	diff.ChangedPositions = findChangedPositions(pre.Positions, post.Positions)

	// Step 6: 订单簿状态对比
	if pre.OrderBook != nil && post.OrderBook != nil {
		diff.OrderBookDiff = compareOrderBooks(pre.OrderBook, post.OrderBook)
	}

	return diff
}

// SnapshotDiff 表示两个快照之间的所有差异
type SnapshotDiff struct {
	PreChecksum  string
	PostChecksum string
	IsIdentical  bool
	Timestamp    time.Time

	// 计数差异
	EventCountMismatch    *CountMismatch
	OrderCountMismatch    *CountMismatch
	TradeCountMismatch    *CountMismatch
	PositionCountMismatch *CountMismatch

	// 订单差异
	MissingOrders []string
	ExtraOrders   []string
	ChangedOrders []*OrderChange

	// 交易差异
	MissingTrades []string
	ExtraTrades   []string

	// 持仓差异
	MissingPositions []string
	ExtraPositions   []string
	ChangedPositions []*PositionChange

	// 订单簿差异
	OrderBookDiff *OrderBookDiff
}

// CountMismatch 表示计数字段的不匹配
type CountMismatch struct {
	Field    string
	Expected int64
	Actual   int64
}

// OrderChange 表示订单的变化
type OrderChange struct {
	OrderID  string
	Field    string // "Price", "Quantity", "FilledQty", "Status"
	Expected interface{}
	Actual   interface{}
}

// PositionChange 表示持仓的变化
type PositionChange struct {
	UserID   string
	Field    string // "QuantityHeld", "CostBase", "RealizedPnl", "UnrealizedPnl"
	Expected int64
	Actual   int64
}

// OrderBookDiff 表示订单簿的差异
type OrderBookDiff struct {
	BestBidMismatch  bool
	BestAskMismatch  bool
	BidDepthMismatch bool
	AskDepthMismatch bool
	SpreadMismatch   bool

	PreBestBid   int64
	PostBestBid  int64
	PreBestAsk   int64
	PostBestAsk  int64
	PreBidDepth  int64
	PostBidDepth int64
	PreAskDepth  int64
	PostAskDepth int64
	PreSpread    int64
	PostSpread   int64
}

// HasDifferences 检查是否有任何差异
func (d *SnapshotDiff) HasDifferences() bool {
	return !d.IsIdentical ||
		d.EventCountMismatch != nil ||
		d.OrderCountMismatch != nil ||
		d.TradeCountMismatch != nil ||
		d.PositionCountMismatch != nil ||
		len(d.MissingOrders) > 0 || len(d.ExtraOrders) > 0 || len(d.ChangedOrders) > 0 ||
		len(d.MissingTrades) > 0 || len(d.ExtraTrades) > 0 ||
		len(d.MissingPositions) > 0 || len(d.ExtraPositions) > 0 || len(d.ChangedPositions) > 0 ||
		d.OrderBookDiff != nil
}

// GenerateReport 生成human-readable的比较报告
func (d *SnapshotDiff) GenerateReport() string {
	var sb strings.Builder

	sb.WriteString("=== State Snapshot Comparison Report ===\n")
	sb.WriteString(fmt.Sprintf("Timestamp: %s\n", d.Timestamp.Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("Pre-Crash Checksum:  %s\n", d.PreChecksum))
	sb.WriteString(fmt.Sprintf("Post-Recovery Checksum: %s\n", d.PostChecksum))
	sb.WriteString(fmt.Sprintf("Identical: %v\n\n", d.IsIdentical))

	// Count mismatches
	if d.EventCountMismatch != nil {
		sb.WriteString(fmt.Sprintf("❌ EventCount: expected=%d, actual=%d\n",
			d.EventCountMismatch.Expected, d.EventCountMismatch.Actual))
	}
	if d.OrderCountMismatch != nil {
		sb.WriteString(fmt.Sprintf("❌ OrderCount: expected=%d, actual=%d\n",
			d.OrderCountMismatch.Expected, d.OrderCountMismatch.Actual))
	}
	if d.TradeCountMismatch != nil {
		sb.WriteString(fmt.Sprintf("❌ TradeCount: expected=%d, actual=%d\n",
			d.TradeCountMismatch.Expected, d.TradeCountMismatch.Actual))
	}
	if d.PositionCountMismatch != nil {
		sb.WriteString(fmt.Sprintf("❌ PositionCount: expected=%d, actual=%d\n",
			d.PositionCountMismatch.Expected, d.PositionCountMismatch.Actual))
	}

	// Order mismatches
	if len(d.MissingOrders) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Missing Orders: %v\n", d.MissingOrders))
	}
	if len(d.ExtraOrders) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Extra Orders: %v\n", d.ExtraOrders))
	}
	if len(d.ChangedOrders) > 0 {
		sb.WriteString("❌ Changed Orders:\n")
		for _, change := range d.ChangedOrders {
			sb.WriteString(fmt.Sprintf("   OrderID=%s: %s expected=%v, actual=%v\n",
				change.OrderID, change.Field, change.Expected, change.Actual))
		}
	}

	// Trade mismatches
	if len(d.MissingTrades) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Missing Trades: %v\n", d.MissingTrades))
	}
	if len(d.ExtraTrades) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Extra Trades: %v\n", d.ExtraTrades))
	}

	// Position mismatches
	if len(d.MissingPositions) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Missing Positions: %v\n", d.MissingPositions))
	}
	if len(d.ExtraPositions) > 0 {
		sb.WriteString(fmt.Sprintf("❌ Extra Positions: %v\n", d.ExtraPositions))
	}
	if len(d.ChangedPositions) > 0 {
		sb.WriteString("❌ Changed Positions:\n")
		for _, change := range d.ChangedPositions {
			sb.WriteString(fmt.Sprintf("   UserID=%s: %s expected=%d, actual=%d\n",
				change.UserID, change.Field, change.Expected, change.Actual))
		}
	}

	// OrderBook mismatches
	if d.OrderBookDiff != nil {
		sb.WriteString("❌ OrderBook Differences:\n")
		if d.OrderBookDiff.BestBidMismatch {
			sb.WriteString(fmt.Sprintf("   BestBid: %d -> %d\n",
				d.OrderBookDiff.PreBestBid, d.OrderBookDiff.PostBestBid))
		}
		if d.OrderBookDiff.BestAskMismatch {
			sb.WriteString(fmt.Sprintf("   BestAsk: %d -> %d\n",
				d.OrderBookDiff.PreBestAsk, d.OrderBookDiff.PostBestAsk))
		}
		if d.OrderBookDiff.SpreadMismatch {
			sb.WriteString(fmt.Sprintf("   Spread: %d -> %d\n",
				d.OrderBookDiff.PreSpread, d.OrderBookDiff.PostSpread))
		}
	}

	if !d.HasDifferences() {
		sb.WriteString("\n✅ No differences detected!\n")
	}

	return sb.String()
}

// Helper functions for finding differences

func findMissingOrders(pre, post map[string]*OrderSnapshot) []string {
	var missing []string
	for id := range pre {
		if _, exists := post[id]; !exists {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing
}

func findExtraOrders(pre, post map[string]*OrderSnapshot) []string {
	var extra []string
	for id := range post {
		if _, exists := pre[id]; !exists {
			extra = append(extra, id)
		}
	}
	sort.Strings(extra)
	return extra
}

func findChangedOrders(pre, post map[string]*OrderSnapshot) []*OrderChange {
	var changes []*OrderChange
	for id, preOrder := range pre {
		postOrder, exists := post[id]
		if !exists {
			continue
		}

		if preOrder.Price != postOrder.Price {
			changes = append(changes, &OrderChange{
				OrderID:  id,
				Field:    "Price",
				Expected: preOrder.Price,
				Actual:   postOrder.Price,
			})
		}
		if preOrder.Quantity != postOrder.Quantity {
			changes = append(changes, &OrderChange{
				OrderID:  id,
				Field:    "Quantity",
				Expected: preOrder.Quantity,
				Actual:   postOrder.Quantity,
			})
		}
		if preOrder.FilledQty != postOrder.FilledQty {
			changes = append(changes, &OrderChange{
				OrderID:  id,
				Field:    "FilledQty",
				Expected: preOrder.FilledQty,
				Actual:   postOrder.FilledQty,
			})
		}
		if preOrder.Status != postOrder.Status {
			changes = append(changes, &OrderChange{
				OrderID:  id,
				Field:    "Status",
				Expected: preOrder.Status,
				Actual:   postOrder.Status,
			})
		}
	}
	return changes
}

func findMissingTrades(pre, post map[string]*TradeSnapshot) []string {
	var missing []string
	for id := range pre {
		if _, exists := post[id]; !exists {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing
}

func findExtraTrades(pre, post map[string]*TradeSnapshot) []string {
	var extra []string
	for id := range post {
		if _, exists := pre[id]; !exists {
			extra = append(extra, id)
		}
	}
	sort.Strings(extra)
	return extra
}

func findMissingPositions(pre, post map[string]*PositionSnapshot) []string {
	var missing []string
	for id := range pre {
		if _, exists := post[id]; !exists {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing
}

func findExtraPositions(pre, post map[string]*PositionSnapshot) []string {
	var extra []string
	for id := range post {
		if _, exists := pre[id]; !exists {
			extra = append(extra, id)
		}
	}
	sort.Strings(extra)
	return extra
}

func findChangedPositions(pre, post map[string]*PositionSnapshot) []*PositionChange {
	var changes []*PositionChange
	for id, prePos := range pre {
		postPos, exists := post[id]
		if !exists {
			continue
		}

		if prePos.QuantityHeld != postPos.QuantityHeld {
			changes = append(changes, &PositionChange{
				UserID:   id,
				Field:    "QuantityHeld",
				Expected: prePos.QuantityHeld,
				Actual:   postPos.QuantityHeld,
			})
		}
		if prePos.CostBase != postPos.CostBase {
			changes = append(changes, &PositionChange{
				UserID:   id,
				Field:    "CostBase",
				Expected: prePos.CostBase,
				Actual:   postPos.CostBase,
			})
		}
		if prePos.RealizedPnl != postPos.RealizedPnl {
			changes = append(changes, &PositionChange{
				UserID:   id,
				Field:    "RealizedPnl",
				Expected: prePos.RealizedPnl,
				Actual:   postPos.RealizedPnl,
			})
		}
		if prePos.UnrealizedPnl != postPos.UnrealizedPnl {
			changes = append(changes, &PositionChange{
				UserID:   id,
				Field:    "UnrealizedPnl",
				Expected: prePos.UnrealizedPnl,
				Actual:   postPos.UnrealizedPnl,
			})
		}
	}
	return changes
}

func compareOrderBooks(pre, post *OrderBookSnapshot) *OrderBookDiff {
	diff := &OrderBookDiff{
		PreBestBid:   pre.BestBid,
		PostBestBid:  post.BestBid,
		PreBestAsk:   pre.BestAsk,
		PostBestAsk:  post.BestAsk,
		PreBidDepth:  pre.BidDepth,
		PostBidDepth: post.BidDepth,
		PreAskDepth:  pre.AskDepth,
		PostAskDepth: post.AskDepth,
		PreSpread:    pre.Spread,
		PostSpread:   post.Spread,
	}

	if pre.BestBid != post.BestBid {
		diff.BestBidMismatch = true
	}
	if pre.BestAsk != post.BestAsk {
		diff.BestAskMismatch = true
	}
	if pre.BidDepth != post.BidDepth {
		diff.BidDepthMismatch = true
	}
	if pre.AskDepth != post.AskDepth {
		diff.AskDepthMismatch = true
	}
	if pre.Spread != post.Spread {
		diff.SpreadMismatch = true
	}

	// 如果没有任何差异就返回nil
	if !diff.BestBidMismatch && !diff.BestAskMismatch &&
		!diff.BidDepthMismatch && !diff.AskDepthMismatch &&
		!diff.SpreadMismatch {
		return nil
	}

	return diff
}
