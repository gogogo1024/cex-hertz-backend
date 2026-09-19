package service

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"

)

// RecoveryCheckpoint 恢复检查点的状态快照
type RecoveryCheckpoint struct {
	Timestamp        int64
	LastEventSeq     uint64
	OrderCount       int64
	TradeCount       int64
	OrderBook        interface{} // 订单簿
	Positions        interface{} // 持仓
	StateChecksum    string
	BidLevels        int
	AskLevels        int
	TotalBidQty      int64
	TotalAskQty      int64
}

// ValidationResult 验证结果
type ValidationResult struct {
	IsValid        bool
	ChecksumMatch  bool
	OrderCountOK   bool
	TradeCountOK   bool
	PositionsOK    bool

	// 详细的差异信息
	ChecksumMismatch      string
	OrderCountDifference  int64
	TradeCountDifference  int64
	PositionDiffs         map[string]string

	// 完整的pre/post日志
	PreState       string
	PostState      string
}

// StateValidator 状态验证器
// 用于验证crash recovery前后的状态是否一致
type StateValidator struct {
	mu sync.RWMutex

	// 缓存最后一次的恢复检查点
	lastPreCheckpoint  *RecoveryCheckpoint
	lastPostCheckpoint *RecoveryCheckpoint
}

// NewStateValidator 创建状态验证器
func NewStateValidator() *StateValidator {
	return &StateValidator{}
}

// CalculateChecksum 计算状态的checksum
// 基于OrderBook和Positions的精确值
func (sv *StateValidator) CalculateChecksum(ob interface{}, positions interface{}) string {
	// 生成一个稳定的哈希值
	// 这里需要对状态进行序列化，然后计算MD5

	// 简单实现：使用fmt.Sprintf生成字符串，然后MD5
	// 在生产环境中，应该使用更精确的序列化方式（如JSON）

	stateStr := fmt.Sprintf("orderbook:%v|positions:%v", ob, positions)
	hash := md5.Sum([]byte(stateStr))
	return hex.EncodeToString(hash[:])
}

// CaptureCurrentState 获取当前内存状态快照
// 注意：这个方法需要从MatchEngine中获取当前状态
// 由于无法直接访问MatchEngine的私有成员，这里返回一个空的snapshot
func (sv *StateValidator) CaptureCurrentState(
	matchEngine *PartitionAwareMatchEngine,
	symbol string,
) *RecoveryCheckpoint {
	if matchEngine == nil {
		hlog.Warnf("[StateValidator] MatchEngine is nil")
		return nil
	}

	// 获取当前的OrderBook和Positions
	orderBook := matchEngine.GetOrderBook(symbol)
	positions := matchEngine.GetPositions()

	// 计算状态的checksum
	checksum := sv.CalculateChecksum(orderBook, positions)

	// 创建快照
	checkpoint := &RecoveryCheckpoint{
		Timestamp:     int64(0), // TODO: 获取当前时间戳
		OrderBook:     orderBook,
		Positions:     positions,
		StateChecksum: checksum,
	}

	return checkpoint
}

// ValidateRecoveryState 验证恢复后的状态是否与pre-crash状态一致
func (sv *StateValidator) ValidateRecoveryState(
	preState *RecoveryCheckpoint,
	postState *RecoveryCheckpoint,
) *ValidationResult {
	result := &ValidationResult{
		PositionDiffs: make(map[string]string),
	}

	if preState == nil || postState == nil {
		result.IsValid = false
		result.PreState = fmt.Sprintf("%v", preState)
		result.PostState = fmt.Sprintf("%v", postState)
		return result
	}

	// 验证checksum
	if preState.StateChecksum != postState.StateChecksum {
		result.ChecksumMatch = false
		result.ChecksumMismatch = fmt.Sprintf("pre=%s, post=%s", 
			preState.StateChecksum, postState.StateChecksum)
		hlog.Errorf("[StateValidator] Checksum mismatch: %s", result.ChecksumMismatch)
	} else {
		result.ChecksumMatch = true
	}

	// 验证订单数
	if preState.OrderCount != postState.OrderCount {
		result.OrderCountOK = false
		result.OrderCountDifference = postState.OrderCount - preState.OrderCount
		hlog.Errorf("[StateValidator] Order count mismatch: pre=%d, post=%d, diff=%d",
			preState.OrderCount, postState.OrderCount, result.OrderCountDifference)
	} else {
		result.OrderCountOK = true
	}

	// 验证成交数
	if preState.TradeCount != postState.TradeCount {
		result.TradeCountOK = false
		result.TradeCountDifference = postState.TradeCount - preState.TradeCount
		hlog.Errorf("[StateValidator] Trade count mismatch: pre=%d, post=%d, diff=%d",
			preState.TradeCount, postState.TradeCount, result.TradeCountDifference)
	} else {
		result.TradeCountOK = true
	}

	// 验证持仓
	if preState.Positions != nil && postState.Positions != nil {
		result.PositionsOK = sv.validatePositions(preState.Positions, postState.Positions, result.PositionDiffs)
	} else {
		result.PositionsOK = true // 如果没有持仓数据，认为验证通过
	}

	// 综合验证结果
	result.IsValid = result.ChecksumMatch && result.OrderCountOK && result.TradeCountOK && result.PositionsOK

	// 记录状态快照
	result.PreState = fmt.Sprintf("seq=%d, orders=%d, trades=%d, checksum=%s",
		preState.LastEventSeq, preState.OrderCount, preState.TradeCount, preState.StateChecksum)
	result.PostState = fmt.Sprintf("seq=%d, orders=%d, trades=%d, checksum=%s",
		postState.LastEventSeq, postState.OrderCount, postState.TradeCount, postState.StateChecksum)

	if result.IsValid {
		hlog.Infof("[StateValidator] Validation passed: %s", result.PreState)
	} else {
		hlog.Errorf("[StateValidator] Validation failed")
	}

	sv.mu.Lock()
	sv.lastPreCheckpoint = preState
	sv.lastPostCheckpoint = postState
	sv.mu.Unlock()

	return result
}

// validatePositions 验证持仓是否相同
// 这是一个简化的实现，生产环境需要更精细的比较
func (sv *StateValidator) validatePositions(
	prePositions interface{},
	postPositions interface{},
	diffs map[string]string,
) bool {
	// 简单实现：比较字符串表示
	preStr := fmt.Sprintf("%v", prePositions)
	postStr := fmt.Sprintf("%v", postPositions)

	if preStr != postStr {
		diffs["positions"] = fmt.Sprintf("pre=%s, post=%s", preStr, postStr)
		hlog.Warnf("[StateValidator] Positions differ: %s", diffs["positions"])
		return false
	}

	return true
}

// CompareOrderBooks 比较两个OrderBook是否相同
// orderbook1, orderbook2: 订单簿对象
// 返回是否相同、差异说明
func (sv *StateValidator) CompareOrderBooks(orderbook1 interface{}, orderbook2 interface{}) (bool, string) {
	str1 := fmt.Sprintf("%v", orderbook1)
	str2 := fmt.Sprintf("%v", orderbook2)

	if str1 != str2 {
		return false, fmt.Sprintf("OrderBook differs: pre=%s, post=%s", str1, str2)
	}

	return true, ""
}

// GetLastValidationResult 获取最后一次验证的结果
func (sv *StateValidator) GetLastValidationResult() (*RecoveryCheckpoint, *RecoveryCheckpoint) {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return sv.lastPreCheckpoint, sv.lastPostCheckpoint
}

// ResetValidationState 重置验证状态
func (sv *StateValidator) ResetValidationState() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.lastPreCheckpoint = nil
	sv.lastPostCheckpoint = nil
}

// DebugPrintCheckpoint 打印检查点信息（用于调试）
func (sv *StateValidator) DebugPrintCheckpoint(label string, cp *RecoveryCheckpoint) {
	if cp == nil {
		hlog.Debugf("[StateValidator] %s: <nil>", label)
		return
	}

	hlog.Debugf("[StateValidator] %s: seq=%d, orders=%d, trades=%d, bids=%d, asks=%d, bid_qty=%d, ask_qty=%d, checksum=%s",
		label,
		cp.LastEventSeq,
		cp.OrderCount,
		cp.TradeCount,
		cp.BidLevels,
		cp.AskLevels,
		cp.TotalBidQty,
		cp.TotalAskQty,
		cp.StateChecksum,
	)
}
