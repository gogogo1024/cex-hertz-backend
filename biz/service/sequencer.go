package service

import (
	"sync"
	"sync/atomic"
)

// Sequencer 生成全局单调递增的序列号（支持分布式）
// 用于确保事件的严格顺序性（即使在并发情况下）
// 分布式设计：GlobalSeq = (NodeID << 40) | LocalSeq
// - NodeID: 10-bit, 支持1024个节点
// - LocalSeq: 40-bit, 每个节点支持1万亿个序列号
type Sequencer struct {
	// 节点ID（10-bit），为0表示单机模式
	nodeID uint64

	// 本地递增的序列号（40-bit）
	localSeq atomic.Uint64

	// 用于保护符号到序列号的映射
	symbolSeqMu sync.RWMutex
	symbolSeq   map[string]uint64 // symbol -> 该symbol的最新序列号
}

// NewSequencer 创建一个新的 Sequencer（单机模式，NodeID=0）
func NewSequencer() *Sequencer {
	return &Sequencer{
		nodeID:    0,
		symbolSeq: make(map[string]uint64),
	}
}

// NewSequencerWithNodeID 创建一个支持分布式的 Sequencer
// nodeID: 1-1023 (10-bit), 由Consul分配
func NewSequencerWithNodeID(nodeID uint64) *Sequencer {
	if nodeID > 1023 {
		panic("nodeID must be <= 1023 (10-bit)")
	}
	return &Sequencer{
		nodeID:    nodeID & 0x3FF, // 只取10-bit
		symbolSeq: make(map[string]uint64),
	}
}

// NextGlobalSeq 获取下一个全局序列号
// 分布式: (NodeID << 40) | LocalSeq
// 单机:   LocalSeq (NodeID=0)
func (s *Sequencer) NextGlobalSeq() uint64 {
	local := s.localSeq.Add(1)
	if s.nodeID == 0 {
		// 单机模式，直接返回本地序列号
		return local
	}
	// 分布式模式：组合NodeID和LocalSeq
	return (s.nodeID << 40) | (local & 0xFFFFFFFFFF) // local取40-bit
}

// NextSymbolSeq 获取特定 symbol 的下一个序列号
// 每个 symbol 保持自己的序列号空间，用于支持 symbol-level recovery
func (s *Sequencer) NextSymbolSeq(symbol string) uint64 {
	s.symbolSeqMu.Lock()
	defer s.symbolSeqMu.Unlock()

	seq := s.symbolSeq[symbol] + 1
	s.symbolSeq[symbol] = seq
	return seq
}

// GetCurrentGlobalSeq 获取当前的全局序列号（无需递增）
func (s *Sequencer) GetCurrentGlobalSeq() uint64 {
	local := s.localSeq.Load()
	if s.nodeID == 0 {
		return local
	}
	return (s.nodeID << 40) | (local & 0xFFFFFFFFFF)
}

// GetNodeID 获取当前的NodeID
func (s *Sequencer) GetNodeID() uint64 {
	return s.nodeID
}

// GetSymbolSeq 获取特定 symbol 当前的序列号
func (s *Sequencer) GetSymbolSeq(symbol string) uint64 {
	s.symbolSeqMu.RLock()
	defer s.symbolSeqMu.RUnlock()
	return s.symbolSeq[symbol]
}

// SetSymbolSeq 设置特定 symbol 的序列号（用于恢复）
func (s *Sequencer) SetSymbolSeq(symbol string, seq uint64) {
	s.symbolSeqMu.Lock()
	defer s.symbolSeqMu.Unlock()
	s.symbolSeq[symbol] = seq
}
