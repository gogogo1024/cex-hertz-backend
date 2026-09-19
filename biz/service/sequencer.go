package service

import (
	"sync"
	"sync/atomic"
)

// Sequencer 生成全局单调递增的序列号
// 用于确保事件的严格顺序性（即使在并发情况下）
type Sequencer struct {
	// 当前序列号
	currentSeq atomic.Uint64

	// 用于保护符号到序列号的映射
	symbolSeqMu sync.RWMutex
	symbolSeq   map[string]uint64 // symbol -> 该symbol的最新序列号
}

// NewSequencer 创建一个新的 Sequencer
func NewSequencer() *Sequencer {
	return &Sequencer{
		symbolSeq: make(map[string]uint64),
	}
}

// NextGlobalSeq 获取下一个全局序列号
// 这确保了所有事件的全局顺序
func (s *Sequencer) NextGlobalSeq() uint64 {
	return s.currentSeq.Add(1)
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
	return s.currentSeq.Load()
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
