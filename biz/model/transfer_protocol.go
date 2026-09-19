package model

import (
	"fmt"
	"time"
)

// ========== Phase 4.2: Partition Ownership Transfer Protocol ==========
// 定义分区迁移的完整消息协议

// TransferRequest: 协调器向源节点发送的迁移请求
type TransferRequest struct {
	PartitionID string
	SourceOwner string    // 当前所有者
	TargetOwner string    // 目标所有者
	TimeoutMs   int64     // 迁移超时（毫秒）
	MaxRetries  int32     // 最大重试次数
	RequestTime time.Time // 请求时间
	RequestID   string    // 唯一的请求 ID
}

// TransferResponse: 源节点响应迁移请求
type TransferResponse struct {
	Accepted      bool
	Reason        string // 拒绝的原因
	CheckpointSeq int64  // 源节点当前的 checkpoint seq
	ResponseTime  time.Time
	RequestID     string
}

// FreezeSignal: 冻结信号（停止接受新写入）
type FreezeSignal struct {
	PartitionID     string
	FreezeTimestamp int64 // 冻结时间点 (Unix 毫秒)
	FreezeTime      time.Time
	SourceOwner     string
}

// FlushAck: 源节点刷盘完成确认
type FlushAck struct {
	PartitionID      string
	LastProcessedSeq int64            // 最后处理的全局序列号
	CheckpointSeq    int64            // Checkpoint 的序列号
	KafkaOffsets     map[string]int64 // 每个 symbol 的 offset
	SourceOwner      string
	FlushTime        time.Time
	RequestID        string
}

// TransferSignal: 转移信号（指示目标节点启动）
type TransferSignal struct {
	PartitionID         string
	SourceOwner         string
	TargetOwner         string
	StartFromSeq        int64            // 从该序列号开始处理
	StartFromCheckpoint int64            // 从该 checkpoint 恢复
	KafkaOffsets        map[string]int64 // 每个 symbol 的 offset
	TransferTime        time.Time
	RequestID           string
}

// CommitSignal: 提交信号（确认转移完成）
type CommitSignal struct {
	PartitionID string
	TargetOwner string
	Success     bool   // 目标节点是否已启动
	Reason      string // 如果失败，说明原因
	CommitTime  time.Time
	RequestID   string
}

// TransferContext: 迁移过程中的上下文信息
type TransferContext struct {
	PartitionID      string
	SourceOwner      string
	TargetOwner      string
	State            OwnershipState
	RequestID        string
	StartTime        time.Time
	LastUpdateTime   time.Time
	TimeoutMs        int64
	RetryCount       int32
	MaxRetries       int32
	LastError        error
	CheckpointSeq    int64
	KafkaOffsets     map[string]int64
	FlushAckReceived bool
	CommitSignalSent bool
}

// TransferProtocol: 迁移协议处理器
type TransferProtocol struct {
	fsm *OwnershipStateMachine
}

// NewTransferProtocol 创建新的迁移协议处理器
func NewTransferProtocol(fsm *OwnershipStateMachine) *TransferProtocol {
	return &TransferProtocol{
		fsm: fsm,
	}
}

// StartTransfer 启动分区迁移
func (tp *TransferProtocol) StartTransfer(req TransferRequest) (*TransferResponse, error) {
	if tp.fsm == nil {
		return nil, fmt.Errorf("state machine not initialized")
	}

	// 检查当前状态是否为 Owned
	if tp.fsm.GetState() != Owned {
		return &TransferResponse{
			Accepted:      false,
			Reason:        fmt.Sprintf("source not in Owned state, current state: %v", tp.fsm.GetState()),
			CheckpointSeq: -1,
			ResponseTime:  time.Now(),
			RequestID:     req.RequestID,
		}, nil
	}

	// 启动迁移
	err := tp.fsm.StartMigration(req.TargetOwner, "protocol")
	if err != nil {
		return &TransferResponse{
			Accepted:      false,
			Reason:        fmt.Sprintf("failed to start migration: %v", err),
			CheckpointSeq: -1,
			ResponseTime:  time.Now(),
			RequestID:     req.RequestID,
		}, nil
	}

	// 获取当前 checkpoint seq
	metadata := tp.fsm.GetMetadata()
	return &TransferResponse{
		Accepted:      true,
		Reason:        "",
		CheckpointSeq: metadata.CheckpointSeq,
		ResponseTime:  time.Now(),
		RequestID:     req.RequestID,
	}, nil
}

// ApplyFreezeSignal 应用冻结信号
func (tp *TransferProtocol) ApplyFreezeSignal(sig FreezeSignal) error {
	if tp.fsm == nil {
		return fmt.Errorf("state machine not initialized")
	}

	// 检查状态是否为 MigrationPending
	if tp.fsm.GetState() != MigrationPending {
		return fmt.Errorf("cannot freeze: not in MigrationPending state, current: %v", tp.fsm.GetState())
	}

	// 应用冻结
	return tp.fsm.Freeze(sig.SourceOwner)
}

// CompleteFlushing 完成刷盘
func (tp *TransferProtocol) CompleteFlushing(sourceOwner string) (*FlushAck, error) {
	if tp.fsm == nil {
		return nil, fmt.Errorf("state machine not initialized")
	}

	// 检查状态是否为 Frozen
	if tp.fsm.GetState() != Frozen {
		return nil, fmt.Errorf("cannot flush: not in Frozen state, current: %v", tp.fsm.GetState())
	}

	// 应用刷盘
	err := tp.fsm.Flush(sourceOwner)
	if err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// 获取元数据
	metadata := tp.fsm.GetMetadata()
	return &FlushAck{
		PartitionID:      metadata.PartitionID,
		LastProcessedSeq: metadata.LastProcessedSeq,
		CheckpointSeq:    metadata.CheckpointSeq,
		KafkaOffsets:     copyKafkaOffsets(metadata.KafkaOffset),
		SourceOwner:      sourceOwner,
		FlushTime:        time.Now(),
	}, nil
}

// ApplyTransferSignal 应用转移信号（目标节点接收）
func (tp *TransferProtocol) ApplyTransferSignal(sig TransferSignal) error {
	if tp.fsm == nil {
		return fmt.Errorf("state machine not initialized")
	}

	// 检查状态是否为 Flushing
	if tp.fsm.GetState() != Flushing {
		return fmt.Errorf("cannot transfer: not in Flushing state, current: %v", tp.fsm.GetState())
	}

	// 应用转移
	err := tp.fsm.Transfer(sig.SourceOwner)
	if err != nil {
		return fmt.Errorf("failed to transfer: %w", err)
	}

	return nil
}

// CommitTransfer 提交转移（转移完成）
func (tp *TransferProtocol) CommitTransfer(targetOwner string) error {
	if tp.fsm == nil {
		return fmt.Errorf("state machine not initialized")
	}

	// 检查状态是否为 Transferring
	if tp.fsm.GetState() != Transferring {
		return fmt.Errorf("cannot commit: not in Transferring state, current: %v", tp.fsm.GetState())
	}

	// 提交转移
	return tp.fsm.Commit(targetOwner)
}

// RollbackTransfer 回滚转移
func (tp *TransferProtocol) RollbackTransfer(reason string) error {
	if tp.fsm == nil {
		return fmt.Errorf("state machine not initialized")
	}

	return tp.fsm.Rollback(reason)
}

// GetTransferContext 获取转移上下文
func (tp *TransferProtocol) GetTransferContext() *TransferContext {
	if tp.fsm == nil {
		return nil
	}

	metadata := tp.fsm.GetMetadata()
	return &TransferContext{
		PartitionID:   metadata.PartitionID,
		SourceOwner:   metadata.CurrentOwner,
		TargetOwner:   metadata.TargetOwner,
		State:         metadata.State,
		StartTime:     time.Now(),
		TimeoutMs:     30000,
		CheckpointSeq: metadata.CheckpointSeq,
		KafkaOffsets:  copyKafkaOffsets(metadata.KafkaOffset),
	}
}

// ValidateTransferInvariants 验证转移不变量
func (tp *TransferProtocol) ValidateTransferInvariants() error {
	if tp.fsm == nil {
		return fmt.Errorf("state machine not initialized")
	}

	metadata := tp.fsm.GetMetadata()

	// 检查 Invariant 1: CheckpointSeq ≤ LastProcessedSeq
	if metadata.CheckpointSeq > metadata.LastProcessedSeq {
		return fmt.Errorf("invariant violation: CheckpointSeq (%d) > LastProcessedSeq (%d)",
			metadata.CheckpointSeq, metadata.LastProcessedSeq)
	}

	// 检查 Invariant 2: 在转移中时有效的 KafkaOffset
	if metadata.State == Transferring || metadata.State == Flushing {
		if len(metadata.KafkaOffset) == 0 {
			return fmt.Errorf("invariant violation: KafkaOffset empty during transfer")
		}
	}

	// 检查 Invariant 3: 转移中时必须有 TargetOwner
	if metadata.State == MigrationPending || metadata.State == Frozen ||
		metadata.State == Flushing || metadata.State == Transferring {
		if metadata.TargetOwner == "" {
			return fmt.Errorf("invariant violation: no TargetOwner during transfer in state %v", metadata.State)
		}
	}

	return nil
}

// copyKafkaOffsets 深拷贝 KafkaOffset
func copyKafkaOffsets(offsets map[string]int64) map[string]int64 {
	result := make(map[string]int64)
	for k, v := range offsets {
		result[k] = v
	}
	return result
}
