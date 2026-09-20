package model

import (
	"fmt"
	"sync"
	"time"
)

// OwnershipState 定义分区所有权的状态
type OwnershipState int

const (
	// Owned: 分区由单个节点拥有，正常处理写入
	Owned OwnershipState = iota

	// MigrationPending: 已启动迁移请求，等待所有者冻结
	MigrationPending

	// Frozen: 所有者已冻结新写入，等待刷盘
	Frozen

	// Flushing: 所有者正在刷盘所有待处理事件
	Flushing

	// Transferring: 等待目标节点确认接收
	Transferring

	// Standby: 分区由新所有者拥有，旧所有者为备用
	Standby

	// Failed: 迁移失败
	Failed
)

// String 返回状态的字符串表示
func (s OwnershipState) String() string {
	states := map[OwnershipState]string{
		Owned:            "Owned",
		MigrationPending: "MigrationPending",
		Frozen:           "Frozen",
		Flushing:         "Flushing",
		Transferring:     "Transferring",
		Standby:          "Standby",
		Failed:           "Failed",
	}
	if name, ok := states[s]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", s)
}

// OwnershipMetadata 记录分区所有权的元数据
type OwnershipMetadata struct {
	// 分区 ID
	PartitionID string

	// 当前所有者节点 ID
	CurrentOwner string

	// 前任所有者节点 ID（用于 standby）
	PreviousOwner string

	// 迁移目标节点 ID（如果正在迁移）
	TargetOwner string

	// 当前所有权状态
	State OwnershipState

	// 该分区负责的所有 symbol 列表
	Symbols []string

	// 最后一个处理的全局序列号
	LastProcessedSeq int64

	// 状态转移时间戳
	StateTransitionTime int64

	// 迁移开始时间（用于超时检测）
	MigrationStartTime int64

	// 检查点信息
	CheckpointSeq int64

	// 转移记录 ID（当使用 TransferCoordinator 时会写入）
	TransferID string

	// Kafka offset 信息
	KafkaOffset map[string]int64 // symbol -> offset

	// 版本号（用于并发控制）
	Version int64

	// 状态转移历史（用于调试）
	TransitionHistory []StateTransition
}

// StateTransition 记录一次状态转移
type StateTransition struct {
	FromState OwnershipState
	ToState   OwnershipState
	Reason    string
	Timestamp int64
	Actor     string // 执行转移的节点
}

// OwnershipStateMachine 管理分区所有权状态转移
type OwnershipStateMachine struct {
	mu       sync.RWMutex
	metadata *OwnershipMetadata

	// 转移条件检查函数
	guards map[StateTransition]func() (bool, error)

	// 转移动作函数
	actions map[StateTransition]func() error

	// 事件监听器
	listeners []OwnershipListener

	// 迁移超时时间
	migrationTimeout time.Duration

	// 可选的迁移协调器（若为 nil 则表示未启用持久化/通知）
	transferCoordinator TransferCoordinator
}

// OwnershipListener 监听所有权变化
type OwnershipListener interface {
	OnOwnershipChange(old, new *OwnershipMetadata)
}

// NewOwnershipStateMachine 创建新的所有权状态机
func NewOwnershipStateMachine(partitionID, owner string, symbols []string) *OwnershipStateMachine {
	fsm := &OwnershipStateMachine{
		metadata: &OwnershipMetadata{
			PartitionID:         partitionID,
			CurrentOwner:        owner,
			State:               Owned,
			Symbols:             symbols,
			StateTransitionTime: time.Now().Unix(),
			KafkaOffset:         make(map[string]int64),
			TransitionHistory:   make([]StateTransition, 0),
			Version:             1,
		},
		guards:           make(map[StateTransition]func() (bool, error)),
		actions:          make(map[StateTransition]func() error),
		listeners:        make([]OwnershipListener, 0),
		migrationTimeout: 30 * time.Second,
	}

	// 回退到全局默认 coordinator（若已通过 wiring 设置）以保持向后兼容
	if tc := GetDefaultTransferCoordinator(); tc != nil {
		fsm.transferCoordinator = tc
	}

	// 注册所有状态转移的 guards 和 actions
	fsm.registerTransitions()
	return fsm
}

// registerTransitions 注册所有合法的状态转移及其 guards 和 actions
func (fsm *OwnershipStateMachine) registerTransitions() {
	// Owned -> MigrationPending: 由外部触发迁移请求
	fsm.registerGuard(Owned, MigrationPending, func() (bool, error) {
		// 必须有目标所有者
		if fsm.metadata.TargetOwner == "" {
			return false, fmt.Errorf("TargetOwner not set")
		}
		// 不能迁移给自己
		if fsm.metadata.TargetOwner == fsm.metadata.CurrentOwner {
			return false, fmt.Errorf("Cannot migrate to same owner")
		}
		return true, nil
	})

	fsm.registerAction(Owned, MigrationPending, func() error {
		fsm.metadata.MigrationStartTime = time.Now().Unix()
		return nil
	})

	// MigrationPending -> Frozen: 所有者收到冻结指令
	fsm.registerGuard(MigrationPending, Frozen, func() (bool, error) {
		// 检查 migration 是否超时
		elapsed := time.Now().Unix() - fsm.metadata.MigrationStartTime
		if elapsed > int64(fsm.migrationTimeout.Seconds()) {
			return false, fmt.Errorf("Migration timeout")
		}
		return true, nil
	})

	fsm.registerAction(MigrationPending, Frozen, func() error {
		// 冻结新写入
		return nil
	})

	// Frozen -> Flushing: 所有者开始刷盘
	fsm.registerGuard(Frozen, Flushing, func() (bool, error) {
		return true, nil
	})

	fsm.registerAction(Frozen, Flushing, func() error {
		// 标记刷盘开始时间
		return nil
	})

	// Flushing -> Transferring: 所有者完成刷盘，等待目标确认
	fsm.registerGuard(Flushing, Transferring, func() (bool, error) {
		// 检查 checkpoint 是否已持久化
		// 如果没有注入 coordinator，则回退到本地检查（CheckpointSeq ≤ LastProcessedSeq）以保持兼容性
		if fsm.transferCoordinator == nil {
			if fsm.metadata.CheckpointSeq <= fsm.metadata.LastProcessedSeq {
				return true, nil
			}
			return false, fmt.Errorf("checkpoint not persisted")
		}

		// 当 coordinator 存在时，仍然以本地 checkpoint 作为最小条件；后续可扩展为 coordinator 提供更强保证
		if fsm.metadata.CheckpointSeq <= fsm.metadata.LastProcessedSeq {
			return true, nil
		}
		return false, fmt.Errorf("checkpoint not persisted")
	})

	fsm.registerAction(Flushing, Transferring, func() error {
		// 发送所有权转移请求到目标节点
		if fsm.transferCoordinator == nil {
			return nil
		}

		transferID, err := fsm.transferCoordinator.InitiateTransfer(fsm.metadata.PartitionID, fsm.metadata.CurrentOwner, fsm.metadata.TargetOwner, fsm.metadata.CheckpointSeq)
		if err != nil {
			return fmt.Errorf("InitiateTransfer failed: %w", err)
		}
		fsm.metadata.TransferID = transferID
		return nil
	})

	// Transferring -> Standby: 目标节点已就绪，转移完成
	fsm.registerGuard(Transferring, Standby, func() (bool, error) {
		// 检查目标节点是否已启动
		if fsm.transferCoordinator == nil {
			return true, nil
		}

		if fsm.metadata.TransferID == "" {
			return false, fmt.Errorf("no transfer record")
		}

		ready, err := fsm.transferCoordinator.IsTargetReady(fsm.metadata.TransferID)
		if err != nil {
			return false, err
		}
		return ready, nil
	})

	fsm.registerAction(Transferring, Standby, func() error {
		// 保存前任所有者，更新当前所有者
		fsm.metadata.PreviousOwner = fsm.metadata.CurrentOwner
		fsm.metadata.CurrentOwner = fsm.metadata.TargetOwner
		fsm.metadata.TargetOwner = ""
		return nil
	})

	// 任何状态 -> Failed: 迁移失败
	fsm.registerGuard(MigrationPending, Failed, func() (bool, error) {
		return true, nil
	})

	fsm.registerGuard(Frozen, Failed, func() (bool, error) {
		return true, nil
	})

	fsm.registerGuard(Flushing, Failed, func() (bool, error) {
		return true, nil
	})

	fsm.registerGuard(Transferring, Failed, func() (bool, error) {
		return true, nil
	})

	fsm.registerAction(MigrationPending, Failed, func() error {
		fsm.metadata.TargetOwner = ""
		return nil
	})

	fsm.registerAction(Frozen, Failed, func() error {
		fsm.metadata.TargetOwner = ""
		return nil
	})

	fsm.registerAction(Flushing, Failed, func() error {
		fsm.metadata.TargetOwner = ""
		return nil
	})

	fsm.registerAction(Transferring, Failed, func() error {
		fsm.metadata.TargetOwner = ""
		return nil
	})

	// Failed -> Owned: 恢复到 Owned 状态（重试或回滚）
	fsm.registerGuard(Failed, Owned, func() (bool, error) {
		return true, nil
	})

	fsm.registerAction(Failed, Owned, func() error {
		fsm.metadata.TargetOwner = ""
		return nil
	})
}

// registerGuard 注册状态转移的条件检查函数
func (fsm *OwnershipStateMachine) registerGuard(from, to OwnershipState, guard func() (bool, error)) {
	transition := StateTransition{FromState: from, ToState: to}
	fsm.guards[transition] = guard
}

// registerAction 注册状态转移的动作函数
func (fsm *OwnershipStateMachine) registerAction(from, to OwnershipState, action func() error) {
	transition := StateTransition{FromState: from, ToState: to}
	fsm.actions[transition] = action
}

// Transition 执行状态转移
func (fsm *OwnershipStateMachine) Transition(toState OwnershipState, reason, actor string) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fromState := fsm.metadata.State

	// 检查转移是否合法
	transition := StateTransition{FromState: fromState, ToState: toState}
	guard, ok := fsm.guards[transition]
	if !ok {
		return fmt.Errorf("Invalid transition: %s -> %s", fromState, toState)
	}

	// 执行转移条件检查
	allowed, err := guard()
	if !allowed || err != nil {
		return fmt.Errorf("Transition guard failed: %w", err)
	}

	// 保存旧状态
	oldMetadata := *fsm.metadata

	// 执行转移动作
	if action, ok := fsm.actions[transition]; ok {
		if err := action(); err != nil {
			return fmt.Errorf("Transition action failed: %w", err)
		}
	}

	// 更新状态
	fsm.metadata.State = toState
	fsm.metadata.StateTransitionTime = time.Now().Unix()
	fsm.metadata.Version++

	// 记录转移历史
	fsm.metadata.TransitionHistory = append(fsm.metadata.TransitionHistory, StateTransition{
		FromState: fromState,
		ToState:   toState,
		Reason:    reason,
		Timestamp: time.Now().Unix(),
		Actor:     actor,
	})

	// 限制历史大小（保留最近 100 次转移）
	if len(fsm.metadata.TransitionHistory) > 100 {
		fsm.metadata.TransitionHistory = fsm.metadata.TransitionHistory[len(fsm.metadata.TransitionHistory)-100:]
	}

	// 通知监听器
	for _, listener := range fsm.listeners {
		listener.OnOwnershipChange(&oldMetadata, fsm.metadata)
	}

	return nil
}

// StartMigration 启动迁移流程
func (fsm *OwnershipStateMachine) StartMigration(targetOwner, actor string) error {
	fsm.mu.Lock()
	fsm.metadata.TargetOwner = targetOwner
	fsm.mu.Unlock()

	return fsm.Transition(MigrationPending, "StartMigration", actor)
}

// Freeze 冻结新写入
func (fsm *OwnershipStateMachine) Freeze(actor string) error {
	return fsm.Transition(Frozen, "Freeze", actor)
}

// Flush 开始刷盘
func (fsm *OwnershipStateMachine) Flush(actor string) error {
	return fsm.Transition(Flushing, "Flush", actor)
}

// Transfer 完成转移
func (fsm *OwnershipStateMachine) Transfer(actor string) error {
	return fsm.Transition(Transferring, "Transfer", actor)
}

// Commit 提交转移（目标节点已就绪）
func (fsm *OwnershipStateMachine) Commit(actor string) error {
	return fsm.Transition(Standby, "Commit", actor)
}

// Rollback 回滚迁移
func (fsm *OwnershipStateMachine) Rollback(actor string) error {
	fsm.mu.Lock()
	fsm.metadata.TargetOwner = ""
	fsm.mu.Unlock()

	return fsm.Transition(Failed, "Rollback", actor)
}

// Recover 从失败状态恢复
func (fsm *OwnershipStateMachine) Recover(actor string) error {
	return fsm.Transition(Owned, "Recover", actor)
}

// GetMetadata 获取所有权元数据（只读副本）
func (fsm *OwnershipStateMachine) GetMetadata() *OwnershipMetadata {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	kafkaOffsetCopy := make(map[string]int64, len(fsm.metadata.KafkaOffset))
	for k, v := range fsm.metadata.KafkaOffset {
		kafkaOffsetCopy[k] = v
	}

	return &OwnershipMetadata{
		PartitionID:         fsm.metadata.PartitionID,
		CurrentOwner:        fsm.metadata.CurrentOwner,
		PreviousOwner:       fsm.metadata.PreviousOwner,
		TargetOwner:         fsm.metadata.TargetOwner,
		State:               fsm.metadata.State,
		Symbols:             append([]string{}, fsm.metadata.Symbols...),
		LastProcessedSeq:    fsm.metadata.LastProcessedSeq,
		StateTransitionTime: fsm.metadata.StateTransitionTime,
		MigrationStartTime:  fsm.metadata.MigrationStartTime,
		CheckpointSeq:       fsm.metadata.CheckpointSeq,
		KafkaOffset:         kafkaOffsetCopy,
		Version:             fsm.metadata.Version,
	}
}

// UpdateLastProcessedSeq 更新最后处理的序列号
func (fsm *OwnershipStateMachine) UpdateLastProcessedSeq(seq int64) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if seq < fsm.metadata.LastProcessedSeq {
		return fmt.Errorf("Seq cannot go backward: %d -> %d", fsm.metadata.LastProcessedSeq, seq)
	}

	fsm.metadata.LastProcessedSeq = seq
	fsm.metadata.Version++
	return nil
}

// UpdateCheckpoint 更新检查点
func (fsm *OwnershipStateMachine) UpdateCheckpoint(seq int64) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if seq > fsm.metadata.LastProcessedSeq {
		return fmt.Errorf("Checkpoint seq cannot exceed LastProcessedSeq: %d > %d", seq, fsm.metadata.LastProcessedSeq)
	}

	fsm.metadata.CheckpointSeq = seq
	fsm.metadata.Version++
	return nil
}

// UpdateKafkaOffset 更新某个 symbol 的 Kafka offset
func (fsm *OwnershipStateMachine) UpdateKafkaOffset(symbol string, offset int64) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if offset < 0 {
		return fmt.Errorf("Invalid Kafka offset: %d", offset)
	}

	fsm.metadata.KafkaOffset[symbol] = offset
	fsm.metadata.Version++
	return nil
}

// GetState 获取当前状态
func (fsm *OwnershipStateMachine) GetState() OwnershipState {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.metadata.State
}

// IsOwner 检查是否为指定的所有者
func (fsm *OwnershipStateMachine) IsOwner(nodeID string) bool {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.metadata.CurrentOwner == nodeID
}

// AddListener 添加状态变化监听器
func (fsm *OwnershipStateMachine) AddListener(listener OwnershipListener) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.listeners = append(fsm.listeners, listener)
}

// SetMigrationTimeout 设置迁移超时时间
func (fsm *OwnershipStateMachine) SetMigrationTimeout(timeout time.Duration) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.migrationTimeout = timeout
}

// SetTransferCoordinator 注入迁移协调器实例到当前 FSM
func (fsm *OwnershipStateMachine) SetTransferCoordinator(tc TransferCoordinator) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.transferCoordinator = tc
}

// GetTransitionHistory 获取状态转移历史
func (fsm *OwnershipStateMachine) GetTransitionHistory() []StateTransition {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	history := make([]StateTransition, len(fsm.metadata.TransitionHistory))
	copy(history, fsm.metadata.TransitionHistory)
	return history
}
