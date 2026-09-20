package model

import "time"

// TransferStatus 表示迁移记录的状态
type TransferStatus string

const (
	TransferPending    TransferStatus = "pending"
	TransferInProgress TransferStatus = "in_progress"
	TransferCompleted  TransferStatus = "completed"
	TransferFailed     TransferStatus = "failed"
)

// OwnershipTransfer 是迁移记录的 GORM 模型
type OwnershipTransfer struct {
	ID            uint           `gorm:"primaryKey;autoIncrement" json:"id"`
	PartitionID   string         `gorm:"index" json:"partition_id"`
	FromOwner     string         `json:"from_owner"`
	ToOwner       string         `json:"to_owner"`
	CheckpointSeq int64          `json:"checkpoint_seq"`
	Status        TransferStatus `gorm:"index" json:"status"`
	Attempts      int            `json:"attempts"`
	LastError     string         `json:"last_error"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// TableName 指定表名
func (OwnershipTransfer) TableName() string {
	return "ownership_transfers"
}

// TransferCoordinator 是迁移协调器接口（可由多种后端实现）
type TransferCoordinator interface {
	// InitiateTransfer 在后端创建迁移记录并返回 transferID
	InitiateTransfer(partitionID, fromOwner, toOwner string, checkpointSeq int64) (string, error)

	// IsTargetReady 检查目标节点是否已确认接收（例如 status == completed）
	IsTargetReady(transferID string) (bool, error)

	// GetTransfer 按 id 获取迁移记录
	GetTransfer(transferID string) (*OwnershipTransfer, error)

	// MarkTransferCompleted 将迁移标记为完成
	MarkTransferCompleted(transferID string) error
}

// DefaultTransferCoordinator 是全局默认的迁移协调器（由 wiring 在启动时设置）
var DefaultTransferCoordinator TransferCoordinator

// SetDefaultTransferCoordinator 设置全局默认迁移协调器
func SetDefaultTransferCoordinator(tc TransferCoordinator) {
	DefaultTransferCoordinator = tc
}

// GetDefaultTransferCoordinator 返回当前的默认迁移协调器
func GetDefaultTransferCoordinator() TransferCoordinator {
	return DefaultTransferCoordinator
}
