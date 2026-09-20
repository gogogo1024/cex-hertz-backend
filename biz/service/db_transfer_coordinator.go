package service

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// DBTransferCoordinator 使用数据库作为迁移记录的持久化后端（方案 A 的关键部件）
type DBTransferCoordinator struct {
	db         *gorm.DB
	httpClient *http.Client
}

// NewDBTransferCoordinator 创建一个 DBTransferCoordinator 实例
func NewDBTransferCoordinator(db *gorm.DB) *DBTransferCoordinator {
	return &DBTransferCoordinator{
		db:         db,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// InitiateTransfer 在 DB 中创建一条迁移记录并返回其 ID（字符串形式）
func (c *DBTransferCoordinator) InitiateTransfer(partitionID, fromOwner, toOwner string, checkpointSeq int64) (string, error) {
	tr := &model.OwnershipTransfer{
		PartitionID:   partitionID,
		FromOwner:     fromOwner,
		ToOwner:       toOwner,
		CheckpointSeq: checkpointSeq,
		Status:        model.TransferPending,
		Attempts:      0,
	}

	if err := c.db.Create(tr).Error; err != nil {
		hlog.Errorf("[DBTransferCoordinator] create transfer record failed: %v", err)
		return "", err
	}

	hlog.Infof("[DBTransferCoordinator] Created transfer record id=%d partition=%s -> %s", tr.ID, partitionID, toOwner)
	return fmt.Sprintf("%d", tr.ID), nil
}

// GetTransfer 根据 transferID 获取记录
func (c *DBTransferCoordinator) GetTransfer(transferID string) (*model.OwnershipTransfer, error) {
	var id uint
	if _, err := fmt.Sscanf(transferID, "%d", &id); err != nil {
		return nil, err
	}

	var tr model.OwnershipTransfer
	if err := c.db.First(&tr, id).Error; err != nil {
		return nil, err
	}
	return &tr, nil
}

// IsTargetReady 检查是否已完成（由目标节点或外部流程设置为 completed）
func (c *DBTransferCoordinator) IsTargetReady(transferID string) (bool, error) {
	tr, err := c.GetTransfer(transferID)
	if err != nil {
		return false, err
	}
	return tr.Status == model.TransferCompleted, nil
}

// MarkTransferCompleted 将迁移标记为完成
func (c *DBTransferCoordinator) MarkTransferCompleted(transferID string) error {
	tr, err := c.GetTransfer(transferID)
	if err != nil {
		return err
	}
	tr.Status = model.TransferCompleted
	if err := c.db.Save(tr).Error; err != nil {
		return err
	}
	hlog.Infof("[DBTransferCoordinator] Marked transfer completed id=%d", tr.ID)
	return nil
}
