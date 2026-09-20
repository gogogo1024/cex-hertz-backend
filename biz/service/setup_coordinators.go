package service

import (
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/gorm"
)

// SetupCoordinators 在启动时配置并注入迁移协调器
// 目前实现：
//   - 对 OwnershipTransfer 执行 AutoMigrate
//   - 创建 DBTransferCoordinator 并设置为 model 的默认协调器
func SetupCoordinators(pm *PartitionManager, db *gorm.DB) error {
	if db == nil {
		hlog.Warnf("SetupCoordinators: db is nil, skipping coordinator setup")
		return nil
	}

	// 自动迁移 OwnershipTransfer 表
	if err := db.AutoMigrate(&model.OwnershipTransfer{}); err != nil {
		hlog.Errorf("SetupCoordinators: AutoMigrate failed: %v", err)
		return err
	}

	// 创建 DBTransferCoordinator 并设置为默认协调器
	coord := NewDBTransferCoordinator(db)
	model.SetDefaultTransferCoordinator(coord)
	hlog.Infof("SetupCoordinators: DBTransferCoordinator initialized and set as default coordinator")

	// 注入到 PartitionManager 的已注册 FSM（如果有）并保存到 pm 以便后续注册也自动注入
	if pm != nil {
		pm.SetTransferCoordinator(coord)
	}

	return nil
}
