package service

import (
	"testing"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDBTransferCoordinator_Basic(t *testing.T) {
	// 使用 sqlite 内存数据库进行简单验证
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite in-memory failed: %v", err)
	}

	if err := db.AutoMigrate(&model.OwnershipTransfer{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}

	coord := NewDBTransferCoordinator(db)

	id, err := coord.InitiateTransfer("partition-1", "node-1", "node-2", 123)
	if err != nil {
		t.Fatalf("InitiateTransfer failed: %v", err)
	}

	tr, err := coord.GetTransfer(id)
	if err != nil {
		t.Fatalf("GetTransfer failed: %v", err)
	}
	if tr.PartitionID != "partition-1" || tr.FromOwner != "node-1" || tr.ToOwner != "node-2" {
		t.Fatalf("unexpected transfer record: %+v", tr)
	}

	ready, err := coord.IsTargetReady(id)
	if err != nil {
		t.Fatalf("IsTargetReady failed: %v", err)
	}
	if ready {
		t.Fatalf("transfer should not be ready immediately")
	}

	if err := coord.MarkTransferCompleted(id); err != nil {
		t.Fatalf("MarkTransferCompleted failed: %v", err)
	}

	ready, err = coord.IsTargetReady(id)
	if err != nil {
		t.Fatalf("IsTargetReady failed: %v", err)
	}
	if !ready {
		t.Fatalf("transfer should be ready after marking completed")
	}
}
