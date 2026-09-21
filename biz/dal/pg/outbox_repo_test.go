package pg

import (
	"testing"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestWriteOutboxEntryIfNotExists_SingleInsert_Sqlite 验证在 sqlite 下单次插入和重复插入行为
func TestWriteOutboxEntryIfNotExists_SingleInsert_Sqlite(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.AutoMigrate(&model.OutboxEntry{}))

	repo := NewOutboxRepo(db)

	entry1 := &model.OutboxEntry{
		EventID:       "unit-test-1",
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       "{}",
	}

	tx := db.Begin()
	inserted, err := repo.WriteOutboxEntryIfNotExists(tx, entry1)
	require.NoError(t, err)
	require.True(t, inserted)
	require.NoError(t, tx.Commit().Error)

	// 再次尝试插入（同 event_id），应返回 inserted=false
	entry2 := &model.OutboxEntry{
		EventID:       "unit-test-1",
		EventType:     "TradeExecuted",
		AggregateID:   "BTC/USDT",
		AggregateType: "OrderBook",
		Payload:       "{}",
	}
	tx2 := db.Begin()
	inserted2, err := repo.WriteOutboxEntryIfNotExists(tx2, entry2)
	require.NoError(t, err)
	require.False(t, inserted2)
	require.NoError(t, tx2.Commit().Error)
}
